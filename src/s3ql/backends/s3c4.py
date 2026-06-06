'''
s3c4.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

import hashlib
import hmac
import logging
import time
import urllib.parse
from xml.sax.saxutils import escape as xml_escape

from more_itertools import chunked

from s3ql.http import CaseInsensitiveDict
from s3ql.types import BackendOptionsProtocol

from . import s3c
from .common import log_delete_progress, retry
from .s3c import get_S3Error, md5sum_b64

log = logging.getLogger(__name__)

# Pylint goes berserk with false positives
# pylint: disable=E1002,E1101

MAX_KEYS = 1000


class AsyncBackend(s3c.AsyncBackend):
    """A backend to stored data in some S3 compatible storage service.

    This classes uses AWS Signature V4 for authorization.
    """

    known_options = s3c.AsyncBackend.known_options | {'sig-region'}

    def __init__(self, *, options: BackendOptionsProtocol, max_connections: int = 1) -> None:
        '''Initialize backend object - use AsyncBackend.create() instead.'''
        sig_region = options.backend_options.get('sig-region', 'us-east-1')
        assert isinstance(sig_region, str)
        self.sig_region = sig_region
        self.signing_key: tuple[bytes, str] | None = None
        super().__init__(options=options, max_connections=max_connections)

    @classmethod
    async def create(
        cls: type['AsyncBackend'],
        options: BackendOptionsProtocol,
        max_connections: int = 1,
    ) -> 'AsyncBackend':
        '''Create a new S3-compatible (v4 signature) backend instance.'''
        self = cls(options=options, max_connections=max_connections)
        self._pool = self._make_pool()
        return self

    def __str__(self):
        return 's3c4://%s/%s/%s' % (self.hostname, self.bucket_name, self.prefix)

    @property
    def has_delete_multi(self) -> bool:
        return True

    async def delete_multi(self, keys: list[str], *, log_progress: bool = False) -> None:
        log.debug('started with %s', keys)
        total = len(keys)
        failures: list[str] = []
        processed = 0
        for batch in chunked(list(keys), MAX_KEYS):
            await self._delete_multi(batch)
            failures.extend(batch)
            processed += len(batch)
            if log_progress:
                log_delete_progress(processed - len(failures), total)
        keys[:] = failures

    @retry
    async def _delete_multi(self, keys: list[str]) -> None:
        body_list = ['<Delete>']
        esc_prefix = xml_escape(self.prefix)
        for key in keys:
            body_list.append('<Object><Key>%s%s</Key></Object>' % (esc_prefix, xml_escape(key)))
        body_list.append('</Delete>')
        body = '\n'.join(body_list).encode('utf-8')
        headers = CaseInsensitiveDict(
            {
                'content-type': 'text/xml; charset=utf-8',
                'content-md5': md5sum_b64(body),
            }
        )

        async with self._pool.get() as conn:
            resp = await self._do_request(
                conn, 'POST', '/', subres='delete', body=[body], headers=headers
            )
            try:
                root = await self._parse_xml_response(conn, resp)
                ns_p = self.xml_ns_prefix

                error_tags = root.findall(ns_p + 'Error')
                if not error_tags:
                    del keys[:]
                    return

                offset = len(self.prefix)
                for tag in root.findall(ns_p + 'Deleted'):
                    key_elem = tag.find(ns_p + 'Key')
                    assert key_elem is not None
                    fullkey = key_elem.text
                    assert fullkey is not None and fullkey.startswith(self.prefix)
                    keys.remove(fullkey[offset:])

                if log.isEnabledFor(logging.DEBUG):
                    for errtag in error_tags:
                        errkey_text = errtag.findtext(ns_p + 'Key')
                        log.debug(
                            'Delete %s failed with %s',
                            errkey_text[offset:] if errkey_text else None,
                            errtag.findtext(ns_p + 'Code'),
                        )

                errcode = error_tags[0].findtext(ns_p + 'Code')
                errmsg = error_tags[0].findtext(ns_p + 'Message')
                errkey_text = error_tags[0].findtext(ns_p + 'Key')
                errkey = errkey_text[offset:] if errkey_text else '<unknown>'

                if errcode == 'NoSuchKey':
                    pass
                else:
                    raise get_S3Error(errcode, 'Error deleting %s: %s' % (errkey, errmsg))

            except:
                await conn.discard()
                raise

    def _authorize_request(self, method, path, headers, subres, query_string):
        '''Add authorization information to *headers*'''

        # See http://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-auth-using-authorization-header.html

        now = time.gmtime()
        # now = time.strptime('Fri, 24 May 2013 00:00:00 GMT',
        #                    '%a, %d %b %Y %H:%M:%S GMT')

        ymd = time.strftime('%Y%m%d', now)
        ymdhms = time.strftime('%Y%m%dT%H%M%SZ', now)

        # add non-standard port to host header, needed for correct signature
        if self.port != 443:
            headers['host'] = '%s:%s' % (self.hostname, self.port)

        headers['x-amz-date'] = ymdhms
        headers['x-amz-content-sha256'] = 'UNSIGNED-PAYLOAD'

        headers.pop('Authorization', None)

        auth_strs = [method]
        auth_strs.append(urllib.parse.quote(path))

        if query_string:
            s = urllib.parse.urlencode(
                query_string, doseq=True, quote_via=urllib.parse.quote
            ).split('&')
        else:
            s = []
        if subres:
            s.append(urllib.parse.quote(subres) + '=')
        if s:
            s = '&'.join(sorted(s))
        else:
            s = ''
        auth_strs.append(s)

        # Headers
        sig_hdrs = sorted(x.lower() for x in headers)
        for hdr in sig_hdrs:
            auth_strs.append('%s:%s' % (hdr, headers[hdr].strip()))
        auth_strs.append('')
        auth_strs.append(';'.join(sig_hdrs))
        auth_strs.append(headers['x-amz-content-sha256'])
        can_req = '\n'.join(auth_strs)
        # log.debug('canonical request: %s', can_req)

        can_req_hash = hashlib.sha256(can_req.encode()).hexdigest()
        str_to_sign = (
            "AWS4-HMAC-SHA256\n"
            + ymdhms
            + '\n'
            + '%s/%s/s3/aws4_request\n' % (ymd, self.sig_region)
            + can_req_hash
        )
        # log.debug('string to sign: %s', str_to_sign)

        if self.signing_key is None or self.signing_key[1] != ymd:
            self.update_signing_key(ymd)
        assert self.signing_key is not None
        signing_key = self.signing_key[0]

        sig = hmac_sha256(signing_key, str_to_sign.encode(), hex=True)

        cred = '%s/%04d%02d%02d/%s/s3/aws4_request' % (
            self.login,
            now.tm_year,
            now.tm_mon,
            now.tm_mday,
            self.sig_region,
        )

        headers['Authorization'] = (
            'AWS4-HMAC-SHA256 '
            'Credential=%s,'
            'SignedHeaders=%s,'
            'Signature=%s' % (cred, ';'.join(sig_hdrs), sig)
        )

    def update_signing_key(self, ymd):
        date_key = hmac_sha256(("AWS4" + self.password).encode(), ymd.encode())
        region_key = hmac_sha256(date_key, self.sig_region.encode())
        service_key = hmac_sha256(region_key, b's3')
        signing_key = hmac_sha256(service_key, b'aws4_request')

        self.signing_key = (signing_key, ymd)


def hmac_sha256(key, msg, hex=False):
    d = hmac.new(key, msg, hashlib.sha256)
    if hex:
        return d.hexdigest()
    else:
        return d.digest()
