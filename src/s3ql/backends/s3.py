'''
s3.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import annotations

import logging
import re
import ssl
from xml.sax.saxutils import escape as xml_escape

from s3ql.http import CaseInsensitiveDict
from s3ql.types import BackendOptionsProtocol

from ..logging import QuietError
from . import s3c4
from .common import retry
from .s3c import get_S3Error

log = logging.getLogger(__name__)

# Maximum number of keys that can be deleted at once
MAX_KEYS = 1000

# Pylint goes berserk with false positives
# pylint: disable=E1002,E1101


class AsyncBackend(s3c4.AsyncBackend):
    """A backend to store data in Amazon S3

    This class uses standard HTTP connections to connect to S3.
    """

    known_options: set[str] = (
        s3c4.AsyncBackend.known_options | {'sse', 'rrs', 'ia', 'oia', 'it'}
    ) - {
        'dumb-copy',
        'disable-expect100',
        'sig-region',
    }
    region: str

    def __init__(self, *, options: BackendOptionsProtocol) -> None:
        '''Initialize backend object - use AsyncBackend.create() instead.'''
        self.region = ''
        super().__init__(options=options)
        self._set_storage_options(self._extra_put_headers)
        self.sig_region = self.region

    @classmethod
    async def create(cls: type[AsyncBackend], options: BackendOptionsProtocol) -> AsyncBackend:
        '''Create a new Amazon S3 backend instance.'''
        return cls(options=options)

    def _parse_storage_url(
        self, storage_url: str, ssl_context: ssl.SSLContext | None
    ) -> tuple[str, int, str, str]:
        hit = re.match(r'^s3s?://([^/]+)/([^/]+)(?:/(.*))?$', storage_url)
        if not hit:
            raise QuietError('Invalid storage URL', exitcode=2)

        self.region = hit.group(1)
        bucket_name = hit.group(2)

        # http://docs.amazonwebservices.com/AmazonS3/2006-03-01/dev/BucketRestrictions.html
        if not re.match('^[a-z0-9][a-z0-9.-]{1,60}[a-z0-9]$', bucket_name):
            raise QuietError('Invalid bucket name.', exitcode=2)

        if self.region.startswith('cn-'):
            hostname = 's3.%s.amazonaws.com.cn' % self.region
        else:
            hostname = 's3.%s.amazonaws.com' % self.region

        prefix = hit.group(3) or ''
        port = 443 if ssl_context else 80
        return (hostname, port, bucket_name, prefix)

    def __str__(self) -> str:
        return 'Amazon S3 bucket %s, prefix %s' % (self.bucket_name, self.prefix)

    @property
    def has_delete_multi(self) -> bool:
        return True

    async def delete_multi(self, keys: list[str]) -> None:
        log.debug('started with %s', keys)

        while len(keys) > 0:
            tmp = keys[:MAX_KEYS]
            try:
                await self._delete_multi(tmp)
            finally:
                keys[:MAX_KEYS] = tmp

    def _set_storage_options(self, headers: CaseInsensitiveDict) -> None:
        if 'sse' in self.options:
            headers['x-amz-server-side-encryption'] = 'AES256'

        if 'ia' in self.options:
            sc = 'STANDARD_IA'
        elif 'oia' in self.options:
            sc = 'ONEZONE_IA'
        elif 'rrs' in self.options:
            sc = 'REDUCED_REDUNDANCY'
        elif 'it' in self.options:
            sc = 'INTELLIGENT_TIERING'
        else:
            sc = 'STANDARD'
        headers['x-amz-storage-class'] = sc

    @retry
    async def _delete_multi(self, keys: list[str]) -> None:
        body_list = ['<Delete>']
        esc_prefix = xml_escape(self.prefix)
        for key in keys:
            body_list.append('<Object><Key>%s%s</Key></Object>' % (esc_prefix, xml_escape(key)))
        body_list.append('</Delete>')
        body = '\n'.join(body_list).encode('utf-8')
        headers = CaseInsensitiveDict({'content-type': 'text/xml; charset=utf-8'})

        resp = await self._do_request('POST', '/', subres='delete', body=body, headers=headers)
        try:
            root = await self._parse_xml_response(resp)
            ns_p = self.xml_ns_prefix

            error_tags = root.findall(ns_p + 'Error')
            if not error_tags:
                # No errors occurred, everything has been deleted
                del keys[:]
                return

            # Some errors occurred, so we need to determine what has
            # been deleted and what hasn't
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
            await self.conn.co_discard()
            raise
