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

from . import s3c

log = logging.getLogger(__name__)

# Maximum number of keys that can be deleted at once
MAX_KEYS = 1000

# Pylint goes berserk with false positives
# pylint: disable=E1002,E1101


class Backend(s3c.Backend):
    """A backend to stored data in some S3 compatible storage service.

    This classes uses AWS Signature V4 for authorization.
    """

    known_options = s3c.Backend.known_options | {'sig-region'}

    def __init__(self, options):
        self.sig_region = options.backend_options.get('sig-region', 'us-east-1')
        self.signing_key = None
        super().__init__(options)

    def __str__(self):
        return 's3c4://%s/%s/%s' % (self.hostname, self.bucket_name, self.prefix)

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
        sig_hdrs = sorted(x.lower() for x in headers.keys())
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
