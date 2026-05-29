'''
s3.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import annotations

import logging
import re
import ssl
from urllib.parse import urlsplit

from s3ql.http import CaseInsensitiveDict
from s3ql.types import BackendOptionsProtocol

from ..logging import QuietError
from . import s3c4

log = logging.getLogger(__name__)

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
        self = cls(options=options)
        await self._discover_region()
        return self

    async def _discover_region(self) -> None:
        '''Probe the configured endpoint; retarget if redirected, error on wrong region.

        Issues a single HEAD request via `_do_request_inner` (bypassing the normal
        request path) to verify that *self.hostname* is the correct S3 endpoint for
        the bucket.  Three outcomes are possible:

        - 2xx: already at the right endpoint, return immediately.
        - 3xx with a `Location` header (302/307, DNS propagation window): follow the
          redirect — update *self.hostname*, *self.port*, and if `x-amz-bucket-region`
          is present also *self.region* / *self.sig_region* / *self.signing_key*.
        - 3xx without a `Location` header (AWS path-style wrong-region 301): raise
          `QuietError` telling the user to correct the region in the storage URL.
        - 4xx/5xx: surface as a typed exception via `_parse_error_response`.
        '''
        resp = await self._do_request_inner('HEAD', '/', headers=CaseInsensitiveDict())
        actual_region = resp.headers.get('x-amz-bucket-region')
        location = resp.headers.get('Location')

        if not (300 <= resp.status <= 399):
            if 200 <= resp.status <= 299:
                await self.conn.discard()
                return
            # 4xx/5xx: _parse_error_response raises; HEAD has no body so it
            # raises HTTPError directly without trying to read one.
            await self._parse_error_response(resp)
            raise AssertionError('unreachable')

        # 3xx: discard any body before raising or retargeting.
        await self.conn.discard()

        if not location:
            # AWS path-style wrong-region 301 deliberately omits Location so
            # that clients cannot blindly follow it — raise an actionable error.
            msg = 'S3 bucket %r is not in region %r' % (self.bucket_name, self.region)
            if actual_region:
                msg += '; update the region in your storage URL to %r' % actual_region
            raise QuietError(msg, exitcode=2)

        # 302/307: follow Location to the correct endpoint.
        o = urlsplit(location)
        if not o.hostname:
            raise RuntimeError('Redirect has no hostname: %s' % location)
        self.hostname = o.hostname
        self.port = o.port if o.port is not None else self.port
        if actual_region and actual_region != self.region:
            self.region = actual_region
            self.sig_region = actual_region
            self.signing_key = None
        await self.conn.aclose()
        self.conn = self._get_conn()
        log.info('Redirected to S3 endpoint %s', self.hostname)

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
