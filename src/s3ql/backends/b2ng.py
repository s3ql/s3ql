'''
b2ng.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import annotations

import base64
import json
import logging
import re
import ssl
from urllib.parse import urlparse

from s3ql.http import CaseInsensitiveDict, HTTPConnection, HTTPResponse
from s3ql.types import BackendOptionsProtocol, BasicMappingT

from ..logging import QuietError
from . import s3c4
from .common import AuthenticationError, AuthorizationError
from .s3c import HTTPError

log = logging.getLogger(__name__)

# Backblaze's S3-compatible endpoints are region-isolated: a request for a
# bucket that does not live in the contacted region returns a plain 403 with
# no `x-amz-bucket-region` hint, so we cannot probe across regions the way AWS
# S3 allows. `DEFAULT_REGION` is only the initial value of `sig_region` before
# `_discover_region` queries the native `b2_authorize_account` endpoint, which
# returns the account's actual S3 hostname. The default value is never used in
# an outgoing S3 request.
DEFAULT_REGION = 'us-east-005'


class AsyncBackend(s3c4.AsyncBackend):
    """Backblaze B2 via its S3-compatible API (SigV4)."""

    known_options: set[str] = s3c4.AsyncBackend.known_options - {'sig-region'}

    def __init__(self, *, options: BackendOptionsProtocol) -> None:
        '''Initialize backend object - use AsyncBackend.create() instead.'''
        super().__init__(options=options)
        # `s3c4` reads `sig-region` from backend options (defaulting to
        # `us-east-1`). We override unconditionally; the real region is
        # discovered in `_discover_region`.
        self.sig_region = DEFAULT_REGION

    @classmethod
    async def create(cls: type[AsyncBackend], options: BackendOptionsProtocol) -> AsyncBackend:
        '''Create a new Backblaze B2 (S3-compatible) backend instance.'''
        self = cls(options=options)
        await self._discover_region()
        return self

    def _parse_storage_url(
        self, storage_url: str, ssl_context: ssl.SSLContext | None
    ) -> tuple[str, int, str, str]:
        hit = re.match(r'^b2://([^/]+)(?:/(.*))?$', storage_url)
        if not hit:
            raise QuietError('Invalid storage URL', exitcode=2)

        bucket_name = hit.group(1)

        # https://www.backblaze.com/docs/cloud-storage-create-and-manage-buckets#bucket-names
        if not re.match(r'^[a-z0-9][a-z0-9-]{4,61}[a-z0-9]$', bucket_name):
            raise QuietError('Invalid bucket name.', exitcode=2)

        prefix = hit.group(2) or ''
        hostname = 's3.%s.backblazeb2.com' % DEFAULT_REGION
        port = 443 if ssl_context else 80
        return (hostname, port, bucket_name, prefix)

    async def _discover_region(self) -> None:
        '''Resolve the bucket's region via Backblaze's native authorize endpoint.

        Backblaze's S3-compatible endpoints are region-isolated; an
        `x-amz-bucket-region` HEAD probe (the pattern that works on AWS S3)
        returns 403 without a region hint when the bucket is not in the
        contacted region. Instead, issue one Basic-auth call to the native
        `b2_authorize_account` endpoint, which returns the account's
        regional `s3ApiUrl`.
        '''

        creds = '%s:%s' % (self.login, self.password)
        auth = 'Basic ' + base64.b64encode(creds.encode('utf-8')).decode('ascii')
        headers = CaseInsensitiveDict({'Authorization': auth})

        async with HTTPConnection('api.backblazeb2.com', 443, ssl_context=self.ssl_context) as conn:
            resp = await conn.send_request('GET', '/b2api/v4/b2_authorize_account', headers=headers)
            body = await conn.readall()

        if resp.status == 401:
            raise AuthenticationError('Invalid Backblaze application key or key ID')
        if resp.status == 403:
            raise AuthorizationError(_b2_error_message(body) or resp.reason)
        if not (200 <= resp.status <= 299):
            body_msg = _b2_error_message(body)
            if body_msg:
                log.error(
                    'Backblaze authorization endpoint returned HTTP %d: %s', resp.status, body_msg
                )
            else:
                log.error(
                    'Backblaze authorization endpoint returned HTTP %d %s; body: %r',
                    resp.status,
                    resp.reason,
                    body[:2048],
                )
            raise HTTPError(resp.status, body_msg or resp.reason, resp.headers)

        try:
            data = json.loads(body)
        except json.JSONDecodeError as exc:
            raise RuntimeError('Could not parse b2_authorize_account response: %s' % exc) from exc

        # B2 API v3+ nests storage info under apiInfo.storageApi; v2 placed
        # `s3ApiUrl` at the top level. Accept either.
        s3_api_url = data.get('apiInfo', {}).get('storageApi', {}).get('s3ApiUrl') or data.get(
            's3ApiUrl'
        )
        if not s3_api_url:
            raise RuntimeError('Backblaze response did not contain s3ApiUrl')

        host = urlparse(s3_api_url).hostname or ''
        hit = re.fullmatch(r's3\.([a-z0-9-]+)\.backblazeb2\.com', host)
        if not hit:
            raise RuntimeError('Unexpected s3ApiUrl from Backblaze: %r' % s3_api_url)
        region = hit.group(1)

        if region == self.sig_region:
            return

        log.debug('Bucket region is %s, re-targeting connection', region)
        self.sig_region = region
        self.hostname = host
        self.signing_key = None
        await self.conn.aclose()
        self.conn = self._get_conn()

    def _extractmeta(self, resp: HTTPResponse, obj_key: str) -> BasicMappingT:
        '''Extract metadata'''

        # An earlier B2 backend used the native (non-S3) API and stored S3QL metadata in
        # `X-Bz-Info-meta-*` file-info entries. Backblaze's S3-compatible endpoint surfaces those as
        # `x-amz-meta-meta-*` headers. When the standard `x-amz-meta-format` header is absent but
        # `x-amz-meta-meta-format` is present, re-run the standard extraction with the header prefix
        # shifted by one `meta-` segment so filesystems created with the legacy backend remain
        # readable.

        if '%smeta-format' % self.hdr_prefix in resp.headers:
            return super()._extractmeta(resp, obj_key)
        if '%smeta-meta-format' % self.hdr_prefix not in resp.headers:
            # Neither layout present — defer to the parent so it raises the
            # standard `Invalid metadata format` error.
            return super()._extractmeta(resp, obj_key)
        saved_prefix = self.hdr_prefix
        self.hdr_prefix = saved_prefix + 'meta-'
        try:
            return super()._extractmeta(resp, obj_key)
        finally:
            self.hdr_prefix = saved_prefix

    def __str__(self) -> str:
        return 'Backblaze B2 bucket %s, prefix %s' % (self.bucket_name, self.prefix)


def _b2_error_message(body: bytes) -> str | None:
    '''Extract Backblaze's `message` field from a JSON error body, if present.'''
    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        return None
    if not isinstance(data, dict):
        return None
    msg = data.get('message')
    return msg if isinstance(msg, str) else None
