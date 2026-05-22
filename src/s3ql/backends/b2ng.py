'''
b2ng.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import annotations

import logging
import re
import ssl

from s3ql.http import CaseInsensitiveDict, HTTPResponse
from s3ql.types import BackendOptionsProtocol, BasicMappingT

from ..logging import QuietError
from . import s3c4

log = logging.getLogger(__name__)

# Backblaze accepts requests for any of its S3 endpoints and replies with a
# 301 carrying `x-amz-bucket-region` pointing at the correct one. We start
# requests against this region and re-target on the first response.
DEFAULT_REGION = 'us-east-005'


class AsyncBackend(s3c4.AsyncBackend):
    """Backblaze B2 via its S3-compatible API (SigV4)."""

    known_options: set[str] = s3c4.AsyncBackend.known_options - {'sig-region'}

    def __init__(self, *, options: BackendOptionsProtocol, max_connections: int = 1) -> None:
        '''Initialize backend object - use AsyncBackend.create() instead.'''
        super().__init__(options=options, max_connections=max_connections)
        # `s3c4` reads `sig-region` from backend options (defaulting to
        # `us-east-1`). We override unconditionally; the real region is
        # discovered in `_discover_region`.
        self.sig_region = DEFAULT_REGION

    @classmethod
    async def create(
        cls: type[AsyncBackend],
        options: BackendOptionsProtocol,
        max_connections: int = 1,
    ) -> AsyncBackend:
        '''Create a new Backblaze B2 (S3-compatible) backend instance.'''
        self = cls(options=options, max_connections=max_connections)
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
        '''Resolve the bucket's region and re-target subsequent requests.'''

        # Backblaze replies to a request to any S3 endpoint with the bucket's actual region in the
        # `x-amz-bucket-region` header, accompanied by a 301 if the endpoint was wrong. We issue a
        # single HEAD bypassing the redirect-following logic in `_do_request` so we can read the
        # header ourselves.

        async with self._pool.acquire() as conn:
            resp = await self._do_request_inner(conn, 'HEAD', '/', headers=CaseInsensitiveDict())
            region = resp.headers.get('x-amz-bucket-region')

            if region is None and not (200 <= resp.status <= 299):
                # Non-2xx with no region hint: nothing to recover. Surface as a
                # typed error (auth failure, no-such-bucket, ...).
                await self._parse_error_response(conn, resp)
                return  # unreachable; _parse_error_response always raises.

            await conn.discard()

        if region is not None and region != self.sig_region:
            log.debug('Bucket region is %s, re-targeting connection', region)
            self.sig_region = region
            self.signing_key = None
            await self._retarget_pool('s3.%s.backblazeb2.com' % region, self.port)

    def _extractmeta(self, resp: HTTPResponse, obj_key: str) -> BasicMappingT:
        '''Extract metadata'''

        # The native (non-S3) B2 backend stored S3QL metadata in `X-Bz-Info-meta-*` file-info
        # entries. Backblaze's S3-compatible endpoint surfaces those as `x-amz-meta-meta-*` headers,
        # so a filesystem originally created via `b2old://` would otherwise look like an unknown
        # metadata format here. When the standard `x-amz-meta-format` header is absent but
        # `x-amz-meta-meta-format` is present, re-run the standard extraction with the header prefix
        # shifted by one `meta-` segment.

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
