'''
s3.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import annotations

import logging
import re
import ssl

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
