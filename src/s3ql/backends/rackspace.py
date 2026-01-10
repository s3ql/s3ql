'''
rackspace.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

import logging
import re

from s3ql.async_bridge import run_async
from s3ql.types import BackendOptionsProtocol

from ..logging import QuietError
from . import swiftks
from .common import AbstractBackend

log = logging.getLogger(__name__)


class AsyncBackend(swiftks.AsyncBackend):
    """A backend to store data in Rackspace CloudFiles"""

    @classmethod
    async def create(cls: type['AsyncBackend'], options: BackendOptionsProtocol) -> 'AsyncBackend':
        '''Create a new Rackspace backend instance with eager authentication.'''
        backend = cls(options=options)
        backend.conn = await backend._get_conn()
        await backend._container_exists()
        return backend

    def _parse_storage_url(self, storage_url, ssl_context):
        hit = re.match(
            r'^rackspace://'  # Backend
            r'([^/:]+)'  # Region
            r'/([^/]+)'  # Bucketname
            r'(?:/(.*))?$',  # Prefix
            storage_url,
        )
        if not hit:
            raise QuietError('Invalid storage URL', exitcode=2)

        region = hit.group(1)
        containername = hit.group(2)
        prefix = hit.group(3) or ''

        if ssl_context:
            port = 443
        else:
            port = 80

        self.hostname = 'auth.api.rackspacecloud.com'
        self.port = port
        self.container_name = containername
        self.prefix = prefix
        self.region = region


class Backend(AbstractBackend):
    '''Synchronous wrapper for AsyncBackend.'''

    needs_login = AsyncBackend.needs_login
    known_options = AsyncBackend.known_options

    def __init__(self, options: BackendOptionsProtocol) -> None:
        async_backend = run_async(AsyncBackend.create, options)
        super().__init__(async_backend)
