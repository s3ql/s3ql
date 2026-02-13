'''
pool.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import annotations

import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import trio

from s3ql.backends.comprenc import AsyncComprencBackend
from s3ql.types import BackendFactory

log = logging.getLogger(__name__)


class BackendPool:
    def __init__(self, factory: BackendFactory, max_connections: int) -> None:
        if max_connections < 1:
            raise ValueError('max_connections must be at least 1')

        self.factory = factory
        self.max_connections = max_connections
        self.pool: list[AsyncComprencBackend] = []
        self.has_delete_multi = factory.has_delete_multi
        self._semaphore = trio.Semaphore(max_connections)

    async def pop_conn(self) -> AsyncComprencBackend:
        '''Pop connection from pool, blocking if max_connections are checked out'''

        await self._semaphore.acquire()
        try:
            if self.pool:
                conn = self.pool.pop()
            else:
                log.debug('creating new backend connection')
                conn = await self.factory()
        except BaseException:
            # Ensure semaphore is released if backend acquisition/creation fails
            self._semaphore.release()
            raise
        return conn

    async def push_conn(self, conn: AsyncComprencBackend) -> None:
        '''Push connection back into pool'''

        try:
            await conn.reset()
            self.pool.append(conn)
        finally:
            self._semaphore.release()

    async def flush(self) -> None:
        '''Close all backends in pool'''

        while self.pool:
            await self.pool.pop().close()

    @asynccontextmanager
    async def __call__(self) -> AsyncGenerator[AsyncComprencBackend, None]:
        '''Provide async connection from pool (async context manager)'''

        conn = await self.pop_conn()
        try:
            yield conn
        finally:
            await self.push_conn(conn)
