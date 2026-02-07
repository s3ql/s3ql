'''
pool.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import annotations

import logging
from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager, contextmanager

import trio

from s3ql.async_bridge import run_async
from s3ql.backends.comprenc import AsyncComprencBackend, ComprencBackend
from s3ql.types import BackendFactory

log = logging.getLogger(__name__)


class BackendPool:


    factory: BackendFactory
    pool: list[AsyncComprencBackend]
    lock: trio.Lock
    has_delete_multi: bool

    def __init__(self, factory: BackendFactory) -> None:
        self.factory = factory
        self.pool = []
        self.lock = trio.Lock()
        self.has_delete_multi = factory.has_delete_multi

    async def pop_conn(self) -> AsyncComprencBackend:
        '''Pop connection from pool'''

        async with self.lock:
            if self.pool:
                return self.pool.pop()
        return await self.factory()

    async def push_conn(self, conn: AsyncComprencBackend) -> None:
        '''Push connection back into pool'''

        await conn.reset()
        async with self.lock:
            self.pool.append(conn)

    async def flush(self) -> None:
        '''Close all backends in pool'''

        async with self.lock:
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

    @contextmanager
    def sync(self) -> Generator[ComprencBackend, None, None]:
        '''Provide sync connection from pool (context manager for worker threads)'''

        conn = run_async(self.pop_conn)
        try:
            yield ComprencBackend.from_async_backend(conn)
        finally:
            run_async(self.push_conn, conn)
