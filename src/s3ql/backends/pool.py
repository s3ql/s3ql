'''
pool.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import annotations

import dataclasses
import logging
from collections.abc import AsyncGenerator, AsyncIterable
from contextlib import asynccontextmanager

import trio

from s3ql.backends.comprenc import AsyncComprencBackend
from s3ql.common import ParallelPipeline
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
        self._limiter = trio.CapacityLimiter(max_connections)

    async def pop_conn(self) -> AsyncComprencBackend:
        '''Pop connection from pool, blocking if max_connections are checked out'''

        await self._limiter.acquire()
        try:
            if self.pool:
                conn = self.pool.pop()
            else:
                log.debug('creating new backend connection')
                conn = await self.factory()
        except BaseException:
            self._limiter.release()
            raise
        return conn

    async def push_conn(self, conn: AsyncComprencBackend) -> None:
        '''Push connection back into pool'''

        try:
            await conn.reset()
            self.pool.append(conn)
        finally:
            self._limiter.release()

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

    async def delete_multi(self, keys: list[str]) -> None:
        '''Delete objects stored under *keys*.

        If the underlying backend has a native batch-delete API, dispatch a
        single `delete_multi` call against one borrowed connection. Otherwise,
        fan out single-key `delete()` calls across the pool, deleting up to
        `max_connections` keys in parallel.

        On return (whether normal or exceptional), the *keys* list contains
        exactly those keys that have not been deleted, matching the contract
        of `AbstractBackend.delete_multi`.
        '''

        if not keys:
            return
        if self.has_delete_multi:
            async with self() as backend:
                await backend.delete_multi(keys)
            return
        await _ParallelDelete(pool=self, keys=keys).run(n_workers=self.max_connections)


@dataclasses.dataclass
class _ParallelDelete(ParallelPipeline[str]):
    '''Emulate `delete_multi` by spreading single-key deletes across a pool.

    Each worker borrows a connection from *pool* per key, so up to
    `pool.max_connections` deletes are in flight at any time.
    '''

    pool: BackendPool
    keys: list[str]

    async def produce(self) -> list[str]:
        # Snapshot so the orchestrator's iteration of the job list does not
        # race with workers mutating `self.keys`.
        return list(self.keys)

    async def consume(self, jobs: AsyncIterable[str]) -> None:
        async for key in jobs:
            async with self.pool() as backend:
                await backend.delete(key)
            self.keys.remove(key)
