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
from more_itertools import chunked

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

    async def delete_multi(
        self, keys: list[str], log_progress: bool = False, chunk_size=250
    ) -> None:
        '''Delete objects stored under *keys*.

        If the underlying backend has a native batch-delete API, dispatch *chunk_size* objects at
        once using `delete_multi` of the backend. Otherwise, fan out single-key `delete()` calls
        across the pool, deleting up to `max_connections` keys in parallel.

        On return (whether normal or exceptional), the *keys* list contains exactly those keys that
        have not been deleted, matching the contract of `AbstractBackend.delete_multi`.

        If *log_progress* is True, emit periodic ``INFO`` log messages (rate-limited to 1 per
        second, updating in-place on a console) showing how many objects have been removed so far.
        '''

        if not keys:
            return
        if self.has_delete_multi:
            total = len(keys)
            failures: list[str] = []
            processed = 0
            async with self() as backend:
                for batch in chunked(list(keys), chunk_size):
                    await backend.delete_multi(batch)
                    failures.extend(batch)
                    processed += len(batch)
                    if log_progress:
                        log.info(
                            'Removed %d/%d objects (%d%%)',
                            processed - len(failures),
                            total,
                            processed * 100 // total,
                            extra={
                                'rate_limit': 1,
                                'update_console': True,
                                'is_last': processed == total,
                            },
                        )
            keys[:] = failures
            return
        await _ParallelDelete(pool=self, keys=keys, log_progress=log_progress).run(
            n_workers=self.max_connections
        )


@dataclasses.dataclass
class _ParallelDelete(ParallelPipeline[str]):
    '''Emulate `delete_multi` by spreading single-key deletes across a pool.

    Each worker borrows a connection from *pool* per key, so up to
    `pool.max_connections` deletes are in flight at any time.
    '''

    pool: BackendPool
    keys: list[str]
    log_progress: bool = False
    _total: int = dataclasses.field(init=False)
    _done: int = dataclasses.field(init=False, default=0)

    def __post_init__(self) -> None:
        self._total = len(self.keys)

    async def produce(self) -> list[str]:
        # Snapshot so the orchestrator's iteration of the job list does not
        # race with workers mutating `self.keys`.
        return list(self.keys)

    async def consume(self, jobs: AsyncIterable[str]) -> None:
        async for key in jobs:
            async with self.pool() as backend:
                await backend.delete(key)
            self.keys.remove(key)
            if self.log_progress:
                self._done += 1
                log.info(
                    'Removed %d/%d objects (%d%%)',
                    self._done,
                    self._total,
                    self._done * 100 // self._total,
                    extra={
                        'rate_limit': 1,
                        'update_console': True,
                        'is_last': self._done == self._total,
                    },
                )
