#!/usr/bin/env python3
'''
t2_block_cache.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

if __name__ == '__main__':
    import sys

    import pytest

    sys.exit(pytest.main([__file__] + sys.argv[1:]))

import logging
import os
import shutil
import stat
import tempfile
from argparse import Namespace
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import pytest
import trio
from common import safe_sleep

from s3ql.backends import local
from s3ql.backends.comprenc import AsyncComprencBackend
from s3ql.backends.pool import BackendPool
from s3ql.block_cache import BlockCache
from s3ql.common import time_ns
from s3ql.database import Connection, create_tables
from s3ql.mkfs import init_tables
from s3ql.types import BasicMappingT, BinaryInput, BinaryOutput

log = logging.getLogger(__name__)


class DummyRemovalChannel:
    """Replacement for the removal send channel that processes removals inline."""

    def __init__(self, cache: BlockCache) -> None:
        self.cache = cache

    async def send(self, obj_id: int) -> None:
        log.debug('DummyRemovalChannel: removing object %d inline', obj_id)
        async with self.cache.backend_pool() as backend:
            from s3ql.backends.common import NoSuchObject

            try:
                await backend.delete('s3ql_data_%d' % obj_id)
            except NoSuchObject:
                log.warning('Backend lost object s3ql_data_%d', obj_id)
                if self.cache.fs is not None:
                    self.cache.fs.failsafe = True

    async def aclose(self) -> None:
        pass


class DummyUploadChannel:
    """Replacement for the upload send channel that processes uploads inline."""

    def __init__(self, cache: BlockCache) -> None:
        self.cache = cache

    async def send(self, arg: tuple) -> None:
        await self.cache._do_upload(*arg)

    async def aclose(self) -> None:
        pass


def random_data(len_):
    with open("/dev/urandom", "rb") as fh:
        return fh.read(len_)


@pytest.fixture
async def ctx():
    ctx = Namespace()
    ctx.backend_dir = tempfile.mkdtemp(prefix='s3ql-backend-')

    async def factory():
        plain = await local.AsyncBackend.create(Namespace(storage_url='local://' + ctx.backend_dir))
        return await AsyncComprencBackend.create(b'foobar', ('zlib', 6), plain)

    factory.has_delete_multi = True
    ctx.backend_pool = BackendPool(factory, max_connections=10)

    ctx.cachedir = tempfile.mkdtemp(prefix='s3ql-cache-')
    ctx.max_obj_size = 1024

    # Destructors are not guaranteed to run, and we can't unlink
    # the file immediately because apsw refers to it by name.
    # Therefore, we unlink the file manually in tearDown()
    ctx.dbfile = tempfile.NamedTemporaryFile(delete=False)  # noqa: SIM115

    ctx.db = Connection(ctx.dbfile.name)
    create_tables(ctx.db)
    init_tables(ctx.db)

    # Create an inode we can work with
    ctx.inode = 42
    now_ns = time_ns()
    ctx.db.execute(
        "INSERT INTO inodes (id,mode,uid,gid,mtime_ns,atime_ns,ctime_ns,refcount,size) "
        "VALUES (?,?,?,?,?,?,?,?,?)",
        (
            ctx.inode,
            stat.S_IFREG
            | stat.S_IRUSR
            | stat.S_IWUSR
            | stat.S_IXUSR
            | stat.S_IRGRP
            | stat.S_IXGRP
            | stat.S_IROTH
            | stat.S_IXOTH,
            os.getuid(),
            os.getgid(),
            now_ns,
            now_ns,
            now_ns,
            1,
            32,
        ),
    )

    cache = BlockCache(
        ctx.backend_pool,
        ctx.db,
        ctx.cachedir + "/cache",
        ctx.max_obj_size * 100,
        upload_send=None,  # type: ignore[assignment]
        remove_send=None,  # type: ignore[assignment]
    )
    ctx.cache = cache

    # Monkeypatch: process uploads and removals inline instead of using worker tasks
    cache._upload_send = DummyUploadChannel(cache)  # type: ignore[assignment]
    cache._remove_send = DummyRemovalChannel(cache)  # type: ignore[assignment]

    try:
        yield ctx
    finally:
        ctx.cache.backend_pool = ctx.backend_pool
        if ctx.cache.destroy is not None:
            await ctx.cache.destroy()
        shutil.rmtree(ctx.cachedir)
        shutil.rmtree(ctx.backend_dir)
        ctx.db.close()
        ctx.dbfile.close()
        os.unlink(ctx.dbfile.name)


@pytest.mark.trio
async def test_dead_worker_channel_error():
    """Sending on a channel raises BrokenResourceError after all receiver clones close.

    This is the mechanism that prevents deadlocks in BlockCache when upload or
    removal workers have died: each worker holds a clone of the receive end, and
    once all clones are closed, sends fail immediately instead of blocking.
    """
    send, recv = trio.open_memory_channel[int](0)

    async def worker(recv_clone: trio.MemoryReceiveChannel[int]) -> None:
        async with recv_clone:
            pass

    async with trio.open_nursery() as nursery:
        nursery.start_soon(worker, recv.clone())
        await recv.aclose()

        with pytest.raises(trio.BrokenResourceError):
            await send.send(42)


async def test_get(ctx):
    inode = ctx.inode
    blockno = 11
    data = random_data(int(0.5 * ctx.max_obj_size))

    # Case 1: Object does not exist yet
    async with ctx.cache.get(inode, blockno) as fh:
        fh.seek(0)
        fh.write(data)

    # Case 2: Object is in cache
    async with ctx.cache.get(inode, blockno) as fh:
        fh.seek(0)
        assert data == fh.read(len(data))

    # Case 3: Object needs to be downloaded
    await ctx.cache.drop()
    async with ctx.cache.get(inode, blockno) as fh:
        fh.seek(0)
        assert data == fh.read(len(data))


@pytest.mark.trio
async def test_expire(ctx):
    inode = ctx.inode

    # Define the 4 most recently accessed ones
    most_recent = [7, 11, 10, 8]
    for i in most_recent:
        safe_sleep(0.2)
        async with ctx.cache.get(inode, i) as fh:
            fh.write(('%d' % i).encode())

    # And some others
    for i in range(20):
        if i in most_recent:
            continue
        async with ctx.cache.get(inode, i) as fh:
            fh.write(('%d' % i).encode())

    # Flush the 2 most recently accessed ones
    await start_flush(ctx.cache, inode, most_recent[-2])
    await start_flush(ctx.cache, inode, most_recent[-3])

    # We want to expire 4 entries, 2 of which are already flushed
    ctx.cache.cache.max_entries = 16
    ctx.cache.backend_pool = MockBackendPool(ctx.backend_pool, no_write=2)
    await ctx.cache.expire()
    ctx.cache.backend_pool.verify()
    assert len(ctx.cache.cache) == 16

    for i in range(20):
        if i in most_recent:
            assert (inode, i) not in ctx.cache.cache
        else:
            assert (inode, i) in ctx.cache.cache


async def test_upload(ctx):
    inode = ctx.inode
    datalen = int(0.1 * ctx.cache.cache.max_size)
    blockno1 = 21
    blockno2 = 25
    blockno3 = 7

    data1 = random_data(datalen)
    data2 = random_data(datalen)
    data3 = random_data(datalen)

    # Case 1: create new object
    ctx.cache.backend_pool = MockBackendPool(ctx.backend_pool, no_write=1)
    async with ctx.cache.get(inode, blockno1) as fh:
        fh.seek(0)
        fh.write(data1)
        el1 = fh
    assert await ctx.cache.upload_if_dirty(el1)
    ctx.cache.backend_pool.verify()

    # Case 2: Link new object
    ctx.cache.backend_pool = MockBackendPool(ctx.backend_pool)
    async with ctx.cache.get(inode, blockno2) as fh:
        fh.seek(0)
        fh.write(data1)
        el2 = fh
    assert not await ctx.cache.upload_if_dirty(el2)
    ctx.cache.backend_pool.verify()

    # Case 3: Upload old object, still has references
    ctx.cache.backend_pool = MockBackendPool(ctx.backend_pool, no_write=1)
    async with ctx.cache.get(inode, blockno1) as fh:
        fh.seek(0)
        fh.write(data2)
    assert await ctx.cache.upload_if_dirty(el1)
    ctx.cache.backend_pool.verify()

    # Case 4: Upload old object, no references left
    ctx.cache.backend_pool = MockBackendPool(ctx.backend_pool, no_del=1, no_write=1)
    async with ctx.cache.get(inode, blockno2) as fh:
        fh.seek(0)
        fh.write(data3)
    assert await ctx.cache.upload_if_dirty(el2)
    ctx.cache.backend_pool.verify()

    # Case 5: Link old object, no references left
    ctx.cache.backend_pool = MockBackendPool(ctx.backend_pool, no_del=1)
    async with ctx.cache.get(inode, blockno2) as fh:
        fh.seek(0)
        fh.write(data2)
    assert not await ctx.cache.upload_if_dirty(el2)
    ctx.cache.backend_pool.verify()

    # Case 6: Link old object, still has references
    # (Need to create another object first)
    ctx.cache.backend_pool = MockBackendPool(ctx.backend_pool, no_write=1)
    async with ctx.cache.get(inode, blockno3) as fh:
        fh.seek(0)
        fh.write(data1)
        el3 = fh
    assert await ctx.cache.upload_if_dirty(el3)
    ctx.cache.backend_pool.verify()

    ctx.cache.backend_pool = MockBackendPool(ctx.backend_pool)
    async with ctx.cache.get(inode, blockno1) as fh:
        fh.seek(0)
        fh.write(data1)
    assert not await ctx.cache.upload_if_dirty(el1)
    await ctx.cache.drop()
    ctx.cache.backend_pool.verify()


async def test_remove_referenced(ctx):
    inode = ctx.inode
    datalen = int(0.1 * ctx.cache.cache.max_size)
    blockno1 = 21
    blockno2 = 24
    data = random_data(datalen)

    ctx.cache.backend_pool = MockBackendPool(ctx.backend_pool, no_write=1)
    async with ctx.cache.get(inode, blockno1) as fh:
        fh.seek(0)
        fh.write(data)
    async with ctx.cache.get(inode, blockno2) as fh:
        fh.seek(0)
        fh.write(data)
    await ctx.cache.drop()
    ctx.cache.backend_pool.verify()

    ctx.cache.backend_pool = MockBackendPool(ctx.backend_pool)
    await ctx.cache.remove(inode, blockno1)
    ctx.cache.backend_pool.verify()


async def test_remove_cache(ctx):
    inode = ctx.inode
    data1 = random_data(int(0.4 * ctx.max_obj_size))

    # Case 1: Elements only in cache
    async with ctx.cache.get(inode, 1) as fh:
        fh.seek(0)
        fh.write(data1)
    await ctx.cache.remove(inode, 1)
    async with ctx.cache.get(inode, 1) as fh:
        fh.seek(0)
        assert fh.read(42) == b''


async def test_upload_race(ctx):
    inode = ctx.inode
    blockno = 1
    data1 = random_data(int(0.4 * ctx.max_obj_size))
    async with ctx.cache.get(inode, blockno) as fh:
        fh.seek(0)
        fh.write(data1)

    # Remove it
    await ctx.cache.remove(inode, blockno)

    # Try to upload it, may happen if CommitThread is interrupted
    await ctx.cache.upload_if_dirty(fh)


async def test_expire_race(ctx):
    # Create element
    inode = ctx.inode
    blockno = 1
    data1 = random_data(int(0.4 * ctx.max_obj_size))
    async with ctx.cache.get(inode, blockno) as fh:
        fh.seek(0)
        fh.write(data1)
    assert await ctx.cache.upload_if_dirty(fh)

    # Make sure entry will be expired
    ctx.cache.cache.max_entries = 0

    # Lock it
    await ctx.cache.mlock.acquire(inode, blockno)

    try:
        async with trio.open_nursery() as nursery:
            # Start expiration, will block on lock
            nursery.start_soon(ctx.cache.expire)

            # Start second expiration, will block
            nursery.start_soon(ctx.cache.expire)

            # Release lock
            await ctx.cache.mlock.release(inode, blockno)

        assert len(ctx.cache.cache) == 0
    finally:
        await ctx.cache.mlock.release(inode, blockno, noerror=True)


async def test_parallel_expire(ctx):
    # Create elements
    inode = ctx.inode
    for i in range(5):
        data1 = random_data(int(0.4 * ctx.max_obj_size))
        async with ctx.cache.get(inode, i) as fh:
            fh.write(data1)

    # We want to expire just one element, but have
    # several threads running expire() simultaneously
    ctx.cache.cache.max_entries = 4

    # Lock first element so that we have time to start threads
    await ctx.cache.mlock.acquire(inode, 0)

    try:
        async with trio.open_nursery() as nursery:
            # Start expiration, will block on lock
            nursery.start_soon(ctx.cache.expire)

            # Start second expiration, will block
            nursery.start_soon(ctx.cache.expire)

            # Release lock
            await ctx.cache.mlock.release(inode, 0)

        assert len(ctx.cache.cache) == 4
    finally:
        await ctx.cache.mlock.release(inode, 0, noerror=True)


async def test_remove_cache_db(ctx):
    inode = ctx.inode
    data1 = random_data(int(0.4 * ctx.max_obj_size))

    # Case 2: Element in cache and db
    async with ctx.cache.get(inode, 1) as fh:
        fh.seek(0)
        fh.write(data1)

    ctx.cache.backend_pool = MockBackendPool(ctx.backend_pool, no_write=1)
    await start_flush(ctx.cache, inode)
    ctx.cache.backend_pool.verify()

    ctx.cache.backend_pool = MockBackendPool(ctx.backend_pool, no_del=1)
    await ctx.cache.remove(inode, 1)
    ctx.cache.backend_pool.verify()

    async with ctx.cache.get(inode, 1) as fh:
        fh.seek(0)
        assert fh.read(42) == b''


async def test_remove_db(ctx):
    inode = ctx.inode
    data1 = random_data(int(0.4 * ctx.max_obj_size))

    # Case 3: Element only in DB
    async with ctx.cache.get(inode, 1) as fh:
        fh.seek(0)
        fh.write(data1)

    ctx.cache.backend_pool = MockBackendPool(ctx.backend_pool, no_write=1)
    await ctx.cache.drop()
    ctx.cache.backend_pool.verify()

    ctx.cache.backend_pool = MockBackendPool(ctx.backend_pool, no_del=1)
    await ctx.cache.remove(inode, 1)
    ctx.cache.backend_pool.verify()
    async with ctx.cache.get(inode, 1) as fh:
        fh.seek(0)
        assert fh.read(42) == b''


class MockBackendPool:
    """A mock backend pool that tracks expected call counts.

    Provides the async context manager protocol matching BackendPool.__call__().
    """

    def __init__(
        self,
        backend_pool: BackendPool,
        no_read: int = 0,
        no_write: int = 0,
        no_del: int = 0,
    ) -> None:
        self.no_read = no_read
        self.no_write = no_write
        self.no_del = no_del
        self.backend_pool = backend_pool
        self._mock = _MockAsyncBackend(self)

    @property
    def has_delete_multi(self) -> bool:
        return self.backend_pool.has_delete_multi

    def verify(self) -> None:
        if self.no_read != 0:
            raise RuntimeError('Got too few readinto_fh calls')
        if self.no_write != 0:
            raise RuntimeError('Got too few write_fh calls')
        if self.no_del != 0:
            raise RuntimeError('Got too few delete calls')

    @asynccontextmanager
    async def __call__(self) -> AsyncGenerator['_MockAsyncBackend', None]:
        '''Provide mock async connection (async context manager)'''
        yield self._mock


class _MockAsyncBackend:
    """Wraps a real async backend connection, counting operations."""

    def __init__(self, pool: MockBackendPool) -> None:
        self._pool = pool
        self._conn: AsyncComprencBackend | None = None

    async def _get_conn(self) -> AsyncComprencBackend:
        if self._conn is None:
            self._conn = await self._pool.backend_pool.pop_conn()
        return self._conn

    async def readinto_fh(
        self, key: str, fh: BinaryOutput, size_hint: int | None = None
    ) -> BasicMappingT:
        self._pool.no_read -= 1
        if self._pool.no_read < 0:
            raise RuntimeError('Got too many readinto_fh calls')
        conn = await self._get_conn()
        return await conn.readinto_fh(key, fh, size_hint=size_hint)

    async def write_fh(
        self,
        key: str,
        fh: BinaryInput,
        len_: int,
        metadata: BasicMappingT | None = None,
        dont_compress: bool = False,
    ) -> int:
        self._pool.no_write -= 1
        if self._pool.no_write < 0:
            raise RuntimeError('Got too many write_fh calls')
        conn = await self._get_conn()
        return await conn.write_fh(key, fh, len_, metadata, dont_compress=dont_compress)

    async def delete(self, key: str) -> None:
        self._pool.no_del -= 1
        if self._pool.no_del < 0:
            raise RuntimeError('Got too many delete calls')
        conn = await self._get_conn()
        return await conn.delete(key)

    async def delete_multi(self, keys: list[str]) -> None:
        self._pool.no_del -= len(keys)
        if self._pool.no_del < 0:
            raise RuntimeError('Got too many delete calls')
        conn = await self._get_conn()
        await conn.delete_multi(keys)

    async def lookup(self, key: str) -> object:
        conn = await self._get_conn()
        return await conn.lookup(key)

    async def contains(self, key: str) -> bool:
        conn = await self._get_conn()
        return await conn.contains(key)

    async def list(self, prefix: str = '') -> object:
        conn = await self._get_conn()
        return conn.list(prefix)

    async def get_size(self, key: str) -> int:
        conn = await self._get_conn()
        return await conn.get_size(key)

    async def reset(self) -> None:
        if self._conn is not None:
            await self._conn.reset()


async def start_flush(cache, inode, block=None):
    """Upload data for `inode`

    This is only for testing purposes, since the method blocks until all current
    uploads have been completed.
    """

    for el in cache.cache.values():
        if el.inode != inode:
            continue
        if not el.dirty:
            continue

        if block is not None and el.blockno != block:
            continue

        await cache.upload_if_dirty(el)
