'''
multi_lock.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

import trio
import logging

try:
    from contextlib import asynccontextmanager
except ModuleNotFoundError:
    from async_generator import asynccontextmanager

__all__ = [ "MultiLock" ]

log = logging.getLogger(__name__)

class MultiLock:
    """Provides locking for multiple objects.

    This class provides locking for a dynamically changing set of objects: The
    `acquire` and `release` methods have an additional argument, the locking
    key. Only locks with the same key can actually see each other, so that
    several threads can hold locks with different locking keys at the same time.

    MultiLock instances can be used as context managers.

    Note that it is actually possible for one task to release a lock that has
    been obtained by a different thread. This is not a bug but a feature.
    """

    def __init__(self):
        self.locked_keys = set()
        self.cond = trio.Condition()

    @asynccontextmanager
    async def __call__(self, *key):
        await self.acquire(*key)
        try:
            yield
        finally:
            await self.release(*key)

    def acquire_nowait(self, *key):
        '''Try to acquire lock for given key.

        Return True on success, False on failure.
        '''

        try:
            self.cond.acquire_nowait()
        except trio.WouldBlock:
            return False

        try:
            if key in self.locked_keys:
                return False
            else:
                self.locked_keys.add(key)
                return True
        finally:
            self.cond.release()

    async def acquire(self, *key):
        '''Acquire lock for given key.'''

        async with self.cond:
            while key in self.locked_keys:
                await self.cond.wait()

            self.locked_keys.add(key)

    async def release(self, *key, noerror=False):
        """Release lock on given key

        If noerror is False, do not raise exception if *key* is
        not locked.
        """

        async with self.cond:
            if noerror:
                self.locked_keys.discard(key)
            else:
                self.locked_keys.remove(key)
            self.cond.notify_all()
