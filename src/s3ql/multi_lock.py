'''
multi_lock.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

import threading
import logging
from contextlib import contextmanager

__all__ = [ "MultiLock" ]

log = logging.getLogger(__name__)

class MultiLock:
    """Provides locking for multiple objects.

    This class provides locking for a dynamically changing set of objects: The
    `acquire` and `release` methods have an additional argument, the locking
    key. Only locks with the same key can actually see each other, so that
    several threads can hold locks with different locking keys at the same time.

    MultiLock instances can be used as context managers.

    Note that it is actually possible for one thread to release a lock that has
    been obtained by a different thread. This is not a bug but a feature.
    """

    def __init__(self):
        self.locked_keys = set()
        self.cond = threading.Condition(threading.Lock())

    @contextmanager
    def __call__(self, *key):
        self.acquire(*key)
        try:
            yield
        finally:
            self.release(*key)

    def acquire(self, *key, timeout=None):
        '''Acquire lock for given key

        If timeout is exceeded, return False. Otherwise return True.
        '''

        with self.cond:
            if not self.cond.wait_for(lambda: key not in self.locked_keys, timeout):
                return False

            self.locked_keys.add(key)

        return True

    def release(self, *key, noerror=False):
        """Release lock on given key

        If noerror is False, do not raise exception if *key* is
        not locked.
        """

        with self.cond:
            if noerror:
                self.locked_keys.discard(key)
            else:
                self.locked_keys.remove(key)
            self.cond.notifyAll()
