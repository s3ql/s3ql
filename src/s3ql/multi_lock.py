'''
multi_lock.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function

import threading
import logging
from contextlib import contextmanager
import time

__all__ = [ "MultiLock" ]

log = logging.getLogger("MultiLock")


# For debugging, can be set to impose an artifical delay when
# obtaining the lock. Introduced to debug a very
# timing-critical bug.
FAKEDELAY = False

class MultiLock(object):
    """Provides locking for multiple objects.
    
    This class provides locking for a dynamically changing set of objects:
    The `acquire` and `release` methods have an additional argument, the
    locking key. Only locks with the same key can actually see each other,
    so that several threads can hold locks with different locking keys
    at the same time.
    
    MultiLock instances can be used with `with` statements as
    
    lock = MultiLock()
    with lock(key):
        pass
        
    Note that it is actually possible for one thread to release a lock
    that has been obtained by a different thread. This is not a bug,
    but a feature used in  `S3Cache._expire_parallel`.
    """

    def __init__(self):
        self.locked_keys = set()
        self.cond = threading.Condition()


    @contextmanager
    def __call__(self, *key):
        self.acquire(*key)
        try:
            yield
        finally:
            self.release(*key)

    def acquire(self, *key):
        '''Acquire lock for given key'''

        if FAKEDELAY:
            time.sleep(FAKEDELAY)

        # Lock set of lockedkeys (global lock)
        with self.cond:

            # Wait for given key becoming unused
            while key in self.locked_keys:
                self.cond.wait()

            # Mark it as used (local lock)
            self.locked_keys.add(key)

    def release(self, *key):
        """Release lock on given key"""

        # Lock set of locked keys (global lock)
        with self.cond:

            # Mark key as free (release local lock)
            self.locked_keys.remove(key)

            # Notify other threads
            self.cond.notifyAll()
