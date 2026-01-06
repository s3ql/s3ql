'''
pool.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import annotations

import logging
import threading
from collections.abc import Callable, Generator
from contextlib import contextmanager

from .common import AbstractBackend

log = logging.getLogger(__name__)


class BackendPool:
    '''A pool of backends

    This class is threadsafe. All methods (except for internal methods
    starting with underscore) may be called concurrently by different
    threads.
    '''

    factory: Callable[[], AbstractBackend]
    pool: list[AbstractBackend]
    lock: threading.Lock

    def __init__(self, factory: Callable[[], AbstractBackend]) -> None:
        '''Init pool

        *factory* should be a callable that provides new
        connections.
        '''

        self.factory = factory
        self.pool = []
        self.lock = threading.Lock()

    def pop_conn(self) -> AbstractBackend:
        '''Pop connection from pool'''

        with self.lock:
            if self.pool:
                return self.pool.pop()
            else:
                return self.factory()

    def push_conn(self, conn: AbstractBackend) -> None:
        '''Push connection back into pool'''

        conn.reset()
        with self.lock:
            self.pool.append(conn)

    def flush(self) -> None:
        '''Close all backends in pool

        This method calls the `close` method on all backends
        currently in the pool.
        '''
        with self.lock:
            while self.pool:
                self.pool.pop().close()

    @contextmanager
    def __call__(self, close: bool = False) -> Generator[AbstractBackend, None, None]:
        '''Provide connection from pool (context manager)

        If *close* is True, the backend's close method is automatically called
        (which frees any allocated resources, but may slow down reuse of the
        backend object).
        '''

        conn = self.pop_conn()
        try:
            yield conn
        finally:
            if close:
                conn.close()
            self.push_conn(conn)
