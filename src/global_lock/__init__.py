#!/usr/bin/env python
'''
global_lock.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.

This module manages a global lock for synchronizing program
execution. Methods may explicitly release this lock for potentially
time consuming operations to allow limited concurrency.

A method that releases the global lock must be prepared to be called
again while it has released the lock. In addition, it (and all its
callers) must not hold any prior locks, since this may lead to deadlocks 
when re-acquiring the global lock.

For this reason it is crucial that every method that directly
or indirectly releases the lock is explicitly marked as such.
'''

import threading
import thread
from contextlib import contextmanager

__all__ = [ 'lock' ]

class Lock(object):
    
    def __init__(self):
        self.holder = None
        self.lock = threading.Lock()
        super(Lock, self).__init__()
        
    def acquire(self):
        me = thread.get_ident()
        if self.holder == me:
            raise RuntimeError('Global lock already acquired')
        self.lock.acquire()
        self.holder = me
        
    def release(self):
        me = thread.get_ident()
        if self.holder != me:
            raise RuntimeError('Global lock may only be released by the holding thread')
        self.holder = None
        self.lock.release()
    
    __enter__ = acquire
    
    def __exit__(self, type_, value, tb):
        self.release()
        return False
        
    @contextmanager
    def unlocked(self):
        self.release()
        try:
            yield
        finally:
            self.acquire()
        
    def held_by(self):
        return self.holder

lock = Lock()
lock.acquire()

