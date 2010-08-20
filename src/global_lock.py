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

__all__ = [ 'lock' ]

lock = threading.RLock()
lock.acquire()

