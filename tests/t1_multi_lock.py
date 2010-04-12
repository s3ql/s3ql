'''
t1_ordered_dict.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function

import unittest
from s3ql.multi_lock import MultiLock
import time
from s3ql.common import ExceptionStoringThread
from _common import TestCase

BASE_DELAY = 0.02

class MultiLockTests(TestCase):

    def test_lock(self):
        mlock = MultiLock()
        key = (22, 'bar')

        def hold():
            mlock.acquire(key)
            time.sleep(2 * BASE_DELAY)
            mlock.release(key)

        t = ExceptionStoringThread(hold, logger=None)
        t.start()
        time.sleep(BASE_DELAY)

        stamp = time.time()
        with mlock(key):
            pass
        self.assertTrue(time.time() - stamp > BASE_DELAY)

        t.join_and_raise()

    def test_nolock(self):
        mlock = MultiLock()
        key1 = (22, 'bar')
        key2 = (23, 'bar')

        def hold():
            mlock.acquire(key1)
            time.sleep(2 * BASE_DELAY)
            mlock.release(key1)

        t = ExceptionStoringThread(hold, logger=None)
        t.start()
        time.sleep(BASE_DELAY)

        stamp = time.time()
        with mlock(key2):
            pass
        self.assertTrue(time.time() - stamp < BASE_DELAY)

        t.join_and_raise()

    def test_multi(self):
        mlock = MultiLock()
        key = (22, 'bar')

        def lock():
            mlock.acquire(key)

        def unlock():
            time.sleep(2 * BASE_DELAY)
            mlock.release(key)

        t1 = ExceptionStoringThread(lock, logger=None)
        t1.start()
        t1.join_and_raise()

        t2 = ExceptionStoringThread(unlock, logger=None)
        t2.start()

        stamp = time.time()
        with mlock(key):
            pass
        self.assertTrue(time.time() - stamp > BASE_DELAY)

        t2.join_and_raise()

def suite():
    return unittest.makeSuite(MultiLockTests)


if __name__ == "__main__":
    unittest.main()
