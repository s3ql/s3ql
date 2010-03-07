'''
$Id: t1_ordered_dict.py 478 2010-01-12 23:12:54Z nikratio $

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function

import unittest
from s3ql.multi_lock import MultiLock
from s3ql.common import ExceptionStoringThread
from _common import TestCase

# TODO: Rewrite this test case

# Each test should correspond to exactly one function in the tested
# module, and testing should be done under the assumption that any
# other functions that are called by the tested function work perfectly.
class MultiLockTests(TestCase):

    def test_acquire(self):
        mlock = MultiLock()

        def hold():
            mlock.acquire()
        t = ExceptionStoringThread(hold, logger=None)
        t.start()

        with mlock('foo', 32):
            pass

        mlock.acquire(43, 'bar')
        mlock.release(43, 'bar')


def suite():
    return unittest.makeSuite(MultiLockTests)


if __name__ == "__main__":
    unittest.main()
