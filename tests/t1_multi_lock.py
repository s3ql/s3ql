'''
$Id: t1_ordered_dict.py 478 2010-01-12 23:12:54Z nikratio $

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function

import unittest
from s3ql.multi_lock import MultiLock
from _common import TestCase 

class MultiLockTests(TestCase):
               
    def test_tuple(self):
        mlock = MultiLock()
        
        with mlock('foo', 32):
            pass
        
        mlock.acquire(43, 'bar')
        mlock.release(43, 'bar')
        
      
def suite():
    return unittest.makeSuite(MultiLockTests)


if __name__ == "__main__":
    unittest.main()