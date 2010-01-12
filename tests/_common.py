'''
$Id: t2_s3cache.py 450 2010-01-10 01:42:14Z nikratio $

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function 

import unittest
from s3ql.common import init_logging

__all__ = [ 'TestCase' ]

# TODO: Integrate _awscred.py into this file

class TestCase(unittest.TestCase):
    
    def __init__(self, *a, **kw):
        super(TestCase, self).__init__(*a, **kw)

        # Init logging, no debug messages
        init_logging(fg=True, quiet=False, debug=[])
        
    def run(self, result=None):
        if result is None:
            result = self.defaultTestResult()
        
        super(TestCase, self).run(result)
        
        # Abort if any test failed
        if result.errors or result.failures:
            result.stop()


    