'''
t2_s3cache.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.

This module defines a new TestCase that aborts the test run as 
soon as a test fails. The module also servers as a storage container
for authentication data that may be required for some test cases.
'''

from __future__ import division, print_function

import unittest

__all__ = [ 'TestCase' ]

aws_credentials = None

class TestCase(unittest.TestCase):

    def __init__(self, *a, **kw):
        super(TestCase, self).__init__(*a, **kw)

    def run(self, result=None):
        if result is None:
            result = self.defaultTestResult()

        super(TestCase, self).run(result)

        # Abort if any test failed
        if result.errors or result.failures:
            result.stop()

    def assertIsNone(self, val):
        self.assertTrue(val is None, '%r is not None' % val)
