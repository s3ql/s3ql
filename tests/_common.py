'''
t2_s3cache.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.

This module defines a new TestCase that aborts the test run as 
soon as a test fails. The module also servers as a storage container
for authentication data that may be required for some test cases.
'''

from __future__ import division, print_function

import unittest2 as unittest
import os
import logging
import sys

__all__ = [ 'TestCase' ]

log = logging.getLogger()

class TestCase(unittest.TestCase):

    def __init__(self, *a, **kw):
        super(TestCase, self).__init__(*a, **kw)

        # Init logging with default settings if not yet done
        if not log.handlers:
            logging.basicConfig(level=logging.WARN, stream=sys.stderr)

    def run(self, result=None):
        if result is None:
            result = self.defaultTestResult()

        super(TestCase, self).run(result)

        # Abort if any test failed
        if result.errors or result.failures:
            result.stop()

# Try to read credentials from file. Meant for developer use only,  
# so that we can run individual tests without the setup.py
# initialization.
def init_credentials():
    keyfile = os.path.join(os.environ['HOME'], '.awssecret')

    if not os.path.isfile(keyfile):
        return None

    with open(keyfile, "r") as fh:
        key = fh.readline().rstrip()
        pw = fh.readline().rstrip()

    return (key, pw)

aws_credentials = init_credentials()



