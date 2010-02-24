'''
$Id: t2_s3cache.py 450 2010-01-10 01:42:14Z nikratio $

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function

import unittest
import os
import stat
import sys

__all__ = [ 'TestCase', 'get_aws_credentials' ]

aws_credentials_available = None
aws_credentials = None
keyfile = os.path.join(os.environ["HOME"], ".awssecret")

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

def get_aws_credentials():
    '''Return AWS credentials if ~/.awssecret exists and is secure.
    
    Otherwise return None and, on first invocation, print warning message.
    '''

    global aws_credentials
    global aws_credentials_available

    # Check only if not done so already
    if aws_credentials_available:
        return aws_credentials

    elif aws_credentials_available is False: # distinguish between None and False
        return

    if os.path.isfile(keyfile):
        mode = os.stat(keyfile).st_mode
        kfile = open(keyfile, "r")
        key = kfile.readline().rstrip()

        if mode & (stat.S_IRGRP | stat.S_IROTH):
            sys.stderr.write("~/.awssecret has insecure permissions, skipping remote tests.\n")
            aws_credentials_available = False
            return
        else:
            pw = kfile.readline().rstrip()
            aws_credentials = (key, pw)
            aws_credentials_available = True
            print ('Will use credentials from ~/.awssecret to access AWS.')
            return aws_credentials
        kfile.close()
    else:
        print('~/.awssecret does not exist. Will skip tests requiring valid AWS credentials.')
        aws_credentials_available = False
        return
