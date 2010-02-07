'''
$Id$

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function, absolute_import

import unittest
import t1_s3_local
import s3ql.s3
from random import randrange
from _common import get_aws_credentials
from time import sleep

# Each test should correspond to exactly one function in the tested
# module, and testing should be done under the assumption that any
# other functions that are called by the tested function work perfectly.
@unittest.skipUnless(get_aws_credentials(), 'remote tests disabled')
class s3_tests_remote(t1_s3_local.s3_tests_local):

    @staticmethod
    def random_name(prefix=""):
        return "s3ql_" + prefix + str(randrange(100, 999, 1))

    def setUp(self):
        (awskey, awspass) = get_aws_credentials()
        self.conn = s3ql.s3.Connection(awskey, awspass)

        self.bucketname = self.random_name()
        tries = 10
        while self.conn.bucket_exists(self.bucketname) and tries > 10:
            self.bucketname = self.random_name()
            tries -= 1

        if tries == 0:
            raise RuntimeError("Failed to find an unused bucket name.")

        self.conn.create_bucket(self.bucketname)
        self.passphrase = 'flurp'

        self.bucket = self.conn.get_bucket(self.bucketname, self.passphrase)

        # This is the time in which we expect S3 changes to propagate. It may
        # be much longer for larger objects, but for tests this is usually enough.
        self.delay = 1

    def tearDown(self):
        self.bucket.clear()
        sleep(self.delay)
        self.conn.delete_bucket(self.bucketname)

    def runTest(self):
        # Run all tests in same environment, creating and deleting
        # the bucket every time just takes too long.

        self.tst_01_store_fetch_lookup_delete_key()
        self.tst_02_meta()
        self.tst_03_list_keys()
        self.tst_04_encryption()
        self.tst_06_copy()
        self.tst_store_wait()



# Somehow important according to pyunit documentation
def suite():
    return unittest.makeSuite(s3_tests_remote)


# Allow calling from command line
if __name__ == "__main__":
    unittest.main()
