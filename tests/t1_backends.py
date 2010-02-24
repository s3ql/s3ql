'''
$Id$

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function

import unittest
from s3ql.backends import local, s3
from s3ql.backends.common import ChecksumError
import tempfile
import os
import time
from _common import TestCase
from _common import get_aws_credentials
from random import randrange

# Each test should correspond to exactly one function in the tested
# module, and testing should be done under the assumption that any
# other functions that are called by the tested function work perfectly.
class BackendTests(object):

    def newname(self):
        self.name_cnt += 1
        # Include special characters
        return "s3ql_=/_%d" % self.name_cnt

    def test_store(self):
        key = self.newname()
        value = self.newname()
        metadata = { 'jimmy': 'jups@42' }

        self.assertRaises(KeyError, self.bucket.lookup, key)
        self.bucket.store(key, value, metadata)
        time.sleep(self.delay)
        self.assertEquals(self.bucket.fetch(key), (value, metadata))
        self.assertEquals(self.bucket[key], value)

    def test_fetch(self):
        key = self.newname()
        value = self.newname()
        metadata = { 'jimmy': 'jups@42' }

        self.assertRaises(KeyError, self.bucket.fetch, key)
        self.bucket.store(key, value, metadata)
        time.sleep(self.delay)
        self.assertEquals(self.bucket.fetch(key), (value, metadata))

    def test_lookup(self):
        key = self.newname()
        value = self.newname()
        metadata = { 'jimmy': 'jups@42' }

        self.assertRaises(KeyError, self.bucket.lookup, key)
        self.bucket.store(key, value, metadata)
        time.sleep(self.delay)
        self.assertEquals(self.bucket.lookup(key), metadata)

    def test_contains(self):
        key = self.newname()
        value = self.newname()

        self.assertFalse(key in self.bucket)
        self.bucket[key] = value
        time.sleep(self.delay)
        self.assertTrue(key in self.bucket)

    def test_delete(self):
        key = self.newname()
        value = self.newname()
        self.bucket[key] = value
        time.sleep(self.delay)

        self.assertTrue(key in self.bucket)
        del self.bucket[key]
        time.sleep(self.delay)
        self.assertFalse(key in self.bucket)

    def test_clear(self):
        self.bucket[self.newname()] = self.newname()
        self.bucket[self.newname()] = self.newname()

        time.sleep(self.delay)
        self.assertEquals(len(list(self.bucket)), 2)
        self.bucket.clear()
        time.sleep(self.delay)
        self.assertEquals(len(list(self.bucket)), 0)

    def test_size(self):
        # We can't test exactly because of compression, metadata etc.
        key1 = self.newname()
        key2 = self.newname()
        self.bucket[key1] = self.newname()
        time.sleep(self.delay)
        size1 = self.bucket.get_size()
        self.assertTrue(size1 > 0)
        self.bucket[key2] = self.newname()
        time.sleep(self.delay)
        size2 = self.bucket.get_size()
        self.assertTrue(size2 > size1)
        del self.bucket[key1]
        time.sleep(self.delay)
        size3 = self.bucket.get_size()
        self.assertTrue(size3 < size2)

    def test_list(self):

        keys = [ self.newname() for dummy in range(12) ]
        values = [ self.newname() for dummy in range(12) ]
        for i in range(12):
            self.bucket[keys[i]] = values[i]

        time.sleep(self.delay)
        self.assertEquals(sorted(self.bucket.list()), sorted(keys))

    def test_encryption(self):
        bucket = self.bucket
        bucket.passphrase = None
        bucket['plain'] = b'foobar452'

        bucket.passphrase = 'schlurp'
        bucket.store('encrypted', 'testdata', { 'tag': True })
        time.sleep(self.delay)
        self.assertEquals(bucket['encrypted'], b'testdata')
        self.assertRaises(ChecksumError, bucket.fetch, 'plain')
        self.assertRaises(ChecksumError, bucket.lookup, 'plain')

        bucket.passphrase = None
        self.assertRaises(ChecksumError, bucket.fetch, 'encrypted')
        self.assertRaises(ChecksumError, bucket.lookup, 'encrypted')

        bucket.passphrase = self.passphrase
        self.assertRaises(ChecksumError, bucket.fetch, 'encrypted')
        self.assertRaises(ChecksumError, bucket.lookup, 'encrypted')
        self.assertRaises(ChecksumError, bucket.fetch, 'plain')
        self.assertRaises(ChecksumError, bucket.lookup, 'plain')

    def test_copy(self):

        key1 = self.newname()
        key2 = self.newname()
        value = self.newname()
        self.assertRaises(KeyError, self.bucket.lookup, key1)
        self.assertRaises(KeyError, self.bucket.lookup, key2)

        self.bucket.store(key1, value)
        time.sleep(self.delay)
        self.bucket.copy(key1, key2)

        time.sleep(self.delay)
        self.assertEquals(self.bucket[key2], value)


# This test just takes too long (because we have to wait really long so that we don't
# get false errors due to propagation delays)
#@unittest.skipUnless(get_aws_credentials(), 'remote tests disabled')
#class S3Tests(BackendTests, TestCase):
class S3Tests(BackendTests):
    @staticmethod
    def random_name(prefix=""):
        return "s3ql-" + prefix + str(randrange(1000, 9999, 1))

    def setUp(self):
        self.name_cnt = 0
        (awskey, awspass) = get_aws_credentials()
        self.conn = s3.Connection(awskey, awspass)

        self.bucketname = self.random_name()
        tries = 10
        while self.conn.bucket_exists(self.bucketname) and tries > 10:
            self.bucketname = self.random_name()
            tries -= 1

        if tries == 0:
            raise RuntimeError("Failed to find an unused bucket name.")

        self.passphrase = 'flurp'
        self.bucket = self.conn.create_bucket(self.bucketname, self.passphrase)

        # This is the time in which we expect S3 changes to propagate. It may
        # be much longer for larger objects, but for tests this is usually enough.
        self.delay = 5
        time.sleep(self.delay)

    def tearDown(self):
        self.conn.delete_bucket(self.bucketname, recursive=True)

class LocalTests(BackendTests, TestCase):

    def setUp(self):
        self.name_cnt = 0
        self.conn = local.Connection()
        self.bucket_dir = tempfile.mkdtemp()
        self.bucketname = os.path.join(self.bucket_dir, 'mybucket')
        self.passphrase = 'flurp'
        self.bucket = self.conn.create_bucket(self.bucketname, self.passphrase)
        self.delay = 0

    def tearDown(self):
        self.conn.delete_bucket(self.bucketname, recursive=True)
        os.rmdir(self.bucket_dir)

# Somehow important according to pyunit documentation
def suite():
    return unittest.makeSuite(LocalTests)


# Allow calling from command line
if __name__ == "__main__":
    unittest.main()
