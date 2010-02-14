'''
$Id$

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function

import unittest
from s3ql import s3
import tempfile
import threading
import os
import shutil
from time import sleep
from _common import TestCase

# TODO: Rewrite this test case

# Each test should correspond to exactly one function in the tested
# module, and testing should be done under the assumption that any
# other functions that are called by the tested function work perfectly.
class s3_tests_local(TestCase):

    def setUp(self):
        self.conn = s3.LocalConnection()
        self.bucket_dir = tempfile.mkdtemp()
        self.bucketname = os.path.join(self.bucket_dir, 'mybucket')
        self.passphrase = 'flurp'
        self.conn.create_bucket(self.bucketname, self.passphrase)
        self.bucket = self.conn.get_bucket(self.bucketname)
        self.name_cnt = 0
        self.delay = s3.LOCAL_PROP_DELAY * 1.1

    def tearDown(self):
        # Wait for pending transactions
        sleep(self.delay)
        shutil.rmtree(self.bucket_dir)

    def newname(self):
        self.name_cnt += 1
        return "s3ql_%d" % self.name_cnt

    def tst_01_store_fetch_lookup_delete_key(self):
        key = self.newname()
        value = self.newname()
        self.assertRaises(KeyError, self.bucket.lookup_key, key)
        self.assertRaises(KeyError, self.bucket.delete_key, key)
        self.assertRaises(KeyError, self.bucket.fetch, key)

        self.bucket.store(key, value)
        sleep(self.delay)
        self.assertEquals(self.bucket[key], value)
        self.bucket.lookup_key(key)

        self.bucket.delete_key(key)
        sleep(self.delay)
        self.assertFalse(self.bucket.has_key(key))
        self.assertRaises(KeyError, self.bucket.lookup_key, key)
        self.assertRaises(KeyError, self.bucket.delete_key, key)
        self.assertRaises(KeyError, self.bucket.fetch, key)

    def tst_02_meta(self):
        key = self.newname()
        value1 = self.newname()
        value2 = self.newname()

        self.bucket.store(key, value1, { 'foo': 42 })
        sleep(self.delay)
        meta1 = self.bucket.fetch(key)[1]

        self.assertEquals(meta1['foo'], 42)

        self.bucket.store(key, value2, { 'bar': 37 })
        sleep(self.delay)
        meta2 = self.bucket.fetch(key)[1]

        self.assertTrue('foo' not in meta2)
        self.assertEquals(meta2['bar'], 37)

        del self.bucket[key]

    def runTest(self):
        # Run all tests in same environment, creating and deleting
        # the bucket every time just takes too long.

        self.tst_01_store_fetch_lookup_delete_key()
        self.tst_02_meta()
        self.tst_03_list_keys()
        self.tst_04_encryption()
        self.tst_04_delays()
        self.tst_05_concurrency()
        self.tst_06_copy()

    def tst_03_list_keys(self):

        # Keys need to be unique
        keys = [ self.newname() for dummy in range(12) ]
        values = [ self.newname() for dummy in range(12) ]

        for i in range(12):
            self.bucket.store(keys[i], values[i])

        sleep(self.delay)
        self.assertEquals(sorted(self.bucket.keys()), sorted(keys))

        for i in range(12):
            del self.bucket[keys[i]]

    def tst_04_delays(self):
        # Required for test to work
        assert s3.LOCAL_TX_DELAY > 0
        assert s3.LOCAL_PROP_DELAY > 3 * s3.LOCAL_TX_DELAY

        key = self.newname()
        value1 = self.newname()
        value2 = self.newname()

        self.assertFalse(self.bucket.has_key(key))
        self.bucket[key] = value1
        self.assertFalse(self.bucket.has_key(key))
        sleep(s3.LOCAL_PROP_DELAY * 1.1)
        self.assertTrue(self.bucket.has_key(key))
        self.assertEquals(self.bucket[key], value1)

        self.bucket[key] = value2
        self.assertEquals(self.bucket[key], value1)
        sleep(s3.LOCAL_PROP_DELAY * 1.1)
        self.assertEquals(self.bucket[key], value2)

        self.bucket.delete_key(key)
        self.assertEquals(self.bucket[key], value2)
        sleep(s3.LOCAL_PROP_DELAY * 1.1)
        self.assertFalse(self.bucket.has_key(key))

    def tst_04_encryption(self):
        bucket = self.bucket
        bucket.passphrase = None
        bucket['plain'] = b'foobar452'

        bucket.passphrase = 'schlurp'
        bucket.store('encrypted', 'testdata', { 'tag': True })
        sleep(self.delay)
        self.assertEquals(bucket['encrypted'], b'testdata')
        self.assertRaises(s3.ChecksumError, bucket.fetch, 'plain')
        self.assertRaises(s3.ChecksumError, bucket.lookup_key, 'plain')

        bucket.passphrase = None
        self.assertRaises(s3.ChecksumError, bucket.fetch, 'encrypted')
        self.assertRaises(s3.ChecksumError, bucket.lookup_key, 'encrypted')

        bucket.passphrase = self.passphrase
        self.assertRaises(s3.ChecksumError, bucket.fetch, 'encrypted')
        self.assertRaises(s3.ChecksumError, bucket.lookup_key, 'encrypted')
        self.assertRaises(s3.ChecksumError, bucket.fetch, 'plain')
        self.assertRaises(s3.ChecksumError, bucket.lookup_key, 'plain')


    def tst_05_concurrency(self):
        tx_delay = s3.LOCAL_TX_DELAY

        # Required for tests to work
        assert tx_delay > 0

        key = self.newname()
        value = self.newname()

        def async1():
            self.bucket[key] = value
        t = threading.Thread(target=async1)
        t.start()
        sleep(tx_delay / 2) # Make sure the other thread is actually running
        self.assertRaises(s3.ConcurrencyError, self.bucket.store, key, value)
        t.join()
        self.bucket.store(key, value)
        sleep(self.delay)

        def async2():
            self.bucket[key] = value
        t = threading.Thread(target=async2)
        t.start()
        sleep(tx_delay / 2) # Make sure the other thread is actually running
        self.assertRaises(s3.ConcurrencyError, self.bucket.fetch, key)
        t.join()
        self.assertTrue(self.bucket.fetch(key) is not None)

        def async3():
            self.bucket.fetch(key)
        t = threading.Thread(target=async3)
        t.start()
        sleep(tx_delay / 2) # Make sure the other thread is actually running
        self.assertRaises(s3.ConcurrencyError, self.bucket.store, key, value)
        t.join()
        self.bucket.store(key, value)

        def async4():
            self.bucket.fetch(key)
        t = threading.Thread(target=async4)
        t.start()
        sleep(tx_delay / 2) # Make sure the other thread is actually running
        self.assertRaises(s3.ConcurrencyError, self.bucket.fetch, key)
        t.join()
        self.assertTrue(self.bucket.fetch(key) is not None)

        sleep(self.delay)
        del self.bucket[key]

    def tst_06_copy(self):

        key1 = self.newname()
        key2 = self.newname()
        value = self.newname()
        self.assertRaises(KeyError, self.bucket.lookup_key, key1)
        self.assertRaises(KeyError, self.bucket.lookup_key, key2)

        self.bucket.store(key1, value)
        sleep(self.delay)
        self.bucket.copy(key1, key2)

        sleep(self.delay)
        self.assertEquals(self.bucket[key2], value)


# Somehow important according to pyunit documentation
def suite():
    return unittest.makeSuite(s3_tests_local)


# Allow calling from command line
if __name__ == "__main__":
    unittest.main()
