'''
$Id$

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function

import unittest
import s3ql.s3
from random import randrange
from _common import TestCase, get_aws_credentials
from time import sleep

@unittest.skipUnless(get_aws_credentials(), 'remote tests disabled')
class s3_tests_remote(TestCase):

    @staticmethod
    def random_name(prefix=""):
        return "s3ql_" + prefix + str(randrange(100, 999, 1))

    def test_01_store_fetch_lookup_delete_key(self):

        key = self.random_name("key_")
        value = self.random_name("value_")
        self.assertRaises(KeyError, self.bucket.lookup_key, key)
        self.assertRaises(KeyError, self.bucket.delete_key, key)
        self.assertRaises(KeyError, self.bucket.fetch, key)

        self.bucket.store(key, value)
        self.assertEquals(self.bucket[key], value)
        self.bucket.lookup_key(key)
        
        self.bucket.delete_key(key)
        self.assertFalse(self.bucket.has_key(key))
        self.assertRaises(KeyError, self.bucket.lookup_key, key)
        self.assertRaises(KeyError, self.bucket.delete_key, key)
        self.assertRaises(KeyError, self.bucket.fetch, key)


    def test_02_meta(self):
        key = self.random_name()
        value1 = self.random_name()
        value2 = self.random_name()

        self.bucket.store(key, value1, { 'foo': 42 })
        meta1 = self.bucket.fetch(key)[1]

        self.assertEquals(meta1['foo'], 42)

        self.bucket.store(key, value2, { 'bar': 37 })
        meta2 = self.bucket.fetch(key)[1]
        
        self.assertTrue('foo' not in meta2)
        self.assertEquals(meta2['bar'], 37)
        
        self.assertTrue(meta1['last-modified'] < meta2['last-modified'])

        del self.bucket[key]


    def test_03_list_keys(self):

        # Keys need to be unique
        keys = [ self.random_name("key_") + str(x) for x in range(12) ]
        values = [ self.random_name("value_") for x in range(12) ]

        for i in range(12):
            self.bucket.store(keys[i], values[i])

        sleep(s3ql.s3.LOCAL_PROP_DELAY)
        self.assertEquals(sorted(self.bucket.keys()), sorted(keys))
            
        for i in range(12):
            del self.bucket[keys[i]]


    def test_05_copy(self):
        key1 = self.random_name("key_1")
        key2 = self.random_name("key_2")
        value = self.random_name("value_")
        self.assertRaises(KeyError, self.bucket.lookup_key, key1)
        self.assertRaises(KeyError, self.bucket.lookup_key, key2)

        self.bucket.store(key1, value)
        sleep(self.delay)
        self.bucket.copy(key1, key2)

        sleep(self.delay)
        self.assertEquals(self.bucket[key2], value)

        
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
        self.bucket = self.conn.get_bucket(self.bucketname)
        
        # This is the time in which we expect S3 changes to propagate. It may
        # be much longer for larger objects, but for tests this is usually enough.
        self.delay = 0.3

    def tearDown(self):
        self.conn.delete_bucket(self.bucketname, recursive=True)


# Somehow important according to pyunit documentation
def suite():
    return unittest.makeSuite(s3_tests_remote)


# Allow calling from command line
if __name__ == "__main__":
    unittest.main()
