#!/usr/bin/env python
#
#    Copyright (C) 2008  Nikolaus Rath <Nikolaus@rath.org>
#
#    This program can be distributed under the terms of the GNU LGPL.
#

import unittest
import s3ql.s3
from s3ql.common import *
from random   import randrange
import threading
from time import sleep

class s3_tests_remote(unittest.TestCase):

    def random_name(self, prefix=""):
        return "s3ql_" + prefix + str(randrange(100,999,1))

    def test_01_fetch_store(self):
        key = self.random_name("key1_")
        value1 = self.random_name("value1_")
        value2 = self.random_name("value2_")
        
        self.assertEquals(self.bucket.lookup_key(key), None)
        self.assertRaises(KeyError, self.bucket.delete_key, key)
        self.assertRaises(KeyError, self.bucket.fetch, key)

        etag = self.bucket.store(key, value1)
        sleep(self.delay)
        (val, meta) = self.bucket.fetch(key)
        self.assertEquals(val, value1)
        self.assertEquals(meta.etag, etag)
        self.assertEquals(meta.size, len(value1))
        self.assertEquals(meta.key, key)

        etag = self.bucket.store(key, value2)
        sleep(self.delay)
        (val, meta2) = self.bucket.fetch(key)
        self.assertEquals(meta2.etag, etag)
        self.assertNotEquals(meta.etag, meta2.etag)
        self.assertEquals(meta2.size, len(value2))
        self.assertEquals(meta2.key, key)
        self.assertTrue(meta.last_modified < meta2.last_modified)

        self.bucket.delete_key(key)
        sleep(self.delay)
        self.assertFalse(self.bucket.has_key(key))
        self.assertEquals(self.bucket.lookup_key(key), None)
        self.assertRaises(KeyError, self.bucket.delete_key, key)
        self.assertRaises(KeyError, self.bucket.fetch, key)

    def test_02_list_keys(self):
        # Keys need to be unique
        keys = [ self.random_name("key" + str(x)) for x in range(12) ]
        values = [ self.random_name("value") for x in range(12) ]

        for i in range(12):
            self.bucket[keys[i]] = values[i]

        sleep(self.delay)
        self.assertEquals(sorted(self.bucket.keys()), sorted(keys))

        for i in range(12):
            del self.bucket[keys[i]]


    def test_03_copy(self):
        key1 = self.random_name("key_1")
        key2 = self.random_name("key_2")
        value = self.random_name("value_")
        self.assertEquals(self.bucket.lookup_key(key1), None)
        self.assertEquals(self.bucket.lookup_key(key2), None)

        self.bucket.store(key1, value)
        sleep(self.delay)
        self.bucket.copy(key1, key2)

        sleep(self.delay)
        self.assertEquals(self.bucket[key2], value)
    
    def setUp(self):
        (awskey, awspass) = get_credentials()
        self.conn = s3ql.s3.Connection(awskey, awspass)
        
        self.bucketname = self.random_name()
        tries = 10
        while self.conn.bucket_exists(self.bucketname) and tries > 10:
            self.bucketname = self.random_name()
            tries -= 1
            
        if tries == 0:
            raise RuntimeException, "Failed to find an unused bucket name."
        
        self.bucket = self.conn.get_bucket(self.bucketname)
        
        # This is the time in which we expect S3 changes to propagate. It may
        # be much longer for larger objects, but for tests this is usually enough.
        self.delay = 1

    def tearDown(self):
        self.conn.delete_bucket(self.bucketname, recursive=True)


# Somehow important according to pyunit documentation
def suite():
    return unittest.makeSuite(s3_tests_remote)

# Allow calling from command line
if __name__ == "__main__":
    unittest.main()
