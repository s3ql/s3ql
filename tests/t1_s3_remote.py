'''
$Id$

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import unicode_literals, division, print_function

import unittest
import hashlib
import s3ql.s3
from random   import randrange
from time import sleep
import sys
import _awscred

# Allow invocation without runall.py
main = sys.modules['__main__']
if not hasattr(main, 'aws_credentials'):
    main.aws_credentials = _awscred.get()

@unittest.skipUnless(main.aws_credentials, 'remote tests disabled')
class s3_tests_remote(unittest.TestCase):

    @staticmethod
    def random_name(prefix=""):
        return "s3ql_" + prefix + str(randrange(100, 999, 1))

    def test_01_fetch_store(self):
        key = self.random_name("key1_")
        value1 = self.random_name("value1_")
        value2 = self.random_name("value2_")
        
        self.assertRaises(KeyError, self.bucket.lookup_key, key)
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
        self.assertRaises(KeyError, self.bucket.lookup_key, key)
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
        self.assertRaises(KeyError, self.bucket.lookup_key, key1)
        self.assertRaises(KeyError, self.bucket.lookup_key, key2)

        self.bucket.store(key1, value)
        sleep(self.delay)
        self.bucket.copy(key1, key2)

        sleep(self.delay)
        self.assertEquals(self.bucket[key2], value)
    
    def test_04_encryption(self):
        key = hashlib.md5('ffo').digest()
        bucket = self.conn.get_bucket(self.bucketname, key)
        
        bucket['foobar'] = b'testdata'
        
        self.assertEquals(bucket['foobar'], b'testdata')
        
        
    def setUp(self):
        (awskey, awspass) = main.aws_credentials
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
        self.delay = 1

    def tearDown(self):
        self.conn.delete_bucket(self.bucketname, recursive=True)


# Somehow important according to pyunit documentation
def suite():
    return unittest.makeSuite(s3_tests_remote)


# Allow calling from command line
if __name__ == "__main__":
    unittest.main()
