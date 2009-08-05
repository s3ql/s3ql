#!/usr/bin/env python
#
#    Copyright (C) 2008  Nikolaus Rath <Nikolaus@rath.org>
#
#    This program can be distributed under the terms of the GNU LGPL.
#

from __future__ import unicode_literals
import unittest
import s3ql.s3
from random   import randrange
import threading
from time import sleep

class s3_tests_local(unittest.TestCase):
    
    def setUp(self):
        self.bucket = s3ql.s3.LocalBucket()

    def random_name(self, prefix=""):
        return "s3ql_" + prefix + str(randrange(100,999,1))

    def test_01_store_fetch_lookup_delete_key(self):
        self.bucket.tx_delay = 0
        self.bucket.prop_delay = 0
        key = self.random_name("key_")
        value = self.random_name("value_")
        self.assertEquals(self.bucket.lookup_key(key), None)
        self.assertRaises(KeyError, self.bucket.delete_key, key)
        self.assertRaises(KeyError, self.bucket.fetch, key)

        self.bucket.store(key, value)
        sleep(self.bucket.prop_delay+0.1)
        self.assertEquals(self.bucket[key], value)

        self.bucket.delete_key(key)
        sleep(self.bucket.prop_delay+0.1)
        self.assertFalse(self.bucket.has_key(key))

    def test_02_meta(self):
        self.bucket.tx_delay = 0
        self.bucket.prop_delay = 0
        key = self.random_name()
        value1 = self.random_name()
        value2 = self.random_name()

        etag1 = self.bucket.store(key, value1)
        sleep(self.bucket.prop_delay+0.1)
        meta1 = self.bucket.fetch(key)[1]

        self.assertEquals(meta1.key, key)
        self.assertEquals(meta1.etag, etag1)
        self.assertEquals(meta1.size, len(value1))

        etag2 = self.bucket.store(key, value2)
        sleep(self.bucket.prop_delay+0.1)
        meta2 = self.bucket.fetch(key)[1]
        
        self.assertEquals(meta2.key, key)
        self.assertEquals(meta2.etag, etag2)
        self.assertEquals(meta2.size, len(value2))

        self.assertTrue(meta1.etag != meta2.etag)
        self.assertTrue(meta1.last_modified < meta2.last_modified)

        del self.bucket[key]


    def test_03_list_keys(self):
        self.bucket.tx_delay = 0
        self.bucket.prop_delay = 0
        # Keys need to be unique
        keys = [ self.random_name("key_") + str(x) for x in range(12) ]
        values = [ self.random_name("value_") for x in range(12) ]

        for i in range(12):
            self.bucket[keys[i]] = values[i]

        sleep(self.bucket.prop_delay+0.1)
        self.assertEquals(sorted(self.bucket.keys()), sorted(keys))

        for i in range(12):
            del self.bucket[keys[i]]


    def test_04_delays(self):
        # The other threads may not start immediately, so
        # we need some tolerance here.
        prop_delay = 0.6
        self.bucket.tx_delay = 0
        self.bucket.prop_delay = 0.3

        key = self.random_name()
        value1 = self.random_name()
        value2 = self.random_name()

        self.assertFalse(self.bucket.has_key(key))
        self.bucket[key] = value1
        self.assertFalse(self.bucket.has_key(key))
        sleep(prop_delay)
        self.assertTrue(self.bucket.has_key(key))
        self.assertEquals(self.bucket[key], value1)

        self.bucket[key] = value2
        self.assertEquals(self.bucket[key], value1)
        sleep(prop_delay)
        self.assertEquals(self.bucket[key], value2)

        self.bucket.delete_key(key)
        self.assertTrue(self.bucket.has_key(key))
        self.assertEquals(self.bucket[key], value2)
        sleep(prop_delay)
        self.assertFalse(self.bucket.has_key(key))


    def test_05_concurrency(self):
        self.bucket.tx_delay = 0.2
        self.bucket.prop_delay = 0
        key = self.random_name()
        value = self.random_name()

        def async1():
            self.bucket[key] = value
        t = threading.Thread(target=async1)
        t.start()
        sleep(0.1) # Make sure the other thread is actually running
        self.assertRaises(s3ql.s3.ConcurrencyError, self.bucket.store, key, value)
        t.join()
        self.assertTrue(self.bucket.store(key, value) is not None)

        def async2():
            self.bucket[key] = value
        t = threading.Thread(target=async2)
        t.start()
        sleep(0.1) # Make sure the other thread is actually running
        self.assertRaises(s3ql.s3.ConcurrencyError, self.bucket.fetch, key)
        t.join()
        self.assertTrue(self.bucket.fetch(key) is not None)

        def async3():
            self.bucket.fetch(key)
        t = threading.Thread(target=async3)
        t.start()
        sleep(0.1) # Make sure the other thread is actually running
        self.assertRaises(s3ql.s3.ConcurrencyError, self.bucket.store, key, value)
        t.join()
        self.assertTrue(self.bucket.store(key, value) is not None)

        def async4():
            self.bucket.fetch(key)
        t = threading.Thread(target=async4)
        t.start()
        sleep(0.1) # Make sure the other thread is actually running
        self.assertRaises(s3ql.s3.ConcurrencyError, self.bucket.fetch, key)
        t.join()
        self.assertTrue(self.bucket.fetch(key) is not None)

        self.bucket.tx_delay = 0
        self.bucket.prop_delay = 0
        del self.bucket[key]

    def test_06_copy(self):
        self.bucket.tx_delay = 0
        self.bucket.prop_delay = 0
        key1 = self.random_name("key_1")
        key2 = self.random_name("key_2")
        value = self.random_name("value_")
        self.assertEquals(self.bucket.lookup_key(key1), None)
        self.assertEquals(self.bucket.lookup_key(key2), None)

        self.bucket.store(key1, value)
        sleep(self.bucket.prop_delay+0.1)
        self.bucket.copy(key1, key2)

        sleep(self.bucket.prop_delay+0.1)
        self.assertEquals(self.bucket[key2], value)


# Somehow important according to pyunit documentation
def suite():
    return unittest.makeSuite(s3_tests_local)


# Allow calling from command line
if __name__ == "__main__":
            unittest.main()
