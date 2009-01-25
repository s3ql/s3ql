#!/usr/bin/env python
#
#    Copyright (C) 2008  Nikolaus Rath <Nikolaus@rath.org>
#
#    This program can be distributed under the terms of the GNU LGPL.
#

from tests import TestCase, assert_true, assert_equals, assert_raises, assert_false, assert_none
import s3ql
from random   import randrange
import threading
from time import sleep

class s3_local(TestCase):
    """Tests s3ql.s3 module
    """

    def __init__(self, cb):
        self.cb = cb
        self.bucket = s3ql.s3.LocalBucket()

    def random_name(self, prefix=""):
        return "s3ql_" + prefix + str(randrange(100,999,1))

    def test_01_store_fetch_lookup_delete_key(self):
        self.bucket.tx_delay = 0
        self.bucket.prop_delay = 0
        key = self.random_name("key_")
        value = self.random_name("value_")
        assert_none(self.bucket.lookup_key(key))
        assert_raises(KeyError, self.bucket.delete_key, key)
        assert_raises(KeyError, self.bucket.fetch, key)
        self.cb()

        self.bucket.store(key, value)
        sleep(self.bucket.prop_delay)
        assert_equals(self.bucket[key], value)
        self.cb()

        self.bucket.delete_key(key)
        sleep(self.bucket.prop_delay)
        assert_false(self.bucket.has_key(key))
        self.cb()


    def test_02_meta(self):
        self.bucket.tx_delay = 0
        self.bucket.prop_delay = 0
        key = self.random_name()
        value1 = self.random_name()
        value2 = self.random_name()

        meta1 = self.bucket.store(key, value1)
        sleep(self.bucket.prop_delay)

        assert_equals(meta1.key, key)
        assert_equals(meta1.size, len(value1))
        assert_equals(self.bucket.fetch(key), (value1,meta1))
        self.cb()

        meta2 = self.bucket.store(key, value2)
        sleep(self.bucket.prop_delay)

        assert_equals(meta2.key, key)
        assert_equals(meta2.size, len(value2))
        assert_equals(self.bucket.fetch(key), (value2,meta2))
        self.cb()

        assert_true(meta1.etag != meta2.etag)
        assert_true(meta1.last_modified < meta2.last_modified)
        self.cb()

        del self.bucket[key]


    def test_03_list_keys(self):
        self.bucket.tx_delay = 0
        self.bucket.prop_delay = 0
        # Keys need to be unique
        keys = [ self.random_name("key_") + str(x) for x in range(12) ]
        values = [ self.random_name("value_") for x in range(12) ]

        for i in range(12):
            self.bucket[keys[i]] = values[i]


        sleep(self.bucket.prop_delay)
        assert_equals(sorted(self.bucket.keys()), sorted(keys))

        for i in range(12):
            del self.bucket[keys[i]]


        sleep(self.bucket.prop_delay)

    def test_04_delays(self):
        # The other threads may not start immediately, so
        # we need some tolerance here.
        prop_delay = 0.6
        self.bucket.tx_delay = 0
        self.bucket.prop_delay = 0.3

        key = self.random_name()
        value1 = self.random_name()
        value2 = self.random_name()

        assert_false(self.bucket.has_key(key))
        self.bucket[key] = value1
        assert_false(self.bucket.has_key(key))
        sleep(prop_delay)
        assert_true(self.bucket.has_key(key))
        assert_equals(self.bucket[key], value1)
        self.cb()

        self.bucket[key] = value2
        assert_equals(self.bucket[key], value1)
        sleep(prop_delay)
        assert_equals(self.bucket[key], value2)
        self.cb()

        self.bucket.delete_key(key)
        assert_true(self.bucket.has_key(key))
        assert_equals(self.bucket[key], value2)
        sleep(prop_delay)
        assert_false(self.bucket.has_key(key))


    def test_05_concurrency(self):
        self.bucket.tx_delay = 0.2
        self.bucket.prop_delay = 0
        key = self.random_name()
        value = self.random_name()

        def async():
           self.bucket[key] = value
        t = threading.Thread(target=async)
        t.start()
        sleep(0.1) # Make sure the other thread is actually running
        assert_raises(s3ql.s3.ConcurrencyError, self.bucket.store, key, value)
        t.join()
        assert_true(self.bucket.store(key, value) is not None)
        self.cb()

        def async():
           self.bucket[key] = value
        t = threading.Thread(target=async)
        t.start()
        sleep(0.1) # Make sure the other thread is actually running
        assert_raises(s3ql.s3.ConcurrencyError, self.bucket.fetch, key)
        t.join()
        assert_true(self.bucket.fetch(key) is not None)
        self.cb()

        def async():
           self.bucket.fetch(key)
        t = threading.Thread(target=async)
        t.start()
        sleep(0.1) # Make sure the other thread is actually running
        assert_raises(s3ql.s3.ConcurrencyError, self.bucket.store, key, value)
        t.join()
        assert_true(self.bucket.store(key, value) is not None)
        self.cb()

        def async():
           self.bucket.fetch(key)
        t = threading.Thread(target=async)
        t.start()
        sleep(0.1) # Make sure the other thread is actually running
        assert_raises(s3ql.s3.ConcurrencyError, self.bucket.fetch, key)
        t.join()
        assert_true(self.bucket.fetch(key) is not None)
        self.cb()


        self.bucket.tx_delay = 0
        self.bucket.prop_delay = 0
        del self.bucket[key]
