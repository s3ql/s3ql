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
        self.bucket.tx_delay = 0
        self.bucket.prop_delay = 0.2

    def random_name(self, prefix=""):
        return "s3ql_" + prefix + str(randrange(100,999,1))

    def test_01_store_fetch_lookup_delete_key(self):
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
        old_tx = self.bucket.tx_delay
        old_prop = self.bucket.prop_delay
        self.bucket.tx_delay = 0
        self.bucket.prop_delay = 0.5

        key = self.random_name()
        value1 = self.random_name()
        value2 = self.random_name()

        assert_false(self.bucket.has_key(key))
        self.bucket[key] = value1
        assert_false(self.bucket.has_key(key))
        sleep(self.bucket.prop_delay)
        assert_true(self.bucket.has_key(key))
        assert_equals(self.bucket[key], value1)
        self.cb()

        self.bucket[key] = value2
        assert_equals(self.bucket[key], value1)
        sleep(self.bucket.prop_delay)
        assert_equals(self.bucket[key], value2)
        self.cb()

        self.bucket.delete_key(key)
        assert_true(self.bucket.has_key(key))
        assert_equals(self.bucket[key], value2)
        sleep(self.bucket.prop_delay)
        assert_false(self.bucket.has_key(key))

        self.bucket.tx_delay = old_tx
        self.bucket.prop_delay = old_prop


    def test_05_concurrency(self):
        old_tx = self.bucket.tx_delay
        old_prop = self.bucket.prop_delay
        tx_delay = 0.4
        self.bucket.tx_delay = 0.2
        self.bucket.prop_delay = 0
        key = self.random_name()
        value = self.random_name()

        def async():
           self.bucket[key] = value
        threading.Thread(target=async).start()
        assert_raises(s3ql.s3.ConcurrencyError, self.bucket.store, key, value)
        sleep(tx_delay)
        assert_true(self.bucket.store(key, value) is not None)
        self.cb()

        def async():
           self.bucket[key] = value
        threading.Thread(target=async).start()
        assert_raises(s3ql.s3.ConcurrencyError, self.bucket.fetch, key)
        sleep(tx_delay)
        assert_true(self.bucket.fetch(key) is not None)
        self.cb()

        def async():
           self.bucket.fetch(key)
        threading.Thread(target=async).start()
        assert_raises(s3ql.s3.ConcurrencyError, self.bucket.store, key, value)
        sleep(tx_delay)
        assert_true(self.bucket.store(key, value) is not None)
        self.cb()

        def async():
           self.bucket.fetch(key)
        threading.Thread(target=async).start()
        assert_raises(s3ql.s3.ConcurrencyError, self.bucket.fetch, key)
        sleep(tx_delay)
        assert_true(self.bucket.fetch(key) is not None)
        self.cb()


        del self.bucket[key]
        self.bucket.tx_delay = old_tx
        self.bucket.prop_delay = old_prop
