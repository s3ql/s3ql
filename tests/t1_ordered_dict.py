'''
t1_ordered_dict.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function

import unittest2 as unittest
from s3ql.ordered_dict import OrderedDict
from _common import TestCase

# TODO: Rewrite this test case

# Each test should correspond to exactly one function in the tested
# module, and testing should be done under the assumption that any
# other functions that are called by the tested function work perfectly.
class OrderedDictTests(TestCase):

    def test_1_add_del(self):
        od = OrderedDict()

        key1 = 'key1'
        val1 = 'val1'

        # Add elements
        def add_one():
            od[key1] = val1
            self.assertEqual(od.get_first(), val1)
            self.assertEquals(od.get_last(), val1)
            self.assertEqual(od.get(key1), val1)
            self.assertTrue(od)
            self.assertTrue(key1 in od)
            self.assertEqual(len(od), 1)


        add_one()
        del od[key1]
        self.assertFalse(od)
        self.assertFalse(key1 in od)
        self.assertEqual(len(od), 0)
        self.assertRaises(IndexError, od.get_first)
        self.assertRaises(IndexError, od.get_last)

        add_one()
        self.assertEqual(od.pop_first(), val1)
        self.assertFalse(od)
        self.assertFalse(key1 in od)
        self.assertEqual(len(od), 0)
        self.assertRaises(IndexError, od.get_first)
        self.assertRaises(IndexError, od.get_last)

        add_one()
        self.assertEqual(od.pop_last(), val1)
        self.assertFalse(od)
        self.assertFalse(key1 in od)
        self.assertEqual(len(od), 0)
        self.assertRaises(IndexError, od.get_first)
        self.assertRaises(IndexError, od.get_last)


    def test_2_order_simple(self):
        od = OrderedDict()

        key1 = 'key1'
        val1 = 'val1'
        key2 = 'key2'
        val2 = 'val2'

        od[key1] = val1
        od[key2] = val2

        self.assertEqual(od.get_first(), val2)
        self.assertEquals(od.get_last(), val1)

        od[key1] = val1
        self.assertEqual(od.get_first(), val2)
        self.assertEquals(od.get_last(), val1)

        od.to_tail(key1)
        self.assertEqual(od.get_first(), val2)
        self.assertEquals(od.get_last(), val1)

        od.to_head(key1)
        self.assertEqual(od.get_first(), val1)
        self.assertEquals(od.get_last(), val2)

    def test_3_order_cmplx(self):
        od = OrderedDict()
        no = 10

        keys = [ 'key number %d' % i for i in range(no) ]
        vals = [ 'value number %d' % i for i in range(no) ]

        for i in range(no):
            od[keys[i]] = vals[i]
        keys.reverse()
        self._compareOrder(od, keys)

        # Move around different elements
        for i in [ 0, int((no - 1) / 2), no - 1]:
            od.to_head(keys[i])
            keys = [ keys[i] ] + keys[:i] + keys[i + 1:]
            self._compareOrder(od, keys)

            od.to_tail(keys[i])
            keys = keys[:i] + keys[i + 1:] + [ keys[i] ]
            self._compareOrder(od, keys)

            remove = keys[i]
            del od[remove]
            keys = keys[:i] + keys[i + 1:]
            self._compareOrder(od, keys)

            od[remove] = 'something new'
            keys.insert(0, remove)
            self._compareOrder(od, keys)

    def _compareOrder(self, od, keys):
        od_i = iter(od)
        keys_i = iter(keys)
        while True:
            try:
                key = keys_i.next()
            except StopIteration:
                break
            self.assertEquals(od_i.next(), key)
        self.assertRaises(StopIteration, od_i.next)

        od_i = reversed(od)
        keys_i = reversed(keys)
        while True:
            try:
                key = keys_i.next()
            except StopIteration:
                break
            self.assertEquals(od_i.next(), key)
        self.assertRaises(StopIteration, od_i.next)


def suite():
    return unittest.makeSuite(OrderedDictTests)

if __name__ == "__main__":
    unittest.main()
