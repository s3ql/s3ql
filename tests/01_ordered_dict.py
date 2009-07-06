#!/usr/bin/env python
#
#    Copyright (C) 2008-2009  Nikolaus Rath <Nikolaus@rath.org>
#
#    This program can be distributed under the terms of the GNU LGPL.
#

import unittest
from s3ql.ordered_dict import OrderedDict

class OrderedDictTests(unittest.TestCase):

    def setUp(self):
        pass


    def tearDown(self):
        pass


    def test_strangebug(self):
        od = OrderedDict()
        
        key1 = 'key1'
        val1 = 'val1'
        key2 = 'key2'
        val2 = 'val2'
               
        od[key1] = val1
        od[key2] = val2     
        
        od.to_front(key1)
        del od[key1]
        
        it = iter(od)
        for dummy in range(len(od)):
            it.next()
            
        self.assertRaises(StopIteration, it.next)
               
    def test_add_del(self):
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
     
          
    def test_order(self):
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
        
        od.to_end(key1)
        self.assertEqual(od.get_first(), val2)
        self.assertEquals(od.get_last(), val1) 

        od.to_front(key1)
        self.assertEqual(od.get_first(), val1)
        self.assertEquals(od.get_last(), val2) 
    
        od.clear()
        keys = [ 'key number %d' % i for i in range(10) ]
        vals = [ 'value number %d' % i for i in range(10) ] 
        for i in range(10):
            od[keys[i]] = vals[i]
        self.assertEquals(list(od), list(reversed(keys))) 
        
        
def suite():
    return unittest.makeSuite(OrderedDictTests)


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()