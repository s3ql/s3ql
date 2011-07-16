'''
t2_inode_cache.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2010 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import division, print_function


from s3ql import inode_cache
from s3ql.common import create_tables, init_tables
from s3ql.database import Connection
from _common import TestCase
import unittest2 as unittest
import time
import tempfile

class cache_tests(TestCase):

    def setUp(self):
        self.dbfile = tempfile.NamedTemporaryFile()
        self.db = Connection(self.dbfile.name)
        create_tables(self.db)
        init_tables(self.db)
        self.cache = inode_cache.InodeCache(self.db)

    def tearDown(self):
        self.cache.destroy()

    def test_create(self):
        attrs = {'mode': 784,
                 'refcount': 3,
                 'uid': 7,
                 'gid': 2,
                 'size': 34674,
                 'target': 'foobar',
                 'rdev': 11,
                 'atime': time.time(),
                 'ctime': time.time(),
                 'mtime': time.time() }

        inode = self.cache.create_inode(**attrs)

        for key in attrs.keys():
            self.assertEqual(attrs[key], getattr(inode, key))

        self.assertTrue(self.db.has_val('SELECT 1 FROM inodes WHERE id=?',
                                          (inode.id,)))


    def test_del(self):
        attrs = {'mode': 784,
                'refcount': 3,
                'uid': 7,
                'target': 'foobar',
                'gid': 2,
                'size': 34674,
                'rdev': 11,
                'atime': time.time(),
                'ctime': time.time(),
                'mtime': time.time() }
        inode = self.cache.create_inode(**attrs)
        del self.cache[inode.id]
        self.assertFalse(self.db.has_val('SELECT 1 FROM inodes WHERE id=?', (inode.id,)))
        self.assertRaises(KeyError, self.cache.__delitem__, inode.id)

    def test_get(self):
        attrs = {'mode': 784,
                'refcount': 3,
                'uid': 7,
                'gid': 2,
                'target': 'foobar',
                'size': 34674,
                'rdev': 11,
                'atime': time.time(),
                'ctime': time.time(),
                'mtime': time.time() }
        inode = self.cache.create_inode(**attrs)
        self.assertEqual(inode, self.cache[inode.id])

        self.db.execute('DELETE FROM inodes WHERE id=?', (inode.id,))
        # Entry should still be in cache
        self.assertEqual(inode, self.cache[inode.id])

        # Now it should be out of the cache
        for _ in xrange(inode_cache.CACHE_SIZE + 1):
            dummy = self.cache[self.cache.create_inode(**attrs).id]

        self.assertRaises(KeyError, self.cache.__getitem__, inode.id)



def suite():
    return unittest.makeSuite(cache_tests)

if __name__ == "__main__":
    unittest.main()
