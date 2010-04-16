'''
t2_attr_cache.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2010 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function


from s3ql import attr_cache
from s3ql import mkfs
from s3ql.database import ConnectionManager
from _common import TestCase
import unittest2 as unittest
import tempfile
import time


class attr_cache_tests(TestCase):

    def setUp(self):
        self.dbfile = tempfile.NamedTemporaryFile()
        self.dbcm = ConnectionManager(self.dbfile.name)
        with self.dbcm() as conn:
            mkfs.setup_db(conn, 1024)

        self.cache = attr_cache.AttrCache(self.dbcm)

    def tearDown(self):
        self.cache.flush()

    def test_create(self):
        attr1 = {'st_mode': 784,
                'refcount': 3,
                'nlink_off': 1,
                'st_uid': 7,
                'st_gid': 2,
                'st_size': 34674,
                'st_rdev': 11,
                'st_atime': time.time(),
                'st_mtime': time.time() }

        attr2 = self.cache.create_inode(**attr1)

        for key in attr1.keys():
            self.assertEqual(attr1[key], attr2[key])

        self.assertTrue(self.dbcm.has_val('SELECT 1 FROM inodes WHERE id=?',
                                          (attr2['st_ino'],)))


    def test_del(self):
        attr1 = {'st_mode': 784,
                'refcount': 3,
                'nlink_off': 1,
                'st_uid': 7,
                'st_gid': 2,
                'st_size': 34674,
                'st_rdev': 11,
                'st_atime': time.time(),
                'st_mtime': time.time() }
        inode = self.cache.create_inode(**attr1)['st_ino']
        del self.cache[inode]
        self.assertFalse(self.dbcm.has_val('SELECT 1 FROM inodes WHERE id=?', (inode,)))
        self.assertRaises(KeyError, self.cache.__delitem__, inode)

    def test_get(self):
        attr1 = {'st_mode': 784,
                'refcount': 3,
                'nlink_off': 1,
                'st_uid': 7,
                'st_gid': 2,
                'st_size': 34674,
                'st_rdev': 11,
                'st_atime': time.time(),
                'st_mtime': time.time() }
        attr2 = self.cache.create_inode(**attr1)
        self.assertDictEqual(attr2, self.cache[attr2['st_ino']])

        self.dbcm.execute('DELETE FROM inodes WHERE id=?', (attr2['st_ino'],))
        # Entry should still be in cache
        self.assertDictEqual(attr2, self.cache[attr2['st_ino']])

        # Now it should be out of the cache
        for _ in xrange(attr_cache.CACHE_SIZE + 1):
            inode = self.cache.create_inode(**attr1)['st_ino']
            dummy = self.cache[inode]

        self.assertRaises(KeyError, self.cache.__getitem__, attr2['st_ino'])



def suite():
    return unittest.makeSuite(attr_cache_tests)

if __name__ == "__main__":
    unittest.main()
