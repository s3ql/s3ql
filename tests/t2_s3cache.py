#!/usr/bin/env python
#
#    Copyright (C) 2008  Nikolaus Rath <Nikolaus@rath.org>
#
#    This program can be distributed under the terms of the GNU LGPL.
#

from random import randrange
from s3ql import mkfs, s3, s3cache, fs
from s3ql.common import MyCursor
import apsw
import os
import tempfile
import unittest
import stat
from time import time 

# For debug messages:
#from s3ql.common import init_logging
#init_logging(True, False, debug=[''])

class s3cache_tests(unittest.TestCase):

    def setUp(self):
        self.bucket = s3.LocalBucket()
        self.bucket.tx_delay = 0
        self.bucket.prop_delay = 0

        self.dbfile = tempfile.NamedTemporaryFile()
        self.cachedir = tempfile.mkdtemp() + "/"
        self.blocksize = 1024
        self.cachesize = int(1.5 * self.blocksize)

        mkfs.setup_db(self.dbfile.name, self.blocksize)
        mkfs.setup_bucket(self.bucket, self.dbfile.name)

        self.cur = MyCursor(apsw.Connection(self.dbfile.name).cursor())
        
        # Create an inode we can work with
        self.inode = 42
        self.cur.execute("INSERT INTO inodes (id, mode,uid,gid,mtime,atime,ctime,refcount, size) "
                   "VALUES (?,?,?,?,?,?,?,?,?)", 
                   (self.inode, stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR
                   | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH,
                    os.getuid(), os.getgid(), time(), time(), time(), 1, 32))
        
        self.cache = s3cache.S3Cache(self.bucket, self.cachedir, self.cachesize,
                                     self.blocksize)

    def tearDown(self):
        # May not have been called if a test failed
        self.cache.close(self.cur)
        self.dbfile.close()
        os.rmdir(self.cachedir)

    @staticmethod
    def random_name(prefix=""):
        return "s3ql" + prefix + str(randrange(100, 999, 1))
    
    @staticmethod   
    def random_data(len_):
        fd = open("/dev/urandom", "rb")
        return fd.read(len_)
      
    def test_01_create_read(self):
        inode = self.inode
        offset = 11
        data = self.random_data(self.blocksize)
        
        # This needs to be kept in sync with S3Cache
        s3key = "s3ql_data_%d_%d" % (inode, offset)
        
        # Write
        with self.cache.get(inode, offset, self.cur, markdirty=True) as fh:
            fh.seek(0)
            fh.write(data)
        
        # Should only be in cache now
        self.assertTrue(s3key not in self.bucket.keys())
        
        # Read cached
        with self.cache.get(inode, offset, self.cur) as fh:
            fh.seek(0)
            self.assertEqual(data, fh.read(len(data)))
            
        # Flush
        self.cache.flush(inode, self.cur)
        
        # Should be committed now
        self.assertTrue(s3key in self.bucket.keys())
        
        # Even if we change in S3, we should get the cached data
        data2 = self.random_data(241)
        self.bucket[s3key] = data2
        with self.cache.get(inode, offset, self.cur) as fh:
            fh.seek(0)
            self.assertEqual(data, fh.read(len(data)))
            
        # This should not upload any data, so now we read the new key
        # and get an etag mismatch
        self.cache.close(self.cur)
        self.cache.timeout = 1
        cm = self.cache.get(inode, offset, self.cur)
        self.assertRaises(fs.FUSEError, cm.__enter__)
            
            
    # TODO: Check that s3 object locking works when retrieving

    # TODO: Check that s3 object locking works when creating

    # TODO: Check that s3 objects are committed after fsync


def suite():
    return unittest.makeSuite(s3cache_tests)

if __name__ == "__main__":
    unittest.main()
