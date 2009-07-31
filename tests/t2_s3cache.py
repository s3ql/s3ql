#!/usr/bin/env python
#
#    Copyright (C) 2008  Nikolaus Rath <Nikolaus@rath.org>
#
#    This program can be distributed under the terms of the GNU LGPL.
#

from s3ql import mkfs, s3, s3cache, fs
from s3ql.common import EmbeddedException, ExceptionStoringThread
from s3ql.cursor_manager import CursorManager
import os
import tempfile
import unittest
import stat
from time import time, sleep
from contextlib import contextmanager

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

        self.cur = CursorManager(self.dbfile.name)
        mkfs.setup_db(self.cur, self.blocksize)
        
        # Create an inode we can work with
        self.inode = 42
        self.cur.execute("INSERT INTO inodes (id, mode,uid,gid,mtime,atime,ctime,refcount, size) "
                   "VALUES (?,?,?,?,?,?,?,?,?)", 
                   (self.inode, stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR
                   | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH,
                    os.getuid(), os.getgid(), time(), time(), time(), 1, 32))
        
        self.cache = s3cache.S3Cache(self.bucket, self.cachedir, self.cachesize,
                                     self.blocksize, self.cur)

    def tearDown(self):
        # May not have been called if a test failed
        self.cache.close()
        self.dbfile.close()
        os.rmdir(self.cachedir)
    
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
        with self.cache.get(inode, offset) as fh:
            fh.seek(0)
            fh.write(data)
        
        # Should only be in cache now
        self.assertTrue(s3key not in self.bucket.keys())
        
        # Read cached
        with self.cache.get(inode, offset) as fh:
            fh.seek(0)
            self.assertEqual(data, fh.read(len(data)))
            
        # Flush
        self.cache.flush(inode)
        
        # Should be committed now
        self.assertTrue(s3key in self.bucket.keys())
        
        # Even if we change in S3, we should get the cached data
        data2 = self.random_data(241)
        self.bucket[s3key] = data2
        with self.cache.get(inode, offset) as fh:
            fh.seek(0)
            self.assertEqual(data, fh.read(len(data)))
            
        # This should not upload any data, so now we read the new key
        # and get an etag mismatch
        self.cache.close()
        self.cache.timeout = 1
        self.cache.expect_mismatch = True
        cm = self.cache.get(inode, offset)
        self.assertRaises(fs.FUSEError, cm.__enter__)
            
    def test_02_locking_meta(self):
        # Test our threading object
        def works():
            pass
        
        def fails():
            raise RuntimeError()
        
        t1 = ExceptionStoringThread(target=works)
        t2 = ExceptionStoringThread(target=fails)      
        t1.start()
        t2.start()  
        
        t1.join_and_raise()
        self.assertRaises(EmbeddedException, t2.join_and_raise)
        
         
    def test_03_access_locking(self):      
        # Test concurrent writes 
        flag = { 'writing': False }
        offset = 102
        
        # Access the same file in two threads
        def access():
            with self.cache.get(self.inode, offset):
                if flag['writing']:
                    raise s3.ConcurrencyError
                flag['writing'] = True
                sleep(1)
                flag['writing'] = False
        
        # This should work nicely
        t1 = ExceptionStoringThread(target=access)
        t2 = ExceptionStoringThread(target=access)      
        t1.start()
        t2.start()  
        t1.join_and_raise()
        t2.join_and_raise()
        
        # After we Monkeypatch the locking away, we except and exception
        self.cache.s3_lock = DummyLock()
        
        t1 = ExceptionStoringThread(target=access)
        t2 = ExceptionStoringThread(target=access)      
        t1.start()
        sleep(0.5)
        t2.start()  
        
        t1.join_and_raise()
        self.assertRaises(EmbeddedException, t2.join_and_raise)
        self.assertTrue(isinstance(t2.exc, s3.ConcurrencyError))        
   
        
    def test_03_expiry_locking(self):      
        offset = 102
        s3key = "s3ql_data_%d_%d" % (self.inode, offset)
        
        # Make sure the threads actually conflict
        self.bucket.tx_delay = 1
        
        # Enfore cache expiration on each expire() call
        self.cache.maxsize = 0
        
        # Access the same file in two threads
        def access():            
            # Make sure the object is dirty
            with self.cache.get(self.inode, offset) as fh:
                fh.write(b'data')
            self.cache.expire()        
        
        # This should work nicely
        t1 = ExceptionStoringThread(target=access)
        t2 = ExceptionStoringThread(target=access)      
        t1.start()
        t2.start()  
        t1.join_and_raise()
        t2.join_and_raise()
        
        # Make sure the cache has actually been flushed
        self.assertTrue(s3key in self.bucket.keys())
        
        # After we Monkeypatch the locking away, we except and exception
        self.cache.s3_lock = DummyLock()
        
        t1 = ExceptionStoringThread(target=access)
        t2 = ExceptionStoringThread(target=access)      
        t1.start()
        sleep(0.5)
        t2.start()  
        
        t1.join_and_raise()
        self.assertRaises(EmbeddedException, t2.join_and_raise)
        self.assertTrue(isinstance(t2.exc, s3.ConcurrencyError))

class DummyLock(object):
    """Dummy MultiLock class doing nothing
    
    This class pretends to be a MultiLock, but it actually does not do 
    anything at all.
    """
    
    @contextmanager
    def __call__(self, _):
        # pylint: disable-msg=R0201
        # Yeah, this could be a function.
        yield
            
    def acquire(self, _):
        pass
        
    def release(self, _):
        pass

        
def suite():
    return unittest.makeSuite(s3cache_tests)

if __name__ == "__main__":
    unittest.main()
