'''
$Id$

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function 

from s3ql import mkfs, s3, s3cache
from s3ql.common import EmbeddedException, ExceptionStoringThread, sha256
from s3ql.database import ConnectionManager
from llfuse import FUSEError
import os
import tempfile
from _common import TestCase
import unittest
import stat
from time import time, sleep
from contextlib import contextmanager

#from s3ql.common import init_logging
#init_logging(True, False, debug=[''])
        
class s3cache_tests(TestCase):
    
    def setUp(self):
        self.bucket =  s3.LocalConnection().create_bucket('foobar', 'brazl')

        self.dbfile = tempfile.NamedTemporaryFile()
        self.cachedir = tempfile.mkdtemp() + "/"
        self.blocksize = 1024
        self.cachesize = int(1.5 * self.blocksize) 

        self.dbcm = ConnectionManager(self.dbfile.name)
        with self.dbcm() as conn:
            mkfs.setup_db(conn, self.blocksize)
        
        # Create an inode we can work with
        self.inode = 42
        self.dbcm.execute("INSERT INTO inodes (id, mode,uid,gid,mtime,atime,ctime,refcount, size) "
                   "VALUES (?,?,?,?,?,?,?,?,?)", 
                   (self.inode, stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR
                   | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH,
                    os.getuid(), os.getgid(), time(), time(), time(), 1, 32))
        
        self.cache = s3cache.S3Cache(self.bucket, self.cachedir, self.cachesize, self.dbcm)

    def tearDown(self):
        self.cache.clear()
        self.dbfile.close()
        os.rmdir(self.cachedir)
    
    @staticmethod   
    def random_data(len_):
        fd = open("/dev/urandom", "rb")
        return fd.read(len_)
      
    def test_01_create_read(self):
        inode = self.inode
        blockno = 11
        data = self.random_data(self.blocksize)
        
        obj_count = bucket_len(self.bucket)
        
        # Write
        with self.cache.get(inode, blockno) as fh:
            fh.seek(0)
            fh.write(data)
        
        # Should only be in cache now
        self.assertEquals(bucket_len(self.bucket), obj_count)
        
        # Read cached
        with self.cache.get(inode, blockno) as fh:
            el = fh
            fh.seek(0)
            self.assertEqual(data, fh.read(len(data)))
            
        # Flush
        self.cache.flush(inode)
        
        # Should be committed now
        sleep(s3.LOCAL_PROP_DELAY*1.1)
        self.assertEqual(bucket_len(self.bucket), obj_count+1)

        s3key = el.s3key
        
        # Even if we change in S3, we should get the cached data
        data2 = self.random_data(241)
        self.bucket.store('s3ql_data_%d' % s3key, data2, { 'hash': sha256(data2) })
        with self.cache.get(inode, blockno) as fh:
            fh.seek(0)
            self.assertEqual(data, fh.read(len(data)))
            
        # Now we remove this element from the cache
        self.cache._expire_parallel()
            
        # This should not upload any data, so now we read the new key
        # and get a hash mismatch
        sleep(s3.LOCAL_PROP_DELAY*1.1)
        self.cache.timeout = 1
        self.cache.expect_mismatch = True
        cm = self.cache.get(inode, blockno)
        
        try:
            self.assertRaises(FUSEError, cm.__enter__)
        finally:
            cm.__exit__(None, None, None)
            
        # Remove scrap
        os.unlink(self.cachedir + str(s3key))
         
    def test_03_access_locking(self):    
        # Make sure the threads actually conflict
        assert s3.LOCAL_TX_DELAY > 0
             
        # Test concurrent writes 
        flag = { 'writing': False }
        blockno = 102
        
        # Access the same file in two threads
        def access():
            with self.cache.get(self.inode, blockno):
                if flag['writing']:
                    raise s3.ConcurrencyError
                flag['writing'] = True
                sleep(s3.LOCAL_TX_DELAY)
                flag['writing'] = False
        
        # This should work nicely
        t1 = ExceptionStoringThread(target=access)
        t2 = ExceptionStoringThread(target=access)      
        t1.start()
        sleep(s3.LOCAL_TX_DELAY/2)
        t2.start()  
        t1.join_and_raise()
        t2.join_and_raise()
        
        # After we Monkeypatch the locking away, we except and exception
        self.cache.mlock = DummyLock()
        
        t1 = ExceptionStoringThread(target=access)
        t2 = ExceptionStoringThread(target=access)      
        t1.start()
        sleep(s3.LOCAL_TX_DELAY/2)
        t2.start()  
        
        t1.join_and_raise()
        self.assertRaises(EmbeddedException, t2.join_and_raise)
        self.assertTrue(isinstance(t2.exc, s3.ConcurrencyError))        
   
    def test_03_flush_locking(self):      
        blockno = 102
        
        # Make sure the threads actually conflict
        assert s3.LOCAL_TX_DELAY > 0

        def flush():
            self.cache.flush(self.inode)
            
        # Create object
        with self.cache.get(self.inode, blockno) as fh:
            fh.write(b'data')  

        # This should work nicely
        t1 = ExceptionStoringThread(flush)
        t1.start()
        sleep(s3.LOCAL_TX_DELAY/2)
        flush()
        t1.join_and_raise()
        
        # After we Monkeypatch the locking away, we expect an exception
        self.cache.mlock = DummyLock()
        
        with self.cache.get(self.inode, blockno) as fh:
            fh.write(b'data')
            
        t1 = ExceptionStoringThread(flush)
        t1.start()
        sleep(s3.LOCAL_TX_DELAY/2)
        self.assertRaises(s3.ConcurrencyError, flush)
        
        t1.join_and_raise()
                
    def test_03_expiry_locking(self):      
        blockno = 102
        
        # Make sure the threads actually conflict
        assert s3.LOCAL_TX_DELAY > 0
            
        # Create object
        with self.cache.get(self.inode, blockno) as fh:
            fh.write(b'data')  

        # This should work nicely
        t1 = ExceptionStoringThread(self.cache._expire_parallel)
        t1.start()
        sleep(s3.LOCAL_TX_DELAY/2)
        self.cache._expire_parallel()
        t1.join_and_raise()
        
        # After we Monkeypatch the locking away, we expect an exception
        self.cache.mlock = DummyLock()
        
        with self.cache.get(self.inode, blockno) as fh:
            fh.write(b'data')
            el = fh
            
        t1 = ExceptionStoringThread(self.cache._expire_parallel)
        t1.start()
        sleep(s3.LOCAL_TX_DELAY/2)
        # Sneak the element back into the cache
        self.cache.cache[(self.inode, blockno)] = el
        try:
            self.cache._expire_parallel()
        except EmbeddedException as exc:
            self.assertTrue(isinstance(exc.exc, s3.ConcurrencyError))     
        else:
            self.fail('Did not raise ConcurrencyError()')   
        
        
        t1.join_and_raise()
        
    def test_expiry(self):
        '''Check if we do not expire more or less than necessary
        '''
        
        # expire will try to free at least 1 MB, so we make each
        # object 1 MB big
        datalen = 1024*1024
        no = 4
        self.cache.maxsize =  no * datalen
        
        for i in range(no):
            with self.cache.get(self.inode, i) as fh: 
                fh.write(self.random_data(datalen))
            
        self.assertEquals(len(self.cache.cache), no)
        
        self.cache.maxsize = (no-1) * datalen
        
        self.cache.expire()
        
        self.assertEquals(len(self.cache.cache), no-1)
        
    
    def test_04_deduplication(self):
        inode = self.inode
        blockno1 = 21
        blockno2 = 25
        
        data1 = self.random_data(325)
        data2 = self.random_data(326)
        
        cnt = bucket_len(self.bucket)
        
        with self.cache.get(inode, blockno1) as fh:
            fh.seek(0)
            fh.write(data1)
            el1 = fh 
        
        with self.cache.get(inode, blockno2) as fh:
            fh.seek(0)
            fh.write(data1)
            el2 = fh         
        
        self.cache.flush(inode)
        
        self.assertEquals(el1.s3key, el2.s3key)
        sleep(s3.LOCAL_PROP_DELAY*1.1)
        self.assertEquals(bucket_len(self.bucket), cnt+1)
        orig_s3key = el1.s3key
        
        with self.cache.get(inode, blockno2) as fh:
            fh.seek(0)
            fh.write(data2)
            el2 = fh
            
        self.cache.flush(inode)
        self.assertNotEquals(orig_s3key, el2.s3key)
        sleep(s3.LOCAL_PROP_DELAY*1.1)
        self.assertEquals(bucket_len(self.bucket), cnt+2)
        
        with self.cache.get(inode, blockno1) as fh:
            fh.seek(0)
            fh.write(data2)
            el1 = fh
            
        self.cache.flush(inode)
        self.assertEquals(el1.s3key, el2.s3key)
        self.assertNotEqual(orig_s3key, el1.s3key)
        sleep(s3.LOCAL_PROP_DELAY*1.1)
        self.assertEquals(bucket_len(self.bucket), cnt+1)
        
    
class DummyLock(object):
    """Dummy MultiLock class doing nothing
    
    This class pretends to be a MultiLock, but it actually does not do 
    anything at all.
    """
    
    @contextmanager
    def __call__(self, *_):
        # pylint: disable-msg=R0201
        # Yeah, this could be a function / static method.
        yield
            
    def acquire(self, *_):
        pass
        
    def release(self, *_):
        pass

def bucket_len(bucket):
    return len(list(bucket.keys()))
            
def suite():
    return unittest.makeSuite(s3cache_tests)

if __name__ == "__main__":
    unittest.main()
