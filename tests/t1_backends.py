'''
t1_backends.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import division, print_function
from _common import TestCase
from s3ql.backends import local, s3, s3s, gs, gss, s3c
from s3ql.backends.common import (ChecksumError, ObjectNotEncrypted, NoSuchObject, 
    BetterBucket)
import ConfigParser
import os
import stat
import tempfile
import time
import unittest2 as unittest
     
class BackendTestsMixin(object):
        
    def newname(self):
        self.name_cnt += 1
        # Include special characters
        return "s3ql_=/_%d" % self.name_cnt

    def test_write(self):
        key = self.newname()
        value = self.newname()
        metadata = { 'jimmy': 'jups@42' }

        self.assertRaises(NoSuchObject, self.bucket.lookup, key)
        self.assertRaises(NoSuchObject, self.bucket.fetch, key)
        
        with self.bucket.open_write(key, metadata) as fh:
            fh.write(value)
            
        time.sleep(self.delay)
        
        with self.bucket.open_read(key) as fh:
            value2 = fh.read()
            
        self.assertEquals(value, value2)
        self.assertEquals(metadata, fh.metadata)
        self.assertEquals(self.bucket[key], value)
        self.assertEquals(self.bucket.lookup(key), metadata)

    def test_setitem(self):
        key = self.newname()
        value = self.newname()
        metadata = { 'jimmy': 'jups@42' }

        self.assertRaises(NoSuchObject, self.bucket.lookup, key)
        self.assertRaises(NoSuchObject, self.bucket.__getitem__, key)
        
        with self.bucket.open_write(key, metadata) as fh:
            fh.write(self.newname()) 
        time.sleep(self.delay)
        self.bucket[key] = value
        time.sleep(self.delay)
        
        with self.bucket.open_read(key) as fh:
            value2 = fh.read()
            
        self.assertEquals(value, value2)
        self.assertEquals(fh.metadata, dict())
        self.assertEquals(self.bucket.lookup(key), dict())
        
    def test_contains(self):
        key = self.newname()
        value = self.newname()

        self.assertFalse(key in self.bucket)
        self.bucket[key] = value
        time.sleep(self.delay)
        self.assertTrue(key in self.bucket)

    def test_delete(self):
        key = self.newname()
        value = self.newname()
        self.bucket[key] = value
        time.sleep(self.delay)

        self.assertTrue(key in self.bucket)
        del self.bucket[key]
        time.sleep(self.delay)
        self.assertFalse(key in self.bucket)

    def test_clear(self):
        key1 = self.newname()
        key2 = self.newname()
        self.bucket[key1] = self.newname()
        self.bucket[key2] = self.newname()

        time.sleep(self.delay)
        self.assertEquals(len(list(self.bucket)), 2)
        self.bucket.clear()
        time.sleep(self.delay)
        self.assertTrue(key1 not in self.bucket)
        self.assertTrue(key2 not in self.bucket)        
        self.assertEquals(len(list(self.bucket)), 0)

    def test_list(self):

        keys = [ self.newname() for dummy in range(12) ]
        values = [ self.newname() for dummy in range(12) ]
        for i in range(12):
            self.bucket[keys[i]] = values[i]

        time.sleep(self.delay)
        self.assertEquals(sorted(self.bucket.list()), sorted(keys))

    def test_copy(self):

        key1 = self.newname()
        key2 = self.newname()
        value = self.newname()
        self.assertRaises(NoSuchObject, self.bucket.lookup, key1)
        self.assertRaises(NoSuchObject, self.bucket.lookup, key2)

        self.bucket.store(key1, value)
        time.sleep(self.delay)
        self.bucket.copy(key1, key2)

        time.sleep(self.delay)
        self.assertEquals(self.bucket[key2], value)

    def test_rename(self):

        key1 = self.newname()
        key2 = self.newname()
        value = self.newname()
        self.assertRaises(NoSuchObject, self.bucket.lookup, key1)
        self.assertRaises(NoSuchObject, self.bucket.lookup, key2)

        self.bucket.store(key1, value)
        time.sleep(self.delay)
        self.bucket.rename(key1, key2)

        time.sleep(self.delay)
        self.assertEquals(self.bucket[key2], value)
        self.assertRaises(NoSuchObject, self.bucket.lookup, key1)
        
# This test just takes too long (because we have to wait really long so that we don't
# get false errors due to propagation delays)
#@unittest.skip('takes too long')
class S3Tests(BackendTestsMixin, TestCase):
    def setUp(self):
        self.name_cnt = 0        
        # This is the time in which we expect S3 changes to propagate. It may
        # be much longer for larger objects, but for tests this is usually enough.
        self.delay = 15

        self.bucket = s3.Bucket(*self.get_credentials('s3-test'))

    def tearDown(self):
        self.bucket.clear()
        
    def get_credentials(self, name):
        
        authfile = os.path.expanduser('~/.s3ql/authinfo2')
        if not os.path.exists(authfile):
            self.skipTest('No authentication file found.')
            
        mode = os.stat(authfile).st_mode
        if mode & (stat.S_IRGRP | stat.S_IROTH):
            self.skipTest("Authentication file has insecure permissions")    
        
        config = ConfigParser.SafeConfigParser()
        config.read(authfile)
                
        try:
            bucket_name = config.get(name, 'test-bucket')
            backend_login = config.get(name, 'backend-login')
            backend_password = config.get(name, 'backend-password')
        except (ConfigParser.NoOptionError, ConfigParser.NoSectionError):
            self.skipTest("Authentication file does not have test section")  
    
        return (bucket_name, backend_login, backend_password)
    
    
class S3STests(S3Tests):
    def setUp(self):
        self.name_cnt = 0        
        # This is the time in which we expect S3 changes to propagate. It may
        # be much longer for larger objects, but for tests this is usually enough.
        self.delay = 15
       
        self.bucket = s3s.Bucket(*self.get_credentials('s3-test'))    

class GSTests(S3Tests):
    def setUp(self):
        self.name_cnt = 0        
        # This is the time in which we expect S3 changes to propagate. It may
        # be much longer for larger objects, but for tests this is usually enough.
        self.delay = 15
       
        self.bucket = gs.Bucket(*self.get_credentials('gs-test')) 
        
class GSSTests(S3Tests):
    def setUp(self):
        self.name_cnt = 0        
        # This is the time in which we expect S3 changes to propagate. It may
        # be much longer for larger objects, but for tests this is usually enough.
        self.delay = 15
        self.bucket = gss.Bucket(*self.get_credentials('gs-test')) 

class S3CTests(S3Tests):
    def setUp(self):
        self.name_cnt = 0        
        # This is the time in which we expect S3 changes to propagate. It may
        # be much longer for larger objects, but for tests this is usually enough.
        self.delay = 0
        self.bucket = s3c.Bucket(*self.get_credentials('s3c-test')) 
                                        
class LocalTests(BackendTestsMixin, TestCase):

    def setUp(self):
        self.name_cnt = 0
        self.bucket_dir = tempfile.mkdtemp()
        self.bucket = local.Bucket(self.bucket_dir, None, None)
        self.delay = 0
    
    def tearDown(self):
        self.bucket.clear()
        os.rmdir(self.bucket_dir)

class CompressionTests(BackendTestsMixin, TestCase):
      
    def setUp(self):
        self.name_cnt = 0        
        self.bucket_dir = tempfile.mkdtemp()
        self.plain_bucket = local.Bucket(self.bucket_dir, None, None)  
        self.bucket = self._wrap_bucket()
        self.delay = 0

    def _wrap_bucket(self):
        return BetterBucket(None, 'zlib', self.plain_bucket)
        
    def tearDown(self):
        self.bucket.clear()
        os.rmdir(self.bucket_dir)
        
class EncryptionTests(CompressionTests):

    def _wrap_bucket(self):
        return BetterBucket('schlurz', None, self.plain_bucket)
        
    def test_encryption(self):

        self.plain_bucket['plain'] = b'foobar452'
        self.bucket.store('encrypted', 'testdata', { 'tag': True })
        time.sleep(self.delay)
        self.assertEquals(self.bucket['encrypted'], b'testdata')
        self.assertNotEquals(self.plain_bucket['encrypted'], b'testdata')
        self.assertRaises(ObjectNotEncrypted, self.bucket.fetch, 'plain')
        self.assertRaises(ObjectNotEncrypted, self.bucket.lookup, 'plain')

        self.bucket.passphrase = None
        self.assertRaises(ChecksumError, self.bucket.fetch, 'encrypted')
        self.assertRaises(ChecksumError, self.bucket.lookup, 'encrypted')

        self.bucket.passphrase = 'jobzrul'
        self.assertRaises(ChecksumError, self.bucket.fetch, 'encrypted')
        self.assertRaises(ChecksumError, self.bucket.lookup, 'encrypted')


class EncryptionCompressionTests(EncryptionTests):

    def _wrap_bucket(self):
        return BetterBucket('schlurz', 'zlib', self.plain_bucket)
        
                
# Somehow important according to pyunit documentation
def suite():
    return unittest.makeSuite(LocalTests)


# Allow calling from command line
if __name__ == "__main__":
    unittest.main()
