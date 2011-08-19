'''
t1_backends.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import division, print_function

import unittest2 as unittest
from s3ql.backends import local, s3
from s3ql.backends.common import ChecksumError, ObjectNotEncrypted, NoSuchObject,\
    BetterBucket
import tempfile
import os
import time
from _common import TestCase
import _common
from random import randrange

try:
    import boto
except ImportError:
    boto = None
     
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
        self.bucket[self.newname()] = self.newname()
        self.bucket[self.newname()] = self.newname()

        time.sleep(self.delay)
        self.assertEquals(len(list(self.bucket)), 2)
        self.bucket.clear()
        time.sleep(self.delay)
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
@unittest.skipUnless(_common.aws_credentials, 'no AWS credentials available')
@unittest.skipUnless(boto, 'python boto module not installed')
class S3Tests(BackendTestsMixin, TestCase):
    @staticmethod
    def random_name(prefix=""):
        return "s3ql-" + prefix + str(randrange(1000, 9999, 1))

    def setUp(self):
        self.name_cnt = 0
        self.boto = boto.connect_s3(*_common.aws_credentials)

        tries = 0
        while tries < 10:
            self.bucketname = self.random_name()
            try:
                self.boto.get_bucket(self.bucketname)
            except boto.exception.S3ResponseError as e:
                if e.status == 404:
                    break
                raise
            tries += 1
        else:
            raise RuntimeError("Failed to find an unused bucket name.")
        
        # Create in EU for more consistency guarantees
        self.boto.create_bucket(self.bucketname, location='EU')

        # This is the time in which we expect S3 changes to propagate. It may
        # be much longer for larger objects, but for tests this is usually enough.
        self.delay = 8

        time.sleep(self.delay) # wait for bucket        
        self.bucket = s3.Bucket(aws_key_id=_common.aws_credentials[0],
                                aws_key=_common.aws_credentials[1],
                                bucket_name=self.bucketname,
                                prefix='')

    def tearDown(self):
        self.bucket.clear()
        time.sleep(self.delay)
        self.boto.delete_bucket(self.bucketname)
    
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
