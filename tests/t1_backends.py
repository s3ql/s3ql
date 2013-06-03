'''
t1_backends.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from s3ql.backends import local, s3, gs, s3c, swift
from s3ql.backends.common import (ChecksumError, ObjectNotEncrypted, NoSuchObject,
    BetterBackend, get_ssl_context, AuthenticationError, AuthorizationError,
    DanglingStorageURLError, MalformedObjectError)
from argparse import Namespace
import configparser
import os
import stat
import tempfile
import time
import unittest

RETRY_DELAY = 1

class BackendTestsMixin(object):

    def newname(self):
        self.name_cnt += 1
        # Include special characters
        return "s3ql_=/_%d" % self.name_cnt

    def newvalue(self):
        return self.newname().encode()

    def retry(self, fn, *exceptions):
        tries = 0
        while True:
            try:
                return fn()
            except Exception as exc:
                if not isinstance(exc, exceptions):
                    raise
                if tries >= self.retries:
                    raise
                tries += 1
                time.sleep(RETRY_DELAY)
            else:
                break
        
    def test_write(self):
        key = self.newname()
        value = self.newvalue()
        metadata = { 'jimmy': 'jups@42' }

        self.assertRaises(NoSuchObject, self.backend.lookup, key)
        self.assertRaises(NoSuchObject, self.backend.fetch, key)

        with self.backend.open_write(key, metadata) as fh:
            fh.write(value)

        def read():
            with self.backend.open_read(key) as fh:
                return (fh.read(), fh.metadata)
        (value2, metadata2) = self.retry(read, NoSuchObject)

        self.assertEqual(value, value2)
        self.assertEqual(metadata, metadata2)
        self.assertEqual(self.backend[key], value)
        self.assertEqual(self.backend.lookup(key), metadata)

    def test_setitem(self):
        key = self.newname()
        value = self.newvalue()
        metadata = { 'jimmy': 'jups@42' }

        self.assertRaises(NoSuchObject, self.backend.lookup, key)
        self.assertRaises(NoSuchObject, self.backend.__getitem__, key)

        with self.backend.open_write(key, metadata) as fh:
            fh.write(self.newvalue())

        def read():
            with self.backend.open_read(key) as fh:
                return fh.read()
        self.retry(read, NoSuchObject)

        self.backend[key] = value

        def reread():
            with self.backend.open_read(key) as fh:
                value2 = fh.read()

            self.assertEqual(value, value2)
            self.assertEqual(fh.metadata, None)
            self.assertEqual(self.backend.lookup(key), None)
        self.retry(reread, NoSuchObject, AssertionError)

    def test_contains(self):
        key = self.newname()
        value = self.newvalue()

        self.assertFalse(key in self.backend)
        self.backend[key] = value

        self.retry(lambda: self.assertTrue(key in self.backend),
                   AssertionError)

    def test_delete(self):
        key = self.newname()
        value = self.newvalue()

        self.backend[key] = value

        self.retry(lambda: self.assertTrue(key in self.backend),
                   AssertionError)

        del self.backend[key]
        
        self.retry(lambda: self.assertFalse(key in self.backend),
                   AssertionError)


    def test_clear(self):
        key1 = self.newname()
        key2 = self.newname()
        self.backend[key1] = self.newvalue()
        self.backend[key2] = self.newvalue()

        def check1():
            self.assertTrue(key1 in self.backend)
            self.assertTrue(key2 in self.backend)
            self.assertEqual(len(list(self.backend)), 2)
        self.retry(check1, AssertionError)
        
        self.backend.clear()

        def check2():
            self.assertTrue(key1 not in self.backend)
            self.assertTrue(key2 not in self.backend)
            self.assertEqual(len(list(self.backend)), 0)
        self.retry(check2, AssertionError)


    def test_list(self):

        keys = [ self.newname() for dummy in range(12) ]
        values = [ self.newvalue() for dummy in range(12) ]
        for i in range(12):
            self.backend[keys[i]] = values[i]

        self.retry(lambda: self.assertEqual(sorted(self.backend.list()), sorted(keys)),
                   AssertionError)

    def test_copy(self):

        key1 = self.newname()
        key2 = self.newname()
        value = self.newvalue()
        self.assertRaises(NoSuchObject, self.backend.lookup, key1)
        self.assertRaises(NoSuchObject, self.backend.lookup, key2)

        self.backend.store(key1, value)

        self.retry(lambda: self.assertTrue(key1 in self.backend),
                   AssertionError)
        self.retry(lambda: self.assertEqual(self.backend[key1], value),
                   NoSuchObject)
        
        self.backend.copy(key1, key2)

        self.retry(lambda: self.assertTrue(key2 in self.backend),
                   AssertionError)
        self.retry(lambda: self.assertEqual(self.backend[key2], value),
                   NoSuchObject)

    def test_rename(self):

        key1 = self.newname()
        key2 = self.newname()
        value = self.newvalue()
        self.assertRaises(NoSuchObject, self.backend.lookup, key1)
        self.assertRaises(NoSuchObject, self.backend.lookup, key2)

        self.backend.store(key1, value)

        self.retry(lambda: self.assertTrue(key1 in self.backend),
                   AssertionError)
        self.retry(lambda: self.assertEqual(self.backend[key1], value),
                   NoSuchObject)
        
        self.backend.rename(key1, key2)

        def check2():
            self.assertTrue(key2 in self.backend)
            self.assertTrue(key1 not in self.backend)
            self.assertRaises(NoSuchObject, self.backend.lookup, key1)
        self.retry(check2, AssertionError, NoSuchObject)
        self.retry(lambda: self.assertEqual(self.backend[key2], value),
                   NoSuchObject)

def get_remote_test_info(name, skipTest):
        authfile = os.path.expanduser('~/.s3ql/authinfo2')
        if not os.path.exists(authfile):
            skipTest('No authentication file found.')

        mode = os.stat(authfile).st_mode
        if mode & (stat.S_IRGRP | stat.S_IROTH):
            skipTest("Authentication file has insecure permissions")

        config = configparser.ConfigParser()
        config.read(authfile)

        try:
            fs_name = config.get(name, 'test-fs')
            backend_login = config.get(name, 'backend-login')
            backend_password = config.get(name, 'backend-password')
        except (configparser.NoOptionError, configparser.NoSectionError):
            skipTest("Authentication file does not have test section")

        # Append prefix to make sure that we're starting with an empty bucket
        fs_name += '-s3ql_test_%d' % time.time()

        return (backend_login, backend_password, fs_name)


class S3Tests(BackendTestsMixin, unittest.TestCase):
        
    def setUp(self):
        self.name_cnt = 0
        self.retries = 45

        self.setUp2(s3.Backend, 's3-test')

    def setUp2(self, backend_class, backend_fs_name):
        
        options = Namespace()
        options.no_ssl = False
        options.ssl_ca_path = None

        (backend_login, backend_password,
         fs_name) = get_remote_test_info(backend_fs_name, self.skipTest)
        
        self.backend = backend_class(fs_name, backend_login, backend_password,
                                     ssl_context=get_ssl_context(options))

        try:
            self.backend.fetch('empty_object')
        except DanglingStorageURLError:
            raise unittest.SkipTest('Bucket %s does not exist' % fs_name) from None
        except AuthorizationError:
            raise unittest.SkipTest('No permission to access %s' % fs_name) from None
        except AuthenticationError:
            raise unittest.SkipTest('Unable to access %s, invalid credentials or skewed '
                                    'system clock?' % fs_name) from None
        except NoSuchObject:
            pass
        else:
            raise unittest.SkipTest('Test bucket not empty') from None

    def tearDown(self):
        self.backend.clear()


class SwiftTests(S3Tests):
    def setUp(self):
        self.name_cnt = 0
        self.retries = 90

        self.setUp2(swift.Backend, 'swift-test')


class GSTests(S3Tests):
    def setUp(self):
        self.name_cnt = 0
        self.retries = 90

        self.setUp2(gs.Backend, 'gs-test')


class S3CTests(S3Tests):
    def setUp(self):
        self.name_cnt = 0
        self.retries = 90

        self.setUp2(s3c.Backend, 's3c-test')


class URLTests(unittest.TestCase):
    
    # access to protected members
    #pylint: disable=W0212
     
    def test_s3(self):
        self.assertEqual(s3.Backend._parse_storage_url('s3://name', ssl_context=None)[2:],
                          ('name', ''))
        self.assertEqual(s3.Backend._parse_storage_url('s3://name/', ssl_context=None)[2:],
                          ('name', ''))
        self.assertEqual(s3.Backend._parse_storage_url('s3://name/pref/', ssl_context=None)[2:],
                          ('name', 'pref/'))
        self.assertEqual(s3.Backend._parse_storage_url('s3://name//pref/', ssl_context=None)[2:],
                          ('name', '/pref/'))

    def test_gs(self):
        self.assertEqual(gs.Backend._parse_storage_url('gs://name', ssl_context=None)[2:],
                          ('name', ''))
        self.assertEqual(gs.Backend._parse_storage_url('gs://name/', ssl_context=None)[2:],
                          ('name', ''))
        self.assertEqual(gs.Backend._parse_storage_url('gs://name/pref/', ssl_context=None)[2:],
                          ('name', 'pref/'))
        self.assertEqual(gs.Backend._parse_storage_url('gs://name//pref/', ssl_context=None)[2:],
                          ('name', '/pref/'))
                        
    def test_s3c(self):
        self.assertEqual(s3c.Backend._parse_storage_url('s3c://host.org/name', ssl_context=None),
                          ('host.org', 80, 'name', ''))
        self.assertEqual(s3c.Backend._parse_storage_url('s3c://host.org:23/name', ssl_context=None),
                          ('host.org', 23, 'name', ''))
        self.assertEqual(s3c.Backend._parse_storage_url('s3c://host.org/name/', ssl_context=None),
                          ('host.org', 80, 'name', ''))
        self.assertEqual(s3c.Backend._parse_storage_url('s3c://host.org/name/pref', ssl_context=None),
                          ('host.org', 80, 'name', 'pref'))
        self.assertEqual(s3c.Backend._parse_storage_url('s3c://host.org:17/name/pref/', ssl_context=None),
                          ('host.org', 17, 'name', 'pref/'))

class LocalTests(BackendTestsMixin, unittest.TestCase):

    def setUp(self):
        self.name_cnt = 0
        self.backend_dir = tempfile.mkdtemp(prefix='s3ql-backend-')
        self.backend = local.Backend('local://' + self.backend_dir, None, None)
        self.retries = 0

    def tearDown(self):
        self.backend.clear()
        os.rmdir(self.backend_dir)

class CompressionTests(BackendTestsMixin, unittest.TestCase):

    def setUp(self):
        self.name_cnt = 0
        self.backend_dir = tempfile.mkdtemp(prefix='s3ql-backend-')
        self.plain_backend = local.Backend('local://' + self.backend_dir, None, None)
        self.backend = self._wrap_backend()
        self.retries = 0

    def _wrap_backend(self):
        return BetterBackend(None, 'zlib', self.plain_backend)

    def tearDown(self):
        self.backend.clear()
        os.rmdir(self.backend_dir)

class EncryptionTests(CompressionTests):

    def _wrap_backend(self):
        return BetterBackend(b'schluz', None, self.plain_backend)

    def test_encryption(self):

        self.plain_backend['plain'] = b'foobar452'
        self.backend.store('encrypted', b'testdata', { 'tag': True })
        
        def check1():
            self.assertEqual(self.backend['encrypted'], b'testdata')
            self.assertNotEqual(self.plain_backend['encrypted'], b'testdata')
            self.assertRaises(MalformedObjectError, self.backend.fetch, 'plain')
            self.assertRaises(MalformedObjectError, self.backend.lookup, 'plain')
        self.retry(check1, NoSuchObject)

        self.backend.passphrase = None
        self.backend.store('not-encrypted', b'testdata2395', { 'tag': False })
        def check2():
            self.assertRaises(ChecksumError, self.backend.fetch, 'encrypted')
            self.assertRaises(ChecksumError, self.backend.lookup, 'encrypted')
        self.retry(check2, NoSuchObject)


        self.backend.passphrase = b'jobzrul'
        def check3():
            self.assertRaises(ChecksumError, self.backend.fetch, 'encrypted')
            self.assertRaises(ChecksumError, self.backend.lookup, 'encrypted')
            self.assertRaises(ObjectNotEncrypted, self.backend.fetch, 'not-encrypted')
            self.assertRaises(ObjectNotEncrypted, self.backend.lookup, 'not-encrypted')
        self.retry(check3, NoSuchObject)

        
class EncryptionCompressionTests(EncryptionTests):

    def _wrap_backend(self):
        return BetterBackend(b'schlurz', 'zlib', self.plain_backend)


# Somehow important according to pyunit documentation
def suite():
    return unittest.makeSuite(LocalTests)


# Allow calling from command line
if __name__ == "__main__":
    unittest.main()
