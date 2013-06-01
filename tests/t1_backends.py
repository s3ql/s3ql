'''
t1_backends.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from s3ql.backends import local, s3, gs, s3c, swift
from s3ql.backends.common import (ChecksumError, ObjectNotEncrypted, NoSuchObject,
    BetterBackend, get_ssl_context)
from argparse import Namespace
import configparser
import os
import stat
import tempfile
import time
import unittest

class BackendTestsMixin(object):

    def newname(self):
        self.name_cnt += 1
        # Include special characters
        return "s3ql_=/_%d" % self.name_cnt

    def newvalue(self):
        return self.newname().encode()
    
    def test_write(self):
        key = self.newname()
        value = self.newvalue()
        metadata = { 'jimmy': 'jups@42' }

        self.assertRaises(NoSuchObject, self.backend.lookup, key)
        self.assertRaises(NoSuchObject, self.backend.fetch, key)

        with self.backend.open_write(key, metadata) as fh:
            fh.write(value)

        time.sleep(self.delay)

        with self.backend.open_read(key) as fh:
            value2 = fh.read()

        self.assertEqual(value, value2)
        self.assertEqual(metadata, fh.metadata)
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
        time.sleep(self.delay)
        self.backend[key] = value
        time.sleep(self.delay)

        with self.backend.open_read(key) as fh:
            value2 = fh.read()

        self.assertEqual(value, value2)
        self.assertEqual(fh.metadata, dict())
        self.assertEqual(self.backend.lookup(key), dict())

    def test_contains(self):
        key = self.newname()
        value = self.newvalue()

        self.assertFalse(key in self.backend)
        self.backend[key] = value
        time.sleep(self.delay)
        self.assertTrue(key in self.backend)

    def test_delete(self):
        key = self.newname()
        value = self.newvalue()
        self.backend[key] = value
        time.sleep(self.delay)

        self.assertTrue(key in self.backend)
        del self.backend[key]
        time.sleep(self.delay)
        self.assertFalse(key in self.backend)

    def test_clear(self):
        key1 = self.newname()
        key2 = self.newname()
        self.backend[key1] = self.newvalue()
        self.backend[key2] = self.newvalue()

        time.sleep(self.delay)
        self.assertEqual(len(list(self.backend)), 2)
        self.backend.clear()
        time.sleep(5*self.delay)
        self.assertTrue(key1 not in self.backend)
        self.assertTrue(key2 not in self.backend)
        self.assertEqual(len(list(self.backend)), 0)

    def test_list(self):

        keys = [ self.newname() for dummy in range(12) ]
        values = [ self.newvalue() for dummy in range(12) ]
        for i in range(12):
            self.backend[keys[i]] = values[i]

        time.sleep(self.delay)
        self.assertEqual(sorted(self.backend.list()), sorted(keys))

    def test_copy(self):

        key1 = self.newname()
        key2 = self.newname()
        value = self.newvalue()
        self.assertRaises(NoSuchObject, self.backend.lookup, key1)
        self.assertRaises(NoSuchObject, self.backend.lookup, key2)

        self.backend.store(key1, value)
        time.sleep(self.delay)
        self.backend.copy(key1, key2)

        time.sleep(self.delay)
        self.assertEqual(self.backend[key2], value)

    def test_rename(self):

        key1 = self.newname()
        key2 = self.newname()
        value = self.newvalue()
        self.assertRaises(NoSuchObject, self.backend.lookup, key1)
        self.assertRaises(NoSuchObject, self.backend.lookup, key2)

        self.backend.store(key1, value)
        time.sleep(self.delay)
        self.backend.rename(key1, key2)

        time.sleep(self.delay)
        self.assertEqual(self.backend[key2], value)
        self.assertRaises(NoSuchObject, self.backend.lookup, key1)

# This test just takes too long (because we have to wait really long so that we don't
# get false errors due to propagation delays)
#@unittest.skip('takes too long')
class S3Tests(BackendTestsMixin, unittest.TestCase):
    def setUp(self):
        self.name_cnt = 0
        # This is the time in which we expect S3 changes to propagate. It may
        # be much longer for larger objects, but for tests this is usually enough.
        self.delay = 15

        self.backend = s3.Backend(*self.get_credentials('s3-test'), ssl_context=None)

    def tearDown(self):
        self.backend.clear()

    def get_credentials(self, name):

        authfile = os.path.expanduser('~/.s3ql/authinfo2')
        if not os.path.exists(authfile):
            self.skipTest('No authentication file found.')

        mode = os.stat(authfile).st_mode
        if mode & (stat.S_IRGRP | stat.S_IROTH):
            self.skipTest("Authentication file has insecure permissions")

        config = configparser.ConfigParser()
        config.read(authfile)

        try:
            fs_name = config.get(name, 'test-fs')
            backend_login = config.get(name, 'backend-login')
            backend_password = config.get(name, 'backend-password')
        except (configparser.NoOptionError, configparser.NoSectionError):
            self.skipTest("Authentication file does not have test section")

        return (fs_name, backend_login, backend_password)

class S3SSLTests(S3Tests):
    def setUp(self):
        self.name_cnt = 0
        self.delay = 15
        options = Namespace()
        options.no_ssl = False
        options.ssl_ca_path = None
        self.backend = s3.Backend(*self.get_credentials('s3-test'),
                                   ssl_context=get_ssl_context(options))
        
class SwiftTests(S3Tests):
    def setUp(self):
        self.name_cnt = 0
        self.delay = 0
        self.backend = swift.Backend(*self.get_credentials('swift-test'))
        
class GSTests(S3Tests):
    def setUp(self):
        self.name_cnt = 0
        self.delay = 15
        self.backend = gs.Backend(*self.get_credentials('gs-test'), ssl_context=None)
        
class S3CTests(S3Tests):
    def setUp(self):
        self.name_cnt = 0
        self.delay = 0
        self.backend = s3c.Backend(*self.get_credentials('s3c-test'), ssl_context=None)   

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
        self.backend_dir = tempfile.mkdtemp()
        self.backend = local.Backend('local://' + self.backend_dir, None, None)
        self.delay = 0

    def tearDown(self):
        self.backend.clear()
        os.rmdir(self.backend_dir)

class CompressionTests(BackendTestsMixin, unittest.TestCase):

    def setUp(self):
        self.name_cnt = 0
        self.backend_dir = tempfile.mkdtemp()
        self.plain_backend = local.Backend('local://' + self.backend_dir, None, None)
        self.backend = self._wrap_backend()
        self.delay = 0

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
        time.sleep(self.delay)
        self.assertEqual(self.backend['encrypted'], b'testdata')
        self.assertNotEqual(self.plain_backend['encrypted'], b'testdata')
        self.assertRaises(ObjectNotEncrypted, self.backend.fetch, 'plain')
        self.assertRaises(ObjectNotEncrypted, self.backend.lookup, 'plain')

        self.backend.passphrase = None
        self.assertRaises(ChecksumError, self.backend.fetch, 'encrypted')
        self.assertRaises(ChecksumError, self.backend.lookup, 'encrypted')

        self.backend.passphrase = b'jobzrul'
        self.assertRaises(ChecksumError, self.backend.fetch, 'encrypted')
        self.assertRaises(ChecksumError, self.backend.lookup, 'encrypted')


class EncryptionCompressionTests(EncryptionTests):

    def _wrap_backend(self):
        return BetterBackend(b'schlurz', 'zlib', self.plain_backend)


# Somehow important according to pyunit documentation
def suite():
    return unittest.makeSuite(LocalTests)


# Allow calling from command line
if __name__ == "__main__":
    unittest.main()
