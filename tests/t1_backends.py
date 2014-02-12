#!/usr/bin/env python3
'''
t1_backends.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

if __name__ == '__main__':
    import pytest
    import sys
    sys.exit(pytest.main([__file__] + sys.argv[1:]))

from s3ql.backends import local, s3, gs, s3c, swift, rackspace, swiftks
from s3ql import backends
from s3ql.backends.common import (ChecksumError, ObjectNotEncrypted, NoSuchObject,
    BetterBackend, get_ssl_context, AuthenticationError, AuthorizationError,
    DanglingStorageURLError, MalformedObjectError, DecryptFilter, DecompressFilter)
from s3ql.common import BUFSIZE
from common import get_remote_test_info
import s3ql.backends.common
from argparse import Namespace
import os
import tempfile
import time
import unittest
import pytest
import struct

RETRY_DELAY = 1

@pytest.yield_fixture(params=('local', 's3', 'gs', 's3c', 'swift',
                              'rackspace', 'swiftks'))
def plain_backend(request):
    backend_name = request.param
    backend_module = getattr(backends, backend_name)
    backend_class = backend_module.Backend
    if backend_name == 'local':
        backend_dir = tempfile.mkdtemp(prefix='s3ql-backend-')
        backend = backend_class('local://' + backend_dir, None, None)
    else:
        options = Namespace()
        options.no_ssl = False
        options.ssl_ca_path = None

        (backend_login, backend_password,
         fs_name) = get_remote_test_info(backend_name + '-test', pytest.skip)
        
        backend = backend_class(fs_name, backend_login, backend_password,
                                ssl_context=get_ssl_context(options))

    try:
        backend.fetch('empty_object')
    except DanglingStorageURLError:
        pytest.skip('Bucket %s does not exist' % fs_name)
    except AuthorizationError:
        pytest.skip('No permission to access %s' % fs_name)
    except AuthenticationError:
        pytest.skip('Unable to access %s, invalid credentials or skewed '
                    'system clock?' % fs_name)
    except NoSuchObject:
        pass
    else:
        pytest.skip('Test bucket not empty')
    
    try:
        yield backend

    finally:
        backend.clear()
        backend.close()
        if backend_name == 'local':
            os.rmdir(backend_dir)

@pytest.fixture(params=('plain', 'aes+lzma', 'zlib', 'bzip2', 'lzma'))
def backend(request, plain_backend):
    arg = request.param
    if arg == 'plain':
        return plain_backend
    elif arg == 'aes+lzma':
        return BetterBackend(b'schlurz', ('lzma', 6), plain_backend)
    else:
        return BetterBackend(None, (arg, 6), plain_backend)

@pytest.fixture()
def retries(request, plain_backend):
    if isinstance(plain_backend, local.Backend):
        return 0
    else:
        return 90
    
@pytest.fixture()
def retry_fn(request, plain_backend):
    if isinstance(plain_backend, local.Backend):
        return lambda x, *a: x()

    def retry(fn, *exceptions):
        tries = 0
        while True:
            try:
                return fn()
            except Exception as exc:
                if not isinstance(exc, exceptions):
                    raise
                if tries >= 90:
                    raise
                tries += 1
                time.sleep(RETRY_DELAY)
            else:
                break
            
    return retry

name_counter = [0]
def newname():
    '''Return random, unique string'''
    name_counter[0] += 1
    return "s3ql/<tag=%d>/!sp ace_'quote\":_&end\\" % name_counter[0]
    
def newvalue():
    return newname().encode()

def test_write(backend, retry_fn):
    key = newname()
    value = newvalue()
    metadata = { 'jimmy': 'jups@42' }

    with pytest.raises(NoSuchObject):
        backend.lookup(key)
        
    with pytest.raises(NoSuchObject):
        backend.fetch(key)
        
    with backend.open_write(key, metadata) as fh:
        fh.write(value)

    def read():
        with backend.open_read(key) as fh:
            return (fh.read(), fh.metadata)
    (value2, metadata2) = retry_fn(read, NoSuchObject)

    assert value == value2
    assert metadata == metadata2
    assert backend[key] == value
    assert backend.lookup(key) == metadata

    
class BackendTestsMixin(object):

    def newname(self):
        self.name_cnt += 1
        # Include special characters
        return "s3ql/<tag=%d>/!sp ace_'quote\":_&end\\" % self.name_cnt

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


    def test_readslowly(self):
        key = self.newname()
        value = self.newvalue()
        metadata = { 'jimmy': 'jups@42' }

        with self.backend.open_write(key, metadata) as fh:
            fh.write(value)

        def read():
            buf = []
            with self.backend.open_read(key) as fh:
                # Force slow reading from underlying layer
                if isinstance(fh, (DecryptFilter, DecompressFilter)):
                    real_read = fh.fh.read
                    def read_slowly(size):
                        return real_read(1)
                    fh.fh.read = read_slowly
                while True:
                    buf.append(fh.read(1))
                    if not buf[-1]:
                        break
                    
                return (b''.join(buf), fh.metadata)

        try:
            s3ql.backends.common.BUFSIZE = 1
            (value2, metadata2) = self.retry(read, NoSuchObject)
        finally:
            s3ql.backends.common.BUFSIZE = BUFSIZE

        self.assertEqual(value, value2)
        self.assertEqual(metadata, metadata2)
        
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

        
    def test_delete_multi(self):
        keys = [ self.newname() for _ in range(5) ]
        value = self.newvalue()

        for key in keys:
            self.backend[key] = value
        for key in keys:
            self.retry(lambda: self.assertTrue(key in self.backend), AssertionError)

        tmp = keys[:2]
        self.backend.delete_multi(tmp)
        self.assertEqual(len(tmp), 0)

        for key in keys[:2]:
            self.retry(lambda: self.assertFalse(key in self.backend),
                       AssertionError)

        for key in keys[2:]:
            self.retry(lambda: self.assertTrue(key in self.backend), AssertionError)

            
    def test_delete_multi2(self):
        keys = [ self.newname() for _ in range(5) ]
        non_existing = self.newname()
        value = self.newvalue()

        for key in keys:
            self.backend[key] = value
        for key in keys:
            self.retry(lambda: self.assertTrue(key in self.backend), AssertionError)

        tmp = keys[:2]
        tmp.insert(1, non_existing)
        try:
            # We don't use force=True but catch the exemption to increase the
            # chance that some existing objects are not deleted because of the
            # error.
            self.backend.delete_multi(tmp)
        except NoSuchObject:
            pass
        
        for key in keys[:2]:
            if key in tmp:
                self.retry(lambda: self.assertTrue(key in self.backend),
                           AssertionError)
            else:
                self.retry(lambda: self.assertTrue(key not in self.backend),
                           AssertionError)

        for key in keys[2:]:
            self.retry(lambda: self.assertTrue(key in self.backend), AssertionError)

            
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


class BackendTestTemplate(BackendTestsMixin):
        
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
        self.backend.close()

        
# Dynamically generate tests for other backends
for backend_display_name in ('Swift', 'S3', 'SwiftKS', 'Rackspace', 'GS', 'S3C'):
    backend_name = backend_display_name.lower()
    backend_class = globals()[backend_name].Backend
    def setUp(self, backend_name=backend_name,
              backend_class=backend_class):
        self.name_cnt = 0
        self.retries = 90
        self.setUp2(backend_class, backend_name + '-test')
    test_class_name = backend_display_name + 'Tests'
    globals()[test_class_name] = type(test_class_name,
                                      (BackendTestTemplate, unittest.TestCase),
                                      { 'setUp': setUp })


class LocalTests(BackendTestsMixin, unittest.TestCase):

    def setUp(self):
        self.name_cnt = 0
        self.backend_dir = tempfile.mkdtemp(prefix='s3ql-backend-')
        self.backend = local.Backend('local://' + self.backend_dir, None, None)
        self.retries = 0

    def tearDown(self):
        self.backend.clear()
        os.rmdir(self.backend_dir)


class CompressionTestsMixin(BackendTestsMixin):

    def setUp(self):
        self.name_cnt = 0
        self.backend_dir = tempfile.mkdtemp(prefix='s3ql-backend-')
        self.plain_backend = local.Backend('local://' + self.backend_dir, None, None)
        self.backend = self._wrap_backend()
        self.retries = 0

    def _wrap_backend(self):
        raise RuntimeError("Must be implemented in derived classes.")

    def tearDown(self):
        self.backend.clear()
        self.backend.close()
        os.rmdir(self.backend_dir)

    def _make_corrupt_obj(self):
        key = self.newname()
        value = self.newvalue()

        self.backend[key] = value

        # Corrupt the data
        compr_value = bytearray(self.plain_backend[key])
        compr_value[-3:] = b'000'
        self.plain_backend.store(key, compr_value,
                                 self.plain_backend.lookup(key))

        return key

    def test_corruption(self):
        key = self._make_corrupt_obj()
        with pytest.raises(ChecksumError) as exc:
            self.backend.fetch(key)
        assert exc.value.str == 'Invalid compressed stream'

    def _make_extended_obj(self):
        key = self.newname()
        value = self.newvalue()

        self.backend[key] = value

        # Add bogus extra data
        compr_value = self.plain_backend[key]
        compr_value += b'000'
        self.plain_backend.store(key, compr_value,
                                 self.plain_backend.lookup(key))

        return key
    
    def test_extra_data(self):
        key = self._make_extended_obj()
        with pytest.raises(ChecksumError) as exc:
            self.backend.fetch(key)
        assert exc.value.str == 'Data after end of compressed stream'

            
class ZlibCompressionTests(CompressionTestsMixin, unittest.TestCase):
    def _wrap_backend(self):
        return BetterBackend(None, ('zlib', 6), self.plain_backend)

        
class BzipCompressionTests(CompressionTestsMixin, unittest.TestCase):
    def _wrap_backend(self):
        return BetterBackend(None, ('bzip2', 6), self.plain_backend)

class LzmaCompressionTests(CompressionTestsMixin, unittest.TestCase):
    def _wrap_backend(self):
        return BetterBackend(None, ('lzma', 6), self.plain_backend)
    
class EncryptionTests(CompressionTestsMixin, unittest.TestCase):

    def _wrap_backend(self):
        return BetterBackend(b'schluz', (None, 0), self.plain_backend)

    def test_multi_packet(self):
        with self.backend.open_write('my_obj') as fh:
            for i in range(5):
                fh.write(b'\xFF' * BUFSIZE)

        with self.backend.open_read('my_obj') as fh:
            while True:
                buf = fh.read(BUFSIZE//2)
                if not buf:
                    break

    def test_issue431(self):
        hdr_len = struct.calcsize(b'<I')
        with self.backend.open_write('my_obj') as fh:
            fh.write(b'\xFF' * 50)
            fh.write(b'\xFF' * 50)

        with self.backend.open_read('my_obj') as fh:
            fh.read(50 + 2*hdr_len)
            fh.read(50)
            assert fh.read(50) == b''

    def test_corruption(self):
        key = self._make_corrupt_obj()
        with pytest.raises(ChecksumError) as exc:
            self.backend.fetch(key)
        assert exc.value.str == 'HMAC mismatch'

    def test_extra_data(self):
        key = self._make_extended_obj()
        with pytest.raises(ChecksumError) as exc:
            self.backend.fetch(key)
        assert exc.value.str == 'Extraneous data at end of object'

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
        return BetterBackend(b'schlurz', ('zlib', 6), self.plain_backend)


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
