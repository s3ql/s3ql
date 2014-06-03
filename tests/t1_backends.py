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

import mock_server
from s3ql import backends
from s3ql.logging import logging
from s3ql.backends.local import Backend as LocalBackend
from s3ql.backends.common import (ChecksumError, ObjectNotEncrypted, NoSuchObject,
    BetterBackend, AuthenticationError, AuthorizationError, DanglingStorageURLError,
    MalformedObjectError)
from s3ql.common import BUFSIZE, get_ssl_context
from common import get_remote_test_info, NoTestSection
import s3ql.backends.common
from argparse import Namespace
import tempfile
import re
import functools
import time
import pytest
from pytest import raises as assert_raises
import shutil
import struct
import threading

log = logging.getLogger(__name__)
empty_set = set()

class BackendWrapper:

    def __init__(self):
        self.name = None
        self.retry_time = 0
        self.mock_server = False

    def init(self):
        '''Return backend instance'''
        pass

    def cleanup(self):
        '''Cleanup when backend is no longer needed'''
        pass

    def __str__(self):
        return self.name

def get_backend_wrappers():
    wrappers = []

    # Local backend is special case
    w = BackendWrapper()
    w.name = 'local'
    def init(self):
        self.backend_dir = tempfile.mkdtemp(prefix='s3ql-backend-')
        self.backend = backends.local.Backend('local://' + self.backend_dir, None, None)
        return self.backend
    def cleanup(self):
        self.backend.close()
        shutil.rmtree(self.backend_dir)
    w.init = functools.partial(init, w)
    w.cleanup = functools.partial(cleanup, w)
    wrappers.append(w)

    # Backends talking to local mock servers
    for (request_handler, storage_url) in mock_server.handler_list:
        backend_name = re.match(r'^([a-zA-Z0-9]+)://', storage_url).group(1)
        backend_class = backends.prefix_map[backend_name]
        w = BackendWrapper()
        w.name = 'mock_' + backend_name
        def init(self, handler=request_handler, class_=backend_class, url=storage_url):
            self.server = mock_server.StorageServer(handler, ('localhost', 0))
            self.thread = threading.Thread(target=self.server.serve_forever)
            self.thread.start()
            self.backend = class_(url % { 'host': self.server.server_address[0],
                                          'port': self.server.server_address[1] },
                                  'joe', 'swordfish')

            # Mock server should never have temporary failure
            self.backend.is_temp_failure = lambda exc: False

            return self.backend
        def cleanup(self):
            self.backend.close()
            self.server.server_close()
            self.server.shutdown()
        w.init = functools.partial(init, w)
        w.cleanup = functools.partial(cleanup, w)
        wrappers.append(w)

    # Backends talking to remote servers
    options = Namespace()
    options.no_ssl = False
    options.ssl_ca_path = None
    ssl_context = get_ssl_context(options)
    for (backend_name, backend_class) in backends.prefix_map.items():
        if backend_name == 'local': # special cased before
            continue

        w = BackendWrapper()
        w.name = backend_name

        try:
            (backend_login, backend_pw,
             backend_url) = get_remote_test_info(backend_name + '-test')
        except NoTestSection as exc:
            log.info('Not doing remote tests for %s backend: %s',
                     backend_name, exc.reason)
            continue

        def init(self, class_=backend_class, login=backend_login,
                 password=backend_pw, url=backend_url):
            self.backend = class_(url, login, password, ssl_context=ssl_context)
            self.retry_time = 600
            try:
                self.backend.fetch('empty_object')
            except DanglingStorageURLError:
                pytest.skip('%s does not exist' % url)
            except AuthorizationError:
                pytest.skip('No permission to access %s' % url)
            except AuthenticationError:
                pytest.skip('Unable to access %s, invalid credentials' % url)
            except NoSuchObject:
                pass
            else:
                pytest.skip('%s not empty' % url)
            return self.backend
        def cleanup(self):
            self.backend.clear()
            self.backend.close()
        w.init = functools.partial(init, w)
        w.cleanup = functools.partial(cleanup, w)
        wrappers.append(w)

    return wrappers

plain_backend_wrappers = get_backend_wrappers()
def pytest_generate_tests(metafunc):
    if 'plain_backend' in metafunc.fixturenames:
        # This is used only internally to construct the `backend` fixture
        metafunc.parametrize("plain_backend", plain_backend_wrappers,
                             ids=[ str(w) for w in plain_backend_wrappers ],
                             scope='module', indirect=True)

    if 'backend' in metafunc.fixturenames:
        # Function scope, so that we can empty the backend after each test
        metafunc.parametrize("backend",
                             [ 'plain', 'aes', 'aes+lzma', 'lzma', 'zlib', 'bzip2' ],
                             scope='function', indirect=True)

@pytest.yield_fixture()
def plain_backend(request):
    bw = request.param
    backend = bw.init()
    assert not hasattr(backend, 'retry_time')
    backend.retry_time = bw.retry_time

    assert not hasattr(backend, 'orig_prefix')
    backend.orig_prefix = backend.prefix

    try:
        yield backend
    finally:
        bw.cleanup()

backend_prefix_counter = [0]
@pytest.fixture()
def backend(request, plain_backend):
    compenc_kind = request.param

    if compenc_kind == 'plain':
        backend = plain_backend
    elif compenc_kind == 'aes+lzma':
        backend = BetterBackend(b'schlurz', ('lzma', 6), plain_backend)
    elif compenc_kind == 'aes':
        backend = BetterBackend(b'schlurz', (None, 6), plain_backend)
    else:
        backend = BetterBackend(None, (compenc_kind, 6), plain_backend)

    if backend is not plain_backend:
        assert not hasattr(backend, 'retry_time')
        backend.retry_time = plain_backend.retry_time

    # "clear" the backend by selecting a different prefix for every
    # test (actually deleting all objects would mean that we have to
    # wait for propagation delays)
    plain_backend.prefix = '%s%3d/' % (plain_backend.orig_prefix,
                                       backend_prefix_counter[0])
    backend_prefix_counter[0] += 1
    return backend

def newname(name_counter=[0]):
    '''Return random, unique string'''
    name_counter[0] += 1
    return "s3ql/<tag=%d>/!sp ace_'quote\":_&end\\" % name_counter[0]

def newvalue():
    return newname().encode()

def fetch_object(backend, key, sleep_time=1):
    '''Read data and metadata for *key* from *backend*

    If `NoSuchObject` exception is encountered, retry for
    up to ``backend.retry_time`` seconds.
    '''
    waited=0
    while True:
        try:
            return backend.fetch(key)
        except NoSuchObject:
            if waited >= backend.retry_time:
                raise
        time.sleep(sleep_time)
        waited += sleep_time

def lookup_object(backend, key, sleep_time=1):
    '''Read metadata for *key* from *backend*

    If `NoSuchObject` exception is encountered, retry for
    up to ``backend.retry_time`` seconds.
    '''
    waited=0
    while True:
        try:
            return backend.lookup(key)
        except NoSuchObject:
            if waited >= backend.retry_time:
                raise
        time.sleep(sleep_time)
        waited += sleep_time

def assert_in_index(backend, keys, sleep_time=1):
    '''Assert that *keys* will appear in index

    Raises assertion error if *keys* do not show up within
    ``backend.retry_timeout`` seconds.
    '''
    waited=0
    keys = set(keys) # copy
    while True:
        index = set(backend.list())
        if not keys - index:
            return
        elif waited >= backend.retry_time:
            assert keys - index == empty_set
        time.sleep(sleep_time)
        waited += sleep_time

def assert_not_in_index(backend, keys, sleep_time=1):
    '''Assert that *keys* will disappear from index

    Raises assertion error if *keys* do not disappear within
    ``backend.retry_timeout`` seconds.
    '''
    waited=0
    keys = set(keys) # copy
    while True:
        index = set(backend.list())
        if keys - index == keys:
            return
        elif waited >= backend.retry_time:
            assert keys - index == keys
        time.sleep(sleep_time)
        waited += sleep_time

def assert_not_readable(backend, key, sleep_time=1):
    '''Assert that *key* does not exist in *backend*

    Asserts that a `NoSuchObject` exception will be raised when trying to read
    the object after at most ``backend.retry_time`` seconds.
    '''
    waited=0
    while True:
        try:
            backend.fetch(key)
        except NoSuchObject:
            return
        if waited >= backend.retry_time:
            pytest.fail('object %s still present in backend' % key)
        time.sleep(sleep_time)
        waited += sleep_time

def test_read_write(backend):
    key = newname()
    value = newvalue()
    metadata = { 'jimmy': 'jups@42' }

    assert key not in backend
    assert_raises(NoSuchObject, backend.lookup, key)
    assert_raises(NoSuchObject, backend.fetch, key)

    def do_write(fh):
        fh.write(value)
    backend.perform_write(do_write, key, metadata)

    assert_in_index(backend, [key])
    (value2, metadata2) = fetch_object(backend, key)

    assert value == value2
    assert metadata == metadata2
    assert lookup_object(backend, key) == metadata

def test_list(backend):
    keys = ([ 'prefixa' + newname() for dummy in range(6) ]
            + [ 'prefixb' + newname() for dummy in range(6) ])
    values = [ newvalue() for dummy in range(12) ]

    assert set(backend.list()) == empty_set
    for i in range(12):
        backend[keys[i]] = values[i]
    assert_in_index(backend, keys)

    assert set(backend.list('prefixa')) == set(keys[:6])
    assert set(backend.list('prefixb')) == set(keys[6:])
    assert set(backend.list('prefixc')) == empty_set

def test_readslowly(backend):
    if backend.retry_time:
        pytest.skip('requires immediate consistency backend')

    key = newname()
    value = newvalue()
    metadata = { 'jimmy': 'jups@42' }

    backend.store(key, value, metadata)

    s3ql.backends.common.BUFSIZE = 1
    try:
        with backend.open_read(key) as fh:
            # Force slow reading from underlying layer
            if hasattr(fh, 'fh'):
                def read_slowly(size, *, real_read=fh.fh.read):
                    return real_read(1)
                fh.fh.read = read_slowly

            buf = []
            while True:
                buf.append(fh.read(1))
                if not buf[-1]:
                    break
            value2 = b''.join(buf)
            metadata2 =  fh.metadata
    finally:
        s3ql.backends.common.BUFSIZE = BUFSIZE

    assert value == value2
    assert metadata == metadata2

def test_delete(backend):
    key = newname()
    value = newvalue()

    backend[key] = value

    # Wait for object to become visible
    assert_in_index(backend, [key])
    fetch_object(backend, key)

    # Delete it
    del backend[key]

    # Make sure that it's truly gone
    assert_not_in_index(backend, [key])
    assert_not_readable(backend, key)

def test_delete_multi(backend):
    keys = [ newname() for _ in range(30) ]
    value = newvalue()

    # Create objects
    for key in keys:
        backend[key] = value

    # Wait for them
    assert_in_index(backend, keys)
    for key in keys:
        fetch_object(backend, key)

    # Delete half of them
    # We don't use force=True but catch the exemption to increase the
    # chance that some existing objects are not deleted because of the
    # error.
    to_delete = keys[::2]
    to_delete.insert(7, 'not_existing')
    try:
        backend.delete_multi(to_delete)
    except NoSuchObject:
        pass

    # Without full consistency, deleting an non-existing object
    # may not give an error
    assert backend.retry_time or len(to_delete) > 0

    deleted = set(keys[::2]) - set(to_delete)
    assert len(deleted) > 0
    remaining = set(keys) - deleted

    assert_not_in_index(backend, deleted)
    for key in deleted:
        assert_not_readable(backend, key)

    assert_in_index(backend, remaining)
    for key in remaining:
        fetch_object(backend, key)

def test_clear(backend):
    keys = [ newname() for _ in range(5) ]
    value = newvalue()

    # Create objects
    for key in keys:
        backend[key] = value

    # Wait for them
    assert_in_index(backend, keys)
    for key in keys:
        fetch_object(backend, key)

    # Delete everything
    backend.clear()

    assert_not_in_index(backend, keys)
    for key in keys:
        assert_not_readable(backend, key)

def test_copy(backend):
    key1 = newname()
    key2 = newname()
    value = newvalue()
    metadata = { 'jimmy': 'jups@42' }

    backend.store(key1, value, metadata)

    # Wait for object to become visible
    assert_in_index(backend, [key1])
    fetch_object(backend, key1)

    assert_not_in_index(backend, [key2])
    assert_not_readable(backend, key2)

    backend.copy(key1, key2)

    assert_in_index(backend, [key2])
    (value2, metadata2) = fetch_object(backend, key2)

    assert value == value2
    assert metadata == metadata2

def test_copy_newmeta(backend):
    if isinstance(backend, BetterBackend):
        pytest.skip('not yet supported for compressed or encrypted backends')
    key1 = newname()
    key2 = newname()
    value = newvalue()
    meta1 = { 'jimmy': 'jups@42' }
    meta2 = { 'jiy': 'jfobauske42' }

    backend.store(key1, value, meta1)

    # Wait for object to become visible
    assert_in_index(backend, [key1])
    fetch_object(backend, key1)

    assert_not_in_index(backend, [key2])
    assert_not_readable(backend, key2)

    backend.copy(key1, key2, meta2)

    assert_in_index(backend, [key2])
    (value2, meta) = fetch_object(backend, key2)

    assert value == value2
    assert meta == meta2

def test_rename(backend):
    key1 = newname()
    key2 = newname()
    value = newvalue()
    metadata = { 'jimmy': 'jups@42' }

    backend.store(key1, value, metadata)

    # Wait for object to become visible
    assert_in_index(backend, [key1])
    fetch_object(backend, key1)

    assert_not_in_index(backend, [key2])
    assert_not_readable(backend, key2)

    backend.rename(key1, key2)

    assert_in_index(backend, [key2])
    (value2, metadata2) = fetch_object(backend, key2)

    assert value == value2
    assert metadata == metadata2

    assert_not_in_index(backend, [key1])
    assert_not_readable(backend, key1)

def test_rename_newmeta(backend):
    if isinstance(backend, BetterBackend):
        pytest.skip('not yet supported for compressed or encrypted backends')
    key1 = newname()
    key2 = newname()
    value = newvalue()
    meta1 = { 'jimmy': 'jups@42' }
    meta2 = { 'apple': 'potatoes' }

    backend.store(key1, value, meta1)

    # Wait for object to become visible
    assert_in_index(backend, [key1])
    fetch_object(backend, key1)

    assert_not_in_index(backend, [key2])
    assert_not_readable(backend, key2)

    backend.rename(key1, key2, meta2)

    assert_in_index(backend, [key2])
    (value2, meta) = fetch_object(backend, key2)

    assert value == value2
    assert meta == meta2

def test_corruption(backend):
    if not isinstance(backend, BetterBackend):
        pytest.skip('only supported for compressed or encrypted backends')
    plain_backend = backend.backend

    # Create compressed object
    key = newname()
    value = newvalue()
    backend[key] = value

    # Retrieve compressed data
    (compr_value, meta) = fetch_object(plain_backend, key)
    compr_value = bytearray(compr_value)

    # Create new, corrupt object
    compr_value[-3:] = b'000'
    key = newname()
    plain_backend.store(key, compr_value, meta)

    with pytest.raises(ChecksumError) as exc:
        fetch_object(backend, key)

    if backend.passphrase is None: # compression only
        assert exc.value.str == 'Invalid compressed stream'
    else:
        assert exc.value.str == 'HMAC mismatch'

def test_extra_data(backend):
    if not isinstance(backend, BetterBackend):
        pytest.skip('only supported for compressed or encrypted backends')
    plain_backend = backend.backend

    # Create compressed object
    key = newname()
    value = newvalue()
    backend[key] = value

    # Retrieve compressed data
    (compr_value, meta) = fetch_object(plain_backend, key)
    compr_value = bytearray(compr_value)

    # Create new, corrupt object
    compr_value += b'000'
    key = newname()
    plain_backend.store(key, compr_value, meta)

    with pytest.raises(ChecksumError) as exc:
        fetch_object(backend, key)

    if backend.passphrase is None: # compression only
        assert exc.value.str == 'Data after end of compressed stream'
    else:
        assert exc.value.str == 'Extraneous data at end of object'

def test_multi_packet(backend):
    '''Write and read packet extending over multiple chunks'''
    key = newname()

    def do_write(fh):
        for i in range(5):
            fh.write(b'\xFF' * BUFSIZE)
    backend.perform_write(do_write, key)

    def do_read(fh):
        buf = bytearray()
        while True:
            tmp = fh.read(BUFSIZE//2)
            if not tmp:
                break
            buf += tmp
        return buf
    res = backend.perform_read(do_read, key)
    assert res == b'\xFF' * (5*BUFSIZE)

def test_issue431(backend):
    if not isinstance(backend, BetterBackend):
        pytest.skip('only supported for compressed or encrypted backends')
    elif not isinstance(backend.backend, LocalBackend):
        pytest.skip('requires backend without short reads')
    key = newname()
    hdr_len = struct.calcsize(b'<I')

    def do_write(fh):
        fh.write(b'\xFF' * 50)
        fh.write(b'\xFF' * 50)
    backend.perform_write(do_write, key)

    def do_read(fh):
        fh.read(50 + 2*hdr_len)
        fh.read(50)
        assert fh.read(50) == b''
    backend.perform_read(do_read, key)

def test_encryption(backend):
    if (not isinstance(backend, BetterBackend)
        or backend.passphrase is None):
        pytest.skip('only supported for encrypted backends')
    elif backend.retry_time:
        pytest.skip('requires backend with immediate consistency')
    plain_backend = backend.backend

    plain_backend['plain'] = b'foobar452'
    backend.store('encrypted', b'testdata', { 'tag': True })

    assert plain_backend['encrypted'] != b'testdata'
    assert_raises(MalformedObjectError, backend.fetch, 'plain')
    assert_raises(MalformedObjectError, backend.lookup, 'plain')

    backend.passphrase = None
    backend.store('not-encrypted', b'testdata2395', { 'tag': False })
    assert_raises(ChecksumError, backend.fetch, 'encrypted')
    assert_raises(ChecksumError, backend.lookup, 'encrypted')

    backend.passphrase = b'jobzrul'
    assert_raises(ChecksumError, backend.fetch, 'encrypted')
    assert_raises(ChecksumError, backend.lookup, 'encrypted')
    assert_raises(ObjectNotEncrypted, backend.fetch, 'not-encrypted')
    assert_raises(ObjectNotEncrypted, backend.lookup, 'not-encrypted')

def test_s3_url_parsing():
    parse = backends.s3.Backend._parse_storage_url
    assert parse('s3://name', ssl_context=None)[2:] == ('name', '')
    assert parse('s3://name/', ssl_context=None)[2:] == ('name', '')
    assert parse('s3://name/pref/', ssl_context=None)[2:] == ('name', 'pref/')
    assert parse('s3://name//pref/', ssl_context=None)[2:] == ('name', '/pref/')

def test_gs_url_parsing():
    parse = backends.gs.Backend._parse_storage_url
    assert parse('gs://name', ssl_context=None)[2:] == ('name', '')
    assert parse('gs://name/', ssl_context=None)[2:] == ('name', '')
    assert parse('gs://name/pref/', ssl_context=None)[2:] == ('name', 'pref/')
    assert parse('gs://name//pref/', ssl_context=None)[2:] == ('name', '/pref/')

def test_s3c_url_parsing():
    parse = backends.s3c.Backend._parse_storage_url
    assert parse('s3c://host.org/name', ssl_context=None) == ('host.org', 80, 'name', '')
    assert parse('s3c://host.org:23/name', ssl_context=None) == ('host.org', 23, 'name', '')
    assert parse('s3c://host.org/name/', ssl_context=None) == ('host.org', 80, 'name', '')
    assert parse('s3c://host.org/name/pref',
                 ssl_context=None) == ('host.org', 80, 'name', 'pref')
    assert parse('s3c://host.org:17/name/pref/',
                 ssl_context=None) == ('host.org', 17, 'name', 'pref/')
