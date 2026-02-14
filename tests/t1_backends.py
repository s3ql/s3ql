#!/usr/bin/env python3
'''
t1_backends.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

if __name__ == '__main__':
    import sys

    import pytest

    sys.exit(pytest.main([__file__] + sys.argv[1:]))

import functools
import logging
import re
import shutil
import tempfile
import threading
import time
from argparse import Namespace
from io import BytesIO

import mock_server
import pytest
from common import CLOCK_GRANULARITY, NoTestSection, get_remote_test_info
from pytest import raises as assert_raises
from pytest_checklogs import assert_logs

from s3ql import BUFSIZE, backends
from s3ql.backends import gs, local
from s3ql.backends.common import AsyncBackend, CorruptedObjectError, NoSuchObject
from s3ql.backends.comprenc import (
    SYNC_COMPRESSION_THRESHOLD,
    SYNC_DECOMPRESSION_THRESHOLD,
    AsyncComprencBackend,
    ObjectNotEncrypted,
)
from s3ql.backends.s3c import BadDigestError, HTTPError, OperationAbortedError, S3Error
from s3ql.http import ConnectionClosed
from s3ql.parse_args import suboptions_type
from s3ql.types import BasicMappingT

log = logging.getLogger(__name__)
empty_set: set[str] = set()


def brace_expand(s):
    hit = re.search(r'^(.*)\{(.+)\}(.*)$', s)
    if not hit:
        return [s]
    (p, e, s) = hit.groups()
    l = []  # noqa: E741 # auto-added, needs manual check!
    for el in e.split(','):
        l.append(p + el + s)
    return l


def enable_temp_fail(backend):
    if isinstance(backend, AsyncComprencBackend):
        backend = backend.backend
    backend.unittest_info.may_temp_fail = True


# It'd be nice if we could use the setup_module hook instead, but
# unfortunately that gets executed *after* pytest_generate_tests.
def _get_backend_info():
    '''Get information about raw backends available for testing'''

    info = []

    # Local backend
    bi = Namespace()
    bi.name = 'local'
    bi.classname = 'local'
    info.append(bi)

    # Backends talking to actual remote servers (if available)
    for name in backends.async_prefix_map:
        if name == 'local':  # local backend has already been handled
            continue

        try:
            (login, password, options, storage_url) = get_remote_test_info(name + '-test')
            if options is None:
                options = {}
            else:
                options = suboptions_type(options)
        except NoTestSection as exc:
            log.info('Not running remote tests for %s backend: %s', name, exc.reason)
            continue

        bi = Namespace()
        bi.name = 'remote-' + name
        bi.classname = name
        bi.storage_url = storage_url
        bi.login = login
        bi.password = password
        bi.options = options
        info.append(bi)

    # Backends talking to local mock servers
    for request_handler, storage_url in mock_server.handler_list:
        name = re.match(r'^([a-zA-Z0-9]+)://', storage_url).group(1)
        bi = Namespace()
        bi.name = 'mock-' + name
        bi.classname = name
        bi.request_handler = request_handler
        bi.storage_url = storage_url
        info.append(bi)

    return info


def pytest_generate_tests(metafunc, _info_cache=[]):  # noqa: B006
    if _info_cache:
        backend_info = _info_cache[0]
    else:
        backend_info = _get_backend_info()
        _info_cache.append(backend_info)

    if 'backend' not in metafunc.fixturenames:
        return

    with_backend_mark = metafunc.definition.get_closest_marker('with_backend')
    assert with_backend_mark

    test_params = []
    for spec in with_backend_mark.args:
        (backend_spec, comprenc_spec) = spec.split('/')

        # Expand compression/encryption specification
        # raw == don't use ComprencBackend at all
        # plain = use ComprencBackend without compression and encryption
        if comprenc_spec == '*':
            comprenc_kinds = ['aes', 'aes+zlib', 'plain', 'zlib', 'bzip2', 'lzma', 'raw']
        else:
            comprenc_kinds = brace_expand(comprenc_spec)

        # Expand backend specification
        if backend_spec == '*':
            test_bi = backend_info
        else:
            test_bi = []
            for name in brace_expand(backend_spec):
                test_bi += [x for x in backend_info if x.classname == name]

        # Filter
        if with_backend_mark.kwargs.get('require_mock_server', False):
            test_bi = [x for x in test_bi if 'request_handler' in x]

        for comprenc_kind in comprenc_kinds:
            for bi in test_bi:
                test_params.append((bi, comprenc_kind))

    metafunc.parametrize(
        "backend",
        test_params,
        indirect=True,
        ids=['%s/%s' % (x[0].name, x[1]) for x in test_params],
    )


@pytest.fixture()
async def backend(request):
    (backend_info, comprenc_kind) = request.param

    if backend_info.classname == 'local':
        gen = yield_local_backend(backend_info)
    elif 'request_handler' in backend_info:
        gen = yield_mock_backend(backend_info)
    else:
        gen = yield_remote_backend(backend_info)

    async for raw_backend in gen:
        if comprenc_kind == 'raw':
            backend = raw_backend
        elif comprenc_kind == 'plain':
            backend = await AsyncComprencBackend.create(None, (None, 6), raw_backend)
        elif comprenc_kind == 'aes+zlib':
            backend = await AsyncComprencBackend.create(b'schlurz', ('zlib', 6), raw_backend)
        elif comprenc_kind == 'aes':
            backend = await AsyncComprencBackend.create(b'schlurz', (None, 6), raw_backend)
        else:
            backend = await AsyncComprencBackend.create(None, (comprenc_kind, 6), raw_backend)

        backend.unittest_info = raw_backend.unittest_info

        yield backend


async def yield_local_backend(bi):
    backend_dir = tempfile.mkdtemp(prefix='s3ql-backend-')
    backend = await local.AsyncBackend.create(Namespace(storage_url='local://' + backend_dir))
    backend.unittest_info = Namespace()
    try:
        yield backend
    finally:
        await backend.close()
        shutil.rmtree(backend_dir)


async def yield_mock_backend(bi):
    backend_class = backends.async_prefix_map[bi.classname]
    server = mock_server.StorageServer(bi.request_handler, ('localhost', 0))
    thread = threading.Thread(target=server.serve_forever)
    thread.daemon = True
    thread.start()

    storage_url = bi.storage_url % {
        'host': server.server_address[0],
        'port': server.server_address[1],
    }
    backend = await backend_class.create(
        Namespace(
            storage_url=storage_url,
            backend_login='joe',
            backend_password='swordfish',
            backend_options={'no-ssl': True},
        )
    )

    # Enable OAuth when using Google Backend
    if isinstance(backend, gs.AsyncBackend):
        backend.use_oauth2 = True
        backend.hdr_prefix = 'x-goog-'  # Normally set in __init__
        backend.access_token[backend.password] = 'foobar'

    backend.unittest_info = Namespace()

    # Mock server should not have temporary failures by default
    is_temp_failure = backend.is_temp_failure

    @functools.wraps(backend.is_temp_failure)
    def wrap(exc):
        return backend.unittest_info.may_temp_fail and is_temp_failure(exc)

    backend.is_temp_failure = wrap
    backend.unittest_info.may_temp_fail = False

    try:
        yield backend
    finally:
        await backend.close()
        server.shutdown()
        server.server_close()


async def yield_remote_backend(bi, _ctr=[0]):  # noqa: B006
    # Add timestamp + ctr to prefix so we don't have to deal with cruft from
    # previous tests
    _ctr[0] += 1
    storage_url = bi.storage_url
    if storage_url[-1] != '/':
        storage_url += '/'
    storage_url += '%d_%d/' % (time.time(), _ctr[0])

    backend_class = backends.async_prefix_map[bi.classname]
    backend = await backend_class.create(
        Namespace(
            storage_url=storage_url,
            backend_login=bi.login,
            backend_password=bi.password,
            backend_options=bi.options,
        )
    )

    backend.unittest_info = Namespace()

    async def cleanup():
        async for name in backend.list():
            await backend.delete(name)
        await backend.close()

    try:
        yield backend
    finally:
        await cleanup()


def newname(name_counter=[0]):  # noqa: B006
    '''Return random, unique string'''
    name_counter[0] += 1
    return "random\\'name' %d" % name_counter[0]


def newvalue():
    return newname().encode()


async def assert_in_index(backend, keys):
    '''Assert that *keys* are in backend list output'''

    keys = set(keys)
    index = {k async for k in backend.list()}
    assert keys - index == empty_set


async def assert_not_in_index(backend, keys):
    '''Assert that *keys* are not in backend list output'''

    keys = set(keys)
    index = {k async for k in backend.list()}
    assert keys - index == keys


@pytest.mark.trio
@pytest.mark.with_backend('*/raw', 'local/*')
async def test_readinto_write_fh(backend: AsyncBackend):
    key = newname()
    value = newvalue()
    metadata: BasicMappingT = {'jimmy': 'jups@42'}
    buf = BytesIO()

    assert not await backend.contains(key)
    with assert_raises(NoSuchObject):
        await backend.lookup(key)
    with assert_raises(NoSuchObject):
        await backend.readinto_fh(key, buf)

    assert await backend.write_fh(key, BytesIO(value), len(value), metadata) > 0

    assert await backend.contains(key)

    metadata2 = await backend.readinto_fh(key, buf)

    assert value == buf.getvalue()
    assert metadata == metadata2
    assert await backend.lookup(key) == metadata


@pytest.mark.trio
@pytest.mark.with_backend('*/{plain,raw,aes+zlib}')
async def test_write_fh_partial(backend: AsyncBackend):
    key = newname()
    metadata: BasicMappingT = {'jimmy': 'jups@42'}
    data = (''.join(str(x) for x in range(100))).encode()
    buf = BytesIO(data)

    buf.seek(10)
    assert await backend.write_fh(key, buf, 20, metadata) > 0

    buf2 = BytesIO()
    await backend.readinto_fh(key, buf2)

    assert buf2.getvalue() == data[10 : 10 + 20]


@pytest.mark.trio
@pytest.mark.with_backend('*/{plain,raw,aes+zlib}')
async def test_write_fh_off(backend: AsyncBackend):
    key = newname()
    metadata: BasicMappingT = {'jimmy': 'jups@42'}
    data = (''.join(str(x) for x in range(100))).encode()
    buf = BytesIO(data)

    buf.seek(10)
    assert await backend.write_fh(key, buf, len(data[10:]), metadata) > 0

    buf2 = BytesIO()
    await backend.readinto_fh(key, buf2)

    assert buf2.getvalue() == data[10:]


@pytest.mark.trio
@pytest.mark.with_backend('swift/raw')
async def test_issue114(backend, monkeypatch):
    key = newname()
    value = newvalue()
    monkeypatch.setitem(backend.options, 'disable-expect100', True)
    await backend.store(key, value)
    await assert_in_index(backend, [key])


@pytest.mark.trio
@pytest.mark.with_backend('*/raw', 'local/aes+zlib')
async def test_complex_meta(backend):
    key = newname()
    value = newvalue()

    metadata = {
        'com\nplex: key': 42,
        'farp_': False,
        'non-farp': True,
        'blu%rz': 23.283475,
        'görp': b'heelo',
        'sch.al': 'gorroobalp\nfurrö!',
        'lo-ng': 'foobarz' * 40,
    }

    assert not await backend.contains(key)
    await backend.store(key, value, metadata)
    (value2, metadata2) = await backend.fetch(key)

    assert value == value2
    assert metadata == metadata2
    assert await backend.lookup(key) == metadata


# No need to run with different encryption/compression settings,
# ComprencBackend should just forward this 1:1 to the raw backend.
@pytest.mark.trio
@pytest.mark.with_backend('*/aes')
async def test_list(backend):
    keys = ['prefixa' + newname() for _ in range(6)] + ['prefixb' + newname() for _ in range(6)]
    values = [newvalue() for _ in range(12)]

    assert {k async for k in backend.list()} == empty_set
    for i in range(12):
        await backend.store(keys[i], values[i])
    await assert_in_index(backend, keys)

    assert {k async for k in backend.list('prefixa')} == set(keys[:6])
    assert {k async for k in backend.list('prefixb')} == set(keys[6:])
    assert {k async for k in backend.list('prefixc')} == empty_set


# No need to run with different encryption/compression settings,
# ComprencBackend should just forward this 1:1 to the raw backend.
@pytest.mark.trio
@pytest.mark.with_backend('*/aes')
async def test_delete(backend):
    key = newname()
    value = newvalue()

    await backend.store(key, value)
    await assert_in_index(backend, [key])
    await backend.fetch(key)

    await backend.delete(key)

    await assert_not_in_index(backend, [key])
    with pytest.raises(NoSuchObject):
        await backend.fetch(key)


@pytest.mark.trio
@pytest.mark.with_backend('*/aes')
async def test_delete_non_existing(backend):
    key = newname()
    await backend.delete(key)


@pytest.mark.trio
@pytest.mark.with_backend('b2/aes')
async def test_delete_b2_versions(backend):
    key = newname()
    values = [newvalue() for _ in range(3)]

    # Write values to same key, should become versions of the key
    for value in values:
        await backend.store(key, value)

    # Wait for it
    await assert_in_index(backend, [key])
    await backend.fetch(key)

    # Delete it, should delete all versions
    await backend.delete(key)

    await assert_not_in_index(backend, [key])
    with pytest.raises(NoSuchObject):
        await backend.fetch(key)


# No need to run with different encryption/compression settings,
# ComprencBackend should just forward this 1:1 to the raw backend.
@pytest.mark.trio
@pytest.mark.with_backend('*/aes')
async def test_delete_multi(backend):
    if not backend.has_delete_multi:
        pytest.skip('backend does not support delete_multi')

    keys = [newname() for _ in range(10)]
    value = newvalue()

    for key in keys:
        await backend.store(key, value)

    # Delete half of them
    to_delete = keys[::2]
    await backend.delete_multi(to_delete)
    assert len(to_delete) == 0

    await assert_not_in_index(backend, keys[::2])
    for key in keys[::2]:
        with pytest.raises(NoSuchObject):
            await backend.fetch(key)

    await assert_in_index(backend, keys[1::2])
    for key in keys[1::2]:
        await backend.fetch(key)


@pytest.mark.trio
@pytest.mark.with_backend('local/{aes,aes+zlib,zlib,bzip2,lzma}')
async def test_corruption(backend):
    plain_backend = backend.backend

    # Create compressed object
    key = newname()
    value = newvalue()
    await backend.store(key, value)

    # Retrieve compressed data
    (compr_value, meta) = await plain_backend.fetch(key)
    compr_value = bytearray(compr_value)

    # Overwrite with corrupted data
    compr_value[-3:] = b'000'
    await plain_backend.store(key, compr_value, meta)

    with pytest.raises(CorruptedObjectError) as exc:
        await backend.fetch(key)

    if backend.passphrase is None:  # compression only
        assert exc.value.str == 'Invalid compressed stream'
    else:
        assert exc.value.str == 'HMAC mismatch'


@pytest.mark.trio
@pytest.mark.with_backend('local/{aes,aes+zlib,zlib,bzip2,lzma}')
async def test_extra_data(backend):
    plain_backend = backend.backend

    # Create compressed object
    key = newname()
    value = newvalue()
    await backend.store(key, value)

    # Retrieve compressed data
    (compr_value, meta) = await plain_backend.fetch(key)
    compr_value = bytearray(compr_value)

    # Overwrite with extended data
    compr_value += b'000'
    await plain_backend.store(key, compr_value, meta)

    with pytest.raises(CorruptedObjectError) as exc:
        await backend.fetch(key)

    if backend.passphrase is None:  # compression only
        assert exc.value.str == 'Data after end of compressed stream'
    else:
        assert exc.value.str == 'Extraneous data at end of object'


@pytest.mark.trio
@pytest.mark.with_backend('local/{aes,aes+zlib}')
async def test_encryption(backend):
    plain_backend = backend.backend

    await plain_backend.store('plain', b'foobar452')
    await backend.store('encrypted', b'testdata', {'tag': True})

    assert (await plain_backend.fetch('encrypted'))[0] != b'testdata'
    with assert_raises(CorruptedObjectError):
        await backend.fetch('plain')
    with assert_raises(CorruptedObjectError):
        await backend.lookup('plain')

    backend.passphrase = None
    await backend.store('not-encrypted', b'testdata2395', {'tag': False})
    with assert_raises(CorruptedObjectError):
        await backend.fetch('encrypted')
    with assert_raises(CorruptedObjectError):
        await backend.lookup('encrypted')

    backend.passphrase = b'jobzrul'
    with assert_raises(CorruptedObjectError):
        await backend.fetch('encrypted')
    with assert_raises(CorruptedObjectError):
        await backend.lookup('encrypted')
    with assert_raises(ObjectNotEncrypted):
        await backend.fetch('not-encrypted')
    with assert_raises(ObjectNotEncrypted):
        await backend.lookup('not-encrypted')


@pytest.mark.trio
@pytest.mark.with_backend('local/{aes,aes+zlib}')
async def test_replay(backend):
    plain_backend = backend.backend

    # Create encrypted object
    key1 = newname()
    key2 = newname()
    value = newvalue()
    await backend.store(key1, value)

    # Retrieve compressed data
    (compr_value, meta) = await plain_backend.fetch(key1)
    compr_value = bytearray(compr_value)

    # Copy into new object
    await plain_backend.store(key2, compr_value, meta)

    with pytest.raises(CorruptedObjectError):
        await backend.fetch(key2)


@pytest.mark.trio
@pytest.mark.with_backend('s3c/raw', require_mock_server=True)
async def test_list_bug(backend, monkeypatch):
    keys = ['prefixa' + newname() for _ in range(6)] + ['prefixb' + newname() for _ in range(6)]
    values = [newvalue() for _ in range(12)]

    assert {k async for k in backend.list()} == empty_set
    for i in range(12):
        await backend.store(keys[i], values[i])
    await assert_in_index(backend, keys)

    # Force reconnect during list
    handler_class = mock_server.S3CRequestHandler

    def do_list(self, q, _real=handler_class.do_list):
        q.params['max_keys'] = ['3']
        self.close_connection = True
        return _real(self, q)

    monkeypatch.setattr(handler_class, 'do_list', do_list)

    enable_temp_fail(backend)
    assert {k async for k in backend.list()} == set(keys)


@pytest.mark.trio
@pytest.mark.with_backend('s3c/aes+zlib', require_mock_server=True)
async def test_corrupted_get(backend, monkeypatch):
    key = 'brafasel'
    value = b'hello there, let us see whats going on'
    await backend.store(key, value)

    # Monkeypatch request handler to produce invalid etag
    handler_class = mock_server.S3CRequestHandler

    def send_header(self, keyword, value, count=[0], send_header_real=handler_class.send_header):  # noqa: B006
        if keyword == 'ETag':
            count[0] += 1
            if count[0] <= 3:
                value = value[::-1]
        return send_header_real(self, keyword, value)

    monkeypatch.setattr(handler_class, 'send_header', send_header)

    with (
        assert_logs('^MD5 mismatch for', count=1, level=logging.WARNING),
        assert_raises(BadDigestError),
    ):
        await backend.fetch(key)

    enable_temp_fail(backend)
    with assert_logs('^MD5 mismatch for', count=2, level=logging.WARNING):
        assert (await backend.fetch(key))[0] == value


@pytest.mark.trio
@pytest.mark.with_backend('s3c/{plain,raw,aes+zlib}', require_mock_server=True)
async def test_corrupted_meta(backend, monkeypatch):
    key = 'brafasel'
    value = b'hello there, let us see whats going on'
    await backend.store(key, value)

    # Monkeypatch request handler to mess up metadata
    handler_class = mock_server.S3CRequestHandler

    def send_header(self, keyword, value, count=[0], send_header_real=handler_class.send_header):  # noqa: B006
        if keyword == self.hdr_prefix + 'Meta-md5':
            count[0] += 1
            if count[0] <= 3:
                value = value[::-1]
        return send_header_real(self, keyword, value)

    monkeypatch.setattr(handler_class, 'send_header', send_header)

    with (
        assert_logs('^MD5 mismatch in metadata for', count=1, level=logging.WARNING),
        assert_raises(BadDigestError),
    ):
        await backend.fetch(key)

    enable_temp_fail(backend)
    with assert_logs('^MD5 mismatch in metadata for', count=2, level=logging.WARNING):
        assert (await backend.fetch(key))[0] == value


@pytest.mark.trio
@pytest.mark.with_backend('s3c/{plain,raw,aes+zlib}', require_mock_server=True)
async def test_corrupted_put(backend, monkeypatch):
    key = 'brafasel'
    value = b'hello there, let us see whats going on'

    # Monkeypatch request handler to produce invalid etag
    handler_class = mock_server.S3CRequestHandler

    def send_header(self, keyword, value, count=[0], send_header_real=handler_class.send_header):  # noqa: B006
        if keyword == 'ETag':
            count[0] += 1
            if count[0] < 3:
                value = value[::-1]
        return send_header_real(self, keyword, value)

    monkeypatch.setattr(handler_class, 'send_header', send_header)

    with pytest.raises(BadDigestError):
        await backend.store(key, value)

    enable_temp_fail(backend)
    await backend.store(key, value)

    assert (await backend.fetch(key))[0] == value


@pytest.mark.trio
@pytest.mark.with_backend('s3c/{plain,raw,aes+zlib}', require_mock_server=True)
async def test_get_s3error(backend, monkeypatch):
    value = b'hello there, let us see whats going on'
    key = 'quote'
    await backend.store(key, value)

    # Monkeypatch request handler to produce 3 errors
    handler_class = mock_server.S3CRequestHandler

    def do_GET(self, real_GET=handler_class.do_GET, count=[0]):  # noqa: B006
        count[0] += 1
        if count[0] > 3:
            return real_GET(self)
        else:
            self.send_error(503, code='OperationAborted')

    monkeypatch.setattr(handler_class, 'do_GET', do_GET)
    with assert_raises(OperationAbortedError):
        await backend.fetch(key)

    enable_temp_fail(backend)
    assert (await backend.fetch(key))[0] == value


@pytest.mark.trio
@pytest.mark.with_backend('s3c/{plain,raw,aes+zlib}', require_mock_server=True)
async def test_head_s3error(backend, monkeypatch):
    value = b'hello there, let us see whats going on'
    key = 'quote'
    meta = {'bar': 42, 'foo': 42**2}
    await backend.store(key, value, metadata=meta)

    # Monkeypatch request handler to produce 3 errors
    handler_class = mock_server.S3CRequestHandler

    def do_HEAD(self, real_HEAD=handler_class.do_HEAD, count=[0]):  # noqa: B006
        count[0] += 1
        if count[0] > 3:
            return real_HEAD(self)
        else:
            self.send_error(503, code='OperationAborted')

    monkeypatch.setattr(handler_class, 'do_HEAD', do_HEAD)
    with pytest.raises(HTTPError) as exc:
        await backend.lookup(key)
    assert exc.value.status == 503

    enable_temp_fail(backend)
    assert await backend.lookup(key) == meta


@pytest.mark.trio
@pytest.mark.with_backend('s3c/raw', require_mock_server=True)
async def test_delete_s3error(backend, monkeypatch):
    value = b'hello there, let us see whats going on'
    key = 'quote'
    await backend.store(key, value)

    # Monkeypatch request handler to produce 3 errors
    handler_class = mock_server.S3CRequestHandler

    def do_DELETE(self, real_DELETE=handler_class.do_DELETE, count=[0]):  # noqa: B006
        count[0] += 1
        if count[0] > 3:
            return real_DELETE(self)
        else:
            self.send_error(503, code='OperationAborted')

    monkeypatch.setattr(handler_class, 'do_DELETE', do_DELETE)
    with assert_raises(OperationAbortedError):
        await backend.delete(key)

    enable_temp_fail(backend)
    await backend.delete(key)


@pytest.mark.trio
@pytest.mark.with_backend('s3c/raw', require_mock_server=True)
async def test_backoff(backend, monkeypatch):
    value = b'hello there, let us see whats going on'
    key = 'quote'
    await backend.store(key, value)

    # Monkeypatch request handler
    handler_class = mock_server.S3CRequestHandler
    timestamps = []

    def do_DELETE(self, real_DELETE=handler_class.do_DELETE):
        timestamps.append(time.time())
        if len(timestamps) < 3:
            self.send_error(503, code='SlowDown', extra_headers={'Retry-After': '1'})
        else:
            return real_DELETE(self)

    monkeypatch.setattr(handler_class, 'do_DELETE', do_DELETE)
    enable_temp_fail(backend)
    await backend.delete(key)

    assert timestamps[1] - timestamps[0] > 1 - CLOCK_GRANULARITY
    assert timestamps[2] - timestamps[1] > 1 - CLOCK_GRANULARITY
    assert timestamps[2] - timestamps[0] < 10


@pytest.mark.trio
@pytest.mark.with_backend('s3c/aes+zlib', require_mock_server=True)
async def test_httperror(backend, monkeypatch):
    value = b'hello there, let us see whats going on'
    key = 'quote'
    await backend.store(key, value)

    # Monkeypatch request handler to produce a HTTP Error
    handler_class = mock_server.S3CRequestHandler

    def do_DELETE(self, real_DELETE=handler_class.do_DELETE, count=[0]):  # noqa: B006
        count[0] += 1
        if count[0] >= 3:
            return real_DELETE(self)
        content = b"I'm a proxy, and I messed up!"
        self.send_response(502, "Bad Gateway")
        self.send_header("Content-Type", 'text/plain; charset="utf-8"')
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        if self.command != 'HEAD':
            self.wfile.write(content)

    monkeypatch.setattr(handler_class, 'do_DELETE', do_DELETE)
    with assert_raises(HTTPError):
        await backend.delete(key)

    enable_temp_fail(backend)
    await backend.delete(key)


@pytest.mark.trio
@pytest.mark.with_backend('s3c/aes+zlib', require_mock_server=True)
async def test_put_s3error_early(backend, monkeypatch):
    '''Fail after expect-100'''

    data = b'hello there, let us see whats going on'
    key = 'borg'

    # Monkeypatch request handler to produce 3 errors
    handler_class = mock_server.S3CRequestHandler

    def handle_expect_100(self, real=handler_class.handle_expect_100, count=[0]):  # noqa: B006
        count[0] += 1
        if count[0] > 3:
            return real(self)
        else:
            self.send_error(503, code='OperationAborted')
            return False

    monkeypatch.setattr(handler_class, 'handle_expect_100', handle_expect_100)
    with pytest.raises(OperationAbortedError):
        await backend.store(key, data)

    enable_temp_fail(backend)
    await backend.store(key, data)


@pytest.mark.trio
@pytest.mark.with_backend('s3c/aes+zlib', require_mock_server=True)
async def test_put_s3error_med(backend, monkeypatch):
    '''Fail as soon as data is received'''
    data = b'hello there, let us see whats going on'
    key = 'borg'

    # Monkeypatch request handler to produce 3 errors
    handler_class = mock_server.S3CRequestHandler

    def do_PUT(self, real_PUT=handler_class.do_PUT, count=[0]):  # noqa: B006
        count[0] += 1
        # Note: every time we return an error, the request will be retried
        # *twice*: once because of the error, and a second time because the
        # connection has been closed by the server.
        if count[0] > 2:
            return real_PUT(self)
        else:
            self.send_error(503, code='OperationAborted')

            # Since we don't read all the data, we have to close
            # the connection
            self.close_connection = True

    monkeypatch.setattr(handler_class, 'do_PUT', do_PUT)
    with pytest.raises(OperationAbortedError):
        await backend.store(key, data)

    enable_temp_fail(backend)
    await backend.store(key, data)


@pytest.mark.trio
@pytest.mark.with_backend('s3c/aes+zlib', require_mock_server=True)
async def test_put_s3error_late(backend, monkeypatch):
    '''Fail after reading all data'''
    data = b'hello there, let us see whats going on'
    key = 'borg'

    # Monkeypatch request handler to produce 3 errors
    handler_class = mock_server.S3CRequestHandler

    def do_PUT(self, real_PUT=handler_class.do_PUT, count=[0]):  # noqa: B006
        count[0] += 1
        if count[0] > 3:
            return real_PUT(self)
        else:
            self.rfile.read(int(self.headers['Content-Length']))
            self.send_error(503, code='OperationAborted')

    monkeypatch.setattr(handler_class, 'do_PUT', do_PUT)
    with pytest.raises(OperationAbortedError):
        await backend.store(key, data)

    enable_temp_fail(backend)
    await backend.store(key, data)


@pytest.mark.trio
@pytest.mark.with_backend('s3c/aes+zlib', require_mock_server=True)
async def test_issue58(backend, monkeypatch):
    '''Send error while client is sending data'''

    # Monkeypatch request handler
    handler_class = mock_server.S3CRequestHandler

    def do_PUT(self, real=handler_class.do_PUT, count=[0]):  # noqa: B006
        count[0] += 1
        if count[0] > 1:
            return real(self)

        # Read half the data
        self.rfile.read(min(BUFSIZE, int(self.headers['Content-Length']) // 2))

        # Then generate an error and close the connection
        self.send_error(401, code='MalformedXML')
        self.close_connection = True

    monkeypatch.setattr(handler_class, 'do_PUT', do_PUT)

    # Write a big object. We need to write random data, or
    # compression while make the payload too small
    with open('/dev/urandom', 'rb') as rnd:
        buf = rnd.read(5 * BUFSIZE)
    with pytest.raises(S3Error) as exc_info:
        await backend.store('borg', buf)
    assert exc_info.value.code == 'MalformedXML'

    enable_temp_fail(backend)
    await backend.store('borg', buf)


@pytest.mark.trio
@pytest.mark.with_backend('s3c/aes+zlib', require_mock_server=True)
async def test_issue58_b(backend, monkeypatch):
    '''Close connection while client is sending data'''

    # Monkeypatch request handler
    handler_class = mock_server.S3CRequestHandler

    def do_PUT(self, real=handler_class.do_PUT, count=[0]):  # noqa: B006
        count[0] += 1
        if count[0] > 1:
            return real(self)

        # Read half the data
        self.rfile.read(min(BUFSIZE, int(self.headers['Content-Length']) // 2))

        # Then close the connection silently
        self.close_connection = True

    monkeypatch.setattr(handler_class, 'do_PUT', do_PUT)

    # Write a big object. We need to write random data, or
    # compression while make the payload too small
    with open('/dev/urandom', 'rb') as rnd:
        buf = rnd.read(5 * BUFSIZE)
    with pytest.raises(ConnectionClosed):
        await backend.store('borg', buf)

    enable_temp_fail(backend)
    await backend.store('borg', buf)


@pytest.mark.trio
@pytest.mark.with_backend('gs/aes+zlib', require_mock_server=True)
async def test_expired_token_get(backend, monkeypatch):
    '''Test handling of expired OAuth token'''

    key = 'borg'
    data = b'hello there, let us see whats going on'

    # Monkeypatch backend class to check if token is refreshed
    token_refreshed = False

    def _get_access_token(self):
        nonlocal token_refreshed
        token_refreshed = True
        self.access_token[self.password] = 'foobar'

    monkeypatch.setattr(gs.AsyncBackend, '_get_access_token', _get_access_token)

    # Store some data
    await backend.store(key, data)

    # Monkeypatch request handler to produce error
    handler_class = mock_server.GSRequestHandler

    def do_GET(self, real=handler_class.do_GET, count=[0]):  # noqa: B006
        count[0] += 1
        if count[0] > 1:
            return real(self)
        else:
            self.send_error(401, code='AuthenticationRequired')

    monkeypatch.setattr(handler_class, 'do_GET', do_GET)

    token_refreshed = False
    assert (await backend.fetch(key))[0] == data
    assert token_refreshed


@pytest.mark.trio
@pytest.mark.with_backend('gs/aes+zlib', require_mock_server=True)
async def test_expired_token_put(backend, monkeypatch):
    '''Test handling of expired OAuth token'''

    key = 'borg'
    data = b'hello there, let us see whats going on'

    # Monkeypatch backend class to check if token is refreshed
    token_refreshed = False

    def _get_access_token(self):
        nonlocal token_refreshed
        token_refreshed = True
        self.access_token[self.password] = 'foobar'

    monkeypatch.setattr(gs.AsyncBackend, '_get_access_token', _get_access_token)

    # Monkeypatch request handler to produce error
    handler_class = mock_server.GSRequestHandler

    def do_PUT(self, real=handler_class.do_PUT, count=[0]):  # noqa: B006
        count[0] += 1
        if count[0] > 1:
            return real(self)
        else:
            self.rfile.read(int(self.headers['Content-Length']))
            self.send_error(401, code='AuthenticationRequired')

    monkeypatch.setattr(handler_class, 'do_PUT', do_PUT)

    token_refreshed = False
    await backend.store(key, data)
    assert token_refreshed


@pytest.mark.trio
@pytest.mark.with_backend('s3c/aes+zlib', require_mock_server=True)
async def test_conn_abort(backend, monkeypatch):
    '''Close connection while sending data'''

    data = b'hello there, let us see whats going on'
    key = 'borg'
    await backend.store(key, data)

    # Monkeypatch request handler
    handler_class = mock_server.S3CRequestHandler

    def send_data(self, data, count=[0]):  # noqa: B006
        count[0] += 1
        if count[0] >= 3:
            self.wfile.write(data)
        else:
            self.wfile.write(data[: len(data) // 2])
            self.close_connection = True

    monkeypatch.setattr(handler_class, 'send_data', send_data)

    with pytest.raises(ConnectionClosed):
        await backend.fetch(key)

    # Since we have disabled retry on failure, the connection is not automatically
    # reset after the error.
    backend.backend.conn.reset()

    enable_temp_fail(backend)
    assert (await backend.fetch(key))[0] == data


@pytest.mark.trio
@pytest.mark.with_backend('local/{aes+zlib,zlib,plain}')
async def test_compression_threshold_boundaries(backend: AsyncComprencBackend):
    '''Test data integrity when compressing data below and above thresholds'''

    compression_algo = backend.compression[0] or 'None'
    threshold = SYNC_COMPRESSION_THRESHOLD[compression_algo]

    # Test data sizes: below threshold and above threshold
    sizes = [int(threshold * 2 / 3), int(threshold * 4 / 3)]

    for size in sizes:
        key = f'test_key_{compression_algo}_{size}'
        data = (b'a' * 100 + b'b' * 100) * (size // 200 + 1)
        data = data[:size]
        metadata: BasicMappingT = {'test': 'metadata', 'size': size}

        await backend.store(key, data, metadata)

        fetched_data, fetched_metadata = await backend.fetch(key)
        assert fetched_data == data
        assert fetched_metadata == metadata


@pytest.mark.trio
@pytest.mark.with_backend('local/{aes+zlib,zlib,plain}')
async def test_decompression_threshold_boundaries(backend: AsyncComprencBackend):
    '''Test decompression works correctly below and above thresholds'''

    compression_algo = backend.compression[0]
    if compression_algo is None:
        compression_key = 'None'
    else:
        compression_key = compression_algo.upper()

    threshold = SYNC_DECOMPRESSION_THRESHOLD[compression_key]

    # Test data sizes: below threshold and above threshold
    sizes = [int(threshold * 2 / 3), int(threshold * 4 / 3)]

    for size in sizes:
        key = f'test_key_decomp_{compression_key}_{size}'
        data = (b'x' * 100 + b'y' * 100) * (size // 200 + 1)
        data = data[:size]

        metadata: BasicMappingT = {'test': 'decompression', 'size': size}

        await backend.store(key, data, metadata)

        fetched_data, fetched_metadata = await backend.fetch(key)
        assert fetched_data == data
        assert fetched_metadata == metadata
