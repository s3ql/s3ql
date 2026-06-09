#!/usr/bin/env python3
'''
t1_config.py - this file is part of S3QL.

Copyright © 2024 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.

Unit tests for the configuration primitives in `s3ql.authinfo` and the typed backend openers in
`s3ql.backends`.
'''

if __name__ == '__main__':
    import sys

    import pytest

    sys.exit(pytest.main([__file__] + sys.argv[1:]))

import os
import shutil
import stat
import tempfile

import pytest
from pytest import raises as assert_raises

from s3ql.authinfo import (
    DEFAULT_CACHEDIR,
    Authinfo,
    CompressAlgorithm,
    CompressSpec,
    pick,
)
from s3ql.backends import local, open_raw_backend_v2
from s3ql.logging import QuietError


def write_authinfo(tmp_path, text: str) -> str:
    path = os.path.join(tmp_path, 'authinfo2')
    with open(path, 'w') as fh:
        fh.write(text)
    os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)
    return path


@pytest.fixture()
def tmp_path():
    path = tempfile.mkdtemp(prefix='s3ql-cfg-test-')
    yield path
    shutil.rmtree(path)


# --- parse_compression -------------------------------------------------------------------------


def test_parse_compression_default_level():
    spec = CompressSpec.parse('lzma')
    assert spec == CompressSpec(algorithm=CompressAlgorithm.LZMA, level=6)


def test_parse_compression_explicit_level():
    spec = CompressSpec.parse('zlib-3')
    assert spec.algorithm is CompressAlgorithm.ZLIB
    assert spec.level == 3


def test_parse_compression_none():
    spec = CompressSpec.parse('none')
    assert spec.algorithm is CompressAlgorithm.NONE
    assert spec.to_comprenc() == (None, 6)


def test_parse_compression_roundtrip():
    assert CompressSpec.parse('lzma-6').to_comprenc() == ('lzma', 6)
    assert CompressSpec.parse('bzip2-1').to_comprenc() == ('bzip2', 1)
    assert CompressSpec.parse('none-4').to_comprenc() == (None, 4)


def test_parse_compression_malformed():
    with assert_raises(QuietError) as exc:
        CompressSpec.parse('lzma!bad')
    assert exc.value.exitcode == 2


def test_parse_compression_unknown_algorithm():
    with assert_raises(QuietError) as exc:
        CompressSpec.parse('bogus')
    assert exc.value.exitcode == 2


# --- pick --------------------------------------------------------------------------------------


def test_pick_none_selects_config():
    assert pick(None, 32) == 32
    assert pick(None, 'cfg') == 'cfg'


def test_pick_falsy_but_set_selects_cli():
    assert pick(0, 32) == 0
    assert pick('', 'cfg') == ''


def test_pick_value_selects_cli():
    assert pick('cli', 'cfg') == 'cli'


# --- Authinfo / read_authinfo2 -----------------------------------------------------------------


def test_authinfo_defaults_when_absent():
    info = Authinfo.from_file('/does/not/exist/authinfo2', 'local:///foo')
    assert info.backend_login is None
    assert info.backend_password is None
    assert info.fs_passphrase is None
    assert info.backend_options == ''
    assert info.cachedir == DEFAULT_CACHEDIR
    assert info.max_connections == 32
    assert info.max_threads is None
    assert info.compress == CompressSpec(algorithm=CompressAlgorithm.LZMA, level=6)


def test_authinfo_section_matching_and_precedence(tmp_path):
    path = write_authinfo(
        tmp_path,
        '[a]\n'
        'storage-url: local:///foo\n'
        'max-connections: 5\n'
        '\n'
        '[b]\n'
        'storage-url: local:///foo/bar\n'
        'max-connections: 9\n'
        '\n'
        '[c]\n'
        'storage-url: s3://other\n'
        'max-connections: 99\n',
    )
    # Both [a] and [b] match; the later section wins. [c] does not match.
    info = Authinfo.from_file(path, 'local:///foo/bar')
    assert info.max_connections == 9

    # Only [a] matches here.
    info = Authinfo.from_file(path, 'local:///foo')
    assert info.max_connections == 5


def test_authinfo_hyphenated_keys_map_to_fields(tmp_path):
    path = write_authinfo(
        tmp_path,
        '[a]\n'
        'storage-url: local:///foo\n'
        'backend-login: theuser\n'
        'backend-password: thepw\n'
        'max-connections: 7\n'
        'max-threads: 3\n',
    )
    info = Authinfo.from_file(path, 'local:///foo')
    assert info.backend_login == 'theuser'
    assert info.backend_password == 'thepw'
    assert info.max_connections == 7
    assert info.max_threads == 3


def test_authinfo_unknown_key_rejected(tmp_path):
    path = write_authinfo(
        tmp_path,
        '[a]\nstorage-url: local:///foo\ncachesize: 12\n',
    )
    with assert_raises(QuietError) as exc:
        Authinfo.from_file(path, 'local:///foo')
    assert exc.value.exitcode == 2
    assert 'cachesize' in str(exc.value)


def test_authinfo_bad_compress_rejected_at_read_time(tmp_path):
    path = write_authinfo(
        tmp_path,
        '[a]\nstorage-url: local:///foo\ncompress: bogus\n',
    )
    with assert_raises(QuietError) as exc:
        Authinfo.from_file(path, 'local:///foo')
    assert exc.value.exitcode == 2


def test_authinfo_compress_parsed(tmp_path):
    path = write_authinfo(
        tmp_path,
        '[a]\nstorage-url: local:///foo\ncompress: zlib-1\n',
    )
    info = Authinfo.from_file(path, 'local:///foo')
    assert info.compress == CompressSpec(algorithm=CompressAlgorithm.ZLIB, level=1)


def test_authinfo_insecure_permissions_rejected(tmp_path):
    path = os.path.join(tmp_path, 'authinfo2')
    with open(path, 'w') as fh:
        fh.write('[a]\nstorage-url: local:///foo\n')
    os.chmod(path, stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP)
    with assert_raises(QuietError) as exc:
        Authinfo.from_file(path, 'local:///foo')
    assert exc.value.exitcode == 12


# --- open_raw_backend_v2 -----------------------------------------------------------------------


def _default_authinfo() -> Authinfo:
    return Authinfo.from_file('/does/not/exist/authinfo2', 'local:///foo')


@pytest.mark.trio
async def test_open_raw_backend_v2_unknown_scheme():
    with assert_raises(QuietError) as exc:
        await open_raw_backend_v2('nosuch://foo', _default_authinfo())
    assert exc.value.exitcode == 11


@pytest.mark.trio
async def test_open_raw_backend_v2_unparseable_url():
    with assert_raises(QuietError) as exc:
        await open_raw_backend_v2('not-a-storage-url', _default_authinfo())
    assert exc.value.exitcode == 2


@pytest.mark.trio
async def test_open_raw_backend_v2_unknown_suboption(tmp_path):
    storage_url = 'local://' + tmp_path
    with assert_raises(QuietError) as exc:
        await open_raw_backend_v2(storage_url, _default_authinfo(), backend_options='badoption')
    assert exc.value.exitcode == 3


@pytest.mark.trio
async def test_open_raw_backend_v2_valid(tmp_path):
    storage_url = 'local://' + tmp_path
    backend = await open_raw_backend_v2(storage_url, _default_authinfo())
    try:
        assert isinstance(backend, local.AsyncBackend)
    finally:
        await backend.close()
