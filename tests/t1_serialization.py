#!/usr/bin/env python3
'''
t1_serialization.py - this file is part of S3QL.

Copyright © 2014 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

if __name__ == '__main__':
    import pytest
    import sys
    sys.exit(pytest.main([__file__] + sys.argv[1:]))

from s3ql.common import ThawError, freeze_basic_mapping, thaw_basic_mapping
from s3ql.backends.common import checksum_basic_mapping
import pytest
from collections import OrderedDict


def test_simple():
    d = { 'hello': 42,
          'world': True,
          'data': b'fooxyz',
          'str': 'nice' }

    assert thaw_basic_mapping(freeze_basic_mapping(d)) == d

def test_wrong_key():
    d = { 'hello': 42,
          True: False }

    with pytest.raises(ValueError):
        freeze_basic_mapping(d)

def test_cmplx_value():
    d = { 'hello': [1,2] }

    with pytest.raises(ValueError):
        freeze_basic_mapping(d)

def test_thaw_errors():
    buf = freeze_basic_mapping({ 'hello': 'world' })

    for s in (buf[1:], buf[:-1],
              b'"foo"[2]', b'open("/dev/null", "r")',
              b'"foo".__class__'):
        with pytest.raises(ThawError):
            thaw_basic_mapping(s)

def test_checksum():
    d1 = OrderedDict()
    d2 = OrderedDict()

    d1['foo'] = 1
    d1['bar'] = None

    d2['bar'] = None
    d2['foo'] = 1

    assert list(d1.keys()) != list(d2.keys())
    assert checksum_basic_mapping(d1) == checksum_basic_mapping(d2)

    d2['foo'] += 1

    assert checksum_basic_mapping(d1) != checksum_basic_mapping(d2)

def test_checksum_bytes():
    d1 = OrderedDict()
    d2 = OrderedDict()

    d1['foo'] = b'foo'
    d2['foo'] = b'f0o'

    assert checksum_basic_mapping(d1) != checksum_basic_mapping(d2)
