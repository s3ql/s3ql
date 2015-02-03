'''
t1_safe_unpickle.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2014 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

if __name__ == '__main__':
    import pytest
    import sys
    sys.exit(pytest.main([__file__] + sys.argv[1:]))

from s3ql.backends.common import safe_unpickle
from s3ql import PICKLE_PROTOCOL
from pytest import raises as assert_raises
import pickle
import os
import pytest
import subprocess

class CustomClass(object):
    pass

def test_elementary():
    for obj in ('a string', b'bytes', 42, 23.222,
                bytearray(b'ascii rules')):
        buf = pickle.dumps(obj, PICKLE_PROTOCOL)
        safe_unpickle(buf)

def test_composite():
    elems = ('a string', b'bytes', 42, 23.222)

    # Tuple
    buf = pickle.dumps(elems, PICKLE_PROTOCOL)
    safe_unpickle(buf)

    # Dict
    buf = pickle.dumps(dict(x for x in enumerate(elems)),
                       PICKLE_PROTOCOL)
    safe_unpickle(buf)

    # List
    buf = pickle.dumps(list(elems), PICKLE_PROTOCOL)
    safe_unpickle(buf)

    # Set
    buf = pickle.dumps(set(elems), PICKLE_PROTOCOL)
    safe_unpickle(buf)

    # Frozenset
    buf = pickle.dumps(frozenset(elems), PICKLE_PROTOCOL)
    safe_unpickle(buf)

def test_python2():
    py2_prog = ('import cPickle as pickle\n'
                'print(pickle.dumps([42, "bytes", u"unicode", '
                'b"more bytes", u"more unicode"], 2))')
    try:
        buf = subprocess.check_output(['python', '-c', py2_prog])
    except FileNotFoundError:
        pytest.skip('failed to execute python2')

    assert safe_unpickle(buf, encoding='latin1') == \
        [ 42, 'bytes', 'unicode', 'more bytes', 'more unicode']

@pytest.mark.parametrize("obj", [CustomClass, CustomClass(), pickle.Pickler,
                                  os.remove])
def test_bad(obj):
    buf = pickle.dumps(obj, PICKLE_PROTOCOL)
    assert_raises(pickle.UnpicklingError, safe_unpickle, buf)

def test_wrong_proto():
    buf = pickle.dumps(b'foo', 3)
    assert_raises(pickle.UnpicklingError, safe_unpickle, buf)
