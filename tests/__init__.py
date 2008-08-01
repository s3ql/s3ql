#!/usr/bin/env python
#
#    Copyright (C) 2008  Nikolaus Rath <Nikolaus@rath.org>
#
#    This program can be distributed under the terms of the GNU LGPL.
#


class TestCase(object):
    """Represents a test case with a shared environment.

    Classes derived from this class should be instantiated only
    once and all included tests should be ran using the same
    instance.
    """

    def destroy(self):
        pass


# Assertions
def assert_true(res):
    if not res:
        raise AssertionError

def assert_raises(exc, fn, *a, **kw):
    try:
        fn(*a, **kw)
    except exc:
        return
    except:
        raise AssertionError, "Expression raised %s rather than %s" \
            % (sys.exc_info()[1], exc)

    else:
        raise AssertionError, "Expression did not raise %s" \
            % exc

def assert_equals(e1, e2):
    if e1 == e2:
        return

    raise AssertionError, "%s != %s" % (e1, e2)


from tests.s3 import *
from tests.fuse import *
from tests.fs import *
