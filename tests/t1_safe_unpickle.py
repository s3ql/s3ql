'''
t1_safe_unpickle.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2014 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import division, print_function, absolute_import

import unittest2 as unittest
import cPickle as pickle
from s3ql.common import safe_unpickle


class CustomClass(object):
    pass

class TestSafeUnpickle(unittest.TestCase):

    def test_elementary(self):
        for obj in ('a string', u'unicode', 42, 23.222):
            buf = pickle.dumps(obj)
            safe_unpickle(buf)

    def test_composite(self):
        elems = ('a string', u'unicode', 42, 23.222)

        # Tuple
        buf = pickle.dumps(elems)
        safe_unpickle(buf)

        # Dict
        buf = pickle.dumps(dict(x for x in enumerate(elems)))
        safe_unpickle(buf)

        # List
        buf = pickle.dumps(list(elems))
        safe_unpickle(buf)

    def test_bad(self):
        import os
        import codecs
        for obj in (CustomClass, CustomClass(), pickle.Pickler, os.remove,
                    codecs.encode):
            buf = pickle.dumps(obj)
            self.assertRaises(pickle.UnpicklingError, safe_unpickle, buf)
