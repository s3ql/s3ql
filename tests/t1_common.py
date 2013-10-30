'''
t1_common.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from s3ql.common import iter_values
from collections import OrderedDict


def test_itervalues():
    d = OrderedDict()
    for i in range(5):
        d[i] = i

    for v in iter_values(d):
        if v == 1:
            del d[1]
            del d[2]

            

