'''
__init__.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''


# (We must not import s3ql.logging.logging as s3ql.logging,
# otherwise future imports of s3ql.logging will incorrectly
# use s3ql.logging.logging).
from . import logging

assert logging.LOG_ONCE  # prevent warnings about unused module

from pyfuse3 import ROOT_INODE

VERSION = '5.1.3'
RELEASE = '%s' % VERSION

# When bumping this up, figure out how database.py's expire_objects() should deal with older
# filesystem revisions.
CURRENT_FS_REV = 26

# Buffer size when writing objects
BUFSIZE = 256 * 1024

# Name and inode of the special s3ql control file
CTRL_NAME = '.__s3ql__ctrl__'
CTRL_INODE = ROOT_INODE + 1

# Maps file system revisions to the last S3QL version that
# supported this revision.
REV_VER_MAP = {
    25: '4.0.0',
    24: '3.8.1',
    23: '2.26',
    22: '2.16',
    21: '2.13',
    20: '2.9',
    16: '1.15',
    15: '1.10',
    14: '1.8.1',
    13: '1.6',
    12: '1.3',
    11: '1.0.1',
}
