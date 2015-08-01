'''
__init__.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

# We must not import s3ql.logging.logging as s3ql.logging,
# otherwise future imports of s3ql.logging will incorrectly
# use s3ql.logging.logging.
from . import logging # Ensure use of custom logger class
from llfuse import ROOT_INODE

# False positives, pylint doesn't grok that these are module names
#pylint: disable=E0603
__all__ = [ 'adm', 'backends', 'block_cache', 'common', 'calc_mro',
            'cp', 'ctrl', 'daemonize', 'database', 'deltadump', 'fs',
            'fsck', 'inherit_docstrings', 'inode_cache', 'lock',
            'logging', 'metadata', 'mkfs', 'mount', 'parse_args',
            'remove', 'statfs', 'umount', 'VERSION', 'CURRENT_FS_REV',
            'REV_VER_MAP', 'RELEASE', 'BUFSIZE',
            'CTRL_NAME', 'CTRL_INODE' ]

VERSION = '2.14'
RELEASE = '%s' % VERSION

# TODO: On next upgrade, remove pickle support from
# common.py:load_params().
CURRENT_FS_REV = 22

# Buffer size when writing objects
BUFSIZE = 64 * 1024

# Name and inode of the special s3ql control file
CTRL_NAME = '.__s3ql__ctrl__'
CTRL_INODE = 2

# Maps file system revisions to the last S3QL version that
# supported this revision.
REV_VER_MAP = {
    21: '2.13',
    20: '2.9',
    16: '1.15',
    15: '1.10',
    14: '1.8.1',
    13: '1.6',
    12: '1.3',
    11: '1.0.1',
    }
