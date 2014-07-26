'''
__init__.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from .logging import logging
from llfuse import ROOT_INODE

# False positives, pylint doesn't grok that these are module names
#pylint: disable=E0603
__all__ = [ 'adm', 'backends', 'block_cache', 'common', 'calc_mro',
            'cp', 'ctrl', 'daemonize', 'database', 'deltadump', 'fs',
            'fsck', 'inherit_docstrings', 'inode_cache', 'lock',
            'logging', 'metadata', 'mkfs', 'mount', 'parse_args',
            'remove', 'statfs', 'umount', 'VERSION', 'CURRENT_FS_REV',
            'REV_VER_MAP', 'RELEASE', 'BUFSIZE', 'PICKLE_PROTOCOL',
            'CTRL_NAME', 'CTRL_INODE' ]

VERSION = '2.10rc'
RELEASE = '%s' % VERSION

CURRENT_FS_REV = 21

# Buffer size when writing objects
BUFSIZE = 64 * 1024

# Pickle protocol version to use.
PICKLE_PROTOCOL = 2

# Name and inode of the special s3ql control file
CTRL_NAME = '.__s3ql__ctrl__'
CTRL_INODE = 2


# Maps file system revisions to the last S3QL version that
# supported this revision.
REV_VER_MAP = {
    20: '2.9',
    16: '1.15',
    15: '1.10',
    14: '1.8.1',
    13: '1.6',
    12: '1.3',
    11: '1.0.1',
    }
