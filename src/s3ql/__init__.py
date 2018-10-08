'''
__init__.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

# First, enable warnings. This should happen before any imports,
# so that we can catch warnings emitted by code that runs during
# the import.
# Sadly, we cannot simply use $PYTHONWARNINGS, because it
# does not allow regexes (cf https://bugs.python.org/issue34920)
import os
if 'S3QL_ENABLE_WARNINGS' in os.environ:
    import warnings
    warnings.resetwarnings()

    # Not sure what this is or what causes it, bug the internet
    # is full of similar reports so probably a false positive.
    warnings.filterwarnings(
        action='ignore', category=ImportWarning,
        message="can't resolve package from __spec__ or __package__, falling "
        "back on __name__ and __path__")

    for cat in (DeprecationWarning, PendingDeprecationWarning):
        warnings.filterwarnings(action='default', category=cat,
                                module='^s3ql', append=True)
        warnings.filterwarnings(action='ignore', category=cat, append=True)
    warnings.filterwarnings(action='default', append=True)

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

VERSION = '2.31'
RELEASE = '%s' % VERSION

# TODO: On next revision bump, remove upgrade code from backend/comprenc.py and
# backends/s3c.py (activated by UPGRADE_MODE variable).
CURRENT_FS_REV = 24

# Buffer size when writing objects
BUFSIZE = 64 * 1024

# Name and inode of the special s3ql control file
CTRL_NAME = '.__s3ql__ctrl__'
CTRL_INODE = 2

# Maps file system revisions to the last S3QL version that
# supported this revision.
REV_VER_MAP = {
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
