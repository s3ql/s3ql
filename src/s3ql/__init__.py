'''
__init__.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import division, print_function

__all__ = [ 'adm', 'backends', 'block_cache', 'cleanup_manager', 'common',
            'cp', 'ctrl', 'daemonize', 'database', 'deltadump', '_deltadump',
            'fs', 'fsck', 'inode_cache', 'lock', 'mkfs', 'mount', 'ordered_dict',
            'parse_args', 'remove', 'statfs', 'umount', 'VERSION', 
            'CURRENT_FS_REV', 'REV_VER_MAP' ]

VERSION = '1.7'
CURRENT_FS_REV = 14

# Maps file system revisions to the last S3QL version that
# supported this revision.
REV_VER_MAP = { 14: '1.7',
                13: '1.6',
                12: '1.3',
                11: '1.0.1' }
