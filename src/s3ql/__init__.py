'''
__init__.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function

__all__ = [ 'block_cache', "common", 'daemonize', 'database', 'fs', 'fsck', 'libc_api',
            'libc', "mkfs", 'multi_lock', 'argparse', 'ordered_dict', 'VERSION', 
            'CURRENT_FS_REV', 'thread_group', 'upload_manager' ]

VERSION = '0.22'
CURRENT_FS_REV = 9
