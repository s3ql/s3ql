'''
__init__.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function

__all__ = [ 'backends', 'cli', 'parse_args', 'block_cache', "common", 'daemonize', 
            'database', 'fs', 'fsck', 'multi_lock', 'ordered_dict', 'thread_group',
            'upload_manager', 'VERSION', 'CURRENT_FS_REV' ]

VERSION = '1.0.1'
CURRENT_FS_REV = 11
