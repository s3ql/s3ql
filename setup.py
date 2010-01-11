#!/usr/bin/env python
'''
$Id$

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function
    
from distutils.core import setup
setup(name='s3ql',
      version='alpha3',
      description='a FUSE filesystem that stores data on Amazon S3',
      author='Nikolaus Rath',
      author_email='Nikolaus@rath.org',
      url='http://code.google.com/p/s3ql/',
      package_dir={'': 'src'},
      packages=['s3ql', 'llfuse'],
      provides=['s3ql'],
      scripts=[ 'bin/fsck.s3ql',
                'bin/mkfs.s3ql',
                'bin/mount.s3ql_local',
                'bin/mount.s3ql' ],
      requires=['apsw', 'boto', 'pycryptopp' ],
     )
