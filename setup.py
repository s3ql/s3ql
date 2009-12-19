#!/usr/bin/env python

import sys

if sys.version_info[0] < 2 or \
    (sys.version_info[0] == 2 and sys.version_info[1] < 6):
    sys.stderr.write('Python version too old, must be between 2.6.0 and 3.0!\n') 
    sys.exit(1)
    
if sys.version_info[0] > 2:
    sys.stderr.write('Python version too new, must be between 2.6.0 and 3.0!\n')
    sys.exit(1)
    
    
from distutils.core import setup

setup(name='s3ql',
      version='1.0',
      description='a FUSE filesystem that stores data on Amazon S3',
      author='Nikolaus Rath',
      author_email='Nikolaus@rath.org',
      url='http://code.google.com/p/s3ql/',
      package_dir={'': 'src'},
      packages=['s3ql'],
      provides=['s3ql'],
      scripts=[ 'bin/fsck.s3ql',
                'bin/mkfs.s3ql',
                'bin/mount.s3ql_local',
                'bin/mount.s3ql' ],
      requires=['apsw', 'boto'],
     )
