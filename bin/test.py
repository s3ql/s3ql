#!/usr/bin/env python
'''
$Id$

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import unicode_literals, division, print_function

import sys
import os.path

# We are running from the S3QL source directory, make sure
# that we use modules from this directory
basedir = os.path.abspath(os.path.join(os.path.dirname(sys.argv[0]), '..'))
if (os.path.exists(os.path.join(basedir, 'setup.py')) and
    os.path.exists(os.path.join(basedir, 'src', 's3ql', '__init__.py'))):
    sys.path = [os.path.join(basedir, 'src')] + sys.path
    
import llfuse
from llfuse.example import XmpOperations
import logging

root_logger = logging.getLogger()
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s,%(msecs)03d %(threadName)s: '
                                                     '[%(name)s] %(message)s',
                                                     datefmt="%H:%M:%S")) 
root_logger.addHandler(handler)
root_logger.setLevel(logging.DEBUG)    
        
mountpoint = b'/home/nikratio/tmp/mnt'
server = llfuse.Server(XmpOperations(), mountpoint, [])
server.main(True, True)

