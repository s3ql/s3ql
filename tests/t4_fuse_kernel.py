'''
$Id: t4_fuse.py 372 2009-12-29 16:36:39Z nikratio $

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import unicode_literals, division, print_function

import unittest



class fuse_kernel_tests(unittest.TestCase):
    pass

    # FIXME: Check if FUSE does not allow us to move a directory inside
    # itself.
    
    # FIXME: Check that FUSE does not allow creation of directory
    # entries during rmdir of the directory