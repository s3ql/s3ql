'''
$Id: t4_fuse.py 372 2009-12-29 16:36:39Z nikratio $

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function

import unittest
from _common import TestCase 

class fuse_kernel_tests(TestCase):
    pass

    # FIXME: Check if FUSE does not allow us to move a directory inside
    # itself.
    
    # FIXME: Check that FUSE does not allow creation of directory
    # entries during rmdir of the directory
    
    # FIXME: Check that FUSE does not allow to change the type of
    # a directory entry
    
    # FIXME: Check that FUSE does not send rmdir(), unlink(), mkdir(),
    # mknod(), symlink(), link() or create() requests
    # for a directory entries passed to a running rename() request.
    

# Somehow important according to pyunit documentation
def suite():
    return unittest.makeSuite(fuse_kernel_tests)


# Allow calling from command line
if __name__ == "__main__":
    unittest.main()