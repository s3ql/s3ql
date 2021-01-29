#!/usr/bin/env python3
'''
This file reproduces mount.s3ql, with pyfuse3.invalidate_inode patched
for the t5_cp.py::TestCp::test_cp_inode_invalidate test.
'''

import sys
import os.path

basedir = os.path.abspath(os.path.join(os.path.dirname(sys.argv[0]), '..'))
sys.path = [os.path.join(basedir, 'src')] + sys.path

# Override pyfuse3.invalidate_inode : Drop kernel dentries and inodes cache
# just before calling pyfuse3.invalidate_inode
import pyfuse3
pyfuse3_invalidate_inode = pyfuse3.invalidate_inode
def patched_pyfuse3_invalidate_inode(inode):
    # echo 2 > /proc/sys/vm/drop_caches : Drop kernel dentries and inodes cache
    with open("/proc/sys/vm/drop_caches", "w") as drop_caches:
        drop_caches.write("2\n")
    pyfuse3_invalidate_inode(inode)
pyfuse3.invalidate_inode = patched_pyfuse3_invalidate_inode

import s3ql.mount
s3ql.mount.main(sys.argv[1:])