#!/usr/bin/env python3

import sys
import os.path

basedir = os.path.abspath(os.path.join(os.path.dirname(sys.argv[0]), '..'))
sys.path = [os.path.join(basedir, 'src')] + sys.path

# Override pyfuse3.invalidate_inode : Drop kernel dentries and inodes cache
# just before calling pyfuse3.invalidate_inode
import pyfuse3
from _pytest.monkeypatch import MonkeyPatch
pyfuse3_invalidate_inode = pyfuse3.invalidate_inode
def patched_pyfuse3_invalidate_inode(inode):
    # echo 2 > /proc/sys/vm/drop_caches : Drop kernel dentries and inodes cache
    with open("/proc/sys/vm/drop_caches", "w") as drop_caches:
        drop_caches.write("2\n")
    pyfuse3_invalidate_inode(inode)
MonkeyPatch().setattr('pyfuse3.invalidate_inode', patched_pyfuse3_invalidate_inode)

import s3ql.mount
s3ql.mount.main(sys.argv[1:])