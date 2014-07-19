#!/usr/bin/env python3
'''
t5_ctrl.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright © 2008 Nikolaus Rath <Nikolaus.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

if __name__ == '__main__':
    import pytest
    import sys
    sys.exit(pytest.main([__file__] + sys.argv[1:]))

import s3ql.ctrl
import sys
import t4_fuse

class CtrlTests(t4_fuse.fuse_tests):

    def runTest(self):
        self.mkfs()
        self.mount()
        self.tst_ctrl_flush()
        self.umount()
        self.fsck()

    def tst_ctrl_flush(self):

        try:
            s3ql.ctrl.main(['flushcache', self.mnt_dir])
        except:
            sys.excepthook(*sys.exc_info())
            self.fail("s3qlctrl raised exception")
