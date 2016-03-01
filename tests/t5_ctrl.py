#!/usr/bin/env python3
'''
t5_ctrl.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

if __name__ == '__main__':
    import pytest
    import sys
    sys.exit(pytest.main([__file__] + sys.argv[1:]))

import s3ql.ctrl
import sys
import t4_fuse

class TestCtrl(t4_fuse.TestFuse):

    def test(self):
        self.mkfs()
        self.mount()
        self.tst_ctrl_flush()
        self.tst_ctrl_log()
        self.tst_ctrl_cachesize()
        self.umount()
        self.fsck()

    def tst_ctrl_flush(self):
        try:
            s3ql.ctrl.main(['flushcache', self.mnt_dir])
        except:
            sys.excepthook(*sys.exc_info())
            pytest.fail("s3qlctrl raised exception")

    def tst_ctrl_log(self):
        try:
            s3ql.ctrl.main(['log', self.mnt_dir, 'warn'])
            s3ql.ctrl.main(['log', self.mnt_dir, 'debug', 's3ql', 'dugong'])
            s3ql.ctrl.main(['log', self.mnt_dir, 'info'])
        except:
            sys.excepthook(*sys.exc_info())
            pytest.fail("s3qlctrl raised exception")

    def tst_ctrl_cachesize(self):
        try:
            s3ql.ctrl.main(['cachesize', self.mnt_dir, '10240'])
        except:
            sys.excepthook(*sys.exc_info())
            pytest.fail("s3qlctrl raised exception")
