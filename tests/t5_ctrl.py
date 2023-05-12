#!/usr/bin/env python3
'''
t5_ctrl.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

if __name__ == '__main__':
    import sys

    import pytest

    sys.exit(pytest.main([__file__] + sys.argv[1:]))

import os
import sys
import time

import t4_fuse

import s3ql.ctrl


class TestCtrl(t4_fuse.TestFuse):
    def test(self):
        self.mkfs()
        self.mount()
        self.tst_ctrl_flush()
        self.tst_ctrl_drop()
        self.tst_ctrl_log()
        self.tst_ctrl_cachesize()
        self.tst_backup_metadata()
        self.umount()
        self.fsck()

    def tst_backup_metadata(self):
        cnt1 = len([x for x in os.listdir(self.backend_dir) if x.startswith('s3ql_params')])

        # First call just finalizes the already increased sequence number
        s3ql.ctrl.main(['backup-metadata', self.mnt_dir])
        time.sleep(1)
        cnt2 = len([x for x in os.listdir(self.backend_dir) if x.startswith('s3ql_params')])
        assert cnt2 == cnt1

        # Second call creates a new one
        s3ql.ctrl.main(['backup-metadata', self.mnt_dir])
        time.sleep(1)
        cnt2 = len([x for x in os.listdir(self.backend_dir) if x.startswith('s3ql_params')])
        assert cnt2 == cnt1 + 1

    def tst_ctrl_flush(self):
        try:
            s3ql.ctrl.main(['flushcache', self.mnt_dir])
        except:
            sys.excepthook(*sys.exc_info())
            pytest.fail("s3qlctrl raised exception")

    def tst_ctrl_drop(self):
        try:
            s3ql.ctrl.main(['dropcache', self.mnt_dir])
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
