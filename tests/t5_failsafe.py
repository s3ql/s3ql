#!/usr/bin/env python3
'''
t5_failsafe.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

if __name__ == '__main__':
    import sys

    import pytest

    sys.exit(pytest.main([__file__] + sys.argv[1:]))

import errno
import os.path
import signal
import time
from argparse import Namespace

import pytest
import t4_fuse
from common import NoTestSection, get_remote_test_info

import s3ql.ctrl
from s3ql import BUFSIZE
from s3ql.backends import gs


@pytest.mark.usefixtures('pass_reg_output')
class TestFailsafe(t4_fuse.TestFuse):
    '''
    Test behavior with corrupted backend. In contrast to the tests
    in t3_fs_api, here we also make sure that remote connections
    are properly reset.
    '''

    def setup_method(self, method):
        super().setup_method(method)
        try:
            (backend_login, backend_pw, self.storage_url) = get_remote_test_info('gs-test')
        except NoTestSection as exc:
            super().teardown_method(method)
            pytest.skip(exc.reason)

        self.backend_login = backend_login
        self.backend_passphrase = backend_pw

        self.backend = gs.Backend(
            Namespace(
                storage_url=self.storage_url,
                backend_login=backend_login,
                backend_password=backend_pw,
                backend_options={},
            )
        )

    def test(self):
        self.mkfs(max_obj_size=10 * 1024**2)
        self.mount()

        fname1 = os.path.join(self.mnt_dir, 'file1')
        fname2 = os.path.join(self.mnt_dir, 'file2')

        # We need lots of data to keep the connection alive
        # and reproduce issue 424
        with open(fname1, 'wb') as fh:
            with open('/dev/urandom', 'rb') as src:
                for _ in range(5):
                    fh.write(src.read(BUFSIZE))
        s3ql.ctrl.main(['flushcache', self.mnt_dir])

        with open(fname2, 'w') as fh:
            fh.write('Hello, second world')
        s3ql.ctrl.main(['flushcache', self.mnt_dir])

        # Unmount required to avoid reading from kernel cache
        self.umount()

        # Modify
        (val, meta) = self.backend.fetch('s3ql_data_1')
        self.backend.store('s3ql_data_1', val[:500] + b'oops' + val[500:], meta)

        # Try to read

        self.mount()
        with pytest.raises(IOError) as exc_info:
            with open(fname1, 'rb') as fh:
                fh.read()
        assert exc_info.value.errno == errno.EIO
        self.reg_output(
            r'^ERROR: Backend returned malformed data for block 0 of inode \d+ .+$', count=1
        )

        # This should still work
        with open(fname2, 'rb') as fh:
            fh.read()

        # But this should not
        with pytest.raises(PermissionError):
            open(fname2, 'wb')
        self.reg_output(
            r'^ERROR: Backend returned malformed data for block 0 of inode \d+ .+$', count=1
        )

        # Printed during umount
        self.reg_output(r'^WARNING: File system errors encountered, marking for fsck.$', count=1)


class TestSigInt(t4_fuse.TestFuse):
    '''
    Make sure that we gracefully exit on SIGINT
    '''

    def test(self):
        self.mkfs()

        # Mount file system
        self.mount()

        # wait until pyfuse3 main loop runs
        time.sleep(2)

        # send SIGINT
        self.mount_process.send_signal(signal.SIGINT)

        # wait for clean unmount
        self.mount_process.wait(5)

        # we exited successfully?
        assert self.mount_process.returncode == 0

        self.fsck()
