#!/usr/bin/env python3
'''
t5_failsafe.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

if __name__ == '__main__':
    import pytest
    import sys
    sys.exit(pytest.main([__file__] + sys.argv[1:]))

import os.path
import t4_fuse
import s3ql.ctrl
import pytest
import errno
import time
from common import get_remote_test_info, NoTestSection
from s3ql.backends import gs
from s3ql.backends.local import Backend as LocalBackend
from s3ql.common import get_seq_no
from s3ql import BUFSIZE

@pytest.mark.usefixtures('pass_reg_output')
class TestFailsafe(t4_fuse.TestFuse):
    '''
    Test behavior with corrupted backend. In contrast to the tests
    in t3_fs_api, here we also make sure that remote connections
    are properly reset.

    We use Google Storage, so that we don't have to worry about
    propagation delays.
    '''

    def setup_method(self, method):
        super().setup_method(method)
        try:
            (backend_login, backend_pw,
             self.storage_url) = get_remote_test_info('gs-test')
        except NoTestSection as exc:
            pytest.skip(exc.reason)

        self.backend_login = backend_login
        self.backend_passphrase = backend_pw

        self.backend = gs.Backend(self.storage_url, backend_login, backend_pw, {})

    def test(self):
        self.mkfs(max_obj_size=10*1024**2)
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
        self.reg_output(r'^ERROR: Backend returned malformed data for '
                        'block 0 of inode \d+ .+$', count=1)

        # This should still work
        with open(fname2, 'rb') as fh:
            fh.read()

        # But this should not
        with pytest.raises(PermissionError):
            open(fname2, 'wb')
        self.reg_output(r'^ERROR: Backend returned malformed data for '
                        'block 0 of inode \d+ .+$', count=1)

        # Printed during umount
        self.reg_output(r'^WARNING: File system errors encountered, '
                        'marking for fsck.$', count=1)


@pytest.mark.usefixtures('pass_reg_output')
class TestNewerMetadata(t4_fuse.TestFuse):
    '''
    Make sure that we turn on failsafe mode and don't overwrite
    remote metadata if it suddenly becomes newer than local.
    '''

    def test(self):
        self.mkfs()

        # Get backend instance
        plain_backend = LocalBackend(self.storage_url, None, None)

        # Save metadata
        meta = plain_backend['s3ql_metadata']

        # Mount file system
        self.mount()

        # Increase sequence number
        seq_no = get_seq_no(plain_backend)
        plain_backend['s3ql_seq_no_%d' % (seq_no+1)] = b'Empty'

        # Create a file, so that there's metadata to flush
        fname = os.path.join(self.mnt_dir, 'file1')
        with open(fname, 'w') as fh:
            fh.write('hello, world')

        # Try to upload metadata
        s3ql.ctrl.main(['upload-meta', self.mnt_dir])

        # Try to write. We repeat a few times, since the metadata upload
        # happens asynchronously.
        with pytest.raises(PermissionError):
            for _ in range(10):
                with open(fname + 'barz', 'w') as fh:
                    fh.write('foobar')
                time.sleep(1)
        self.reg_output(r'^ERROR: Remote metadata is newer than local '
                        '\(\d+ vs \d+\), refusing to overwrite(?: and switching '
                        'to failsafe mode)?!$', count=2)
        self.reg_output(r'^WARNING: File system errors encountered, marking for '
                        'fsck\.$', count=1)
        self.reg_output(r'^ERROR: The locally cached metadata will be '
                        '\*lost\* the next .+$', count=1)
        self.umount()

        # Assert that remote metadata has not been overwritten
        assert meta == plain_backend['s3ql_metadata']

        plain_backend.close()
