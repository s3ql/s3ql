#!/usr/bin/env python3
'''
t5_failsafe.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright © 2008 Nikolaus Rath <Nikolaus.org>

This program can be distributed under the terms of the GNU GPLv3.
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
from common import get_remote_test_info, NoTestSection
from s3ql.backends import gs
from argparse import Namespace
from s3ql.backends.local import Backend as LocalBackend
from s3ql.common import get_ssl_context, get_seq_no
from s3ql import BUFSIZE

class FailsafeTest(t4_fuse.fuse_tests):
    '''
    Test behavior with corrupted backend. In contrast to the tests
    in t3_fs_api, here we also make sure that remote connections
    are properly reset.

    We use Google Storage, so that we don't have to worry about
    propagation delays.
    '''

    def setUp(self):
        super().setUp()
        try:
            (backend_login, backend_pw,
             self.storage_url) = get_remote_test_info('gs-test')
        except NoTestSection as exc:
            self.skipTest(exc.reason)

        self.backend_login = backend_login
        self.backend_passphrase = backend_pw

        options = Namespace()
        options.no_ssl = False
        options.ssl_ca_path = None

        self.backend = gs.Backend(self.storage_url, backend_login, backend_pw,
                                  ssl_context=get_ssl_context(options))

    def runTest(self):
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
        self.mount(fatal_warnings=False)
        with pytest.raises(IOError) as exc_info:
            with open(fname1, 'rb') as fh:
                fh.read()
        assert exc_info.value.errno == errno.EIO

        # This should still work
        with open(fname2, 'rb') as fh:
            fh.read()

        # But this should not
        with pytest.raises(PermissionError):
            open(fname2, 'wb')



class NewerMetadataTest(t4_fuse.fuse_tests):
    '''
    Make sure that we turn on failsafe mode and don't overwrite
    remote metadata if it suddenly becomes newer than local.
    '''

    def runTest(self):
        self.mkfs()

        # Get backend instance
        plain_backend = LocalBackend(self.storage_url, None, None)

        # Save metadata
        meta = plain_backend['s3ql_metadata']

        # Mount file system
        self.mount(fatal_warnings=False)

        # Increase sequence number
        seq_no = get_seq_no(plain_backend)
        plain_backend['s3ql_seq_no_%d' % (seq_no+1)] = b'Empty'

        # Create a file, so that there's metadata to flush
        fname = os.path.join(self.mnt_dir, 'file1')
        with open(fname, 'w') as fh:
            fh.write('hello, world')

        # Try to upload metadata
        s3ql.ctrl.main(['upload-meta', self.mnt_dir])

        # Try to write
        with pytest.raises(PermissionError):
            with open(fname + 'barz', 'w') as fh:
                fh.write('foobar')

        self.umount()

        # Assert that remote metadata has not been overwritten
        assert meta == plain_backend['s3ql_metadata']

        plain_backend.close()
