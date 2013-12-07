'''
t5_failsafe.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

import os.path
import t4_fuse
import s3ql.ctrl
import pytest
import errno
from common import get_remote_test_info
from s3ql.backends import gs
from argparse import Namespace
from s3ql.common import BUFSIZE
from s3ql.backends.common import get_ssl_context

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
        (backend_login, backend_pw,
         self.storage_url) = get_remote_test_info('gs-test', self.skipTest)
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


