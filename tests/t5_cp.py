#!/usr/bin/env python3
'''
t5_cp.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

if __name__ == '__main__':
    import pytest
    import sys
    sys.exit(pytest.main([__file__] + sys.argv[1:]))

from common import populate_dir, skip_without_rsync, retry
import os.path
import sys
import shutil
import subprocess
from subprocess import check_output, CalledProcessError
import t4_fuse
import tempfile
import pytest

class TestCp(t4_fuse.TestFuse):

    def test(self):
        skip_without_rsync()
        self.mkfs()
        self.mount()
        self.tst_cp()
        self.umount()
        self.fsck()

    def tst_cp(self):

        tempdir = tempfile.mkdtemp(prefix='s3ql-cp-')
        try:
            populate_dir(tempdir)

            # Rsync
            subprocess.check_call(['rsync', '-aHAX', tempdir + '/',
                                   os.path.join(self.mnt_dir, 'orig') + '/'])

            # copy
            subprocess.check_call(self.s3ql_cmd_argv('s3qlcp') +
                                  [ '--quiet', os.path.join(self.mnt_dir, 'orig'),
                                    os.path.join(self.mnt_dir, 'copy')])

            # compare
            try:
                out = check_output(['rsync', '-anciHAX', '--delete', tempdir + '/',
                                    os.path.join(self.mnt_dir, 'copy') + '/'],
                                   universal_newlines=True, stderr=subprocess.STDOUT)
            except CalledProcessError as exc:
                pytest.fail('rsync failed with ' + exc.output)

            if out:
                pytest.fail('Copy not equal to original, rsync says:\n' + out)

        finally:
            shutil.rmtree(tempdir)


    def test_cp_inode_invalidate(self):
        if os.getuid() != 0:
            pytest.skip('test_cp_inode_invalidate requires root, skipping.')

        self.passphrase = None
        self.mkfs()

        # Run monkeypatched mount.s3ql with overriden pyfuse3.invalidate_inode : 
        # Drop kernel dentries and inodes cache just before calling pyfuse3.invalidate_inode
        cmd = ([sys.executable, os.path.join(os.path.dirname(__file__), 'mount_helper.py'), "--fg", '--cachedir', 
                self.cache_dir, '--log', 'none',
                '--compress', 'zlib', '--quiet', self.storage_url, self.mnt_dir,
                '--authfile', '/dev/null' ])
        self.mount_process = subprocess.Popen(cmd, universal_newlines=True)
        def poll():
            if os.path.ismount(self.mnt_dir):
                return True
            assert self.mount_process.poll() is None
        retry(10, poll)


        os.mkdir(os.path.join(self.mnt_dir, 'orig'))

        cmd = (self.s3ql_cmd_argv('s3qlcp') +
            [ '--quiet', os.path.join(self.mnt_dir, 'orig'),
            os.path.join(self.mnt_dir, 'copy')])
        cp_process = subprocess.Popen(cmd)
        retry(5, lambda : cp_process.poll() is not None)
        assert cp_process.wait() == 0

        self.umount()
        self.fsck()
