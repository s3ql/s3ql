#!/usr/bin/env python3
'''
t6_upgrade.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

if __name__ == '__main__':
    import pytest
    import sys
    sys.exit(pytest.main([__file__] + sys.argv[1:]))

from common import populate_dir, skip_without_rsync, retry
from t1_backends import get_remote_test_info, NoTestSection
import shutil
import subprocess
from subprocess import check_output, CalledProcessError
import t4_fuse
import tempfile
import os
import unittest
import pytest

@pytest.mark.usefixtures('s3ql_cmd_argv')
class UpgradeTest(t4_fuse.fuse_tests):

    def setUp(self):
        skip_without_rsync()

        basedir_old = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                                   '..', 's3ql.old'))
        if not os.path.exists(os.path.join(basedir_old, 'bin', 'mkfs.s3ql')):
            raise unittest.SkipTest('no previous S3QL version found')

        super().setUp()
        self.ref_dir = tempfile.mkdtemp(prefix='s3ql-ref-')
        self.bak_dir = tempfile.mkdtemp(prefix='s3ql-bak-')
        self.basedir_old = basedir_old

    def tearDown(self):
        super().tearDown()
        shutil.rmtree(self.ref_dir)
        shutil.rmtree(self.bak_dir)

    def mkfs_old(self):
        proc = subprocess.Popen([os.path.join(self.basedir_old, 'bin', 'mkfs.s3ql'),
                                 '-L', 'test fs', '--max-obj-size', '500', '--authfile',
                                 '/dev/null', '--cachedir', self.cache_dir, '--quiet',
                                 self.storage_url ], stdin=subprocess.PIPE,
                                universal_newlines=True)

        if self.backend_login is not None:
            print(self.backend_login, file=proc.stdin)
            print(self.backend_passphrase, file=proc.stdin)
        print(self.passphrase, file=proc.stdin)
        print(self.passphrase, file=proc.stdin)
        proc.stdin.close()
        self.assertEqual(proc.wait(), 0)

    def mount_old(self):
        self.mount_process = subprocess.Popen([os.path.join(self.basedir_old, 'bin', 'mount.s3ql'),
                                               "--fg", '--cachedir', self.cache_dir, '--log',
                                               'none', '--quiet', '--authfile', '/dev/null',
                                               '--compress', 'zlib', self.storage_url, self.mnt_dir],
                                              stdin=subprocess.PIPE, universal_newlines=True)
        if self.backend_login is not None:
            print(self.backend_login, file=self.mount_process.stdin)
            print(self.backend_passphrase, file=self.mount_process.stdin)
        print(self.passphrase, file=self.mount_process.stdin)
        self.mount_process.stdin.close()
        def poll():
            if os.path.ismount(self.mnt_dir):
                return True
            self.assertIsNone(self.mount_process.poll())
        retry(30, poll)

    def umount_old(self):
        with open('/dev/null', 'wb') as devnull:
            retry(5, lambda: subprocess.call(['fuser', '-m', self.mnt_dir],
                                             stdout=devnull, stderr=devnull) == 1)

        proc = subprocess.Popen([os.path.join(self.basedir_old, 'bin', 'umount.s3ql'),
                                 '--quiet', self.mnt_dir])
        retry(90, lambda : proc.poll() is not None)
        self.assertEqual(proc.wait(), 0)

        self.assertEqual(self.mount_process.poll(), 0)
        self.assertFalse(os.path.ismount(self.mnt_dir))

    def upgrade(self):
        proc = subprocess.Popen(self.s3ql_cmd_argv('s3qladm') +
                                [ '--fatal-warnings', '--cachedir', self.cache_dir, '--authfile',
                                  '/dev/null', '--quiet', 'upgrade', self.storage_url ],
                                stdin=subprocess.PIPE, universal_newlines=True)

        if self.backend_login is not None:
            print(self.backend_login, file=proc.stdin)
            print(self.backend_passphrase, file=proc.stdin)
        print(self.passphrase, file=proc.stdin)
        print('yes', file=proc.stdin)
        proc.stdin.close()

        self.assertEqual(proc.wait(), 0)

    def compare(self):

        # Compare
        self.fsck()
        self.mount()
        try:
            out = check_output(['rsync', '-anciHAX', '--delete', '--exclude', '/lost+found',
                               self.ref_dir + '/', self.mnt_dir + '/'], universal_newlines=True,
                              stderr=subprocess.STDOUT)
        except CalledProcessError as exc:
            self.fail('rsync failed with ' + exc.output)
        if out:
            self.fail('Copy not equal to original, rsync says:\n' + out)

        self.umount()

    def runTest(self):
        populate_dir(self.ref_dir)

        # Create and mount using previous S3QL version
        self.mkfs_old()
        self.mount_old()
        subprocess.check_call(['rsync', '-aHAX', self.ref_dir + '/', self.mnt_dir + '/'])
        self.umount_old()

        # Copy old bucket
        shutil.copytree(self.backend_dir, os.path.join(self.bak_dir, 'copy'),
                        symlinks=True)

        # Upgrade and compare with cache
        self.upgrade()
        self.compare()

        # Upgrade and compare without cache
        shutil.rmtree(self.backend_dir)
        shutil.rmtree(self.cache_dir)
        self.cache_dir = tempfile.mkdtemp(prefix='s3ql-cache-')
        shutil.copytree(os.path.join(self.bak_dir, 'copy'),
                        self.backend_dir, symlinks=True)
        self.upgrade()
        self.compare()


class RemoteUpgradeTest:
    def setUp(self, name):
        super().setUp()
        try:
            (backend_login, backend_pw,
             self.storage_url) = get_remote_test_info(name)
        except NoTestSection as exc:
            self.skipTest(exc.reason)
        self.backend_login = backend_login
        self.backend_passphrase = backend_pw

    def runTest(self):
        populate_dir(self.ref_dir, entries=50, size=5*1024*1024)

        # Create and mount using previous S3QL version
        self.mkfs_old()
        self.mount_old()
        subprocess.check_call(['rsync', '-aHAX', self.ref_dir + '/', self.mnt_dir + '/'])
        self.umount_old()

        # Upgrade and compare without cache
        shutil.rmtree(self.cache_dir)
        self.cache_dir = tempfile.mkdtemp(prefix='s3ql-cache-')
        self.upgrade()
        self.compare()

    def tearDown(self):
        super().tearDown()

        proc = subprocess.Popen(self.s3ql_cmd_argv('s3qladm') +
                                [ '--quiet', '--authfile', '/dev/null', '--fatal-warnings',
                                  'clear', self.storage_url ],
                                stdin=subprocess.PIPE, universal_newlines=True)
        if self.backend_login is not None:
            print(self.backend_login, file=proc.stdin)
            print(self.backend_passphrase, file=proc.stdin)
        print('yes', file=proc.stdin)
        proc.stdin.close()

        self.assertEqual(proc.wait(), 0)

class S3UpgradeTest(RemoteUpgradeTest, UpgradeTest):
    def setUp(self):
        super().setUp('s3-test')

class SwiftUpgradeTest(RemoteUpgradeTest, UpgradeTest):
    def setUp(self):
        super().setUp('swift-test')
