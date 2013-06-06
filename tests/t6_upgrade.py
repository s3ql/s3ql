'''
t6_upgrade.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from t4_fuse import populate_dir, skip_without_rsync, BASEDIR, retry
from t1_backends import get_remote_test_info
import shutil
import subprocess
import t4_fuse
import tempfile
import os
import unittest
import sys

class UpgradeTest(t4_fuse.fuse_tests):

    def setUp(self):
        skip_without_rsync()
        basedir_old = os.path.join(BASEDIR, 's3ql.old')
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

        if self.backend_login_str is not None:
            print(self.backend_login_str, file=proc.stdin)
        print(self.passphrase, file=proc.stdin)
        print(self.passphrase, file=proc.stdin)
        proc.stdin.close()
        self.assertEqual(proc.wait(), 0)

    def mount_old(self):
        self.mount_process = subprocess.Popen([os.path.join(self.basedir_old, 'bin', 'mount.s3ql'),
                                               "--fg", '--cachedir', self.cache_dir, '--log',
                                               'none', '--quiet', '--authfile', '/dev/null',
                                               self.storage_url, self.mnt_dir],
                                              stdin=subprocess.PIPE, universal_newlines=True)
        if self.backend_login_str is not None:
            print(self.backend_login_str, file=self.mount_process.stdin)
        print(self.passphrase, file=self.mount_process.stdin)
        self.mount_process.stdin.close()
        def poll():
            if os.path.ismount(self.mnt_dir):
                return True
            self.assertIsNone(self.mount_process.poll())
        retry(30, poll)

    def umount_old(self):
        with open('/dev/null', 'wb') as devnull:
            subprocess.call(['fusermount', '-z', '-u', self.mnt_dir],
                            stderr=devnull)

        retry(90, lambda : self.mount_process.poll() is not None)
        self.assertEqual(self.mount_process.wait(), 0)
        self.assertFalse(os.path.ismount(self.mnt_dir))

    def upgrade(self):
        proc = subprocess.Popen([sys.executable, os.path.join(BASEDIR, 'bin', 's3qladm'),
                                 '--fatal-warnings', '--cachedir', self.cache_dir, '--authfile',
                                 '/dev/null', '--quiet', 'upgrade', self.storage_url ],
                                stdin=subprocess.PIPE, universal_newlines=True)

        if self.backend_login_str is not None:
            print(self.backend_login_str, file=proc.stdin)
        print(self.passphrase, file=proc.stdin)
        print('yes', file=proc.stdin)
        proc.stdin.close()

        self.assertEqual(proc.wait(), 0)

    def compare(self):
        
        # Compare
        self.fsck()
        self.mount()
        with subprocess.Popen(['rsync', '-anciHAX', '--delete', '--exclude', '/lost+found',
                               self.ref_dir + '/', self.mnt_dir + '/'],
                              stdout=subprocess.PIPE, universal_newlines=True,
                              stderr=subprocess.STDOUT) as rsync:
            out = rsync.communicate()[0]
            if out:
                self.fail('Copy not equal to original, rsync says:\n' + out)
            elif rsync.returncode != 0:
                self.fail('rsync failed with ' + out)
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
        (backend_login, backend_pw,
         self.storage_url) = get_remote_test_info(name, self.skipTest)
        self.backend_login_str = '%s\n%s' % (backend_login, backend_pw)
        
    def runTest(self):
        populate_dir(self.ref_dir, max_size=1024*1024)

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
        
        proc = subprocess.Popen([sys.executable, os.path.join(BASEDIR, 'bin', 's3qladm'),
                                 '--quiet', '--authfile', '/dev/null', '--fatal-warnings',
                                 'clear', self.storage_url ],
                                stdin=subprocess.PIPE, universal_newlines=True)
        if self.backend_login_str is not None:
            print(self.backend_login_str, file=proc.stdin)
        print('yes', file=proc.stdin)
        proc.stdin.close()

        self.assertEqual(proc.wait(), 0)

class S3UpgradeTest(RemoteUpgradeTest, UpgradeTest):
    def setUp(self):
        super().setUp('s3-test')

class GSUpgradeTest(RemoteUpgradeTest, UpgradeTest):
    def setUp(self):
        super().setUp('gs-test')
        
class S3CUpgradeTest(RemoteUpgradeTest, UpgradeTest):
    def setUp(self):
        super().setUp('s3c-test')

class SwiftUpgradeTest(RemoteUpgradeTest, UpgradeTest):
    def setUp(self):
        super().setUp('swift-test')
        
