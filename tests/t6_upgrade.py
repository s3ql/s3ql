'''
t6_upgrade.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from t4_fuse import populate_dir, skip_without_rsync, BASEDIR, retry
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
        self.ref_dir = tempfile.mkdtemp()
        self.bak_dir = tempfile.mkdtemp()
        self.basedir_old = basedir_old
        
    def tearDown(self):
        super().tearDown()
        shutil.rmtree(self.ref_dir)
        shutil.rmtree(self.bak_dir)

    def mkfs_old(self):
        proc = subprocess.Popen([os.path.join(self.basedir_old, 'bin', 'mkfs.s3ql'),
                                 '-L', 'test fs', '--max-obj-size', '500',
                                 '--cachedir', self.cache_dir, '--quiet',
                                 self.storage_url ], stdin=subprocess.PIPE,
                                universal_newlines=True)

        print(self.passphrase, file=proc.stdin)
        print(self.passphrase, file=proc.stdin)
        proc.stdin.close()
        self.assertEqual(proc.wait(), 0)

    def mount_old(self):
        self.mount_process = subprocess.Popen([os.path.join(self.basedir_old, 'bin', 'mount.s3ql'),
                                               "--fg", '--cachedir', self.cache_dir,
                                               '--log', 'none', '--quiet',
                                               self.storage_url, self.mnt_dir],
                                              stdin=subprocess.PIPE, universal_newlines=True)
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

    def upgrade(self, basedir=BASEDIR):
        proc = subprocess.Popen([sys.executable, os.path.join(basedir, 'bin', 's3qladm'),
                                 '--fatal-warnings', '--cachedir', self.cache_dir,
                                 '--quiet', 'upgrade', self.storage_url ], stdin=subprocess.PIPE,
                                universal_newlines=True)
        
        print(self.passphrase, file=proc.stdin)
        print('yes', file=proc.stdin)
        proc.stdin.close()

        self.assertEqual(proc.wait(), 0)

    def compare(self):
        
        # Compare
        self.fsck()
        self.mount()
        with subprocess.Popen(['rsync', '-anciHAX', '--delete',
                               '--exclude', '/lost+found',
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
        self.cache_dir = tempfile.mkdtemp()
        shutil.copytree(os.path.join(self.bak_dir, 'copy'),
                        self.backend_dir, symlinks=True)
        self.upgrade()
        self.compare()


# Somehow important according to pyunit documentation
def suite():
    return unittest.makeSuite(FullTests)

# Allow calling from command line
if __name__ == "__main__":
    unittest.main()
