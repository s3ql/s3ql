'''
t5_full.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from common import populate_dir, skip_without_rsync, BASEDIR, get_remote_test_info
import shutil
import subprocess
from subprocess import check_output, CalledProcessError
import t4_fuse
import tempfile
import sys
import os

class FullTest(t4_fuse.fuse_tests):

    def populate_dir(self, path):
        populate_dir(path)
        
    def runTest(self):
        skip_without_rsync()

        ref_dir = tempfile.mkdtemp(prefix='s3ql-ref-')
        try:
            self.populate_dir(ref_dir)
            
            # Copy source data
            self.mkfs()
            self.mount()
            subprocess.check_call(['rsync', '-aHAX', ref_dir + '/',
                                   self.mnt_dir + '/'])
            self.umount()
            self.fsck()

            # Delete cache, run fsck and compare
            shutil.rmtree(self.cache_dir)
            self.cache_dir = tempfile.mkdtemp('s3ql-cache-')
            self.fsck()
            self.mount()
            try:
                out = check_output(['rsync', '-anciHAX', '--delete', '--exclude', '/lost+found',
                                    ref_dir + '/', self.mnt_dir + '/'], universal_newlines=True,
                                  stderr=subprocess.STDOUT)
            except CalledProcessError as exc:
                self.fail('rsync failed with ' + exc.output)
            if out:
                self.fail('Copy not equal to original, rsync says:\n' + out)

            self.umount()

            # Delete cache and mount
            shutil.rmtree(self.cache_dir)
            self.cache_dir = tempfile.mkdtemp(prefix='s3ql-cache-')
            self.mount()
            self.umount()

        finally:
            shutil.rmtree(ref_dir)


class RemoteTest:
    def setUp(self, name):
        super().setUp()
        (backend_login, backend_pw,
         self.storage_url) = get_remote_test_info(name, self.skipTest)
        self.backend_login = backend_login
        self.backend_passphrase = backend_pw

    def populate_dir(self, path):
        populate_dir(path, entries=50, size=5*1024*1024)

    def tearDown(self):
        super().tearDown()
    
        proc = subprocess.Popen([sys.executable, os.path.join(BASEDIR, 'bin', 's3qladm'),
                                 '--quiet', '--authfile', '/dev/null', '--fatal-warnings',
                                 'clear', self.storage_url ],
                                stdin=subprocess.PIPE, universal_newlines=True)
        if self.backend_login is not None:
            print(self.backend_login, file=proc.stdin)
            print(self.backend_passphrase, file=proc.stdin)
        print('yes', file=proc.stdin)
        proc.stdin.close()

        self.assertEqual(proc.wait(), 0)

        
class S3FullTest(RemoteTest, FullTest):
    def setUp(self):
        super().setUp('s3-test')

class SwiftksFullTest(RemoteTest, FullTest):
    def setUp(self):
        super().setUp('swiftks-test')
