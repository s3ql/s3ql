#!/usr/bin/env python3
'''
t5_full.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright © 2008 Nikolaus Rath <Nikolaus.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

if __name__ == '__main__':
    import pytest
    import sys
    sys.exit(pytest.main([__file__] + sys.argv[1:]))

from common import populate_dir, skip_without_rsync, get_remote_test_info, NoTestSection
from s3ql import backends
import shutil
import subprocess
from subprocess import check_output, CalledProcessError
import t4_fuse
import tempfile
import pytest


@pytest.mark.usefixtures('s3ql_cmd_argv')
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
        try:
            (backend_login, backend_pw,
             self.storage_url) = get_remote_test_info(name)
        except NoTestSection as exc:
            self.skipTest(exc.reason)
        self.backend_login = backend_login
        self.backend_passphrase = backend_pw

    def populate_dir(self, path):
        populate_dir(path, entries=50, size=5*1024*1024)

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


# Dynamically generate tests for other backends
for backend_name in backends.prefix_map:
    if backend_name == 'local':
        continue
    def setUp(self, backend_name=backend_name):
        RemoteTest.setUp(self, backend_name + '-test')
    test_class_name = backend_name + 'FullTests'
    globals()[test_class_name] = type(test_class_name,
                                      (RemoteTest, FullTest),
                                      { 'setUp': setUp })
