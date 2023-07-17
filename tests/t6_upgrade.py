#!/usr/bin/env python3
'''
t6_upgrade.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

if __name__ == '__main__':
    import sys

    import pytest

    sys.exit(pytest.main([__file__] + sys.argv[1:]))

import os
import shutil
import subprocess
import tempfile
from subprocess import CalledProcessError, check_output

import pytest
import t4_fuse
from common import populate_dir, retry, skip_without_rsync
from t1_backends import NoTestSection, get_remote_test_info

from s3ql import backends


def Popen_old(*a, **kw):
    '''Run subprocess.Popen after unsetting PYTHONWARNINGS'''

    # We do not want to see warnings from the old S3QL version when testing
    # the new one.
    env = os.environ.copy()
    env.pop('PYTHONWARNINGS', None)

    return subprocess.Popen(*a, **kw, env=env)


@pytest.mark.usefixtures('pass_reg_output')
class TestUpgrade(t4_fuse.TestFuse):
    def setup_method(self, method):
        skip_without_rsync()

        self.basedir_old = os.path.abspath(
            os.path.join(os.path.dirname(__file__), '..', 's3ql.old')
        )
        if not os.path.exists(os.path.join(self.basedir_old, 'bin', 'mkfs.s3ql')):
            pytest.skip('no previous S3QL version found')

        super().setup_method(method)
        self.tempdir = tempfile.mkdtemp(prefix='s3ql-upgrade-test')
        self.ref_dir = tempfile.mkdtemp(prefix='s3ql-upgrade-ref')

    def teardown_method(self, method):
        super().teardown_method(method)
        shutil.rmtree(self.tempdir)
        shutil.rmtree(self.ref_dir)

    def mkfs_old(self, force=False, max_obj_size=500):
        argv = [
            os.path.join(self.basedir_old, 'bin', 'mkfs.s3ql'),
            '-L',
            'test fs',
            '--max-obj-size',
            str(max_obj_size),
            '--cachedir',
            self.cache_dir,
            '--quiet',
            '--authfile',
            '/dev/null',
            self.storage_url,
        ]
        if force:
            argv.append('--force')
        if self.passphrase is None:
            argv.append('--plain')
        proc = Popen_old(argv, stdin=subprocess.PIPE, universal_newlines=True)

        if self.backend_login is not None:
            print(self.backend_login, file=proc.stdin)
            print(self.backend_passphrase, file=proc.stdin)
        if self.passphrase is not None:
            print(self.passphrase, file=proc.stdin)
            print(self.passphrase, file=proc.stdin)
        proc.stdin.close()
        assert proc.wait() == 0
        self.reg_output(
            r'^WARNING: Maximum object sizes less than 1 MiB will degrade performance\.$',
            count=1,
        )

    def mount_old(self):
        self.mount_process = Popen_old(
            [
                os.path.join(self.basedir_old, 'bin', 'mount.s3ql'),
                "--fg",
                '--cachedir',
                self.cache_dir,
                '--log',
                'none',
                '--quiet',
                '--authfile',
                '/dev/null',
                '--compress',
                'zlib',
                self.storage_url,
                self.mnt_dir,
            ],
            stdin=subprocess.PIPE,
            universal_newlines=True,
        )
        if self.backend_login is not None:
            print(self.backend_login, file=self.mount_process.stdin)
            print(self.backend_passphrase, file=self.mount_process.stdin)
        if self.passphrase is not None:
            print(self.passphrase, file=self.mount_process.stdin)
        self.mount_process.stdin.close()

        def poll():
            if os.path.ismount(self.mnt_dir):
                return True
            assert self.mount_process.poll() is None

        retry(30, poll)

    def umount_old(self):
        with open('/dev/null', 'wb') as devnull:
            retry(
                5,
                lambda: subprocess.call(
                    ['fuser', '-m', self.mnt_dir], stdout=devnull, stderr=devnull
                )
                == 1,
            )

        proc = Popen_old(
            [os.path.join(self.basedir_old, 'bin', 'umount.s3ql'), '--quiet', self.mnt_dir]
        )
        retry(90, lambda: proc.poll() is not None)
        assert proc.wait() == 0

        assert self.mount_process.poll() == 0
        assert not os.path.ismount(self.mnt_dir)

    def upgrade(self):
        proc = subprocess.Popen(
            self.s3ql_cmd_argv('s3qladm')
            + [
                '--cachedir',
                self.cache_dir,
                '--authfile',
                '/dev/null',
                '--quiet',
                'upgrade',
                self.storage_url,
            ],
            stdin=subprocess.PIPE,
            universal_newlines=True,
        )

        if self.backend_login is not None:
            print(self.backend_login, file=proc.stdin)
            print(self.backend_passphrase, file=proc.stdin)
        if self.passphrase is not None:
            print(self.passphrase, file=proc.stdin)
        print('yes', file=proc.stdin)
        proc.stdin.close()

        assert proc.wait() == 0

    def compare(self):
        try:
            out = check_output(
                [
                    'rsync',
                    '-anciHAX',
                    '--delete',
                    '--exclude',
                    '/lost+found',
                    self.ref_dir + '/',
                    self.mnt_dir + '/',
                ],
                universal_newlines=True,
                stderr=subprocess.STDOUT,
            )
        except CalledProcessError as exc:
            pytest.fail('rsync failed with ' + exc.output)
        if out:
            pytest.fail('Copy not equal to original, rsync says:\n' + out)

    def populate(self):
        populate_dir(self.ref_dir)

    def test(self):
        self.populate()

        # Create and mount using previous S3QL version
        self.mkfs_old()
        self.mount_old()
        subprocess.check_call(['rsync', '-aHAX', self.ref_dir + '/', self.mnt_dir + '/'])
        self.umount_old()

        # Preserve a copy of the old cache
        prev_cache = os.path.join(self.tempdir, 'cache_prev')
        shutil.copytree(self.cache_dir, prev_cache)

        # Try to access with new version (should fail)
        self.reg_output(r'^ERROR: File system revision too old', count=1)
        self.mount(expect_fail=32)

        # Try to access with new version without cache (should also fail)
        self.reg_output(r'^ERROR: File system revision too old', count=1)
        orig_cache = self.cache_dir
        self.cache_dir = os.path.join(self.tempdir, 'empty-cache')
        try:
            self.mount(expect_fail=32)
        finally:
            self.cache_dir = orig_cache

        # Upgrade to revision 26 is only supported with populated local cache
        self.upgrade()

        # Test with local cache
        self.fsck()
        self.mount()
        self.compare()
        self.umount()

        # Test with old cache
        orig_cache = self.cache_dir
        self.cache_dir = prev_cache
        try:
            self.fsck()
            self.mount()
            self.compare()
            self.umount()
        finally:
            self.cache_dir = orig_cache

        # Test without cache
        orig_cache = self.cache_dir
        self.cache_dir = os.path.join(self.tempdir, 'empty-cache2')
        try:
            self.fsck()
            self.mount()
            self.compare()
            self.umount()
        finally:
            self.cache_dir = orig_cache


class TestPlainUpgrade(TestUpgrade):
    def setup_method(self, method):
        super().setup_method(method)
        self.passphrase = None


class RemoteUpgradeTest:
    def setup_method(self, method, name):
        super().setup_method(method)
        try:
            (backend_login, backend_pw, self.storage_url) = get_remote_test_info(name)
        except NoTestSection as exc:
            super().teardown_method(method)
            pytest.skip(exc.reason)
        self.backend_login = backend_login
        self.backend_passphrase = backend_pw

    def populate(self):
        populate_dir(self.ref_dir, entries=50, size=5 * 1024 * 1024)

    def teardown_method(self, method):
        super().teardown_method(method)

        proc = subprocess.Popen(
            self.s3ql_cmd_argv('s3qladm')
            + ['--quiet', '--authfile', '/dev/null', 'clear', self.storage_url],
            stdin=subprocess.PIPE,
            universal_newlines=True,
        )
        if self.backend_login is not None:
            print(self.backend_login, file=proc.stdin)
            print(self.backend_passphrase, file=proc.stdin)
        print('yes', file=proc.stdin)
        proc.stdin.close()

        assert proc.wait() == 0


# Dynamically generate tests for other backends
for backend_name in backends.prefix_map:
    if backend_name == 'local':
        continue

    # Plain
    def setup_method(self, method, backend_name=backend_name):
        RemoteUpgradeTest.setup_method(self, method, backend_name + '-test')
        self.passphrase = None

    test_class_name = 'TestPlain' + backend_name + 'Upgrade'
    globals()[test_class_name] = type(
        test_class_name, (RemoteUpgradeTest, TestUpgrade), {'setup_method': setup_method}
    )

    # Encrypted
    def setup_method(self, method, backend_name=backend_name):
        RemoteUpgradeTest.setup_method(self, method, backend_name + '-test')

    test_class_name = 'Test' + backend_name + 'Upgrade'
    globals()[test_class_name] = type(
        test_class_name, (RemoteUpgradeTest, TestUpgrade), {'setup_method': setup_method}
    )
