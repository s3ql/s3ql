#!/usr/bin/env python3
'''
t4_adm.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

if __name__ == '__main__':
    import sys

    import pytest

    sys.exit(pytest.main([__file__] + sys.argv[1:]))

import os.path
import re
import shutil
import subprocess
import tempfile
import unittest
from argparse import Namespace

import pytest
from t4_fuse import TestFuse

from s3ql.backends import local
from s3ql.backends.common import CorruptedObjectError
from s3ql.backends.comprenc import ComprencBackend


@pytest.mark.usefixtures('pass_s3ql_cmd_argv', 'pass_reg_output')
class AdmTests(unittest.TestCase):
    def setUp(self):
        self.cache_dir = tempfile.mkdtemp(prefix='s3ql-cache-')
        self.backend_dir = tempfile.mkdtemp(prefix='s3ql-backend-')

        self.storage_url = 'local://' + self.backend_dir
        self.passphrase = 'oeut3d'

    def tearDown(self):
        shutil.rmtree(self.cache_dir)
        shutil.rmtree(self.backend_dir)

    def mkfs(self):
        proc = subprocess.Popen(
            self.s3ql_cmd_argv('mkfs.s3ql')
            + [
                '-L',
                'test fs',
                '--data-block-size',
                '500',
                '--authfile',
                '/dev/null',
                '--cachedir',
                self.cache_dir,
                '--quiet',
                self.storage_url,
            ],
            stdin=subprocess.PIPE,
            universal_newlines=True,
            stdout=subprocess.PIPE,
        )

        print(self.passphrase, file=proc.stdin)
        print(self.passphrase, file=proc.stdin)
        proc.stdin.close()
        stdout = proc.stdout.read()
        proc.stdout.close()
        self.assertEqual(proc.wait(), 0)

        return stdout

    def test_passphrase(self):
        self.mkfs()

        passphrase_new = 'sd982jhd'

        proc = subprocess.Popen(
            self.s3ql_cmd_argv('s3qladm')
            + [
                '--quiet',
                '--log',
                'none',
                '--authfile',
                '/dev/null',
                'passphrase',
                self.storage_url,
            ],
            stdin=subprocess.PIPE,
            universal_newlines=True,
        )

        print(self.passphrase, file=proc.stdin)
        print(passphrase_new, file=proc.stdin)
        print(passphrase_new, file=proc.stdin)
        proc.stdin.close()

        self.assertEqual(proc.wait(), 0)

        plain_backend = local.Backend(Namespace(storage_url=self.storage_url))
        backend = ComprencBackend(passphrase_new.encode(), ('zlib', 6), plain_backend)

        backend.fetch('s3ql_passphrase')  # will fail with wrong pw

    def test_clear(self):
        self.mkfs()

        proc = subprocess.Popen(
            self.s3ql_cmd_argv('s3qladm')
            + ['--quiet', '--log', 'none', '--authfile', '/dev/null', 'clear', self.storage_url],
            stdin=subprocess.PIPE,
            universal_newlines=True,
        )
        print('yes', file=proc.stdin)
        proc.stdin.close()
        self.assertEqual(proc.wait(), 0)

        plain_backend = local.Backend(Namespace(storage_url=self.storage_url))
        assert list(plain_backend.list()) == []

    def test_key_recovery(self):
        mkfs_output = self.mkfs()

        hit = re.search(
            r'^---BEGIN MASTER KEY---\n' r'(.+)\n' r'---END MASTER KEY---$',
            mkfs_output,
            re.MULTILINE,
        )
        assert hit
        master_key = hit.group(1)

        plain_backend = local.Backend(Namespace(storage_url=self.storage_url))
        del plain_backend['s3ql_passphrase']  # Oops

        backend = ComprencBackend(self.passphrase.encode(), ('zlib', 6), plain_backend)
        with pytest.raises(CorruptedObjectError):
            backend.fetch('s3ql_params')

        passphrase_new = 'sd982jhd'

        proc = subprocess.Popen(
            self.s3ql_cmd_argv('s3qladm')
            + [
                '--quiet',
                '--log',
                'none',
                '--authfile',
                '/dev/null',
                'recover-key',
                self.storage_url,
            ],
            stdin=subprocess.PIPE,
            universal_newlines=True,
        )

        print(master_key, file=proc.stdin)
        print(passphrase_new, file=proc.stdin)
        print(passphrase_new, file=proc.stdin)
        proc.stdin.close()
        self.assertEqual(proc.wait(), 0)

        backend = ComprencBackend(passphrase_new.encode(), ('zlib', 6), plain_backend)

        backend.fetch('s3ql_passphrase')  # will fail with wrong pw


class TestMetadataRestore(TestFuse):
    def restore_backup(self):
        proc = subprocess.Popen(
            self.s3ql_cmd_argv('s3qladm')
            + [
                '--quiet',
                '--log',
                'none',
                '--authfile',
                '/dev/null',
                '--cachedir',
                self.cache_dir,
                'restore-metadata',
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
        # Restore second metadata backup
        print('1', file=proc.stdin)
        proc.stdin.close()
        assert proc.wait() == 0

    def test(self):
        self.mkfs()
        self.mount()
        with open(self.mnt_dir + "/mount1.txt", 'w') as fh:
            print('Data written on first mount', file=fh)
        self.umount()

        self.mount()
        with open(self.mnt_dir + "/mount2.txt", 'w') as fh:
            print('Data written on second mount', file=fh)
        self.umount()

        self.restore_backup()

        self.reg_output(
            r'^WARNING: Deleted spurious object \d+$',
            count=1,
        )
        self.fsck(expect_retcode=128)

        self.mount()
        assert not os.path.exists(self.mnt_dir + '/mount2.txt')
        assert os.path.exists(self.mnt_dir + '/mount1.txt')
