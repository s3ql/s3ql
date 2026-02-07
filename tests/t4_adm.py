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

import os
import re
import shutil
import subprocess
from argparse import Namespace

import pytest
from common import populate_dir
from t4_fuse import TestFuse

from s3ql.backends import local
from s3ql.backends.common import CorruptedObjectError
from s3ql.backends.comprenc import AsyncComprencBackend


@pytest.mark.usefixtures('pass_reg_output')
class TestAdm:
    @pytest.fixture(autouse=True)
    def setup(self, tmp_path):
        self.cache_dir = str(tmp_path / 'cache')
        os.makedirs(self.cache_dir)
        self.backend_dir = str(tmp_path / 'backend')
        os.makedirs(self.backend_dir)

        self.storage_url = 'local://' + self.backend_dir
        self.passphrase = 'oeut3d'

    def mkfs(self):
        proc = subprocess.Popen(
            [
                'mkfs.s3ql',
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
        assert proc.wait() == 0

        return stdout

    @pytest.mark.trio
    async def test_passphrase(self):
        self.mkfs()

        passphrase_new = 'sd982jhd'

        proc = subprocess.Popen(
            [
                's3qladm',
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

        assert proc.wait() == 0

        plain_backend = await local.AsyncBackend.create(Namespace(storage_url=self.storage_url))
        backend = await AsyncComprencBackend.create(
            passphrase_new.encode(), ('zlib', 6), plain_backend
        )

        await backend.fetch('s3ql_passphrase')  # will fail with wrong pw

    @pytest.mark.trio
    async def test_clear(self):
        self.mkfs()

        proc = subprocess.Popen(
            [
                's3qladm',
                '--quiet',
                '--log',
                'none',
                '--authfile',
                '/dev/null',
                'clear',
                self.storage_url,
            ],
            stdin=subprocess.PIPE,
            universal_newlines=True,
        )
        print('yes', file=proc.stdin)
        proc.stdin.close()
        assert proc.wait() == 0

        plain_backend = await local.AsyncBackend.create(Namespace(storage_url=self.storage_url))
        assert [k async for k in plain_backend.list()] == []

    @pytest.mark.trio
    async def test_key_recovery(self):
        mkfs_output = self.mkfs()

        hit = re.search(
            r'^---BEGIN MASTER KEY---\n' r'(.+)\n' r'---END MASTER KEY---$',
            mkfs_output,
            re.MULTILINE,
        )
        assert hit
        master_key = hit.group(1)

        plain_backend = await local.AsyncBackend.create(Namespace(storage_url=self.storage_url))
        await plain_backend.delete('s3ql_passphrase')  # Oops

        backend = await AsyncComprencBackend.create(
            self.passphrase.encode(), ('zlib', 6), plain_backend
        )
        with pytest.raises(CorruptedObjectError):
            await backend.fetch('s3ql_params')

        passphrase_new = 'sd982jhd'

        proc = subprocess.Popen(
            [
                's3qladm',
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
        assert proc.wait() == 0

        backend = await AsyncComprencBackend.create(
            passphrase_new.encode(), ('zlib', 6), plain_backend
        )

        await backend.fetch('s3ql_passphrase')  # will fail with wrong pw


class TestMetadataRestore(TestFuse):
    def restore_backup(self):
        proc = subprocess.Popen(
            [
                's3qladm',
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


class TestShrink(TestFuse):
    def get_db_size(self):
        return os.path.getsize(self.cachepath + '.db')

    def test(self):
        testdir = os.path.join(self.mnt_dir, 'testdir')
        self.mkfs()
        self.mount()
        os.mkdir(testdir)
        populate_dir(testdir, 2000, 1024)
        populate_dir(self.mnt_dir, 50, 1024)
        self.umount()

        old_size = self.get_db_size()

        self.mount()
        shutil.rmtree(testdir)
        self.umount()

        proc = subprocess.Popen(
            [
                's3qladm',
                '--quiet',
                '--log',
                'none',
                '--authfile',
                '/dev/null',
                '--cachedir',
                self.cache_dir,
                'shrink-db',
                self.storage_url,
            ],
            universal_newlines=True,
            stdin=subprocess.PIPE,
        )
        if self.backend_login is not None:
            print(self.backend_login, file=proc.stdin)
            print(self.backend_passphrase, file=proc.stdin)
        if self.passphrase is not None:
            print(self.passphrase, file=proc.stdin)
        proc.stdin.close()
        assert proc.wait() == 0

        new_size = self.get_db_size()

        self.fsck()

        assert old_size > new_size
