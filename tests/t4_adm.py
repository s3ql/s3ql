#!/usr/bin/env python3
'''
t4_adm.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright © 2008 Nikolaus Rath <Nikolaus.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

if __name__ == '__main__':
    import pytest
    import sys
    sys.exit(pytest.main([__file__] + sys.argv[1:]))

from s3ql.backends import local
from s3ql.backends.common import BetterBackend
import shutil
import tempfile
import unittest
import subprocess
import pytest


@pytest.mark.usefixtures('s3ql_cmd_argv')
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
        proc = subprocess.Popen(self.s3ql_cmd_argv('mkfs.s3ql') +
                                ['-L', 'test fs', '--max-obj-size', '500', '--fatal-warnings',
                                 '--authfile', '/dev/null', '--cachedir', self.cache_dir,
                                 '--quiet', self.storage_url ],
                                stdin=subprocess.PIPE, universal_newlines=True)

        print(self.passphrase, file=proc.stdin)
        print(self.passphrase, file=proc.stdin)
        proc.stdin.close()

        self.assertEqual(proc.wait(), 0)

    def test_passphrase(self):
        self.mkfs()

        passphrase_new = 'sd982jhd'

        proc = subprocess.Popen(self.s3ql_cmd_argv('s3qladm') +
                                [ '--quiet', '--fatal-warnings', '--log', 'none', '--authfile',
                                  '/dev/null', 'passphrase', self.storage_url ],
                                stdin=subprocess.PIPE, universal_newlines=True)

        print(self.passphrase, file=proc.stdin)
        print(passphrase_new, file=proc.stdin)
        print(passphrase_new, file=proc.stdin)
        proc.stdin.close()

        self.assertEqual(proc.wait(), 0)

        plain_backend = local.Backend(self.storage_url, None, None)
        backend = BetterBackend(passphrase_new.encode(), ('zlib', 6), plain_backend)

        backend.fetch('s3ql_passphrase') # will fail with wrong pw


    def test_authinfo(self):
        self.mkfs()

        with tempfile.NamedTemporaryFile('wt') as fh:
            print('[entry1]',
                  'storage-url: local://',
                  'fs-passphrase: clearly wrong',
                  '',
                  '[entry2]',
                  'storage-url: %s' % self.storage_url,
                  'fs-passphrase: %s' % self.passphrase,
                  file=fh, sep='\n')
            fh.flush()

            proc = subprocess.Popen(self.s3ql_cmd_argv('fsck.s3ql') +
                                    [ '--quiet', '--fatal-warnings', '--authfile', fh.name,
                                      '--cachedir', self.cache_dir, '--log', 'none', self.storage_url ],
                                    stdin=subprocess.PIPE, universal_newlines=True)

            proc.stdin.close()
            self.assertEqual(proc.wait(), 0)
