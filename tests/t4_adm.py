'''
t4_adm.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from s3ql.backends import local
from s3ql.backends.common import BetterBackend
import shutil
import sys
import tempfile
import unittest
import subprocess
import os.path

if __name__ == '__main__':
    mypath = sys.argv[0]
else:
    mypath = __file__
BASEDIR = os.path.abspath(os.path.join(os.path.dirname(mypath), '..'))

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
        proc = subprocess.Popen([sys.executable, os.path.join(BASEDIR, 'bin', 'mkfs.s3ql'),
                                 '-L', 'test fs', '--max-obj-size', '500', '--fatal-warnings',
                                 '--cachedir', self.cache_dir, '--quiet',
                                 self.storage_url ], stdin=subprocess.PIPE,
                                universal_newlines=True)

        print(self.passphrase, file=proc.stdin)
        print(self.passphrase, file=proc.stdin)
        proc.stdin.close()

        self.assertEqual(proc.wait(), 0)

    def test_passphrase(self):
        self.mkfs()

        passphrase_new = 'sd982jhd'

        proc = subprocess.Popen([sys.executable, os.path.join(BASEDIR, 'bin', 's3qladm'),
                                 '--quiet', '--fatal-warnings', 'passphrase',
                                 self.storage_url ], stdin=subprocess.PIPE,
                                universal_newlines=True)

        print(self.passphrase, file=proc.stdin)
        print(passphrase_new, file=proc.stdin)
        print(passphrase_new, file=proc.stdin)
        proc.stdin.close()

        self.assertEqual(proc.wait(), 0)

        plain_backend = local.Backend(self.storage_url, None, None)
        backend = BetterBackend(passphrase_new.encode(), ('zlib', 6), plain_backend)
        
        backend.fetch('s3ql_passphrase') # will fail with wrong pw 
