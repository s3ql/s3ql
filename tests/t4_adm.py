'''
t4_adm.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import division, print_function
from _common import TestCase
from s3ql.backends import local
from s3ql.backends.common import BetterBucket
import shutil
import sys
import tempfile
import unittest2 as unittest
import subprocess
import os.path

if __name__ == '__main__':
    mypath = sys.argv[0]
else:
    mypath = __file__
BASEDIR = os.path.abspath(os.path.join(os.path.dirname(mypath), '..'))

class AdmTests(TestCase):

    def setUp(self):
        self.cache_dir = tempfile.mkdtemp()
        self.bucket_dir = tempfile.mkdtemp()

        self.bucketname = 'local://' + self.bucket_dir
        self.passphrase = 'oeut3d'

    def tearDown(self):
        shutil.rmtree(self.cache_dir)
        shutil.rmtree(self.bucket_dir)

    def mkfs(self):
        proc = subprocess.Popen([os.path.join(BASEDIR, 'bin', 'mkfs.s3ql'),
                                 '-L', 'test fs', '--max-obj-size', '500',
                                 '--cachedir', self.cache_dir, '--quiet',
                                 self.bucketname ], stdin=subprocess.PIPE)

        print(self.passphrase, file=proc.stdin)
        print(self.passphrase, file=proc.stdin)
        proc.stdin.close()

        self.assertEqual(proc.wait(), 0)

    def test_passphrase(self):
        self.mkfs()

        passphrase_new = 'sd982jhd'

        proc = subprocess.Popen([os.path.join(BASEDIR, 'bin', 's3qladm'),
                                 '--quiet', 'passphrase',
                                 self.bucketname ], stdin=subprocess.PIPE)

        print(self.passphrase, file=proc.stdin)
        print(passphrase_new, file=proc.stdin)
        print(passphrase_new, file=proc.stdin)
        proc.stdin.close()

        self.assertEqual(proc.wait(), 0)

        plain_bucket = local.Bucket(self.bucketname, None, None)
        bucket = BetterBucket(passphrase_new, 'bzip2', plain_bucket)
        self.assertTrue(isinstance(bucket['s3ql_passphrase'], str))


# Somehow important according to pyunit documentation
def suite():
    return unittest.makeSuite(AdmTests)


# Allow calling from command line
if __name__ == "__main__":
    unittest.main()
