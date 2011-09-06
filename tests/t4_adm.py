'''
t4_adm.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import division, print_function
from _common import TestCase
from cStringIO import StringIO
from s3ql.backends import local
from s3ql.backends.common import BetterBucket
import s3ql.cli.adm
import s3ql.cli.mkfs
import shutil
import sys
import tempfile
import unittest2 as unittest

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
        sys.stdin = StringIO('%s\n%s\n' % (self.passphrase, self.passphrase))
        try:
            s3ql.cli.mkfs.main(['--cachedir', self.cache_dir, self.bucketname ])
        except BaseException as exc:
            self.fail("mkfs.s3ql failed: %s" % exc)

    def test_passphrase(self):
        self.mkfs()

        passphrase_new = 'sd982jhd'
        sys.stdin = StringIO('%s\n%s\n%s\n' % (self.passphrase,
                                               passphrase_new, passphrase_new))
        try:
            s3ql.cli.adm.main(['passphrase', self.bucketname ])
        except BaseException as exc:
            raise
            self.fail("s3qladm failed: %s" % exc)

        plain_bucket = local.Bucket(self.bucket_dir, None, None)
        bucket = BetterBucket(passphrase_new, 'bzip2', plain_bucket)
        self.assertTrue(isinstance(bucket['s3ql_passphrase'], str))


# Somehow important according to pyunit documentation
def suite():
    return unittest.makeSuite(AdmTests)


# Allow calling from command line
if __name__ == "__main__":
    unittest.main()
