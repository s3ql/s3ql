'''
t4_adm.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function
from _common import TestCase
import unittest2 as unittest
import tempfile
import sys
import os
from cStringIO import StringIO
import shutil
import s3ql.cli.mkfs
import s3ql.cli.adm
from s3ql.backends import local

class AdmTests(TestCase):

    def setUp(self):
        self.cache_dir = tempfile.mkdtemp()
        self.bucket_dir = tempfile.mkdtemp()

        self.bucketname = 'local://' + os.path.join(self.bucket_dir, 'mybucket')
        self.passphrase = 'oeut3d'

    def tearDown(self):
        shutil.rmtree(self.cache_dir)
        shutil.rmtree(self.bucket_dir)

    def mkfs(self):
        sys.stdin = StringIO('%s\n%s\n' % (self.passphrase, self.passphrase))
        try:
            s3ql.cli.mkfs.main(['--homedir', self.cache_dir, self.bucketname ])
        except BaseException as exc:
            self.fail("mkfs.s3ql failed: %s" % exc)

    def test_passphrase(self):
        self.mkfs()

        passphrase_new = 'sd982jhd'
        sys.stdin = StringIO('%s\n%s\n%s\n' % (self.passphrase,
                                               passphrase_new, passphrase_new))
        try:
            s3ql.cli.adm.main(['--change-passphrase', self.bucketname ])
        except BaseException as exc:
            self.fail("s3qladm failed: %s" % exc)


        bucket = local.Connection().get_bucket(os.path.join(self.bucket_dir, 'mybucket'))
        bucket.passphrase = passphrase_new

        bucket.passphrase = bucket['s3ql_passphrase']
        self.assertTrue(isinstance(bucket['s3ql_seq_no_0'], str))


# Somehow important according to pyunit documentation
def suite():
    return unittest.makeSuite(AdmTests)


# Allow calling from command line
if __name__ == "__main__":
    unittest.main()
