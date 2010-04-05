'''
$Id: t4_tune.py 890 2010-02-20 12:49:42Z nikratio $

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function
from _common import TestCase
import unittest
import tempfile
import sys
import os
from cStringIO import StringIO
import shutil
import s3ql.cli.mkfs
import s3ql.common
import s3ql.cli.tune
from s3ql.backends import local

class TuneTests(TestCase):

    def setUp(self):
        self.cache_dir = tempfile.mkdtemp()
        self.bucket_dir = tempfile.mkdtemp()

        self.bucketname = 'local://' + os.path.join(self.bucket_dir, 'mybucket')
        self.passphrase = 'oeut3d'

        # Make sure that the logging settings remain unchanged
        s3ql.common.init_logging = lambda * a, **kw: None

    def tearDown(self):
        shutil.rmtree(self.cache_dir)
        shutil.rmtree(self.bucket_dir)

    def mkfs(self):
        sys.stdin = StringIO('%s\n%s\n' % (self.passphrase, self.passphrase))
        try:
            s3ql.cli.mkfs.main(['--encrypt', '--homedir', self.cache_dir, self.bucketname ])
        except SystemExit as exc:
            self.fail("mkfs.s3ql failed: %s" % exc)

    def test_passphrase(self):
        self.mkfs()

        passphrase_new = 'sd982jhd'
        sys.stdin = StringIO('%s\n%s\n%s\n' % (self.passphrase,
                                               passphrase_new, passphrase_new))
        try:
            s3ql.cli.tune.main(['--change-passphrase', self.bucketname ])
        except SystemExit as exc:
            self.fail("tune.s3ql failed: %s" % exc)


        bucket = local.Connection().get_bucket(os.path.join(self.bucket_dir, 'mybucket'))
        bucket.passphrase = passphrase_new

        bucket.passphrase = bucket['s3ql_passphrase']
        self.assertTrue(isinstance(bucket['s3ql_parameters_0'], str))


    # TODO: test tune.s3ql --copy

    # TODO: test tune.s3ql --delete


# Somehow important according to pyunit documentation
def suite():
    return unittest.makeSuite(TuneTests)


# Allow calling from command line
if __name__ == "__main__":
    unittest.main()
