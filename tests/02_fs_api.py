#!/usr/bin/env python
#
#    Copyright (C) 2008  Nikolaus Rath <Nikolaus@rath.org>
#
#    This program can be distributed under the terms of the GNU LGPL.
#

import tempfile
import unittest
import s3ql
import stat
import os
from random   import randrange

class fs_api_tests(unittest.TestCase):

    def setUp(self):
        self.bucket = s3ql.s3.LocalBucket()
        self.bucket.tx_delay = 0
        self.bucket.prop_delay = 0

        self.dbfile = tempfile.mktemp()
        self.cachedir = tempfile.mkdtemp() + "/"
        self.blocksize = 1024

        s3ql.setup_db(self.dbfile, self.blocksize)
        s3ql.setup_bucket(self.bucket, self.dbfile)

        self.server = s3ql.fs(self.bucket, self.dbfile, self.cachedir)

    def random_name(self):
        return "s3ql" + str(randrange(100,999,1))

    def test_getattr_root(self):
        fstat = self.server.getattr("/")
        self.assertTrue(stat.S_ISDIR(fstat.st_mode))

    def test_mkdir_rmdir(self):
        linkcnt = self.server.getattr("/").st_nlink

        name = os.path.join("/",  self.random_name())
        self.assertRaises(s3ql.FUSEError, self.server.getattr, name)
        self.server.mkdir(name, stat.S_IRUSR | stat.S_IXUSR)
        fstat = self.server.getattr(name)

        self.assertEquals(self.server.getattr("/").st_nlink, linkcnt+1)
        self.assertTrue(stat.S_ISDIR(fstat.st_mode))
        self.assertEquals(fstat.st_nlink, 2)

        sub = os.path.join(name, self.random_name())
        self.assertRaises(s3ql.FUSEError, self.server.getattr, sub)
        self.server.mkdir(sub, stat.S_IRUSR | stat.S_IXUSR)

        fstat = self.server.getattr(name)
        fstat2 = self.server.getattr(sub)

        self.assertTrue(stat.S_ISDIR(fstat2.st_mode))
        self.assertEquals(fstat.st_nlink, 3)
        self.assertEquals(fstat2.st_nlink, 2)
        self.assertTrue(self.server.getattr("/").st_nlink == linkcnt+1)

        self.assertRaises(s3ql.FUSEError, self.server.rmdir, name)
        self.server.rmdir(sub)
        self.assertRaises(s3ql.FUSEError, self.server.getattr, sub)
        self.assertEquals(self.server.getattr(name).st_nlink, 2)

        self.server.rmdir(name)
        self.assertRaises(s3ql.FUSEError, self.server.getattr, name)
        self.assertTrue(self.server.getattr("/").st_nlink == linkcnt)

    def test_symlink(self):
        name = os.path.join("/",  self.random_name())
        target = "../../wherever/this/is"
        self.assertRaises(s3ql.FUSEError, self.server.getattr, name)
        self.server.symlink(target, name)
        fstat = self.server.getattr(name)

        self.assertTrue(stat.S_ISLNK(fstat.st_mode))
        self.assertEquals(fstat.st_nlink, 1)

        self.assertEquals(self.server.readlink(name), target)

        self.server.unlink(name)
        self.assertRaises(s3ql.FUSEError, self.server.getattr, name)


    # Check that s3 object locking works when retrieving

    # Check that s3 object locking works when creating

    # Check that s3 objects are committed after fsync

    def tearDown(self):

        self.server.close()
        os.unlink(self.dbfile)
        os.rmdir(self.cachedir)


# Somehow important according to pyunit documentation
def suite():
    return unittest.makeSuite(fs_api_tests)


# Allow calling from command line
if __name__ == "__main__":
            unittest.main()
