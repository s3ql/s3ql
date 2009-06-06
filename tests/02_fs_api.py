#!/usr/bin/env python
#
#    Copyright (C) 2008  Nikolaus Rath <Nikolaus@rath.org>
#
#    This program can be distributed under the terms of the GNU LGPL.
#

import tempfile
import unittest
import s3ql
import apsw
import stat
import os
import time
import fuse
import resource
from random import randrange

class fs_api_tests(unittest.TestCase):

    def setUp(self):
        self.bucket = s3ql.s3.LocalBucket()
        self.bucket.tx_delay = 0
        self.bucket.prop_delay = 0

        self.dbfile = tempfile.mktemp()
        self.cachedir = tempfile.mkdtemp() + "/"
        self.blocksize = 1024

        # Only warnings and errors
        s3ql.log_level = 0

        s3ql.setup_db(self.dbfile, self.blocksize)
        s3ql.setup_bucket(self.bucket, self.dbfile)

        self.server = s3ql.server(self.bucket, self.dbfile, self.cachedir)


    def tearDown(self):
        # May not have been called if a test failed
        if hasattr(self, "server"):
            self.server.close()
        os.unlink(self.dbfile)
        os.rmdir(self.cachedir)


    def fsck(self):
        self.server.close()
        del self.server
        conn = apsw.Connection(self.dbfile)
        self.assertTrue(s3ql.fsck.a_check_parameters(conn, checkonly=True))
        self.assertTrue(s3ql.fsck.b_check_cache(conn, self.cachedir, self.bucket, checkonly=True))
        self.assertTrue(s3ql.fsck.c_check_contents(conn, checkonly=True))
        self.assertTrue(s3ql.fsck.d_check_inodes(conn, checkonly=True))
        self.assertTrue(s3ql.fsck.e_check_s3(conn, self.bucket, checkonly=True))
        self.assertTrue(s3ql.fsck.f_check_keylist(conn, self.bucket, checkonly=True))

    def random_name(self, prefix=""):
        return "s3ql" + prefix + str(randrange(100,999,1))

    def test_01_getattr_root(self):
        fstat = self.server.getattr("/")
        self.assertTrue(stat.S_ISDIR(fstat["st_mode"]))
        self.fsck()

    def test_02_utimens(self):
        # We work on the root directory
        path="/"
        fstat_old = self.server.getattr(path)
        atime_new = fstat_old["st_atime"] - 72
        mtime_new = fstat_old["st_mtime"] - 72
        self.server.utimens(path, (atime_new, mtime_new))
        fstat_new = self.server.getattr(path)

        self.assertEquals(fstat_new["st_mtime"], mtime_new)
        self.assertEquals(fstat_new["st_atime"], atime_new)
        self.assertTrue(fstat_new["st_ctime"] > fstat_old["st_ctime"])

        self.fsck()

    def test_03_mkdir_rmdir(self):
        linkcnt = self.server.getattr("/")["st_nlink"]

        name = os.path.join("/",  self.random_name())
        mtime_old = self.server.getattr("/")["st_mtime"]
        self.assertRaises(s3ql.FUSEError, self.server.getattr, name)
        self.server.mkdir(name, stat.S_IRUSR | stat.S_IXUSR)
        self.assertTrue(self.server.getattr("/")["st_mtime"] > mtime_old)
        fstat = self.server.getattr(name)

        self.assertEquals(self.server.getattr("/")["st_nlink"], linkcnt+1)
        self.assertTrue(stat.S_ISDIR(fstat["st_mode"]))
        self.assertEquals(fstat["st_nlink"], 2)

        sub = os.path.join(name, self.random_name())
        self.assertRaises(s3ql.FUSEError, self.server.getattr, sub)
        self.server.mkdir(sub, stat.S_IRUSR | stat.S_IXUSR)

        fstat = self.server.getattr(name)
        fstat2 = self.server.getattr(sub)

        self.assertTrue(stat.S_ISDIR(fstat2["st_mode"]))
        self.assertEquals(fstat["st_nlink"], 3)
        self.assertEquals(fstat2["st_nlink"], 2)
        self.assertTrue(self.server.getattr("/")["st_nlink"] == linkcnt+1)

        self.assertRaises(s3ql.FUSEError, self.server.rmdir, name)
        self.server.rmdir(sub)
        self.assertRaises(s3ql.FUSEError, self.server.getattr, sub)
        self.assertEquals(self.server.getattr(name)["st_nlink"], 2)

        mtime_old = self.server.getattr("/")["st_mtime"]
        self.server.rmdir(name)
        self.assertTrue(self.server.getattr("/")["st_mtime"] > mtime_old)
        self.assertRaises(s3ql.FUSEError, self.server.getattr, name)
        self.assertTrue(self.server.getattr("/")["st_nlink"] == linkcnt)

        self.fsck()

    def test_04_symlink(self):
        name = os.path.join("/",  self.random_name())
        target = "../../wherever/this/is"
        self.assertRaises(s3ql.FUSEError, self.server.getattr, name)
        mtime_old = self.server.getattr("/")["st_mtime"]
        self.server.symlink(name, target)
        self.assertTrue(self.server.getattr("/")["st_mtime"] > mtime_old)
        fstat = self.server.getattr(name)

        self.assertTrue(stat.S_ISLNK(fstat["st_mode"]))
        self.assertEquals(fstat["st_nlink"], 1)

        self.assertEquals(self.server.readlink(name), target)

        mtime_old = self.server.getattr("/")["st_mtime"]
        self.server.unlink(name)
        self.assertTrue(self.server.getattr("/")["st_mtime"] > mtime_old)
        self.assertRaises(s3ql.FUSEError, self.server.getattr, name)

        self.fsck()

    def test_05_create_unlink(self):
        name = os.path.join("/",  self.random_name())
        mode = ( stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR | stat.S_IRGRP )

        self.assertRaises(s3ql.FUSEError, self.server.getattr, name)
        mtime_old = self.server.getattr("/")["st_mtime"]
        fh = self.server.create(name, mode)
        self.server.release(name, fh)
        self.server.flush(name, fh)

        self.assertEquals(self.server.getattr(name)["st_mode"], mode | stat.S_IFREG)
        self.assertEquals(self.server.getattr(name)["st_nlink"], 1)
        self.assertTrue(self.server.getattr("/")["st_mtime"] > mtime_old)

        mtime_old = self.server.getattr("/")["st_mtime"]
        self.server.unlink(name)
        self.assertTrue(self.server.getattr("/")["st_mtime"] > mtime_old)
        self.assertRaises(s3ql.FUSEError, self.server.getattr, name)

        self.fsck()


    def test_06_chmod_chown(self):
        # Create file
        name = os.path.join("/",  self.random_name())
        mode = ( stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR | stat.S_IRGRP )
        fh = self.server.create(name, mode)
        self.server.release(name, fh)
        self.server.flush(name, fh)

        mode_new = ( stat.S_IFREG |
                     stat.S_IROTH | stat.S_IWOTH | stat.S_IXGRP | stat.S_IRGRP )
        ctime_old = self.server.getattr(name)["st_ctime"]
        self.server.chmod(name, mode_new)
        self.assertEquals(self.server.getattr(name)["st_mode"], mode_new | stat.S_IFREG)
        self.assertTrue(self.server.getattr(name)["st_ctime"] > ctime_old)

        uid_new = 1231
        gid_new = 3213
        ctime_old = self.server.getattr(name)["st_ctime"]
        self.server.chown(name, uid_new, gid_new)
        self.assertEquals(self.server.getattr(name)["st_uid"], uid_new)
        self.assertEquals(self.server.getattr(name)["st_gid"], gid_new)
        self.assertTrue(self.server.getattr(name)["st_ctime"] > ctime_old)

        self.server.unlink(name)
        self.fsck()

    def test_07_open_write_read(self):
        # Create file
        name = os.path.join("/",  self.random_name())
        mode = ( stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR | stat.S_IRGRP )
        fh = self.server.create(name, mode)
        self.server.release(name, fh)
        self.server.flush(name, fh)

        # Write testfile
        destfh = self.server.open(name, os.O_RDWR)
        bufsize = resource.getpagesize()

        srcfh = open(__file__, "rb")

        buf = srcfh.read(bufsize)
        off = 0
        while len(buf) != 0:
            self.assertEquals(self.server.write(name, buf, off, destfh), len(buf))
            off += len(buf)
            buf = srcfh.read(bufsize)

        # Read testfile
        srcfh.seek(0)
        buf = srcfh.read(bufsize)
        off = 0
        while len(buf) != 0:
            self.assertTrue(buf == self.server.read(name, bufsize, off, destfh))
            off += len(buf)
            buf = srcfh.read(bufsize)
        self.server.release(name, fh)
        self.server.flush(name, fh)

        srcfh.close()
        self.fsck()


    def test_08_link(self):
        # Create file
        target = os.path.join("/",  self.random_name("target"))
        mode = ( stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR | stat.S_IRGRP )
        fh = self.server.create(target, mode)
        self.server.release(target, fh)
        self.server.flush(target, fh)


        name = os.path.join("/",  self.random_name())
        self.assertRaises(s3ql.FUSEError, self.server.getattr, name)
        mtime_old = self.server.getattr("/")["st_mtime"]
        self.server.link(name, target)
        self.assertTrue(self.server.getattr("/")["st_mtime"] > mtime_old)
        fstat = self.server.getattr(name)

        self.assertEquals(fstat, self.server.getattr(target))
        self.assertEquals(fstat["st_nlink"], 2)

        self.server.unlink(name)
        self.assertEquals(self.server.getattr(target)["st_nlink"], 1)
        self.assertRaises(s3ql.FUSEError, self.server.getattr, name)

        self.server.unlink(target)
        self.assertRaises(s3ql.FUSEError, self.server.getattr, target)

        self.fsck()

    def test_09_write_read_cmplx(self):
        # Create file
        name = os.path.join("/",  self.random_name())
        mode = ( stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR | stat.S_IRGRP )
        fh = self.server.create(name, mode)
        self.server.release(name, fh)
        self.server.flush(name, fh)

        # Create file with holes
        data = "Teststring" * int(0.2 * self.blocksize)
        off = int(5.9 * self.blocksize)
        fh = self.server.open(name, os.O_RDWR)
        self.server.write(name, data, off, fh)
        filelen = len(data) + off
        self.assertEquals(self.server.getattr(name)["st_size"], filelen)

        off2 = int(0.5 * self.blocksize)
        self.assertEquals(self.server.read(name, len(data)+off2, off, fh), data)
        self.assertEquals(self.server.read(name, len(data)+off2, off-off2, fh), "\0" * off2 + data)
        self.assertEquals(self.server.read(name, 182, off+len(data), fh), "")


        off = int(1.9 * self.blocksize)
        self.server.write(name, data, off, fh)
        self.assertEquals(self.server.getattr(name)["st_size"], filelen)
        self.assertEquals(self.server.read(name, len(data)+off2, off, fh), data + "\0" * off2)

        self.server.release(name, fh)
        self.server.flush(name, fh)

        self.fsck()

    # Also check the addfile function from fsck.py here

    # Check that s3 object locking works when retrieving

    # Check that s3 object locking works when creating

    # Check that s3 objects are committed after fsync


# Somehow important according to pyunit documentation
def suite():
    return unittest.makeSuite(fs_api_tests)


# Allow calling from command line
if __name__ == "__main__":
    unittest.main()
