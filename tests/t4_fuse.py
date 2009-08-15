#!/usr/bin/env python
#
#    Copyright (C) 2008  Nikolaus Rath <Nikolaus@rath.org>
#
#    This program can be distributed under the terms of the GNU LGPL.
#

from __future__ import unicode_literals
import shutil
import os
import stat
from os.path  import basename
from random   import randrange
from s3ql.common import waitfor
import filecmp
import tempfile
import time
import posixpath
import unittest

class fuse_tests(unittest.TestCase): 
    def setUp(self):
        self.base = tempfile.mkdtemp()

        # We need this to test multi block operations
        self.src = __file__
        fstat = os.stat(self.src)
        if fstat.st_size <= 1024: # 1 kb blocksize, see below
            raise RuntimeError("test file %s should be bigger than 1 kb" % self.src)

    @staticmethod
    def random_name():
        return "s3ql" + str(randrange(100, 999, 1))

    def test_mount(self):
        """Operations on mounted filesystem
        """

              
        # Mount
        path = os.path.join(os.path.dirname(__file__), "..", "mount.s3ql_local")
        pid = os.spawnl(os.P_NOWAIT, path, "mount.s3ql_local",
                             "--fg", "--fsck", "--blocksize", "1",
                             "--quiet", self.base)

        # Wait for mountpoint to come up
        self.assertTrue(waitfor(10, posixpath.ismount, self.base))

        # Run Subtests
        try:
            self.t_mkdir()
            self.t_symlink()
            self.t_mknod()
            self.t_readdir()
            self.t_symlink()
            self.t_truncate()
        finally:
            # Umount
            time.sleep(1)
            self.assertEquals(os.spawnlp(os.P_WAIT, "fusermount",
                                    "fusermount", "-u", self.base), 0)
            (dummy, status) = os.waitpid(pid, 0)

            self.assertTrue(os.WIFEXITED(status))
            self.assertEquals(os.WEXITSTATUS(status), 0)
            self.assertFalse(posixpath.ismount(self.base))
            os.rmdir(self.base)


    def t_mkdir(self):
        dirname = self.random_name()
        fullname = self.base + "/" + dirname
        os.mkdir(fullname)
        fstat = os.stat(fullname)
        self.assertTrue(stat.S_ISDIR(fstat.st_mode))
        self.assertEquals(os.listdir(fullname), [])
        self.assertEquals(fstat.st_nlink, 2)
        self.assertTrue(dirname in os.listdir(self.base))
        os.rmdir(fullname)
        self.assertRaises(OSError, os.stat, fullname)
        self.assertTrue(dirname not in os.listdir(self.base))

    def t_symlink(self):
        linkname = self.random_name()
        fullname = self.base + "/" + linkname
        os.symlink("/imaginary/dest", fullname)
        fstat = os.lstat(fullname)
        self.assertTrue(stat.S_ISLNK(fstat.st_mode))
        self.assertEquals(os.readlink(fullname), "/imaginary/dest")
        self.assertEquals(fstat.st_nlink, 1)
        self.assertTrue(linkname in os.listdir(self.base))
        os.unlink(fullname)
        self.assertRaises(OSError, os.lstat, fullname)
        self.assertTrue(linkname not in os.listdir(self.base))

    def t_mknod(self):
        filename = self.base + "/" + self.random_name()
        src = self.src
        shutil.copyfile(src, filename)
        fstat = os.lstat(filename)
        self.assertTrue(stat.S_ISREG(fstat.st_mode))
        self.assertEquals(fstat.st_nlink, 1)
        self.assertTrue(basename(filename) in os.listdir(self.base))
        self.assertTrue(filecmp.cmp(src, filename, False))
        os.unlink(filename)
        self.assertRaises(OSError, os.stat, filename)
        self.assertTrue(basename(filename) not in os.listdir(self.base))


    def t_link(self):
        name1 = self.base + "/" + self.random_name()
        name2 = self.base + "/" + self.random_name()
        src = self.src
        shutil.copyfile(src, name1)
        os.link(name1, name2)

        fstat1 = os.lstat(name1)
        fstat2 = os.lstat(name2)

        self.assertEquals(fstat1, fstat2)
        self.assertEquals(fstat1.st_nlink, 2)

        self.assertTrue(basename(name2) in os.listdir(self.base))
        self.assertTrue(filecmp.cmp(name1, name2, False))
        os.unlink(name2)
        fstat1 = os.lstat(name1)
        self.assertEquals(fstat1.st_nlink, 1)
        os.unlink(name1)

    def t_readdir(self):
        dir_ = self.base + "/" + self.random_name()
        file_ = dir_ + "/" + self.random_name()
        subdir = dir_ + "/" + self.random_name()
        subfile = subdir + "/" + self.random_name()
        src = self.src

        os.mkdir(dir_)
        shutil.copyfile(src, file_)
        os.mkdir(subdir)
        shutil.copyfile(src, subfile)

        listdir_is = os.listdir(dir_)
        listdir_is.sort()
        listdir_should = [ basename(file_), basename(subdir) ]
        listdir_should.sort()
        self.assertEquals(listdir_is, listdir_should)

        os.unlink(file_)
        os.unlink(subfile)
        os.rmdir(subdir)
        os.rmdir(dir_)

    def t_truncate(self):
        filename = self.base + "/" + self.random_name()
        src = self.src
        shutil.copyfile(src, filename)
        fstat = os.stat(filename)
        size = fstat.st_size
        fd = os.open(filename, os.O_RDWR)

        os.ftruncate(fd, size + 1024) # add > 1 block
        self.assertEquals(os.stat(filename).st_size, size + 1024)

        os.ftruncate(fd, size - 1024) # Truncate > 1 block
        self.assertEquals(os.stat(filename).st_size, size - 1024)

        os.close(fd)
        os.unlink(filename)


# Somehow important according to pyunit documentation
def suite():
    return unittest.makeSuite(fuse_tests)


# Allow calling from command line
if __name__ == "__main__":
    unittest.main()
