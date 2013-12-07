'''
t4_fuse.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from os.path import basename
from s3ql.common import CTRL_NAME, PICKLE_PROTOCOL, path2bytes
from common import retry, BASEDIR, skip_if_no_fusermount
import pickle
import filecmp
import llfuse
import os.path
import shutil
import platform
import stat
import subprocess
import sys
import tempfile
import unittest

# For debugging
USE_VALGRIND = False

class fuse_tests(unittest.TestCase):

    def setUp(self):
        if platform.system() != 'Darwin':
            skip_if_no_fusermount()

        # We need this to test multi block operations
        self.src = __file__
        if os.path.getsize(self.src) < 1048:
            raise RuntimeError("test file %s should be bigger than 1 KiB" % self.src)

        self.mnt_dir = tempfile.mkdtemp(prefix='s3ql-mnt-')
        self.cache_dir = tempfile.mkdtemp(prefix='s3ql-cache-')
        self.backend_dir = tempfile.mkdtemp(prefix='s3ql-backend-')

        self.storage_url = 'local://' + self.backend_dir
        self.passphrase = 'oeut3d'
        self.backend_login = None
        self.backend_passphrase = None

        self.mount_process = None
        self.name_cnt = 0

    def mkfs(self, max_obj_size=500):
        proc = subprocess.Popen([sys.executable, os.path.join(BASEDIR, 'bin', 'mkfs.s3ql'),
                                 '-L', 'test fs', '--max-obj-size', str(max_obj_size),
                                 '--fatal-warnings', '--cachedir', self.cache_dir, '--quiet',
                                 '--authfile', '/dev/null', self.storage_url ],
                                stdin=subprocess.PIPE, universal_newlines=True)

        if self.backend_login is not None:
            print(self.backend_login, file=proc.stdin)
            print(self.backend_passphrase, file=proc.stdin)
        print(self.passphrase, file=proc.stdin)
        print(self.passphrase, file=proc.stdin)
        proc.stdin.close()

        self.assertEqual(proc.wait(), 0)

    def mount(self, fatal_warnings=True):
        cmd = [sys.executable, os.path.join(BASEDIR, 'bin', 'mount.s3ql'),
               "--fg", '--cachedir', self.cache_dir, '--log', 'none',
               '--compress', 'zlib', '--quiet', self.storage_url, self.mnt_dir,
               '--authfile', '/dev/null' ]
        if fatal_warnings:
            cmd.append('--fatal-warnings')
        self.mount_process = subprocess.Popen(cmd, stdin=subprocess.PIPE,
                                              universal_newlines=True)
        if self.backend_login is not None:
            print(self.backend_login, file=self.mount_process.stdin)
            print(self.backend_passphrase, file=self.mount_process.stdin)
        print(self.passphrase, file=self.mount_process.stdin)
        self.mount_process.stdin.close()
        def poll():
            if os.path.ismount(self.mnt_dir):
                return True
            self.assertIsNone(self.mount_process.poll())
        retry(30, poll)

    def umount(self):
        with open('/dev/null', 'wb') as devnull:
            retry(5, lambda: subprocess.call(['fuser', '-m', self.mnt_dir],
                                             stdout=devnull, stderr=devnull) == 1)

        proc = subprocess.Popen([sys.executable, os.path.join(BASEDIR, 'bin', 'umount.s3ql'),
                                 '--quiet', self.mnt_dir])
        retry(90, lambda : proc.poll() is not None)
        self.assertEqual(proc.wait(), 0)

        self.assertEqual(self.mount_process.poll(), 0)
        self.assertFalse(os.path.ismount(self.mnt_dir))

    def fsck(self):
        # Use fsck to test authinfo reading
        with tempfile.NamedTemporaryFile('wt') as authinfo_fh:
            print('[entry1]',
                  'storage-url: %s' % self.storage_url[:6],
                  'fs-passphrase: clearly wrong',
                  'backend-login: bla',
                  'backend-password: not much better',
                  '',
                  '[entry2]',
                  'storage-url: %s' % self.storage_url,
                  'fs-passphrase: %s' % self.passphrase,
                  'backend-login: %s' % self.backend_login,
                  'backend-password:%s' % self.backend_passphrase,
                  file=authinfo_fh, sep='\n')
            authinfo_fh.flush()

            proc = subprocess.Popen([sys.executable, os.path.join(BASEDIR, 'bin', 'fsck.s3ql'),
                                     '--force', '--quiet', '--log', 'none', '--cachedir',
                                     self.cache_dir, '--fatal-warnings', '--authfile',
                                     authinfo_fh.name, self.storage_url ],
                                    stdin=subprocess.PIPE, universal_newlines=True)
            proc.stdin.close()
            self.assertEqual(proc.wait(), 0)

    def tearDown(self):
        with open('/dev/null', 'wb') as devnull:
            if platform.system() == 'Darwin':
                subprocess.call(['umount', '-l', self.mnt_dir], stderr=devnull)
            else:
                subprocess.call(['fusermount', '-z', '-u', self.mnt_dir],
                                stderr=devnull)
        os.rmdir(self.mnt_dir)

        # Give mount process a little while to terminate
        if self.mount_process is not None:
            try:
                retry(90, lambda : self.mount_process.poll() is not None)
            except TimeoutError:
                # Ignore errors  during teardown
                pass

        shutil.rmtree(self.cache_dir)
        shutil.rmtree(self.backend_dir)


    def test_all(self):
        # Workaround py.test not calling runTest   
        return self.runTest()
        
    def runTest(self):
        # Run all tests in same environment, mounting and umounting
        # just takes too long otherwise

        self.mkfs()
        self.mount()
        self.tst_chown()
        self.tst_link()
        self.tst_mkdir()
        self.tst_mknod()
        self.tst_readdir()
        self.tst_statvfs()
        self.tst_symlink()
        self.tst_truncate()
        self.tst_truncate_nocache()
        self.tst_write()
        self.tst_bug382()
        self.umount()
        self.fsck()

    def newname(self):
        self.name_cnt += 1
        return "s3ql_%d" % self.name_cnt

    def tst_mkdir(self):
        dirname = self.newname()
        fullname = self.mnt_dir + "/" + dirname
        os.mkdir(fullname)
        fstat = os.stat(fullname)
        self.assertTrue(stat.S_ISDIR(fstat.st_mode))
        self.assertEqual(llfuse.listdir(fullname), [])
        self.assertEqual(fstat.st_nlink, 1)
        self.assertTrue(dirname in llfuse.listdir(self.mnt_dir))
        os.rmdir(fullname)
        self.assertRaises(FileNotFoundError, os.stat, fullname)
        self.assertTrue(dirname not in llfuse.listdir(self.mnt_dir))

    def tst_symlink(self):
        linkname = self.newname()
        fullname = self.mnt_dir + "/" + linkname
        os.symlink("/imaginary/dest", fullname)
        fstat = os.lstat(fullname)
        self.assertTrue(stat.S_ISLNK(fstat.st_mode))
        self.assertEqual(os.readlink(fullname), "/imaginary/dest")
        self.assertEqual(fstat.st_nlink, 1)
        self.assertTrue(linkname in llfuse.listdir(self.mnt_dir))
        os.unlink(fullname)
        self.assertRaises(FileNotFoundError, os.lstat, fullname)
        self.assertTrue(linkname not in llfuse.listdir(self.mnt_dir))

    def tst_mknod(self):
        filename = os.path.join(self.mnt_dir, self.newname())
        src = self.src
        shutil.copyfile(src, filename)
        fstat = os.lstat(filename)
        self.assertTrue(stat.S_ISREG(fstat.st_mode))
        self.assertEqual(fstat.st_nlink, 1)
        self.assertTrue(basename(filename) in llfuse.listdir(self.mnt_dir))
        self.assertTrue(filecmp.cmp(src, filename, False))
        os.unlink(filename)
        self.assertRaises(FileNotFoundError, os.stat, filename)
        self.assertTrue(basename(filename) not in llfuse.listdir(self.mnt_dir))

    def tst_chown(self):
        filename = os.path.join(self.mnt_dir, self.newname())
        os.mkdir(filename)
        fstat = os.lstat(filename)
        uid = fstat.st_uid
        gid = fstat.st_gid

        uid_new = uid + 1
        os.chown(filename, uid_new, -1)
        fstat = os.lstat(filename)
        self.assertEqual(fstat.st_uid, uid_new)
        self.assertEqual(fstat.st_gid, gid)

        gid_new = gid + 1
        os.chown(filename, -1, gid_new)
        fstat = os.lstat(filename)
        self.assertEqual(fstat.st_uid, uid_new)
        self.assertEqual(fstat.st_gid, gid_new)

        os.rmdir(filename)
        self.assertRaises(FileNotFoundError, os.stat, filename)
        self.assertTrue(basename(filename) not in llfuse.listdir(self.mnt_dir))


    def tst_write(self):
        name = os.path.join(self.mnt_dir, self.newname())
        src = self.src
        shutil.copyfile(src, name)
        self.assertTrue(filecmp.cmp(name, src, False))

        # Don't unlink file, we want to see if cache flushing
        # works

    def tst_statvfs(self):
        os.statvfs(self.mnt_dir)

    def tst_link(self):
        name1 = os.path.join(self.mnt_dir, self.newname())
        name2 = os.path.join(self.mnt_dir, self.newname())
        src = self.src
        shutil.copyfile(src, name1)
        self.assertTrue(filecmp.cmp(name1, src, False))
        os.link(name1, name2)

        fstat1 = os.lstat(name1)
        fstat2 = os.lstat(name2)

        self.assertEqual(fstat1, fstat2)
        self.assertEqual(fstat1.st_nlink, 2)

        self.assertTrue(basename(name2) in llfuse.listdir(self.mnt_dir))
        self.assertTrue(filecmp.cmp(name1, name2, False))
        os.unlink(name2)
        fstat1 = os.lstat(name1)
        self.assertEqual(fstat1.st_nlink, 1)
        os.unlink(name1)

    def tst_readdir(self):
        dir_ = os.path.join(self.mnt_dir, self.newname())
        file_ = dir_ + "/" + self.newname()
        subdir = dir_ + "/" + self.newname()
        subfile = subdir + "/" + self.newname()
        src = self.src

        os.mkdir(dir_)
        shutil.copyfile(src, file_)
        os.mkdir(subdir)
        shutil.copyfile(src, subfile)

        listdir_is = llfuse.listdir(dir_)
        listdir_is.sort()
        listdir_should = [ basename(file_), basename(subdir) ]
        listdir_should.sort()
        self.assertEqual(listdir_is, listdir_should)

        os.unlink(file_)
        os.unlink(subfile)
        os.rmdir(subdir)
        os.rmdir(dir_)

    def tst_truncate(self):
        filename = os.path.join(self.mnt_dir, self.newname())
        src = self.src
        shutil.copyfile(src, filename)
        self.assertTrue(filecmp.cmp(filename, src, False))
        fstat = os.stat(filename)
        size = fstat.st_size
        fd = os.open(filename, os.O_RDWR)

        os.ftruncate(fd, size + 1024) # add > 1 block
        self.assertEqual(os.stat(filename).st_size, size + 1024)

        os.ftruncate(fd, size - 1024) # Truncate > 1 block
        self.assertEqual(os.stat(filename).st_size, size - 1024)

        os.close(fd)
        os.unlink(filename)

    def tst_truncate_nocache(self):
        filename = os.path.join(self.mnt_dir, self.newname())
        src = self.src
        shutil.copyfile(src, filename)
        self.assertTrue(filecmp.cmp(filename, src, False))
        fstat = os.stat(filename)
        size = fstat.st_size

        subprocess.check_call([sys.executable, os.path.join(BASEDIR, 'bin', 's3qlctrl'),
                               '--quiet', 'flushcache', self.mnt_dir ])

        fd = os.open(filename, os.O_RDWR)

        os.ftruncate(fd, size + 1024) # add > 1 block
        self.assertEqual(os.stat(filename).st_size, size + 1024)

        os.ftruncate(fd, size - 1024) # Truncate > 1 block
        self.assertEqual(os.stat(filename).st_size, size - 1024)

        os.close(fd)
        os.unlink(filename)

    def tst_bug382(self):
        dirname = self.newname()
        fullname = self.mnt_dir + "/" + dirname
        os.mkdir(fullname)
        self.assertTrue(stat.S_ISDIR(os.stat(fullname).st_mode))
        self.assertTrue(dirname in llfuse.listdir(self.mnt_dir))
        llfuse.setxattr('%s/%s' % (self.mnt_dir, CTRL_NAME), 
                        'rmtree', pickle.dumps((llfuse.ROOT_INODE, path2bytes(dirname)),
                                               PICKLE_PROTOCOL))
        self.assertRaises(FileNotFoundError, os.stat, fullname)
        self.assertTrue(dirname not in llfuse.listdir(self.mnt_dir))

        
