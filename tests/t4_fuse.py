#!/usr/bin/env python3
'''
t4_fuse.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

if __name__ == '__main__':
    import pytest
    import sys
    sys.exit(pytest.main([__file__] + sys.argv[1:]))

from os.path import basename
from s3ql import CTRL_NAME
from s3ql.common import path2bytes
from common import retry, skip_if_no_fusermount, RetryTimeoutError
import filecmp
import pyfuse3
import os.path
import shutil
import platform
import stat
import subprocess
import tempfile
import pytest
from pytest import raises as assert_raises

# For debugging
USE_VALGRIND = False

@pytest.mark.usefixtures('pass_s3ql_cmd_argv', 'pass_reg_output')
class TestFuse:

    def setup_method(self, method):
        if platform.system() != 'Darwin':
            skip_if_no_fusermount()

        # We need this to test multi block operations
        self.src = __file__
        if os.path.getsize(self.src) < 1048:
            raise RuntimeError("test file %s should be bigger than 1 KiB" % self.src)

        self.mnt_dir = tempfile.mkdtemp(prefix='s3ql-mnt-')
        self.cache_dir = tempfile.mkdtemp(prefix='s3ql-cache-')
        self.backend_dir = tempfile.mkdtemp(prefix='s3ql-backend-')

        self.storage_url = 'local://%s/' % (self.backend_dir,)
        self.passphrase = 'oeut3d'
        self.backend_login = None
        self.backend_passphrase = None

        self.mount_process = None
        self.name_cnt = 0

    def mkfs(self, max_obj_size=500):
        argv = (self.s3ql_cmd_argv('mkfs.s3ql') +
                [ '-L', 'test fs', '--max-obj-size', str(max_obj_size),
                  '--cachedir', self.cache_dir, '--quiet',
                  '--authfile', '/dev/null', self.storage_url ])
        if self.passphrase is None:
            argv.append('--plain')

        proc = subprocess.Popen(argv, stdin=subprocess.PIPE, universal_newlines=True)

        if self.backend_login is not None:
            print(self.backend_login, file=proc.stdin)
            print(self.backend_passphrase, file=proc.stdin)
        if self.passphrase is not None:
            print(self.passphrase, file=proc.stdin)
            print(self.passphrase, file=proc.stdin)
        proc.stdin.close()

        assert proc.wait() == 0
        self.reg_output(r'^WARNING: Maximum object sizes less than '
                        '1 MiB will degrade performance\.$', count=1)

    def mount(self, expect_fail=None, extra_args=[]):
        cmd = (self.s3ql_cmd_argv('mount.s3ql') +
               ["--fg", '--cachedir', self.cache_dir, '--log', 'none',
                '--compress', 'zlib', '--quiet', self.storage_url, self.mnt_dir,
                '--authfile', '/dev/null' ] + extra_args)
        self.mount_process = subprocess.Popen(cmd, stdin=subprocess.PIPE,
                                              universal_newlines=True)
        if self.backend_login is not None:
            print(self.backend_login, file=self.mount_process.stdin)
            print(self.backend_passphrase, file=self.mount_process.stdin)
        if self.passphrase is not None:
            print(self.passphrase, file=self.mount_process.stdin)
        self.mount_process.stdin.close()

        if expect_fail:
            retry(10, self.mount_process.poll)
            assert self.mount_process.returncode == expect_fail
        else:
            def poll():
                if os.path.ismount(self.mnt_dir):
                    return True
                assert self.mount_process.poll() is None
            retry(10, poll)

    def umount(self):
        with open('/dev/null', 'wb') as devnull:
            retry(5, lambda: subprocess.call(['fuser', '-m', self.mnt_dir],
                                             stdout=devnull, stderr=devnull) == 1)

        proc = subprocess.Popen(self.s3ql_cmd_argv('umount.s3ql') +
                                ['--quiet', self.mnt_dir])
        retry(30, lambda : proc.poll() is not None)
        assert proc.wait() == 0

        assert self.mount_process.poll() == 0
        assert not os.path.ismount(self.mnt_dir)

    def fsck(self, expect_retcode=0, args=[]):
        proc = subprocess.Popen(self.s3ql_cmd_argv('fsck.s3ql') +
                                [ '--force', '--quiet', '--log', 'none', '--cachedir',
                                  self.cache_dir, '--authfile', '/dev/null',
                                  self.storage_url ] + args,
                                stdin=subprocess.PIPE, universal_newlines=True)
        if self.backend_login is not None:
            print(self.backend_login, file=proc.stdin)
            print(self.backend_passphrase, file=proc.stdin)
        if self.passphrase is not None:
            print(self.passphrase, file=proc.stdin)
        proc.stdin.close()
        assert proc.wait() == expect_retcode

    def umount_fuse(self):
        with open('/dev/null', 'wb') as devnull:
            if platform.system() == 'Darwin':
                subprocess.call(['umount', '-l', self.mnt_dir], stderr=devnull)
            else:
                subprocess.call(['fusermount', '-z', '-u', self.mnt_dir],
                                stderr=devnull)

    def flush_cache(self):
        subprocess.check_call(self.s3ql_cmd_argv('s3qlctrl') +
                              [ '--quiet', 'flushcache', self.mnt_dir ])


    def teardown_method(self, method):
        self.umount_fuse()
        os.rmdir(self.mnt_dir)

        # Give mount process a little while to terminate
        if self.mount_process is not None:
            try:
                retry(10, lambda : self.mount_process.poll() is not None)
            except TimeoutError:
                self.mount_process.terminate()
                try:
                    self.mount_process.wait(1)
                except subprocess.TimeoutExpired:
                    self.mount_process.kill()

        shutil.rmtree(self.cache_dir)
        shutil.rmtree(self.backend_dir)

    def test(self):
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

        # Test metadata recovery
        shutil.rmtree(self.cache_dir)
        self.cache_dir = tempfile.mkdtemp(prefix='s3ql-cache-')
        self.fsck()

        shutil.rmtree(self.cache_dir)
        self.cache_dir = tempfile.mkdtemp(prefix='s3ql-cache-')
        self.mount()
        self.umount()


    def newname(self):
        self.name_cnt += 1
        return "s3ql_%d" % self.name_cnt

    def tst_mkdir(self):
        dirname = self.newname()
        fullname = self.mnt_dir + "/" + dirname
        os.mkdir(fullname)
        fstat = os.stat(fullname)
        assert stat.S_ISDIR(fstat.st_mode)
        assert pyfuse3.listdir(fullname) ==  []
        assert fstat.st_nlink == 1
        assert dirname in pyfuse3.listdir(self.mnt_dir)
        os.rmdir(fullname)
        assert_raises(FileNotFoundError, os.stat, fullname)
        assert dirname not in pyfuse3.listdir(self.mnt_dir)

    def tst_symlink(self):
        linkname = self.newname()
        fullname = self.mnt_dir + "/" + linkname
        os.symlink("/imaginary/dest", fullname)
        fstat = os.lstat(fullname)
        assert stat.S_ISLNK(fstat.st_mode)
        assert os.readlink(fullname) == "/imaginary/dest"
        assert fstat.st_nlink == 1
        assert linkname in pyfuse3.listdir(self.mnt_dir)
        os.unlink(fullname)
        assert_raises(FileNotFoundError, os.lstat, fullname)
        assert linkname not in pyfuse3.listdir(self.mnt_dir)

    def tst_mknod(self):
        filename = os.path.join(self.mnt_dir, self.newname())
        src = self.src
        shutil.copyfile(src, filename)
        fstat = os.lstat(filename)
        assert stat.S_ISREG(fstat.st_mode)
        assert fstat.st_nlink == 1
        assert basename(filename) in pyfuse3.listdir(self.mnt_dir)
        assert filecmp.cmp(src, filename, False)
        os.unlink(filename)
        assert_raises(FileNotFoundError, os.stat, filename)
        assert basename(filename) not in pyfuse3.listdir(self.mnt_dir)

    def tst_chown(self):
        filename = os.path.join(self.mnt_dir, self.newname())
        os.mkdir(filename)
        fstat = os.lstat(filename)
        uid = fstat.st_uid
        gid = fstat.st_gid

        uid_new = uid + 1
        os.chown(filename, uid_new, -1)
        fstat = os.lstat(filename)
        assert fstat.st_uid == uid_new
        assert fstat.st_gid == gid

        gid_new = gid + 1
        os.chown(filename, -1, gid_new)
        fstat = os.lstat(filename)
        assert fstat.st_uid == uid_new
        assert fstat.st_gid == gid_new

        os.rmdir(filename)
        assert_raises(FileNotFoundError, os.stat, filename)
        assert basename(filename) not in pyfuse3.listdir(self.mnt_dir)

    def tst_write(self):
        name = os.path.join(self.mnt_dir, self.newname())
        src = self.src
        shutil.copyfile(src, name)
        assert filecmp.cmp(name, src, False)

        # Don't unlink file, we want to see if cache flushing
        # works

    def tst_statvfs(self):
        os.statvfs(self.mnt_dir)

    def tst_link(self):
        name1 = os.path.join(self.mnt_dir, self.newname())
        name2 = os.path.join(self.mnt_dir, self.newname())
        src = self.src
        shutil.copyfile(src, name1)
        assert filecmp.cmp(name1, src, False)
        os.link(name1, name2)

        fstat1 = os.lstat(name1)
        fstat2 = os.lstat(name2)

        assert fstat1 == fstat2
        assert fstat1.st_nlink == 2

        assert basename(name2) in pyfuse3.listdir(self.mnt_dir)
        assert filecmp.cmp(name1, name2, False)
        os.unlink(name2)
        fstat1 = os.lstat(name1)
        assert fstat1.st_nlink == 1
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

        listdir_is = pyfuse3.listdir(dir_)
        listdir_is.sort()
        listdir_should = [ basename(file_), basename(subdir) ]
        listdir_should.sort()
        assert listdir_is == listdir_should

        os.unlink(file_)
        os.unlink(subfile)
        os.rmdir(subdir)
        os.rmdir(dir_)

    def tst_truncate(self):
        filename = os.path.join(self.mnt_dir, self.newname())
        src = self.src
        shutil.copyfile(src, filename)
        assert filecmp.cmp(filename, src, False)
        fstat = os.stat(filename)
        size = fstat.st_size
        fd = os.open(filename, os.O_RDWR)

        os.ftruncate(fd, size + 1024) # add > 1 block
        assert os.stat(filename).st_size == size + 1024

        os.ftruncate(fd, size - 1024) # Truncate > 1 block
        assert os.stat(filename).st_size == size - 1024

        os.close(fd)
        os.unlink(filename)

    def tst_truncate_nocache(self):
        filename = os.path.join(self.mnt_dir, self.newname())
        src = self.src
        shutil.copyfile(src, filename)
        assert filecmp.cmp(filename, src, False)
        fstat = os.stat(filename)
        size = fstat.st_size

        self.flush_cache()

        fd = os.open(filename, os.O_RDWR)

        os.ftruncate(fd, size + 1024) # add > 1 block
        assert os.stat(filename).st_size == size + 1024

        os.ftruncate(fd, size - 1024) # Truncate > 1 block
        assert os.stat(filename).st_size == size - 1024

        os.close(fd)
        os.unlink(filename)

    def tst_bug382(self):
        dirname = self.newname()
        fullname = self.mnt_dir + "/" + dirname
        os.mkdir(fullname)
        assert stat.S_ISDIR(os.stat(fullname).st_mode)
        assert dirname in pyfuse3.listdir(self.mnt_dir)
        cmd = ('(%d, %r)' % (pyfuse3.ROOT_INODE, path2bytes(dirname))).encode()
        pyfuse3.setxattr('%s/%s' % (self.mnt_dir, CTRL_NAME), 'rmtree', cmd)
        # Invalidation is asynchronous...
        try:
            retry(5, lambda: not os.path.exists(fullname))
        except RetryTimeoutError:
            pass # assert_raises should fail
        assert_raises(FileNotFoundError, os.stat, fullname)
        assert dirname not in pyfuse3.listdir(self.mnt_dir)
