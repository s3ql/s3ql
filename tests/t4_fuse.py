'''
t4_fuse.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import absolute_import, division, print_function
from _common import TestCase
from os.path import basename
import filecmp
import llfuse
import os.path
import shutil
import stat
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import unittest2 as unittest
import logging

log = logging.getLogger()

# For debugging
USE_VALGRIND = False

class ExceptionStoringThread(threading.Thread):
    def __init__(self):
        super(ExceptionStoringThread, self).__init__()
        self._exc_info = None
        self._joined = False

    def run_protected(self):
        pass

    def run(self):
        try:
            self.run_protected()
        except:
            # This creates a circular reference chain
            self._exc_info = sys.exc_info()

    def join_get_exc(self):
        self._joined = True
        self.join()
        return self._exc_info

    def join_and_raise(self):
        '''Wait for the thread to finish, raise any occurred exceptions'''

        self._joined = True
        if self.is_alive():
            self.join()

        if self._exc_info is not None:
            # Break reference chain
            exc_info = self._exc_info
            self._exc_info = None
            raise EmbeddedException(exc_info, self.name)

    def __del__(self):
        if not self._joined:
            raise RuntimeError("ExceptionStoringThread instance was destroyed "
                               "without calling join_and_raise()!")

class EmbeddedException(Exception):
    '''Encapsulates an exception that happened in a different thread
    '''

    def __init__(self, exc_info, threadname):
        super(EmbeddedException, self).__init__()
        self.exc_info = exc_info
        self.threadname = threadname

        log.error('Thread %s terminated with exception:\n%s',
                  self.threadname, ''.join(traceback.format_exception(*self.exc_info)))

    def __str__(self):
        return ''.join(['caused by an exception in thread %s.\n' % self.threadname,
                       'Original/inner traceback (most recent call last): \n' ] +
                       traceback.format_exception(*self.exc_info))

class AsyncFn(ExceptionStoringThread):
    def __init__(self, fn, *args, **kwargs):
        super(AsyncFn, self).__init__()
        self.target = fn
        self.args = args
        self.kwargs = kwargs

    def run_protected(self):
        self.target(*self.args, **self.kwargs)

def retry(timeout, fn, *a, **kw):
    """Wait for fn(*a, **kw) to return True.
    
    If the return value of fn() returns something True, this value
    is returned. Otherwise, the function is called repeatedly for
    `timeout` seconds. If the timeout is reached, `TimeoutError` is
    raised.
    """

    step = 0.2
    waited = 0
    while waited < timeout:
        ret = fn(*a, **kw)
        if ret:
            return ret
        time.sleep(step)
        waited += step
        if step < waited / 30:
            step *= 2

    raise TimeoutError()

class TimeoutError(Exception):
    '''Raised by `retry()` when a timeout is reached.'''

    pass

def skip_if_no_fusermount():
    '''Raise SkipTest if fusermount is not available'''

    which = subprocess.Popen(['which', 'fusermount'], stdout=subprocess.PIPE)
    fusermount_path = which.communicate()[0].strip()

    if not fusermount_path or which.wait() != 0:
        raise unittest.SkipTest("Can't find fusermount executable")

    if not os.path.exists('/dev/fuse'):
        raise unittest.SkipTest("FUSE kernel module does not seem to be loaded")

    if os.getuid() == 0:
        return

    mode = os.stat(fusermount_path).st_mode
    if mode & stat.S_ISUID == 0:
        raise unittest.SkipTest('fusermount executable not setuid, and we are not root.')

    try:
        subprocess.check_call([fusermount_path, '-V'],
                              stdout=open('/dev/null', 'wb'))
    except subprocess.CalledProcessError:
        raise unittest.SkipTest('Unable to execute fusermount')

if __name__ == '__main__':
    mypath = sys.argv[0]
else:
    mypath = __file__
BASEDIR = os.path.abspath(os.path.join(os.path.dirname(mypath), '..'))

class fuse_tests(TestCase):

    def setUp(self):
        skip_if_no_fusermount()

        # We need this to test multi block operations
        self.src = __file__
        if os.path.getsize(self.src) < 1048:
            raise RuntimeError("test file %s should be bigger than 1 kb" % self.src)

        self.mnt_dir = tempfile.mkdtemp()
        self.cache_dir = tempfile.mkdtemp()
        self.backend_dir = tempfile.mkdtemp()

        self.storage_url = 'local://' + self.backend_dir
        self.passphrase = 'oeut3d'

        self.mount_process = None
        self.name_cnt = 0

    def mkfs(self):
        proc = subprocess.Popen([os.path.join(BASEDIR, 'bin', 'mkfs.s3ql'),
                                 '-L', 'test fs', '--max-obj-size', '500',
                                 '--cachedir', self.cache_dir, '--quiet',
                                 self.storage_url ], stdin=subprocess.PIPE)

        print(self.passphrase, file=proc.stdin)
        print(self.passphrase, file=proc.stdin)
        proc.stdin.close()

        self.assertEqual(proc.wait(), 0)

    def mount(self):
        self.mount_process = subprocess.Popen([os.path.join(BASEDIR, 'bin', 'mount.s3ql'),
                                                "--fg", '--cachedir', self.cache_dir,
                                                '--log', 'none', '--quiet',
                                                  self.storage_url, self.mnt_dir],
                                                  stdin=subprocess.PIPE)
        print(self.passphrase, file=self.mount_process.stdin)
        self.mount_process.stdin.close()
        def poll():
            if os.path.ismount(self.mnt_dir):
                return True
            self.assertIsNone(self.mount_process.poll())
        retry(30, poll)

    def umount(self):
        devnull = open('/dev/null', 'wb')
        retry(5, lambda: subprocess.call(['fuser', '-m', self.mnt_dir],
                                         stdout=devnull, stderr=devnull) == 1)

        proc = subprocess.Popen([os.path.join(BASEDIR, 'bin', 'umount.s3ql'),
                                 '--quiet', self.mnt_dir])
        retry(90, lambda : proc.poll() is not None)
        self.assertEquals(proc.wait(), 0)

        self.assertEqual(self.mount_process.wait(), 0)
        self.assertFalse(os.path.ismount(self.mnt_dir))

    def fsck(self):
        proc = subprocess.Popen([os.path.join(BASEDIR, 'bin', 'fsck.s3ql'),
                                 '--force', '--quiet', '--log', 'none',
                                 '--cachedir', self.cache_dir,
                                 self.storage_url ], stdin=subprocess.PIPE)
        print(self.passphrase, file=proc.stdin)
        proc.stdin.close()
        self.assertEqual(proc.wait(), 0)

    def tearDown(self):
        subprocess.call(['fusermount', '-z', '-u', self.mnt_dir],
                        stderr=open('/dev/null', 'wb'))
        os.rmdir(self.mnt_dir)

        # Give mount process a little while to terminate
        if self.mount_process is not None:
            retry(10, lambda : self.mount_process.poll() is not None)

        shutil.rmtree(self.cache_dir)
        shutil.rmtree(self.backend_dir)


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
        self.assertEquals(llfuse.listdir(fullname), [])
        self.assertEquals(fstat.st_nlink, 1)
        self.assertTrue(dirname in llfuse.listdir(self.mnt_dir))
        os.rmdir(fullname)
        self.assertRaises(OSError, os.stat, fullname)
        self.assertTrue(dirname not in llfuse.listdir(self.mnt_dir))

    def tst_symlink(self):
        linkname = self.newname()
        fullname = self.mnt_dir + "/" + linkname
        os.symlink("/imaginary/dest", fullname)
        fstat = os.lstat(fullname)
        self.assertTrue(stat.S_ISLNK(fstat.st_mode))
        self.assertEquals(os.readlink(fullname), "/imaginary/dest")
        self.assertEquals(fstat.st_nlink, 1)
        self.assertTrue(linkname in llfuse.listdir(self.mnt_dir))
        os.unlink(fullname)
        self.assertRaises(OSError, os.lstat, fullname)
        self.assertTrue(linkname not in llfuse.listdir(self.mnt_dir))

    def tst_mknod(self):
        filename = os.path.join(self.mnt_dir, self.newname())
        src = self.src
        shutil.copyfile(src, filename)
        fstat = os.lstat(filename)
        self.assertTrue(stat.S_ISREG(fstat.st_mode))
        self.assertEquals(fstat.st_nlink, 1)
        self.assertTrue(basename(filename) in llfuse.listdir(self.mnt_dir))
        self.assertTrue(filecmp.cmp(src, filename, False))
        os.unlink(filename)
        self.assertRaises(OSError, os.stat, filename)
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
        self.assertEquals(fstat.st_uid, uid_new)
        self.assertEquals(fstat.st_gid, gid)

        gid_new = gid + 1
        os.chown(filename, -1, gid_new)
        fstat = os.lstat(filename)
        self.assertEquals(fstat.st_uid, uid_new)
        self.assertEquals(fstat.st_gid, gid_new)

        os.rmdir(filename)
        self.assertRaises(OSError, os.stat, filename)
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

        self.assertEquals(fstat1, fstat2)
        self.assertEquals(fstat1.st_nlink, 2)

        self.assertTrue(basename(name2) in llfuse.listdir(self.mnt_dir))
        self.assertTrue(filecmp.cmp(name1, name2, False))
        os.unlink(name2)
        fstat1 = os.lstat(name1)
        self.assertEquals(fstat1.st_nlink, 1)
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
        self.assertEquals(listdir_is, listdir_should)

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
        self.assertEquals(os.stat(filename).st_size, size + 1024)

        os.ftruncate(fd, size - 1024) # Truncate > 1 block
        self.assertEquals(os.stat(filename).st_size, size - 1024)

        os.close(fd)
        os.unlink(filename)

    def tst_truncate_nocache(self):
        filename = os.path.join(self.mnt_dir, self.newname())
        src = self.src
        shutil.copyfile(src, filename)
        self.assertTrue(filecmp.cmp(filename, src, False))
        fstat = os.stat(filename)
        size = fstat.st_size

        subprocess.check_call([os.path.join(BASEDIR, 'bin', 's3qlctrl'),
                               '--quiet', 'flushcache', self.mnt_dir ])

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
