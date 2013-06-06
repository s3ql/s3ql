'''
t4_fuse.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from os.path import basename
from s3ql.common import CTRL_NAME, PICKLE_PROTOCOL, path2bytes
import pickle
import filecmp
import llfuse
import logging
import os.path
import random
import shutil
import stat
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import unittest


log = logging.getLogger(__name__)

# For debugging
USE_VALGRIND = False

class ExceptionStoringThread(threading.Thread):
    def __init__(self):
        super().__init__()
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
        super().__init__()
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
        super().__init__()
        self.target = fn
        self.args = args
        self.kwargs = kwargs

    def run_protected(self):
        self.target(*self.args, **self.kwargs)

def retry(timeout, fn, *a, **kw):
    """Wait for fn(*a, **kw) to return True.
    
    If the return value of fn() returns something True, this value
    is returned. Otherwise, the function is called repeatedly for
    `timeout` seconds. If the timeout is reached, `RetryTimeoutError` is
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

    raise RetryTimeoutError()

class RetryTimeoutError(Exception):
    '''Raised by `retry()` when a timeout is reached.'''

    pass

def skip_if_no_fusermount():
    '''Raise SkipTest if fusermount is not available'''

    with subprocess.Popen(['which', 'fusermount'], stdout=subprocess.PIPE,
                          universal_newlines=True) as which:
        fusermount_path = which.communicate()[0].strip()
    
    if not fusermount_path or which.returncode != 0:
        raise unittest.SkipTest("Can't find fusermount executable")

    if not os.path.exists('/dev/fuse'):
        raise unittest.SkipTest("FUSE kernel module does not seem to be loaded")

    if os.getuid() == 0:
        return

    mode = os.stat(fusermount_path).st_mode
    if mode & stat.S_ISUID == 0:
        raise unittest.SkipTest('fusermount executable not setuid, and we are not root.')

    try:
        with open('/dev/null', 'wb') as null:
            subprocess.check_call([fusermount_path, '-V'], stdout=null)
    except subprocess.CalledProcessError:
        raise unittest.SkipTest('Unable to execute fusermount') from None

def skip_without_rsync():
    try:
        with open('/dev/null', 'wb') as null:        
            subprocess.call(['rsync', '--version'], stdout=null,
                            stderr=subprocess.STDOUT,)
    except FileNotFoundError:
        raise unittest.SkipTest('rsync not installed') from None

    
if __name__ == '__main__':
    mypath = sys.argv[0]
else:
    mypath = __file__
BASEDIR = os.path.abspath(os.path.join(os.path.dirname(mypath), '..'))


class fuse_tests(unittest.TestCase):

    def setUp(self):
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
        self.backend_login_str = None

        self.mount_process = None
        self.name_cnt = 0

    def mkfs(self):
        proc = subprocess.Popen([sys.executable, os.path.join(BASEDIR, 'bin', 'mkfs.s3ql'),
                                 '-L', 'test fs', '--max-obj-size', '500', '--fatal-warnings',
                                 '--cachedir', self.cache_dir, '--quiet', '--authfile',
                                 '/dev/null', self.storage_url ], stdin=subprocess.PIPE,
                                universal_newlines=True)

        if self.backend_login_str:
            print(self.backend_login_str, file=proc.stdin)
        print(self.passphrase, file=proc.stdin)
        print(self.passphrase, file=proc.stdin)
        proc.stdin.close()

        self.assertEqual(proc.wait(), 0)

    def mount(self):
        self.mount_process = \
            subprocess.Popen([sys.executable, os.path.join(BASEDIR, 'bin', 'mount.s3ql'),
                              "--fg", '--cachedir', self.cache_dir, '--log', 'none',
                              '--quiet', '--fatal-warnings', self.storage_url, self.mnt_dir,
                              '--authfile', '/dev/null' ], stdin=subprocess.PIPE,
                             universal_newlines=True)
        if self.backend_login_str:
            print(self.backend_login_str, file=self.mount_process.stdin)
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

        proc = subprocess.Popen([os.path.join(BASEDIR, 'bin', 'umount.s3ql'),
                                 '--quiet', self.mnt_dir])
        retry(90, lambda : proc.poll() is not None)
        self.assertEqual(proc.wait(), 0)

        self.assertEqual(self.mount_process.wait(), 0)
        self.assertFalse(os.path.ismount(self.mnt_dir))

    def fsck(self):
        proc = subprocess.Popen([sys.executable, os.path.join(BASEDIR, 'bin', 'fsck.s3ql'),
                                 '--force', '--quiet', '--log', 'none', '--cachedir',
                                 self.cache_dir, '--fatal-warnings', '--authfile',
                                 '/dev/null', self.storage_url ], stdin=subprocess.PIPE, 
                                universal_newlines=True)
        if self.backend_login_str:
            print(self.backend_login_str, file=proc.stdin)
        print(self.passphrase, file=proc.stdin)
        proc.stdin.close()
        self.assertEqual(proc.wait(), 0)

    def tearDown(self):
        with open('/dev/null', 'wb') as devnull:
            subprocess.call(['fusermount', '-z', '-u', self.mnt_dir],
                            stderr=devnull)
        os.rmdir(self.mnt_dir)

        # Give mount process a little while to terminate
        if self.mount_process is not None:
            retry(10, lambda : self.mount_process.poll() is not None)

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

        
# Somehow important according to pyunit documentation
def suite():
    return unittest.makeSuite(fuse_tests)

def populate_dir(path, entries=4096, max_size=10*1024*1024,
                 pooldir='/usr/bin', seed=None):
    '''Populate directory with random data
    
    *entires* specifies the total number of directory entries that
    are created in the tree. *max_size* specifies the maximum size
    occupied by all files. The files in *pooldir* are used as a 
    source of directory names and file contents.
    
    *seed* is used to initalize the random number generator and
    can be used to make the created structure reproducible
    (provided that the contents of *pooldir* don't change).
    '''

    poolnames = os.listdir(pooldir)
    if seed is None:
        seed = len(poolnames)
    random.seed(seed)
     
    # Special characters for use in filenames
    special_chars = [ chr(x) for x in range(256) 
                      if x not in (0, ord('/')) ]

    def random_name(path):
        '''Get random, non-existing file name underneath *path*
        
        Returns a fully qualified path with a filename chosen
        from *poolnames*.
        '''
        while True:
            name = poolnames[random.randrange(len(poolnames))]
            
            # Special characters
            len_ = random.randrange(4)
            if len_ > 0:
                pos = random.choice((-1,0,1)) # Prefix, Middle, Suffix
                s = ''.join(special_chars[random.randrange(len(special_chars))]
                            for _ in range(len_))
                if pos == -1:
                    name = s + name
                elif pos == 1:
                    name += s
                else:
                    name += s + poolnames[random.randrange(len(poolnames))]
                
            fullname = os.path.join(path, name)
            if not os.path.lexists(fullname):
                break
        return fullname
    
    
    # 
    # Step 1: create directory tree
    #
    subdir_cnt = random.randint(0, int(0.1 * entries))
    entries -= subdir_cnt
    dirs = [ path ]
    for _ in range(subdir_cnt):
        idx = random.randrange(len(dirs))
        name = random_name(dirs[idx])
        os.mkdir(name)
        dirs.append(name)
    
    
    #
    # Step 2: populate the tree with files
    #
    file_cnt = random.randint(int(entries/3), int(3*entries/4))
    entries -= file_cnt
    files = []
    for _ in range(file_cnt):
        idx = random.randrange(len(dirs))
        name = random_name(dirs[idx])
        size = random.randint(0, int(0.01 * max_size))
        max_size -= size
        with open(name, 'wb') as dst:
            while size > 0:
                idx = random.randrange(len(poolnames))
                srcname = os.path.join(pooldir, poolnames[idx])
                if not os.path.isfile(srcname):
                    continue
                with open(srcname, 'rb') as src:
                    buf = src.read(size)
                    dst.write(buf)
                size -= len(buf)
        files.append(name)
     
    #
    # Step 3: Special files
    #
    fifo_cnt = random.randint(int(entries/3), int(2*entries/3))
    entries -= fifo_cnt
    for _ in range(fifo_cnt):
        name = random_name(dirs[random.randrange(len(dirs))])
        os.mkfifo(name)
        files.append(name)
             
    #   
    # Step 4: populate tree with symlinks 
    #
    symlink_cnt = random.randint(int(entries/3), int(2*entries/3))
    entries -= symlink_cnt
    for _ in range(symlink_cnt):
        relative = random.choice((True, False))
        existing = random.choice((True, False))
        idx = random.randrange(len(dirs))
        dir_ = dirs[idx]
        name = random_name(dir_)
    
        if existing:
            directory = random.choice((True, False))
            if directory:
                target = dirs[random.randrange(len(dirs))]
            else:
                target = files[random.randrange(len(files))]
        else:
            target = random_name(dirs[random.randrange(len(dirs))])
    
        if relative:
            target = os.path.relpath(target, dir_)
        else:
            target = os.path.abspath(target) 
            
        os.symlink(target, name)
        
    #
    # Step 5: Create some hardlinks
    #
    hardlink_cnt = random.randint(int(entries/3), int(2*entries/3))
    entries -= hardlink_cnt
    for _ in range(hardlink_cnt):
        samedir = random.choice((True, False))
        
        target = files[random.randrange(len(files))]
        if samedir:
            dir_ = os.path.dirname(target)
        else:
            dir_ = dirs[random.randrange(len(dirs))]
        name = random_name(dir_)
        os.link(target, name)
        files.append(name)
