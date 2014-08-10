'''
common.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.


This module contains common functions used by multiple unit tests.
'''

from contextlib import contextmanager
from functools import wraps
import re
import threading
import time
import traceback
import sys
import os
import subprocess
import stat
import unittest
import random
import configparser
import logging

def get_clock_granularity():
    stamp1 = time.time()
    stamp2 = stamp1
    while stamp1 == stamp2:
        stamp2 = time.time()
    return 2 * (stamp2 - stamp1)
CLOCK_GRANULARITY = get_clock_granularity()

def safe_sleep(secs):
    '''Like time.sleep(), but sleep for at least *secs*

    `time.sleep` may sleep less than the given period if a signal is
    received. This function ensures that we sleep for at least the
    desired time.
    '''

    now = time.time()
    end = now + secs
    while now < end:
        time.sleep(max(end - now, CLOCK_GRANULARITY))
        now = time.time()

@contextmanager
def catch_logmsg(pattern, level=logging.WARNING, count=None):
    '''Catch (and ignore) log messages matching *pattern*

    *pattern* is matched against the *unformatted* log message, i.e. before any
    arguments are merged.

    If *count* is not None, raise an exception unless exactly *count* matching
    messages are caught.
    '''

    logger_class = logging.getLoggerClass()
    handle_orig = logger_class.handle
    caught = [0]

    @wraps(handle_orig)
    def handle_new(self, record):
        if (record.levelno != level
            or not re.search(pattern, record.msg)):
            return handle_orig(self, record)
        caught[0] += 1

    logger_class.handle = handle_new
    try:
        yield

    finally:
        logger_class.handle = handle_orig

        if count is not None and caught[0] != count:
            raise AssertionError('Expected to catch %d log messages, but got only %d'
                                 % (count, caught[0]))

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
        fd = os.open('/dev/fuse', os.O_RDWR)
    except OSError as exc:
        raise unittest.SkipTest('Unable to open /dev/fuse: %s' % exc.strerror)
    else:
        os.close(fd)

def skip_without_rsync():
    try:
        with open('/dev/null', 'wb') as null:
            subprocess.call(['rsync', '--version'], stdout=null,
                            stderr=subprocess.STDOUT,)
    except FileNotFoundError:
        raise unittest.SkipTest('rsync not installed') from None

def populate_dir(path, entries=1000, size=20*1024*1024,
                 pooldir='/usr/bin', seed=None):
    '''Populate directory with random data

    *entries* specifies the total number of directory entries that are created
    in the tree. *size* specifies the size occupied by all files together. The
    files in *pooldir* are used as a source of directory names and file
    contents.

    *seed* is used to initalize the random number generator and can be used to
    make the created structure reproducible (provided that the contents of
    *pooldir* don't change).
    '''

    poolnames = os.listdir(pooldir)
    if seed is None:
        # We want tests to be reproducible on a given system, so users
        # can report meaningful bugs
        seed = len(poolnames)
    random.seed(seed)

    # Entries in percentages
    subdir_cnt = random.randint(5, 10)
    file_cnt = random.randint(60, 70)
    fifo_cnt = random.randint(5, 10)
    symlink_cnt = random.randint(10, 20)
    hardlink_cnt = random.randint(5, 15)

    # Normalize to desired entry count
    scale = entries / sum((subdir_cnt, file_cnt, fifo_cnt, symlink_cnt, hardlink_cnt))
    subdir_cnt = int(scale * subdir_cnt)
    file_cnt = int(scale * file_cnt)
    fifo_cnt = int(scale * fifo_cnt)
    symlink_cnt = int(scale * symlink_cnt)
    hardlink_cnt = int(scale * hardlink_cnt)

    # Sizes, make sure there is at least one big file
    file_sizes = [ random.randint(0, 100) for _ in range(file_cnt-1) ]
    scale = 0.5 * size / sum(file_sizes)
    file_sizes = [ int(scale * x) for x in file_sizes ]
    file_sizes.append(int(0.5 * size))

    # Special characters for use in filenames
    special_chars = [ chr(x) for x in range(128)
                      if x not in (0, ord('/')) ]

    def random_name(path):
        '''Get random, non-existing file name underneath *path*

        Returns a fully qualified path with a filename chosen from *poolnames*.
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
    dirs = [ path ]
    for _ in range(subdir_cnt):
        idx = random.randrange(len(dirs))
        name = random_name(dirs[idx])
        os.mkdir(name)
        dirs.append(name)


    #
    # Step 2: populate the tree with files
    #
    files = []
    for size in file_sizes:
        idx = random.randrange(len(dirs))
        name = random_name(dirs[idx])
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
    for _ in range(fifo_cnt):
        name = random_name(dirs[random.randrange(len(dirs))])
        os.mkfifo(name)
        files.append(name)

    #
    # Step 4: populate tree with symlinks
    #
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


class NoTestSection(Exception):
    '''
    Raised by get_remote_test_info if no matching test
    section was found.
    '''

    def __init__(self, reason):
        self.reason = reason

def get_remote_test_info(name):
        authfile = os.path.expanduser('~/.s3ql/authinfo2')
        if not os.path.exists(authfile):
            raise NoTestSection('No authentication file found.')

        mode = os.stat(authfile).st_mode
        if mode & (stat.S_IRGRP | stat.S_IROTH):
            raise NoTestSection("Authentication file has insecure permissions")

        config = configparser.ConfigParser()
        config.read(authfile)

        try:
            fs_name = config.get(name, 'test-fs')
            backend_login = config.get(name, 'backend-login')
            backend_password = config.get(name, 'backend-password')
        except (configparser.NoOptionError, configparser.NoSectionError):
            raise NoTestSection("Authentication file does not have %s section" % name)

        # Append prefix to make sure that we're starting with an empty bucket
        if fs_name[-1] != '/':
            fs_name += '/'
        fs_name += 's3ql_test_%d/' % time.time()

        return (backend_login, backend_password, fs_name)
