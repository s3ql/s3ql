'''
$Id$

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function
from getpass import getpass
from time import sleep
import hashlib
import logging.handlers
import boto.exception
import os
import re
import stat
import sys
import threading
import traceback
import cPickle as pickle

__all__ = [ "get_cachedir", "init_logging", 'sha256', 'sha256_fh', 'get_parameters',
           "get_credentials", "get_dbfile", "inode_for_path", "get_path",
           "ROOT_INODE", "ExceptionStoringThread", 'retry', 'retry_boto', 'retry_exc',
           "EmbeddedException", 'CTRL_NAME', 'CTRL_INODE', 'unlock_bucket',
           'stacktraces', 'init_logging_from_options', 'QuietError' ]

def unlock_bucket(bucket):
    '''Ask for passphrase if bucket requires one'''

    if bucket.has_key('s3ql_passphrase'):
        if sys.stdin.isatty():
            wrap_pw = getpass("Enter encryption password: ")
        else:
            wrap_pw = sys.stdin.readline().rstrip()
        bucket.passphrase = wrap_pw
        data_pw = bucket['s3ql_passphrase']
        bucket.passphrase = data_pw

def get_parameters(bucket):
    '''Return file system parameters.
    
    If the file system is too old or too new, raises `QuietError`.
    '''

    seq_nos = [ int(x[len('s3ql_parameters_'):]) for x in bucket.keys('s3ql_parameters_') ]
    if not seq_nos:
        raise QuietError('Old file system revision, please run tune.s3ql --upgrade first.')
    seq_no = max(seq_nos)
    param = pickle.loads(bucket['s3ql_parameters_%d' % seq_no])
    assert seq_no == param['mountcnt']

    if param['revision'] < 2:
        raise QuietError('File system revision too old, please run tune.s3ql --upgrade first.')
    elif param['revision'] > 2:
        raise QuietError('File system revision too new, please update your '
                         'S3QL installation.')

    # Delete old parameter objects 
    for i in seq_nos:
        if i < seq_no:
            del bucket['s3ql_parameters_%d' % i ]

    return param

class QuietError(SystemExit):
    '''QuietError is an exception that should not result in a
    stack trace being printed. It is typically raised if the
    exception can generally only be handled by the user invoking
    a non-interactive program.
    The exception argument should be a string containing sufficient
    information about the problem.
    '''
    pass


def stacktraces():
    '''Return stack trace for every running thread'''

    code = []
    for threadId, frame in sys._current_frames().items():
        code.append("\n# ThreadID: %s" % threadId)
        for filename, lineno, name, line in traceback.extract_stack(frame):
            code.append('%s:%d, in %s' % (os.path.basename(filename), lineno, name))
            if line:
                code.append("    %s" % (line.strip()))

    return "\n".join(code)

class Filter(object):
    """
    For use with the logging module as a message filter.
    
    This filter accepts all messages which have the specified priority 
    or come from a configured list of loggers.    
    """

    def __init__(self, acceptnames, acceptlevel):
        """Initializes a Filter object.
        
        Passes through all messages with priority higher than
        `acceptlevel` or coming from a logger in `acceptnames`. 
        """
        if acceptnames is None:
            acceptnames = list()

        self.acceptlevel = acceptlevel
        self.acceptnames = [ x.lower() for x in acceptnames ]

    def filter(self, record):
        '''Determine if the log message should be printed'''

        if record.levelno > self.acceptlevel:
            return True

        if record.name.lower() in self.acceptnames:
            return True

        return False

def init_logging_from_options(options, daemon=False):
    '''Call init_logging according to command line arguments
    
    `options` should have attributes ``quiet``, ``debug`` and ``logfile``.
    '''

    if options.quiet:
        stdout_level = logging.WARN
    else:
        stdout_level = logging.INFO

    if daemon:
        stdout_level = None

    if options.debug and 'all' in options.debug:
        file_level = logging.DEBUG
        file_loggers = None
    elif options.debug:
        file_level = logging.DEBUG
        file_loggers = options.debug
    else:
        file_level = logging.INFO
        file_loggers = None

    init_logging(options.logfile, stdout_level, file_level, file_loggers)


def init_logging(logfile, stdout_level=logging.INFO, file_level=logging.INFO,
                 file_loggers=None):
    """Initialize logging
    
    `file_logger` can be set to a list of logger names. In that case, debug
    messages are only written into `logfile` if they come from one of these
    loggers.
    """
    root_logger = logging.getLogger()

    # Remove existing handlers. We have to copy the list
    # since it is going to change during iteration
    for hdlr in list(root_logger.handlers):
        root_logger.removeHandler(hdlr)

    quiet_formatter = logging.Formatter('%(message)s')
    verbose_formatter = logging.Formatter('%(asctime)s,%(msecs)03d %(threadName)s: '
                                          '[%(name)s] %(message)s', datefmt="%Y-%m-%d %H:%M:%S")

    # Add stdout logger
    if stdout_level is not None:
        handler = logging.StreamHandler()
        if stdout_level <= logging.DEBUG:
            handler.setFormatter(verbose_formatter)
        else:
            handler.setFormatter(quiet_formatter)
        handler.setLevel(stdout_level)
        root_logger.addHandler(handler)
        root_logger.setLevel(stdout_level)

    # Add file logger
    if logfile is not None:
        if not os.path.exists(os.path.dirname(logfile)):
            os.mkdir(os.path.dirname(logfile), 0700)
        handler = logging.handlers.RotatingFileHandler(logfile, maxBytes=1 * 1024 * 1024, backupCount=5)
        handler.setFormatter(verbose_formatter)
        handler.setLevel(file_level)
        root_logger.addHandler(handler)

        if file_loggers:
            if not isinstance(file_loggers, list):
                raise ValueError('file_loggers must be list of strings')
            handler.addFilter(Filter(acceptnames=file_loggers, acceptlevel=logging.DEBUG))

        root_logger.setLevel(min(stdout_level, file_level))


def inode_for_path(path, conn):
    """Return inode of directory entry at `path`
    
     Raises `KeyError` if the path does not exist.
    """

    if not isinstance(path, bytes):
        raise TypeError('path must be of type bytes')

    # Remove leading and trailing /
    path = path.lstrip(b"/").rstrip(b"/")

    # Traverse
    inode = ROOT_INODE
    for el in path.split(b'/'):
        try:
            inode = conn.get_val("SELECT inode FROM contents WHERE name=? AND parent_inode=?",
                                (el, inode))
        except KeyError:
            raise KeyError('Path %s does not exist' % path)

    return inode


def get_path(name, inode_p, conn):
    """Return the full path of `name` with parent inode `inode_p`"""

    if not isinstance(name, bytes):
        raise TypeError('name must be of type bytes')

    path = [ name ]

    maxdepth = 255
    while inode_p != ROOT_INODE:
        # This can be ambigious if directories are hardlinked
        (name2, inode_p) = conn.get_row("SELECT name, parent_inode FROM contents WHERE inode=?",
                                       (inode_p,))
        path.append(name2)
        maxdepth -= 1
        if maxdepth == 0:
            raise RuntimeError('Failed to resolve name "%s" at inode %d to path',
                               name, inode_p)

    path.append(b'')
    path.reverse()

    return b'/'.join(path)


def get_cachedir(bucketname, path):
    """get directory to put cache files in.
    """

    if not os.path.exists(path):
        os.mkdir(path)

    # Escape backslash
    bucketname = re.sub('_', '__', bucketname)
    bucketname = re.sub('/', '_', bucketname)
    return os.path.join(path, "%s-cache" % bucketname)


def get_dbfile(bucketname, path):
    """get filename for metadata db.
    """

    if not os.path.exists(path):
        os.mkdir(path)

    # Escape backslash
    bucketname = re.sub('_', '__', bucketname)
    bucketname = re.sub('/', '_', bucketname)
    return os.path.join(path, "%s.db" % bucketname)


def get_credentials(keyfile, key=None):
    """Get AWS credentials.

    If `key` has been specified, use this as access key and
    read the password from stdin. Otherwise, tries to read
    ~/.awssecret.
    """

    pw = None

    if key:
        if sys.stdin.isatty():
            pw = getpass("Enter AWS password: ")
        else:
            pw = sys.stdin.readline().rstrip()

    else:

        if os.path.isfile(keyfile):
            mode = os.stat(keyfile).st_mode
            kfile = open(keyfile, "r")
            key = kfile.readline().rstrip()

            if mode & (stat.S_IRGRP | stat.S_IROTH):
                sys.stderr.write("~/.awssecret has insecure permissions, "
                                 "reading password from terminal instead!\n")
            else:
                pw = kfile.readline().rstrip()
            kfile.close()

        if not key:
            if sys.stdin.isatty():
                print("Enter AWS access key: ", end='')
            key = sys.stdin.readline().rstrip()

        if not pw:
            if sys.stdin.isatty():
                pw = getpass("Enter AWS password: ")
            else:
                pw = sys.stdin.readline().rstrip()

    return (key, pw)

def retry(timeout, fn, *a, **kw):
    """Wait for fn(*a, **kw) to return True.
    
    If the return value of fn() returns something True, this value
    is returned. Otherwise, the function is called repeatedly for
    `timeout` seconds. If the timeout is reached, `TimeoutError` is
    raised.
    """

    step = 0.2
    while timeout > 0:
        ret = fn(*a, **kw)
        if ret:
            return ret
        sleep(step)
        timeout -= step
        step *= 2

    raise TimeoutError()

def retry_boto(timeout, errcodes, fn, *a, **kw):
    """Wait for fn(*a, **kw) to succeed
    
    If `fn(*a, **kw)` raises `boto.exception.S3ResponseError` with errorcode
    in `errcodes`, the function is called again. If the timeout is reached, 
    `TimeoutError` is raised.
    """

    step = 0.2
    while timeout > 0:
        try:
            return fn(*a, **kw)
        except boto.exception.S3ResponseError as exc:
            if exc.error_code in errcodes:
                pass
            else:
                raise

        sleep(step)
        timeout -= step
        step *= 2

    raise TimeoutError()

def retry_exc(timeout, exc_types, fn, *a, **kw):
    """Wait for fn(*a, **kw) to succeed
    
    If `fn(*a, **kw)` raises an exception in `exc_types`, the function is called again.
    If the timeout is reached, `TimeoutError` is raised.
    """

    step = 0.2
    while timeout > 0:
        try:
            return fn(*a, **kw)
        except BaseException as exc:
            for exc_type in exc_types:
                if isinstance(exc, exc_type):
                    break
            else:
                raise exc

        sleep(step)
        timeout -= step
        step *= 2

    raise TimeoutError()

class TimeoutError(Exception):
    '''Raised by `retry()` when a timeout is reached.'''

    pass



# Define inode of root directory
ROOT_INODE = 1

# Name and inode of the special s3ql control file
CTRL_NAME = b'.__s3ql__ctrl__'
CTRL_INODE = 2

class ExceptionStoringThread(threading.Thread):
    '''Catch all exceptions and store them
    '''

    def __init__(self, target, args=(), kwargs={}):
        # Default value isn't dangerous
        #pylint: disable-msg=W0102
        super(ExceptionStoringThread, self).__init__()
        if target is not None:
            self.target = target
        self.exc = None
        self.tb = None
        self.joined = False
        self.args = args
        self.kwargs = kwargs

    def run(self):
        try:
            self.target(*self.args, **self.kwargs)
        except BaseException as exc:
            self.exc = exc
            self.tb = sys.exc_info()[2] # This creates a circular reference chain

    def join_get_exc(self):
        self.joined = True
        self.join()
        return self.exc

    def join_and_raise(self):
        '''Wait for the thread to finish, raise any occured exceptions
        '''
        self.joined = True
        self.join()
        if self.exc is not None:
            # Break reference chain
            tb = self.tb
            del self.tb
            raise EmbeddedException(self.exc, tb, self.name)


    def __del__(self):
        if not self.joined:
            raise RuntimeError("ExceptionStoringThread instance was destroyed "
                               "without calling join_and_raise()!")

class EmbeddedException(Exception):
    '''Encapsulates an exception that happened in a different thread
    '''

    def __init__(self, exc, tb, threadname):
        super(EmbeddedException, self).__init__()
        self.exc = exc
        self.tb = tb
        self.threadname = threadname

    def __str__(self):
        return ''.join(['caused by an exception in thread %s.\n' % self.threadname,
                       'Original/inner traceback (most recent call last): \n' ] +
                       traceback.format_tb(self.tb) +
                       traceback.format_exception_only(type(self.exc), self.exc))


def sha256_fh(fh):
    fh.seek(0)
    sha = hashlib.sha256()

    while True:
        buf = fh.read(128 * 1024)
        if not buf:
            break
        sha.update(buf)

    return sha.digest()


def sha256(s):
    return hashlib.sha256(s).digest()
