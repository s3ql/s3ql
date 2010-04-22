'''
common.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function
from getpass import getpass
from time import sleep
import hashlib
import logging.handlers
import os
import stat
import sys
import threading
import traceback
import re
import cPickle as pickle
from contextlib import contextmanager

__all__ = ["get_cachedir", "init_logging", 'sha256', 'sha256_fh', 'get_parameters',
           "get_credentials", "get_dbfile", "inode_for_path", "get_path", 'get_lockfile',
           "ROOT_INODE", "ExceptionStoringThread", 'retry', 'get_stdout_handler',
           "EmbeddedException", 'CTRL_NAME', 'CTRL_INODE', 'unlock_bucket',
           'stacktraces', 'init_logging_from_options', 'QuietError', 'get_backend',
           'cycle_metadata', 'CURRENT_FS_REV', 'VERSION' ]

VERSION = '0.10'
CURRENT_FS_REV = 3

log = logging.getLogger('common')

@contextmanager
def get_backend(options):
    '''Get backend connection and bucket name
    
    This is a context manager, since some connections need to be cleaned 
    up properly. For the local backend, this function also canonicalizes
    the path stored in options.storage_url
    '''

    from .backends import s3, local, ftp
    storage_url = options.storage_url

    if storage_url.startswith('local://'):
        conn = local.Connection()
        bucketname = os.path.abspath(storage_url[len('local://'):])
        options.storage_url = 'local://%s' % bucketname

    elif storage_url.startswith('s3://'):
        (login, password) = get_credentials(options.homedir, 's3', None)
        conn = s3.Connection(login, password)
        bucketname = storage_url[len('s3://'):]

    else:
        pat = r'^([a-z]+)://([a-zA-Z0-9.-]+)(?::([0-9]+))?(/[a-zA-Z0-9./_-]+)$'
        match = re.match(pat, storage_url)
        if not match:
            raise QuietError('Invalid storage url: %r' % storage_url)
        (backend, host, port, bucketname) = match.groups()
        (login, password) = get_credentials(options.homedir, backend, host)

        if backend == 'ftp':
            conn = ftp.Connection(host, port, login, password)
        elif backend == 'ftps':
            conn = ftp.TLSConnection(host, port, login, password)
        elif backend == 'sftp':
            from .backends import sftp
            conn = sftp.Connection(host, port, login, password)
        else:
            raise QuietError('Unknown backend: %s' % backend)

    try:
        yield (conn, bucketname)
    finally:
        conn.close()

def cycle_metadata(bucket):
    from .backends.common import UnsupportedError
    if "s3ql_metadata_bak_2" in bucket:
        try:
            bucket.rename("s3ql_metadata_bak_2", "s3ql_metadata_bak_3")
        except UnsupportedError:
            bucket.copy("s3ql_metadata_bak_2", "s3ql_metadata_bak_3")
    if "s3ql_metadata_bak_1" in bucket:
        try:
            bucket.rename("s3ql_metadata_bak_1", "s3ql_metadata_bak_2")
        except UnsupportedError:
            bucket.copy("s3ql_metadata_bak_1", "s3ql_metadata_bak_2")
    try:
        bucket.rename("s3ql_metadata", "s3ql_metadata_bak_1")
    except UnsupportedError:
        bucket.copy("s3ql_metadata", "s3ql_metadata_bak_1")

def unlock_bucket(bucket):
    '''Ask for passphrase if bucket requires one'''

    if 's3ql_passphrase' in bucket:
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

    seq_nos = [ int(x[len('s3ql_parameters_'):]) for x in bucket.list('s3ql_parameters_') ]
    if not seq_nos:
        raise QuietError('Old file system revision, please run tune.s3ql --upgrade first.')
    seq_no = max(seq_nos)
    param = pickle.loads(bucket['s3ql_parameters_%d' % seq_no])
    assert seq_no == param['mountcnt']

    if param['revision'] < CURRENT_FS_REV:
        raise QuietError('File system revision too old, please run tune.s3ql --upgrade first.')
    elif param['revision'] > CURRENT_FS_REV:
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

def init_logging_from_options(options, logfile):
    '''Call init_logging according to command line arguments
    
    `options` should have attributes ``quiet`` and ``debug``.
    '''

    if options.quiet:
        stdout_level = logging.WARN
    else:
        stdout_level = logging.INFO

    if options.debug and 'all' in options.debug:
        file_level = logging.DEBUG
        file_loggers = None
    elif options.debug:
        file_level = logging.DEBUG
        file_loggers = options.debug
    else:
        file_level = logging.INFO
        file_loggers = None

    init_logging(os.path.join(options.homedir, logfile), stdout_level, file_level, file_loggers)


logging_initialized = False
stdout_handler = None
def init_logging(logfile, stdout_level=logging.INFO, file_level=logging.INFO,
                 file_loggers=None):
    """Initialize logging
    
    `file_logger` can be set to a list of logger names. In that case, debug
    messages are only written into `logfile` if they come from one of these
    loggers.
    """

    global logging_initialized
    global stdout_handler
    if logging_initialized:
        raise RuntimeError('Logging already initialized')

    root_logger = logging.getLogger()

    quiet_formatter = logging.Formatter('%(message)s')
    verbose_formatter = logging.Formatter('%(asctime)s,%(msecs)03d %(threadName)s: '
                                          '[%(name)s] %(message)s', datefmt="%Y-%m-%d %H:%M:%S")

    # Add stdout logger
    if stdout_level is not None:
        stdout_handler = logging.StreamHandler()
        if stdout_level <= logging.DEBUG:
            stdout_handler.setFormatter(verbose_formatter)
        else:
            stdout_handler.setFormatter(quiet_formatter)
        stdout_handler.setLevel(stdout_level)
        root_logger.addHandler(stdout_handler)
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

    # Make sure that top-level exceptions are logged
    def log_main(*a):
        root_logger.error('Uncaught top-level exception',
                          exc_info=a)

    sys.excepthook = log_main
    logging_initialized = True

def get_stdout_handler():
    return stdout_handler

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


def _escape(s):
    '''Escape '/', '=' and '\0' in s'''

    s = s.replace('=', '=3D')
    s = s.replace('/', '=2F')
    s = s.replace('\0', '=00')

    return s

def get_cachedir(storage_url, homedir):
    if not os.path.exists(homedir):
        os.mkdir(homedir)
    return os.path.join(homedir, "%s-cache" % _escape(storage_url))

def get_dbfile(storage_url, homedir):
    if not os.path.exists(homedir):
        os.mkdir(homedir)
    return os.path.join(homedir, "%s.db" % _escape(storage_url))

def get_lockfile(storage_url, homedir):
    if not os.path.exists(homedir):
        os.mkdir(homedir)
    return os.path.join(homedir, "%s.lock" % _escape(storage_url))


def get_credentials(homedir, backend, host):
    """Get credentials for given backend and protocol"""

    # Try to read from file
    keyfile = os.path.join(homedir, 'authinfo')
    pattern = r'^backend\s+(\S+)\s+machine\s+(\S+)\s+login\s+(\S+)\s+password\s+(\S+)$'
    if os.path.isfile(keyfile):
        mode = os.stat(keyfile).st_mode
        if mode & (stat.S_IRGRP | stat.S_IROTH):
            raise QuietError("%s has insecure permissions, aborting." % keyfile)

        fh = open(keyfile, "r")
        for line in fh:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            res = re.match(pattern, line)
            if not res:
                log.warn('Cannot parse line in %s:\n %s', keyfile, line)
                continue

            if backend == res.group(1) and (host is None or host == res.group(2)):
                return res.group(3, 4)

    # Otherwise from stdin
    if sys.stdin.isatty():
        print("Enter login for %s: " % host, end='')
    key = sys.stdin.readline().rstrip()

    if sys.stdin.isatty():
        pw = getpass("Enter password for %s: " % host)
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
    waited = 0
    while waited < timeout:
        ret = fn(*a, **kw)
        if ret:
            return ret
        sleep(step)
        waited += step
        if step < waited / 30:
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
    '''Catch all exceptions and store them'''

    def __init__(self, target, logger, args=(), kwargs={}, pass_self=False):
        # Default value isn't dangerous
        #pylint: disable-msg=W0102
        super(ExceptionStoringThread, self).__init__()
        self.target = target
        self.exc = None
        self.tb = None
        self.joined = False
        self.args = args
        self.kwargs = kwargs
        self.pass_self = pass_self
        self.logger = logger

    def run(self):
        try:
            if self.pass_self:
                self.target(self, *self.args, **self.kwargs)
            else:
                self.target(*self.args, **self.kwargs)
        except BaseException as exc:
            self.exc = exc
            self.tb = sys.exc_info()[2] # This creates a circular reference chain
            if self.logger:
                self.logger.warn('Thread terminated with exception, saving.', exc_info=True)

    def join_get_exc(self):
        self.joined = True
        self.join()
        return self.exc

    def join_and_raise(self):
        '''Wait for the thread to finish, raise any occurred exceptions
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
