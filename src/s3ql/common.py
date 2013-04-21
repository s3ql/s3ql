'''
common.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''


from cgitb import scanvars, __UNDEF__
from llfuse import ROOT_INODE
import bz2
import hashlib
import inspect
import errno
import linecache
import llfuse
import logging
import os
import posixpath
import pydoc
import stat
import sys
import warnings

# Buffer size when writing objects
BUFSIZE = 256 * 1024

log = logging.getLogger('common')

def setup_logging(options):
    root_logger = logging.getLogger()
    if root_logger.handlers:
        log.debug("Logging already initialized.")
        return

    stdout_handler = add_stdout_logging(options.quiet)
    if hasattr(options, 'log') and options.log:
        root_logger.addHandler(options.log)
        debug_handler = options.log
    else:
        debug_handler = stdout_handler
    setup_excepthook()

    if options.debug:
        root_logger.setLevel(logging.DEBUG)
        debug_handler.setLevel(logging.NOTSET)
        if 'all' not in options.debug:
            # Adding the filter to the root logger has no effect.
            debug_handler.addFilter(LoggerFilter(options.debug, logging.INFO))
        logging.disable(logging.NOTSET)
    else:
        root_logger.setLevel(logging.INFO)
        logging.disable(logging.DEBUG)

    return stdout_handler


class LoggerFilter(object):
    """
    For use with the logging module as a message filter.
    
    This filter accepts all messages which have at least the specified
    priority *or* come from a configured list of loggers.
    """

    def __init__(self, acceptnames, acceptlevel):
        """Initializes a Filter object"""

        self.acceptlevel = acceptlevel
        self.acceptnames = [ x.lower() for x in acceptnames ]

    def filter(self, record):
        '''Determine if the log message should be printed'''

        if record.levelno >= self.acceptlevel:
            return True

        if record.name.lower() in self.acceptnames:
            return True

        return False

def add_stdout_logging(quiet=False):
    '''Add stdout logging handler to root logger'''

    root_logger = logging.getLogger()
    formatter = logging.Formatter('%(message)s')
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    if quiet:
        handler.setLevel(logging.WARN)
    else:
        handler.setLevel(logging.INFO)
    root_logger.addHandler(handler)
    return handler

def get_seq_no(backend):
    '''Get current metadata sequence number'''
    from .backends.common import NoSuchObject

    seq_nos = list(backend.list('s3ql_seq_no_'))
    if not seq_nos:
        # Maybe list result is outdated
        seq_nos = [ 's3ql_seq_no_1' ]

    if (seq_nos[0].endswith('.meta')
        or seq_nos[0].endswith('.dat')):
        raise QuietError('Old file system revision, please run `s3qladm upgrade` first.')

    seq_nos = [ int(x[len('s3ql_seq_no_'):]) for x in seq_nos ]
    seq_no = max(seq_nos)

    # Make sure that object really exists
    while ('s3ql_seq_no_%d' % seq_no) not in backend:
        seq_no -= 1
        if seq_no == 0:
            raise QuietError('No S3QL file system found at given storage URL.')
    while ('s3ql_seq_no_%d' % seq_no) in backend:
        seq_no += 1
    seq_no -= 1

    # Delete old seq nos
    for i in [ x for x in seq_nos if x < seq_no - 10 ]:
        try:
            del backend['s3ql_seq_no_%d' % i]
        except NoSuchObject:
            pass # Key list may not be up to date

    return seq_no

def stream_write_bz2(ifh, ofh):
    '''Compress *ifh* into *ofh* using bz2 compression'''

    compr = bz2.BZ2Compressor(9)
    while True:
        buf = ifh.read(BUFSIZE)
        if not buf:
            break
        buf = compr.compress(buf)
        if buf:
            ofh.write(buf)
    buf = compr.flush()
    if buf:
        ofh.write(buf)

def stream_read_bz2(ifh, ofh):
    '''Uncompress bz2 compressed *ifh* into *ofh*'''

    decompressor = bz2.BZ2Decompressor()
    while True:
        buf = ifh.read(BUFSIZE)
        if not buf:
            break
        buf = decompressor.decompress(buf)
        if buf:
            ofh.write(buf)

    if decompressor.unused_data or ifh.read(1) != '':
        raise ChecksumError('Data after end of bz2 stream')

class ChecksumError(Exception):
    """
    Raised if there is a checksum error in the data that we received.
    """

    def __init__(self, str_):
        super(ChecksumError, self).__init__()
        self.str = str_

    def __str__(self):
        return self.str

class QuietError(Exception):
    '''
    QuietError is the base class for exceptions that should not result
    in a stack trace being printed.
    
    It is typically used for exceptions that are the result of the user
    supplying invalid input data. The exception argument should be a
    string containing sufficient information about the problem.
    '''

    def __init__(self, msg=''):
        super(QuietError, self).__init__()
        self.msg = msg

    def __str__(self):
        return self.msg

# Adapted from cgitb.text, but less verbose
def format_tb(einfo):
    """Return a plain text document describing a given traceback."""

    etype, evalue, etb = einfo
    if type(etype) is type:
        etype = etype.__name__

    frames = [ 'Traceback (most recent call last):' ]
    records = inspect.getinnerframes(etb, context=7)
    for (frame, file_, lnum, func, lines, index) in records:
        (args, varargs, varkw, locals_) = inspect.getargvalues(frame)
        sig = inspect.formatargvalues(args, varargs, varkw, locals_,
                                      formatvalue=lambda value: '=' + pydoc.text.repr(value))

        rows = ['  File %r, line %d, in %s%s' % (file_, lnum, func, sig) ]

        # To print just current line
        if index is not None:
            rows.append('    %s' % lines[index].strip())

#        # To print with context:
#        if index is not None:
#            i = lnum - index
#            for line in lines:
#                num = '%5d ' % i
#                rows.append(num+line.rstrip())
#                i += 1

        def reader(lnum=[lnum]): #pylint: disable=W0102 
            try:
                return linecache.getline(file_, lnum[0])
            finally:
                lnum[0] += 1

        printed = set()
        rows.append('  Current bindings:')
        for (name, where, value) in scanvars(reader, frame, locals_):
            if name in printed:
                continue
            printed.add(name)
            if value is not __UNDEF__:
                if where == 'global':
                    where = '(global)'
                elif where != 'local':
                    name = where + name.split('.')[-1]
                    where = '(local)'
                else:
                    where = ''
                rows.append('    %s = %s %s' % (name, pydoc.text.repr(value), where))
            else:
                rows.append(name + ' undefined')

        rows.append('')
        frames.extend(rows)

    exception = ['Exception: %s: %s' % (etype.__name__, evalue)]
    if isinstance(evalue, BaseException):

        # We may list deprecated attributes when iteracting, but obviously
        # we do not need any warnings about that.
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
                        
            for name in dir(evalue):
                if name.startswith('__'):
                    continue
    
                value = pydoc.text.repr(getattr(evalue, name))
                exception.append('  %s = %s' % (name, value))

    return '%s\n%s' % ('\n'.join(frames), '\n'.join(exception))

def setup_excepthook():
    '''Modify sys.excepthook to log exceptions
    
    Also makes sure that exceptions derived from `QuietException`
    do not result in stacktraces.
    '''

    def excepthook(type_, val, tb):
        root_logger = logging.getLogger()
        if isinstance(val, QuietError):
            root_logger.error(val.msg)
        else:
            # Customized exception handler has shown to just blow up the size
            # of error messages and potentially include confidential data
            # without providing any significant benefits
#            try:
#                msg = format_tb((type_, val, tb))
#            except:
#                root_logger.error('Uncaught top-level exception -- and tb handler failed!',
#                                  exc_info=(type_, val, tb))
#            else:
#                root_logger.error('Uncaught top-level exception. %s', msg)
            root_logger.error('Uncaught top-level exception:',
                              exc_info=(type_, val, tb))
            
    sys.excepthook = excepthook

def inode_for_path(path, conn):
    """Return inode of directory entry at `path`
    
     Raises `KeyError` if the path does not exist.
    """
    from .database import NoSuchRowError

    if not isinstance(path, bytes):
        raise TypeError('path must be of type bytes')

    # Remove leading and trailing /
    path = path.lstrip(b"/").rstrip(b"/")

    # Traverse
    inode = ROOT_INODE
    for el in path.split(b'/'):
        try:
            inode = conn.get_val("SELECT inode FROM contents_v WHERE name=? AND parent_inode=?",
                                 (el, inode))
        except NoSuchRowError:
            raise KeyError('Path %s does not exist' % path) from None

    return inode

def get_path(id_, conn, name=None):
    """Return a full path for inode `id_`.
    
    If `name` is specified, it is appended at the very end of the
    path (useful if looking up the path for file name with parent
    inode).
    """

    if name is None:
        path = list()
    else:
        if not isinstance(name, bytes):
            raise TypeError('name must be of type bytes')
        path = [ name ]

    maxdepth = 255
    while id_ != ROOT_INODE:
        # This can be ambiguous if directories are hardlinked
        (name2, id_) = conn.get_row("SELECT name, parent_inode FROM contents_v "
                                    "WHERE inode=? LIMIT 1", (id_,))
        path.append(name2)
        maxdepth -= 1
        if maxdepth == 0:
            raise RuntimeError('Failed to resolve name "%s" at inode %d to path',
                               name, id_)

    path.append(b'')
    path.reverse()

    return b'/'.join(path)


def _escape(s):
    '''Escape '/', '=' and '\0' in s'''

    s = s.replace('=', '=3D')
    s = s.replace('/', '=2F')
    s = s.replace('\0', '=00')

    return s

def get_backend_cachedir(storage_url, cachedir):
    if not os.path.exists(cachedir):
        try:
            os.mkdir(cachedir, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
        except PermissionError:
            raise QuietError('No permission to create cache directory (%s)' % cachedir) from None
        
    if not os.access(cachedir, os.R_OK | os.W_OK | os.X_OK):
        raise QuietError('No permission to access cache directory (%s)' % cachedir)
    
    return os.path.join(cachedir, _escape(storage_url))

# Name and inode of the special s3ql control file
CTRL_NAME = b'.__s3ql__ctrl__'
CTRL_INODE = 2

def sha256_fh(fh):
    fh.seek(0)

    # Bogus error about hashlib not having a sha256 member
    #pylint: disable=E1101
    sha = hashlib.sha256()

    while True:
        buf = fh.read(BUFSIZE)
        if not buf:
            break
        sha.update(buf)

    return sha.digest()


def assert_s3ql_fs(path):
    '''Raise `QuietError` if *path* is not on an S3QL file system
    
    Returns name of the S3QL control file.
    '''

    try:
        os.stat(path)
    except FileNotFoundError:
        raise QuietError('%s does not exist' % path) from None
    except OSError as exc:
        if exc.errno is errno.ENOTCONN:
            raise QuietError('File system appears to have crashed.') from None
        raise

    ctrlfile = os.path.join(path, CTRL_NAME)
    if not (CTRL_NAME not in llfuse.listdir(path)
            and os.path.exists(ctrlfile)):
        raise QuietError('%s is not on an S3QL file system' % path)

    return ctrlfile
    
    
def assert_fs_owner(path, mountpoint=False):
    '''Raise `QuietError` if user is not owner of S3QL fs at *path*
    
    Implicitly calls `assert_s3ql_fs` first. Returns name of the 
    S3QL control file.
    
    If *mountpoint* is True, also call `assert_s3ql_mountpoint`, i.e.
    fail if *path* is not the mount point of the file system.
    '''
        
    if mountpoint:
        ctrlfile = assert_s3ql_mountpoint(path)
    else:
        ctrlfile = assert_s3ql_fs(path)
    
    if os.stat(ctrlfile).st_uid != os.geteuid() and os.geteuid() != 0:
        raise QuietError('Permission denied. %s is was not mounted by you '
                         'and you are not root.' % path)

    return ctrlfile
    
def assert_s3ql_mountpoint(mountpoint):
    '''Raise QuietError if *mountpoint* is not an S3QL mountpoint
    
    Implicitly calls `assert_s3ql_fs` first. Returns name of the 
    S3QL control file.
    '''

    ctrlfile = assert_s3ql_fs(mountpoint)
    if not posixpath.ismount(mountpoint):
        raise QuietError('%s is not a mount point' % mountpoint)

    return ctrlfile