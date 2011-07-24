'''
common.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import division, print_function, absolute_import

from getpass import getpass
from time import sleep
import hashlib
import os
import stat
import sys
import threading
import logging.handlers
import traceback
import time
import re
import cPickle as pickle
from contextlib import contextmanager
from llfuse import ROOT_INODE
from .backends.common import NoSuchObject

__all__ = ["get_bucket_home", 'sha256_fh', 'add_stdout_logging',
           "get_credentials", "get_dbfile", "inode_for_path", "get_path",
           "ROOT_INODE", "ExceptionStoringThread", 'retry', 'LoggerFilter',
           "EmbeddedException", 'CTRL_NAME', 'CTRL_INODE', 'unlock_bucket',
           'QuietError', 'get_backend', 'add_file_logging', 'setup_excepthook',
           'cycle_metadata', 'restore_metadata', 'dump_metadata', 
           'setup_logging', 'AsyncFn', 'init_tables', 'create_indices',
           'create_tables', 'get_seq_no' ]


AUTHINFO_BACKEND_PATTERN = r'^backend\s+(\S+)\s+machine\s+(\S+)\s+login\s+(\S+)\s+password\s+(.+)$'
AUTHINFO_BUCKET_PATTERN = r'^storage-url\s+(\S+)\s+password\s+(.+)$'

log = logging.getLogger('common')
        
def setup_logging(options, logfile=None):        
    root_logger = logging.getLogger()
    if root_logger.handlers:
        log.debug("Logging already initialized.")
        return
        
    stdout_handler = add_stdout_logging(options.quiet)
    if logfile:
        debug_handler = add_file_logging(os.path.join(options.homedir, logfile))
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

    
def add_file_logging(logfile):

    root_logger = logging.getLogger()
    formatter = logging.Formatter('%(asctime)s.%(msecs)03d [%(process)s] %(threadName)s: '
                                          '[%(name)s] %(message)s', datefmt="%Y-%m-%d %H:%M:%S")
    handler = logging.handlers.RotatingFileHandler(logfile, maxBytes=1024**2,
                                                   backupCount=5)
    handler.setFormatter(formatter)  
    root_logger.addHandler(handler)
    return handler

    
@contextmanager
def get_backend(storage_url, homedir, use_ssl):
    '''Return backend connection and bucket name
    
    This is a context manager, since some connections need to be cleaned 
    up properly. 
    '''

    from .backends import s3, local, ftp

    if storage_url.startswith('local://'):
        conn = local.Connection()
        bucketname = storage_url[len('local://'):]

    elif storage_url.startswith('s3://'):
        (login, password) = get_backend_credentials(homedir, 's3', None)
        conn = s3.Connection(login, password, use_ssl)
        bucketname = storage_url[len('s3://'):]

    elif storage_url.startswith('s3rr://'):
        log.warn('Warning: Using S3 reduced redundancy storage (S3) is *not* recommended!')
        (login, password) = get_backend_credentials(homedir, 's3', None)
        conn = s3.Connection(login, password, use_ssl, reduced_redundancy=True)
        bucketname = storage_url[len('s3rr://'):]

    else:
        pat = r'^([a-z]+)://([a-zA-Z0-9.-]+)(?::([0-9]+))?(/[a-zA-Z0-9./_-]+)$'
        match = re.match(pat, storage_url)
        if not match:
            raise QuietError('Invalid storage url: %r' % storage_url)
        (backend, host, port, bucketname) = match.groups()
        (login, password) = get_backend_credentials(homedir, backend, host)

        if backend == 'ftp' and not use_ssl:
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

def get_seq_no(bucket):
    '''Get current metadata sequence number'''
        
    seq_nos = [ int(x[len('s3ql_seq_no_'):]) for x in bucket.list('s3ql_seq_no_') ]
    if not seq_nos:
        raise QuietError('Old file system revision, please run `s3qladm upgrade` first.')
    seq_no = max(seq_nos) 
    for i in [ x for x in seq_nos if x < seq_no - 10 ]:
        try:
            del bucket['s3ql_seq_no_%d' % i ]
        except NoSuchObject:
            pass # Key list may not be up to date
        
    return seq_no   

def cycle_metadata(bucket):
    from .backends.common import UnsupportedError

    for i in reversed(range(10)):
        if "s3ql_metadata_bak_%d" % i in bucket:
            try:
                bucket.rename("s3ql_metadata_bak_%d" % i, "s3ql_metadata_bak_%d" % (i + 1))
            except UnsupportedError:
                bucket.copy("s3ql_metadata_bak_%d" % i, "s3ql_metadata_bak_%d" % (i + 1))
                
    try:
        bucket.rename("s3ql_metadata", "s3ql_metadata_bak_0")
    except UnsupportedError:
        bucket.copy("s3ql_metadata", "s3ql_metadata_bak_0")             


def unlock_bucket(homedir, storage_url, bucket):
    '''Ask for passphrase if bucket requires one'''

    if 's3ql_passphrase' not in bucket:
        return

    # Try to read from file
    keyfile = os.path.join(homedir, 'authinfo')
    wrap_pw = None

    if os.path.isfile(keyfile):
        mode = os.stat(keyfile).st_mode
        if mode & (stat.S_IRGRP | stat.S_IROTH):
            raise QuietError("%s has insecure permissions, aborting." % keyfile)

        fh = open(keyfile, "r")
        for line in fh:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if re.match(AUTHINFO_BACKEND_PATTERN, line):
                continue
            res = re.match(AUTHINFO_BUCKET_PATTERN, line)
            if not res:
                log.warn('Cannot parse line in %s:\n %s', keyfile, line)
                continue

            if storage_url == res.group(1):
                wrap_pw = res.group(2)
                log.info('Using encryption password from %s', keyfile)
                break

    # Otherwise from stdin
    if wrap_pw is None:
        if sys.stdin.isatty():
            wrap_pw = getpass("Enter bucket encryption passphrase: ")
        else:
            wrap_pw = sys.stdin.readline().rstrip()

    bucket.passphrase = wrap_pw
    data_pw = bucket['s3ql_passphrase']
    bucket.passphrase = data_pw


def dump_metadata(ofh, conn):
    pickler = pickle.Pickler(ofh, 2)
    data_start = 2048
    bufsize = 256
    buf = range(bufsize)
    tables_to_dump = [('inodes', 'id'),
                      ('contents', 'name, parent_inode'),
                      ('ext_attributes', 'inode, name'),
                      ('objects', 'id'),
                      ('blocks', 'inode, blockno')]

    columns = dict()
    for (table, _) in tables_to_dump:
        columns[table] = list()
        for row in conn.query('PRAGMA table_info(%s)' % table):
            columns[table].append(row[1])

    ofh.seek(data_start)
    sizes = dict()
    for (table, order) in tables_to_dump:
        log.info('Saving %s' % table)
        pickler.clear_memo()
        sizes[table] = 0
        i = 0
        for row in conn.query('SELECT * FROM %s ORDER BY %s' % (table, order)):
            buf[i] = row
            i += 1
            if i == bufsize:
                pickler.dump(buf)
                pickler.clear_memo()
                sizes[table] += 1
                i = 0

        if i != 0:
            pickler.dump(buf[:i])
            sizes[table] += 1

    ofh.seek(0)
    pickler.dump((data_start, tables_to_dump, sizes, columns))
    assert ofh.tell() < data_start

def restore_metadata(ifh, conn):

    unpickler = pickle.Unpickler(ifh)
    (data_start, to_dump, sizes, columns) = unpickler.load()
    ifh.seek(data_start)
    create_tables(conn)
    for (table, _) in to_dump:
        log.info('Loading %s', table)
        col_str = ', '.join(columns[table])
        val_str = ', '.join('?' for _ in columns[table])
        sql_str = 'INSERT INTO %s (%s) VALUES(%s)' % (table, col_str, val_str)
        for _ in xrange(sizes[table]):
            buf = unpickler.load()
            for row in buf:
                conn.execute(sql_str, row)

    create_indices(conn)
    conn.execute('ANALYZE')
    
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
            root_logger.error('Uncaught top-level exception', 
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
            inode = conn.get_val("SELECT inode FROM contents WHERE name=? AND parent_inode=?",
                                (el, inode))
        except NoSuchRowError:
            raise KeyError('Path %s does not exist' % path)

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
        # This can be ambigious if directories are hardlinked
        (name2, id_) = conn.get_row("SELECT name, parent_inode FROM contents WHERE inode=? LIMIT 1",
                                    (id_,))
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

def get_bucket_home(storage_url, homedir):
    if not os.path.exists(homedir):
        os.mkdir(homedir)
    return os.path.join(homedir, _escape(storage_url))


def get_backend_credentials(homedir, backend, host):
    """Get credentials for given backend and host"""

    # Try to read from file
    keyfile = os.path.join(homedir, 'authinfo')

    if os.path.isfile(keyfile):
        mode = os.stat(keyfile).st_mode
        if mode & (stat.S_IRGRP | stat.S_IROTH):
            raise QuietError("%s has insecure permissions, aborting." % keyfile)

        fh = open(keyfile, "r")
        for line in fh:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if re.match(AUTHINFO_BUCKET_PATTERN, line):
                continue
            res = re.match(AUTHINFO_BACKEND_PATTERN, line)
            if not res:
                log.warn('Cannot parse line in %s:\n %s', keyfile, line)
                continue

            if backend == res.group(1) and (host is None or host == res.group(2)):
                log.info('Using backend credentials from %s', keyfile)
                return res.group(3, 4)

    # Otherwise from stdin
    if sys.stdin.isatty():
        if host:
            print("Enter backend login for %s: " % host, end='')
        else:
            print("Enter backend login: ", end='')
    key = sys.stdin.readline().rstrip()

    if sys.stdin.isatty():
        if host:
            pw = getpass("Enter backend password for %s: " % host)
        else:
            pw = getpass("Enter backend password: ")
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

# Name and inode of the special s3ql control file
CTRL_NAME = b'.__s3ql__ctrl__'
CTRL_INODE = 2

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


class AsyncFn(ExceptionStoringThread):
    def __init__(self, fn, *args, **kwargs):
        super(AsyncFn, self).__init__()
        self.target = fn
        self.args = args
        self.kwargs = kwargs
        
    def run_protected(self):
        self.target(*self.args, **self.kwargs)
                
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


def sha256_fh(fh):
    fh.seek(0)
    
    # Bogus error about hashlib not having a sha256 member
    #pylint: disable=E1101
    sha = hashlib.sha256()

    while True:
        buf = fh.read(128 * 1024)
        if not buf:
            break
        sha.update(buf)

    return sha.digest()

def init_tables(conn):
    # Insert root directory
    timestamp = time.time() - time.timezone
    conn.execute("INSERT INTO inodes (id,mode,uid,gid,mtime,atime,ctime,refcount) "
                   "VALUES (?,?,?,?,?,?,?,?)",
                   (ROOT_INODE, stat.S_IFDIR | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR
                   | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH,
                    os.getuid(), os.getgid(), timestamp, timestamp, timestamp, 1))

    # Insert control inode, the actual values don't matter that much 
    conn.execute("INSERT INTO inodes (id,mode,uid,gid,mtime,atime,ctime,refcount) "
                 "VALUES (?,?,?,?,?,?,?,?)",
                 (CTRL_INODE, stat.S_IFIFO | stat.S_IRUSR | stat.S_IWUSR,
                  0, 0, timestamp, timestamp, timestamp, 42))

    # Insert lost+found directory
    inode = conn.rowid("INSERT INTO inodes (mode,uid,gid,mtime,atime,ctime,refcount) "
                       "VALUES (?,?,?,?,?,?,?)",
                       (stat.S_IFDIR | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR,
                        os.getuid(), os.getgid(), timestamp, timestamp, timestamp, 1))
    conn.execute("INSERT INTO contents (name, inode, parent_inode) VALUES(?,?,?)",
                 (b"lost+found", inode, ROOT_INODE))

def create_tables(conn):
    # Table with filesystem metadata
    # The number of links `refcount` to an inode can in theory
    # be determined from the `contents` table. However, managing
    # this separately should be significantly faster (the information
    # is required for every getattr!)
    conn.execute("""
    CREATE TABLE inodes (
        -- id has to specified *exactly* as follows to become
        -- an alias for the rowid.
        id        INTEGER PRIMARY KEY,
        uid       INT NOT NULL,
        gid       INT NOT NULL,
        mode      INT NOT NULL,
        mtime     REAL NOT NULL,
        atime     REAL NOT NULL,
        ctime     REAL NOT NULL,
        refcount  INT NOT NULL,
        target    BLOB(256) ,
        size      INT NOT NULL DEFAULT 0,
        rdev      INT NOT NULL DEFAULT 0,
        locked    BOOLEAN NOT NULL DEFAULT 0
    )
    """)

    # Table of filesystem objects
    # id is used by readdir() to restart at the correct
    # position
    conn.execute("""
    CREATE TABLE contents (
        rowid     INTEGER PRIMARY KEY AUTOINCREMENT,
        name      BLOB(256) NOT NULL,
        inode     INT NOT NULL REFERENCES inodes(id),
        parent_inode INT NOT NULL REFERENCES inodes(id),
        
        UNIQUE (name, parent_inode)
    )""")

    # Extended attributes
    conn.execute("""
    CREATE TABLE ext_attributes (
        inode     INTEGER NOT NULL REFERENCES inodes(id),
        name      BLOB NOT NULL,
        value     BLOB NOT NULL,
 
        PRIMARY KEY (inode, name)               
    )""")

    # Refcount is included for performance reasons
    conn.execute("""
    CREATE TABLE objects (
        id        INTEGER PRIMARY KEY AUTOINCREMENT,
        refcount  INT NOT NULL,
        hash      BLOB(16) UNIQUE,
        size      INT NOT NULL,
        compr_size INT                  
    )""")


    # Maps blocks to objects
    conn.execute("""
    CREATE TABLE blocks (
        inode     INTEGER NOT NULL REFERENCES inodes(id),
        blockno   INT NOT NULL,
        obj_id    INTEGER NOT NULL REFERENCES objects(id),
         
        PRIMARY KEY (inode, blockno)
    )""")
    
def create_indices(conn):
    conn.execute('CREATE INDEX IF NOT EXISTS ix_contents_parent_inode ON contents(parent_inode)')
    conn.execute('CREATE INDEX IF NOT EXISTS ix_contents_inode ON contents(inode)')
    conn.execute('CREATE INDEX IF NOT EXISTS ix_ext_attributes_inode ON ext_attributes(inode)')
    conn.execute('CREATE INDEX IF NOT EXISTS ix_objects_hash ON objects(hash)')
    conn.execute('CREATE INDEX IF NOT EXISTS ix_blocks_obj_id ON blocks(obj_id)')
    conn.execute('CREATE INDEX IF NOT EXISTS ix_blocks_inode ON blocks(inode)')
