'''
common.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import division, print_function, absolute_import

import hashlib
import os
import stat
import sys
import threading
import traceback
import time
from functools import wraps
import cPickle as pickle
from llfuse import ROOT_INODE
import logging

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

RETRY_TIMEOUT=60*60*24
def retry(fn):
    '''Decorator for retrying a method on some exceptions
    
    If the decorated method raises an exception for which the instance's
    `_retry_on(exc)` method is true, the decorated method is called again at
    increasing intervals. If this persists for more than *timeout* seconds,
    the most-recently caught exception is re-raised.
    '''
    
    @wraps(fn)
    def wrapped(self, *a, **kw):    
        interval = 1/50
        waited = 0
        while True:
            try:
                return fn(self, *a, **kw)
            except Exception as exc:
                if not self._retry_on(exc):
                    raise
                if waited > RETRY_TIMEOUT:
                    log.err('%s.%s(*): Timeout exceeded, re-raising %r exception', 
                            fn.im_class.__name__, fn.__name__, exc)
                    raise
                
                log.debug('%s.%s(*): trying again after %r exception:', 
                          fn.im_class.__name__, fn.__name__, exc)
                
            time.sleep(interval)
            waited += interval
            if interval < 20*60:
                interval *= 2   
                
    wrapped.__doc__ += '''
This method has been decorated and will automatically recall itself in
increasing intervals for up to s3ql.common.RETRY_TIMEOUT seconds if it raises an
exception for which the instance's `_retry_on` method returns True.
'''
              
    return wrapped 


def get_seq_no(bucket):
    '''Get current metadata sequence number'''
       
    from s3ql.backends.local import Bucket as LocalBucket
    from .backends.common import NoSuchObject
    
    seq_nos = list(bucket.list('s3ql_seq_no_')) 
    if (not seq_nos or
        (isinstance(bucket, LocalBucket) and
         (seq_nos[0].endswith('.meta') or seq_nos[0].endswith('.dat')))): 
        raise QuietError('Old file system revision, please run `s3qladm upgrade` first.')
    
    seq_nos = [ int(x[len('s3ql_seq_no_'):]) for x in seq_nos ]
    seq_no = max(seq_nos) 
    for i in [ x for x in seq_nos if x < seq_no - 10 ]:
        try:
            del bucket['s3ql_seq_no_%d' % i ]
        except NoSuchObject:
            pass # Key list may not be up to date
        
    return seq_no   

def cycle_metadata(bucket):
    from .backends.common import NoSuchObject
    
    for i in reversed(range(10)):
        try:
            bucket.copy("s3ql_metadata_bak_%d" % i, "s3ql_metadata_bak_%d" % (i + 1))
        except NoSuchObject:
            pass
                
    bucket.copy("s3ql_metadata", "s3ql_metadata_bak_0")             

def dump_metadata(ofh, conn):
    pickler = pickle.Pickler(ofh, 2)
    bufsize = 256
    buf = range(bufsize)
    tables_to_dump = [('objects', 'id'), ('blocks', 'id'),
                      ('inode_blocks', 'inode, blockno'),
                      ('inodes', 'id'), ('symlink_targets', 'inode'),
                      ('names', 'id'), ('contents', 'parent_inode, name_id'),
                      ('ext_attributes', 'inode, name')]

    columns = dict()
    for (table, _) in tables_to_dump:
        columns[table] = list()
        for row in conn.query('PRAGMA table_info(%s)' % table):
            columns[table].append(row[1])

    pickler.dump((tables_to_dump, columns))
    
    for (table, order) in tables_to_dump:
        log.info('Saving %s' % table)
        pickler.clear_memo()
        i = 0
        for row in conn.query('SELECT %s FROM %s ORDER BY %s' 
                              % (','.join(columns[table]), table, order)):
            buf[i] = row
            i += 1
            if i == bufsize:
                pickler.dump(buf)
                pickler.clear_memo()
                i = 0

        if i != 0:
            pickler.dump(buf[:i])
        
        pickler.dump(None)


def restore_metadata(ifh, conn):

    unpickler = pickle.Unpickler(ifh)
    (to_dump, columns) = unpickler.load()
    create_tables(conn)
    for (table, _) in to_dump:
        log.info('Loading %s', table)
        col_str = ', '.join(columns[table])
        val_str = ', '.join('?' for _ in columns[table])
        sql_str = 'INSERT INTO %s (%s) VALUES(%s)' % (table, col_str, val_str)
        while True:
            buf = unpickler.load()
            if not buf:
                break
            for row in buf:
                conn.execute(sql_str, row)

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
            inode = conn.get_val("SELECT inode FROM contents_v WHERE name=? AND parent_inode=?", 
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

def get_bucket_cachedir(storage_url, cachedir):
    if not os.path.exists(cachedir):
        os.mkdir(cachedir)
    return os.path.join(cachedir, _escape(storage_url))

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
    name_id = conn.rowid('INSERT INTO names (name, refcount) VALUES(?,?)',
                         (b'lost+found', 1))
    conn.execute("INSERT INTO contents (name_id, inode, parent_inode) VALUES(?,?,?)",
                 (name_id, inode, ROOT_INODE))

def create_tables(conn): 
    # Table of storage objects
    # Refcount is included for performance reasons
    conn.execute("""
    CREATE TABLE objects (
        id        INTEGER PRIMARY KEY AUTOINCREMENT,
        refcount  INT NOT NULL, 
        compr_size INT  
    )""")

    # Table of known data blocks
    # Refcount is included for performance reasons
    conn.execute("""
    CREATE TABLE blocks (
        id        INTEGER PRIMARY KEY,
        hash      BLOB(16) UNIQUE,
        refcount  INT NOT NULL,
        size      INT NOT NULL,    
        obj_id    INTEGER NOT NULL REFERENCES objects(id)
    )""")
                
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
        size      INT NOT NULL DEFAULT 0,
        rdev      INT NOT NULL DEFAULT 0,
        locked    BOOLEAN NOT NULL DEFAULT 0,
        
        -- id of first block (blockno == 0)
        -- since most inodes have only one block, we can make the db 20%
        -- smaller by not requiring a separate inode_blocks row for these
        -- cases. 
        block_id  INT REFERENCES blocks(id)
    )""")

    # Further Blocks used by inode (blockno >= 1)
    conn.execute("""
    CREATE TABLE inode_blocks (
        inode     INTEGER NOT NULL REFERENCES inodes(id),
        blockno   INT NOT NULL,
        block_id    INTEGER NOT NULL REFERENCES blocks(id),
        PRIMARY KEY (inode, blockno)
    )""")
    
    # Symlinks
    conn.execute("""
    CREATE TABLE symlink_targets (
        inode     INTEGER PRIMARY KEY REFERENCES inodes(id),
        target    BLOB NOT NULL
    )""")
    
    # Names of file system objects
    conn.execute("""
    CREATE TABLE names (
        id     INTEGER PRIMARY KEY,
        name   BLOB NOT NULL,
        refcount  INT NOT NULL,
        UNIQUE (name)
    )""")

    # Table of filesystem objects
    # rowid is used by readdir() to restart at the correct position
    conn.execute("""
    CREATE TABLE contents (
        rowid     INTEGER PRIMARY KEY AUTOINCREMENT,
        name_id   INT NOT NULL REFERENCES names(id),
        inode     INT NOT NULL REFERENCES inodes(id),
        parent_inode INT NOT NULL REFERENCES inodes(id),
        
        UNIQUE (parent_inode, name_id)
    )""")

    # Extended attributes
    conn.execute("""
    CREATE TABLE ext_attributes (
        inode     INTEGER NOT NULL REFERENCES inodes(id),
        name      BLOB NOT NULL,
        value     BLOB NOT NULL,
 
        PRIMARY KEY (inode, name)               
    )""")

    # Shortcurts
    conn.execute("""
    CREATE VIEW contents_v AS
    SELECT * FROM contents JOIN names ON names.id = name_id       
    """)    
    
    conn.execute("""
    CREATE VIEW inode_blocks_v AS
    SELECT * FROM inode_blocks
    UNION
    SELECT id as inode, 0 as blockno, block_id FROM inodes WHERE block_id IS NOT NULL       
    """)        