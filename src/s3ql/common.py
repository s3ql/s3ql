"""
Common functions for S3QL

Copyright (C) 2008  Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL. 
"""


import sys
import os
import tempfile
import resource
import stat
import traceback
import threading
import logging
from time import time, sleep
from getpass import getpass
from contextlib import contextmanager

__all__ = [ "decrease_refcount",  "get_cachedir", "init_logging",
           "get_credentials", "get_dbfile", "get_inode", "get_path",
           "increase_refcount", "unused_name", "addfile", "get_inodes",
           "MyCursor", "update_atime", "update_mtime", "update_ctime", 
           "waitfor", "ROOT_INODE", "writefile", "ExceptionStoringThread",
           "EmbeddedException" ]

class Filter(object):
    """
    For use with the logging module as a message filter.
    
    This filter accepts all messages with priority higher than
    a given value or coming from a configured list of loggers.    
    """
   
    def __init__(self, acceptnames=None, acceptlevel=logging.DEBUG):
        """Initializes a Filter object.
        
        Passes through all messages with priority higher than
        `acceptlevel` or coming from a logger with name in `acceptnames`. 
        """
        if acceptnames is None:
            acceptnames = list()
            
        self.acceptlevel = acceptlevel
        self.acceptnames = acceptnames
        
    def filter(self, record):
        '''Determine if the log message should be printed
        '''
        if record.levelno > self.acceptlevel:
            return True
        
        for name in self.acceptnames:
            if record.name.startswith(name):
                return True
        
        return False
            
def init_logging(fg, quiet=False, debug=None):
    """Initializes logging system.
    
    If `fg` is set, logging messages are send to stdout. Otherwise logging
    is done via unix syslog.
    
    If `quiet` is set, only messages with priority larger than
    logging.WARN are printed.
    
    `debug` can be set to a list of logger names from which debugging
    messages are to be printed.
    """            
    if debug is None:
        debug = list()
        
    root_logger = logging.getLogger()
    log_filter = Filter()
    
    if fg:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter('%(asctime)s,%(msecs)03d %(threadName)s: '
                                               '[%(name)s] %(message)s',
                                               datefmt="%H:%M:%S"))
    else:
        handler = logging.handlers.SysLogHandler("/dev/log")
        handler.setFormatter(logging.Formatter('s3ql[%(process)d, %(threadName)s]: '
                                               '[%(name)s] %(message)s'))

    handler.addFilter(log_filter)  # If we add the filter to the logger, it has no effect!
    root_logger.addHandler(handler)

    # Default
    root_logger.setLevel(logging.INFO)
    
    if quiet:
        root_logger.setLevel(logging.WARN)
        
    if debug:
        root_logger.setLevel(logging.DEBUG)
        log_filter.acceptnames = debug
    
class MyCursor(object):
    """Wraps an apsw cursor to add some convenience functions.
    """

    def __init__(self, cursor):
        self.cursor = cursor
        self.conn = cursor.getconnection()

    @contextmanager
    def transaction(self, name):
        '''Create savepoint, rollback on exceptions, commit on success
        
        This context manager creates a savepoint named `name` and returns
        a cursor. If the managed block evaluates without exceptions, the
        savepoint is committed at the end. Otherwise it is rolled back.         
        '''
        self.cursor.execute('SAVEPOINT ?', (name,))
        try:
            yield self
        except:
            self.cursor.execute('ROLLBACK TO ?', (name,))
            raise
        finally:
            self.cursor.execute('RELEASE ?', (name,))

    def execute(self, *a, **kw):
        '''Execute the given SQL statement
        '''
        
        return self.cursor.execute(*a, **kw)


    def get_val(self, *a, **kw):
        """Executes a select statement and returns first element of first row.
        
        If there is no result row, raises StopIteration.
        """

        return self.get_row(*a, **kw)[0]

    def get_list(self, *a, **kw):
        """Executes a select statement and returns result list.
        """

        return list(self.execute(*a, **kw))

    def get_row(self, *a, **kw):
        """Executes a select statement and returns first row.
        
        If there are no result rows, raises StopIteration.
        """

        # It is ABSOLUTELY CRUCIAL that we retrieve all the rows.
        # Otherwise the cursor is not destroyed and the database
        # stays locked.
        res = self.execute(*a, **kw)
        row = res.next()
        try:
            res.next()
        except StopIteration:
            # Fine, we only wanted one row
            pass
        else:
            # There are more results? That shouldn't be
            raise RuntimeError('Query returned more than one result row')
        
        return row
     
    def last_rowid(self):
        """Returns last inserted rowid.

        Note that this returns the last rowid that has been inserted using
        this *connection*, not cursor.
        """
        return self.conn.last_insert_rowid()
    
    def changes(self):
        """Returns number of rows affected by last statement

        Note that this returns the number of changed rows due to the last statement
        executed in this connection*, not cursor.
        """
        return self.conn.changes()
    

def update_atime(inode, cur):
    """Updates the atime of the specified object.

    The objects atime will be set to the current time.
    """
    cur.execute("UPDATE inodes SET atime=? WHERE id=?", (time(), inode))

def update_ctime(inode, cur):
    """Updates the ctime of the specified object.

    The objects ctime will be set to the current time.
    """
    cur.execute("UPDATE inodes SET ctime=? WHERE id=?", (time(), inode))


def update_mtime(inode, cur):
    """Updates the mtime of the specified object.

    The objects mtime will be set to the current time.
    """
    cur.execute("UPDATE inodes SET mtime=? WHERE id=?", (time(), inode))

def update_mtime_parent(path, cur):
    """Updates the mtime of the parent of the specified object.

    The mtime will be set to the current time.
    """
    inode = get_inode(os.path.dirname(path), cur)
    update_mtime(inode, cur)

def get_inode(path, cur):
    """Returns inode of object at `path`.
    
    Raises `KeyError` if the path does not exist.
    """
    return get_inodes(path, cur)[-1]
    
def get_inodes(path, cur):
    """Returns the inodes of the elements in `path`.
    
    The first inode of the resulting list will always be the inode
    of the root directory. Raises `KeyError` if the path
    does not exist.
    """
    
    # Remove leading and trailing /
    path = path.lstrip("/").rstrip("/") 

    inode = ROOT_INODE
    
    # Root directory requested
    if not path:
        return [inode]
    
    # Traverse
    visited = [inode]
    for el in path.split(os.sep):
        try:
            inode = cur.get_val("SELECT inode FROM contents WHERE name=? AND parent_inode=?",
                                (el, inode))
        except StopIteration:
            raise KeyError('Path does not exist', path)
        
        visited.append(inode)

    return visited
    
def get_path(name, inode_p, cur):
    """Returns the full path of `name` with parent inode `inode_p`.
    """
    
    path = list() 
    maxdepth = 255
    while inode_p != ROOT_INODE:
        # This can be ambigious if directories are hardlinked
        (name2, inode_p) = cur.get_row("SELECT name, parent_inode FROM contents "
                                      "WHERE inode=? AND name != ? AND name != ?",
                                       (inode_p, '.', '..')) 
        path.append(name2)
        maxdepth -= 1
        if maxdepth == 0:
            raise RuntimeError('Failed to resolve name "%s" at inode %d to path',
                               name, inode_p)
        
    path.append(name)
    path = path.reverse()
    
    return "/" + os.path.join(*path)
    
    
def decrease_refcount(inode, cur):
    """Decrease reference count for inode by 1.

    Also updates ctime.
    """
    cur.execute("UPDATE inodes SET refcount=refcount-1,ctime=? WHERE id=?",
             (time(), inode))

def increase_refcount(inode, cur):
    """Increase reference count for inode by 1.

    Also updates ctime.
    """
    cur.execute("UPDATE inodes SET refcount=refcount+1, ctime=? WHERE id=?",
             (time(), inode))


def get_cachedir(bucketname):
    """get directory to put cache files in.
    """

    path = os.environ["HOME"].rstrip("/") + "/.s3ql"

    if not os.path.exists(path):
        os.mkdir(path)

    return path + ("/%s-cache/" % bucketname)


def get_dbfile(bucketname):
    """get filename for metadata db.
    """

    path = os.environ["HOME"].rstrip("/") + "/.s3ql"

    if not os.path.exists(path):
        os.mkdir(path)

    return path + ("/%s.db" % bucketname)


def get_credentials(key=None):
    """Get AWS credentials.

    If `key` has been specified, use this as access key and
    read the password from stdin. Otherwise, tries to read
    ~/.awssecret.
    """
    
    pw = None
    keyfile = os.path.join(os.environ["HOME"], ".awssecret")
    
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
                print "Enter AWS access key: ",
            key = sys.stdin.readline().rstrip()
           
        if not pw:
            if sys.stdin.isatty():
                pw = getpass("Enter AWS password: ")
            else:
                pw = sys.stdin.readline().rstrip()

    return (key, pw)

def waitfor(timeout, fn, *a, **kw):
    """Wait for fn(*a, **kw) to return True.
    
    Waits in increasing periods. Returns False if a timeout occurs, 
    True otherwise.
    """
    
    if fn(*a, **kw):
        return True
    
    step = 0.2
    while timeout > 0:    
        sleep(step)
        timeout -= step
        step *= 2
        if fn(*a, **kw):
            return True
        
    return False
    
def writefile(src, dest, server):
    """Copies the local file `src' into the fs as `dest`
    
    `dest` must not be opened yet by the server.
    """

    try:
        destfd = server.open(dest, None)
    except KeyError:
        mode = ( stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR )
        destfd = server.create(dest, mode)
        
    srcfh =  open(src, "rb")
    chunksize = resource.getpagesize()

    buf = srcfh.read(chunksize)
    off = 0
    while buf:
        server.write(dest, buf, off, destfd)
        off += len(buf)
        buf = srcfh.read(chunksize)        
  
    srcfh.close()
    
    server.release(dest, destfd)
    server.flush(dest, destfd)

def unused_name(path, cursor):
    '''Append suffix to path so that it does not exist
    '''
    
    i = 0
    newpath = path
    try:
        while True:
            get_inode(newpath, cursor)            
            i += 1
            newpath = '%s-%d' % (path, i)
            
    except KeyError:
        pass
    
    return newpath
        
def addfile(src, dest, cursor, bucket):
    """Copies the local file `src' into the fs as `dest`

    If `dest` is already existing, numerical suffixes are appended
    to the name to prevent existing files from being overwritten.
    
    This function is mainly used by fsck to add files to
    lost+found.
    
    Returns the filename that has been written to.
    """

    # Instantiate the regular server
    from s3ql import fs, s3cache
    cachedir = tempfile.mkdtemp() + "/"
    cache = s3cache.S3Cache(bucket, cachedir, 0)
    server = fs.Server(cache, cursor.getconnection().filename)
        
    dest = unused_name(dest, cursor)
    writefile(src, dest, server)
    
    cache.close(cursor)
    os.rmdir(cachedir)    
       
    return dest

# Define inode of root directory
ROOT_INODE = 0

class ExceptionStoringThread(threading.Thread):
    '''Catch all exceptions and store them
    '''
    
    def __init__(self, target):
        super(ExceptionStoringThread, self).__init__()
        self.target = target
        self.exc = None
        self.tb = None
        self.joined = False
        
    def run(self):
        try:
            self.target()
        except BaseException as exc:
            self.exc = exc
            self.tb = sys.exc_info()[2] # This creates a circular reference chain
    
    def join_and_raise(self):
        '''Wait for the thread to finish, raise any occured exceptions
        '''
        self.joined = True
        self.join()
        if self.exc is not None:
            try:
                raise EmbeddedException(self.exc, self.tb, self.name)
            finally: 
                # Here we break the chain
                self.tb = None  
                
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