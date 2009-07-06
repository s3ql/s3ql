#!/usr/bin/env python
#
#    Copyright (C) 2008  Nikolaus Rath <Nikolaus@rath.org>
#
#    This program can be distributed under the terms of the GNU LGPL.
#


import sys
import os
import tempfile
import resource
import stat
import logging
from time import time, sleep
from getpass import getpass

__all__ = [ "decrease_refcount",  "get_cachedir", "init_logging",
           "get_credentials", "get_dbfile", "get_inode", "get_path",
           "increase_refcount", "unused_name", "addfile", "get_inodes",
           "my_cursor", "update_atime", "update_mtime", "update_ctime", 
           "waitfor" ]

class Filter(object):
    """
    For use with the logging module as a message filter.
    
    This filter accepts all messages with priority higher than
    a given value or coming from a configured list of loggers.    
    """
    def __init__(self, acceptnames=[], acceptlevel=logging.DEBUG):
        """Initializes a Filter object.
        
        Passes through all messages with priority higher than
        `acceptlevel` or coming from a logger with name in `acceptnames`. 
        """
        self.acceptlevel = acceptlevel
        self.acceptnames = acceptnames
        
    def filter(self, record):
        if record.levelno > self.acceptlevel:
            return True
        
        for name in self.acceptnames:
            if record.name.startswith(name):
                return True
        
        return False
            
def init_logging(fg, quiet=False, debug=[]):
    """Initializes logging system.
    
    If `fg` is set, logging messages are send to stdout. Otherwise logging
    is done via unix syslog.
    
    If `quiet` is set, only messages with priority larger than
    logging.WARN are printed.
    
    `debug` can be set to a list of logger names from which debugging
    messages are to be printed.
    """            
    root_logger = logging.getLogger()
    log_filter = Filter()
    
    if fg:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter('%(asctime)s [%(name)s] %(message)s',
                                               datefmt="%Y-%m-%d %H:%M:%S"))
    else:
        handler = logging.handlers.SysLogHandler("/dev/log")
        handler.setFormatter(logging.Formatter('s3ql[%(process)d]: [%(name)s] %(message)s'))

    handler.addFilter(log_filter)  # If we add the filter to the logger, it has no effect!
    root_logger.addHandler(handler)

    # Default
    root_logger.setLevel(logging.INFO)
    
    if quiet:
        root_logger.setLevel(logging.WARN)
        
    if debug:
        root_logger.setLevel(logging.DEBUG)
        log_filter.acceptnames = debug
    
class my_cursor(object):
    """Wraps an apsw cursor to add some convenience functions.
    """

    def __init__(self, cursor):
        self.cursor = cursor
        self.conn = cursor.getconnection()

    def execute(self, *a, **kw):
        return self.cursor.execute(*a, **kw)

    def get_val(self, *a, **kw):
        """Executes a select statement and returns first element of first row.
        
        If there is no result row, raises StopIteration.
        """

        return self.execute(*a, **kw).next()[0]

    def get_list(self, *a, **kw):
        """Executes a select statement and returns result list.
        """

        return list(self.execute(*a, **kw))

    def get_row(self, *a, **kw):
        """Executes a select statement and returns first row.
        
        If there are no result rows, raises StopIteration.
        """

        return self.execute(*a, **kw).next()
     
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
    
    # Root inode
    inode = cur.get_val("SELECT inode FROM contents WHERE inode=parent_inode")
    
    # Root directory requested
    if not path:
        return [inode]
    
    # Traverse
    visited = [inode]
    for el in path.split(os.sep):
        try:
            inode = cur.get_val("SELECT inode FROM contents WHERE name=? AND parent_inode=?",
                                (buffer(el), inode))
        except StopIteration:
            raise KeyError('Path does not exist', path)
        
        visited.append(inode)

    return visited
    
def get_path(name, inode_p, cur):
    """Returns the full path of `name` with parent inode `inode_p`.
    """
    
    # Root inode
    inode_r = cur.get_val("SELECT inode FROM contents WHERE inode=parent_inode")
    
    path = list()
    while inode_p != inode_r:
        (name2, inode_p) = cur.get_row("SELECT name, parent_inode FROM contents "
                                      "WHERE inode=?", (inode_p,)) # Not ambigious, since we don't allow directory hardlinks
        name2 = str(name2)
        path.append(name2)
        
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
            file = open(keyfile, "r")
            key = file.readline().rstrip()

            if mode & (stat.S_IRGRP | stat.S_IROTH):
                sys.stderr.write("~/.awssecret has insecure permissions, reading password from terminal instead!\n")        
            else:    
                pw = file.readline().rstrip()
            file.close()    
            
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
    
def addfile(remote, local, inode_p, cursor, bucket):
    """Adds the specified local file to the fs in directory `inode_p`
    """

    cursor.execute("INSERT INTO inodes (mode,uid,gid,mtime,atime,ctime,refcount) "
                   "VALUES (?,?,?,?,?,?,?)",
                   (stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR,
                    os.getuid(), os.getgid(), time(), time(), time(), 1))
    inode = cursor.last_rowid()

    cursor.execute("INSERT INTO contents (name, inode, parent_inode) VALUES(?,?,?)",
                   (buffer(remote), inode, inode_p))

    # Add s3 objects
    blocksize = cursor.get_val("SELECT blocksize FROM parameters")

    # Since the blocksize might be large, we work in chunks rather
    # than in memory
    chunksize = resource.getpagesize()

    fh = open(local, "rb")
    tmp = tempfile.NamedTemporaryFile()
    cursize = 0
    blockno = 0
    buf = fh.read(chunksize)
    while True:

        # S3 Block completed or end of file
        if cursize + len(buf) >= blocksize or len(buf) == 0:
            tmp.write(buf[:blocksize-cursize])
            buf = buf[blocksize-cursize:]
            s3key = "%d_%d" % (inode, blockno * blocksize)
            etag = bucket.store_from_file(s3key, tmp.name)
            cursor.execute("INSERT INTO s3_objects (id, etag, last_modified, refcount) VALUES(?,?,?,?)", 
                           (s3key, etag, time(), 1))
            cursor.execute("INSERT INTO inode_s3key (inode,offset,s3key) "
                           "VALUES (?,?,?)", (inode, blockno * blocksize, s3key))
            
            cursize = 0
            blockno += 1
            tmp.seek(0)
            tmp.truncate(0)

            # End of file
            if len(buf) == 0:
                break

        # Write until we have a complete block
        else:
            tmp.write(buf)
            cursize += len(buf)
            buf = fh.read(chunksize)

    tmp.close()
    fh.close()

        
def unused_name(cur, name, inode_p):
    """Returns an unused name for a file in the directory `inode_p_

    If `name` does not already exist, it is returned. Otherwise
    it is made unique by adding suffixes and then returned.
    """
    
    if not cur.get_row("SELECT inode FROM contents WHERE name=? AND parent_inode=?",
                           (buffer(name), inode_p)):
        return name

    i=0
    while cur.get_row("SELECT inode FROM contents WHERE name=? AND parent_inode=?",
                          (buffer("%s-%d" % (name,i)), inode_p)):
        i += 1
    return "%s-%d" % (name,i)
