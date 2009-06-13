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
from time import time, sleep
from getpass import getpass

__all__ = [ "debug", "decrease_refcount", "error", "get_cachedir",
           "get_credentials", "get_dbfile", "get_inode", "get_path",
           "increase_refcount", "log", "logger", "unused_name", "addfile",
           "my_cursor", "update_atime", "update_mtime", "update_ctime", 
           "waitfor", "warn", "io2s3key" ]

# Initialize logging
### FIXME: We really want to use the standard logging module instead
class logger_class(object):
    def __init__(self, log_fn=sys.stderr.writelines, log_level=1):
        self.log_fn = log_fn
        self.log_level = log_level
        
logger = logger_class()    


class my_cursor(object):
    """Wraps an apsw cursor to add some convenience functions.
    """

    def __init__(self, cursor):
        self.cursor = cursor

    def execute(self, *a, **kw):
        return self.cursor.execute(*a, **kw)

    def get_val(self, *a, **kw):
        """Executes a select statement and returns first element of first row.
        
        Throws `StopIteration` if there is no result row.
        """

        return self.execute(*a, **kw).next()[0]

    def get_list(self, *a, **kw):
        """Executes a select statement and returns result list.
        """

        return list(self.execute(*a, **kw))

    def get_row(self, *a, **kw):
        """Executes a select statement and returns first row.
        
        If there are no result rows, returns None.
        """

        res = self.execute(*a, **kw)
        try:
            row = res.next()
        except StopIteration:
            row = None
       
        return row        
     
    def last_rowid(self):
        """Returns last inserted rowid.

        Note that this returns the last rowid that has been inserted using
        this *connection*, not cursor.
        """
        return self.cursor.getconnection().last_insert_rowid()

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

def get_inode(path, cur, trace=False):
    """Returns inode of object at `path`.
    
    If not found, returns None. If `trace` is `True`, returns a list
    of all (directory) inodes that need to be traversed to reach `path`
    """
    
    # Remove leading and trailing /
    path = path.lstrip("/").rstrip("/")
    
    # Root inode
    inode = cur.get_val("SELECT inode FROM contents WHERE inode=parent_inode")
    
    # Root directory requested
    if not path:
        return [inode] if trace else inode
    
    # Traverse
    visited = [inode]
    for el in path.split(os.sep):
        res = cur.get_list("SELECT inode FROM contents WHERE name=? AND parent_inode=?",
                             (buffer(el), inode))
        if not res:
            return None
        inode = res[0][0]
        visited.append(inode)

    if trace:
        return visited
    else:
        return inode

def get_path(name, inode_p, cur):
    """Returns the full path of `name` with parent inode `inode_p`.
    """
    
    # Root inode
    inode_r = cur.get_val("SELECT inode FROM contents WHERE inode=parent_inode")
    
    path = [name]
    
    while inode_p != inode_r:
        (name, inode_p) = cur.get_row("SELECT name, parent_inode FROM contents "
                                      "WHERE inode=?", (inode_p,)) # Not ambigious, since we don't allow directory hardlinks
        name = str(name)
        path.insert(0, name)
        
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


def debug(arg):
    """Log message if debug output has been activated
    """

    if logger.log_level < 2:
        return

    if type(arg) != type([]):
        arg = [arg, "\n"]

    logger.log_fn(arg)

def log(arg):
    """ Log info message
    """

    if logger.log_level < 1:
        return

    if type(arg) != type([]):
        arg = [arg, "\n"]

    logger.log_fn(arg)


def error(arg):
    """ Log error message
    """

    if type(arg) != type([]):
        arg = [arg, "\n"]

    logger.log_fn(arg)


def warn(arg):
    """ Log warning message
    """

    if type(arg) != type([]):
        arg = [arg, "\n"]

    logger.log_fn(arg)


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
            s3key = io2s3key(inode,blockno * blocksize)
            etag = bucket.store_from_file(s3key, tmp.name)
            cursor.execute("INSERT INTO s3_objects (inode,offset,s3key,size,etag) "
                           "VALUES (?,?,?,?)", (inode, blockno * blocksize,
                                                buffer(s3key), cursize, etag))
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


def io2s3key(inode, offset):
    """Gives s3key corresponding to given inode and starting offset.
    """

    return "s3ql_data_%d-%d" % (inode, offset)

        
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
