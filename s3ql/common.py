#!/usr/bin/env python
#
#    Copyright (C) 2008  Nikolaus Rath <Nikolaus@rath.org>
#
#    This program can be distributed under the terms of the GNU LGPL.
#


import traceback
import errno
import sys
import os
from itertools import chain
import functools
import s3ql
import hashlib
import stat
from time import time
from getpass import getpass

class my_cursor(object):
    """Wraps an apsw cursor to add some convenience functions.
    """

    def __init__(self, cursor):
        self.cursor = cursor

    def execute(self, *a, **kw):
        return self.cursor.execute(*a, **kw)

    def get_val(self, *a, **kw):
        """Executes a select statement and returns first element of first row.
        """

        return self.execute(*a, **kw).next()[0]

    def get_list(self, *a, **kw):
        """Executes a select statement and returns result list.
        """

        return list(self.execute(*a, **kw))

    def get_row(self, *a, **kw):
        """Executes a select statement and returns first row.
        """

        return self.execute(*a, **kw).next()

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

def get_inode(path, cur):
    """Returns inode of object at `path`.
    """
    inode = cur.get_val("SELECT inode FROM contents WHERE name=?",
                        (buffer(path),))
    return inode

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
    """ Log message if debug output has been activated
    """

    if s3ql.log_level < 2:
        return

    if type(arg) != type([]):
        arg = [arg, "\n"]

    s3ql.log_fn(arg)

def log(arg):
    """ Log info message
    """

    if s3ql.log_level < 1:
        return

    if type(arg) != type([]):
        arg = [arg, "\n"]

    s3ql.log_fn(arg)


def error(arg):
    """ Log error message
    """

    if type(arg) != type([]):
        arg = [arg, "\n"]

    s3ql.log_fn(arg)


def warn(arg):
    """ Log warning message
    """

    if type(arg) != type([]):
        arg = [arg, "\n"]

    s3ql.log_fn(arg)


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


    if key:
        if sys.stdin.isatty():
            pw = getpass("Enter AWS password: ")
        else:
            pw = sys.stdin.readline().rstrip()
        return (key, pw)

    # Read file
    path = os.environ["HOME"].rstrip("/") + "/.awssecret"
    mode = os.stat(path).st_mode
    if mode & stat.S_IRGRP or \
            mode & stat.S_IROTH:
        sys.stderr.write("%s has insecure permissions, bailing out\n" % path)
        sys.exit(1)
    file = open(path, "r")
    awskey = file.readline().rstrip()
    pw = file.readline().rstrip()

    return (awskey, pw)
