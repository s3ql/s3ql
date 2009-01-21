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
import fuse
from getpass import getpass

def setup_excepthook(fs):
    """Install Top Level Exception Handler for FUSE.

    If a FUSE Python program encounters an unhandled exception, all
    information about this exception is lost. This is because the
    default global exception handlers prints to stdout, which is
    closed for a daemon process.

    Therefore, this function installs a new global exception handler
    that logs the exception using the defined facilities and marks the
    filesystem as needing fsck as a safety measure.


    Note: FUSE Python by default catches all `OSError` and `IOError`
    exceptions, so they are never seen by the global exception
    handler. To avoid this, this function also patches into the FUSE
    Python code and replaces the `fuse.ErrnoWrapper` class by
    `s3ql.ErrnoWrapper`.
    """


    # The Handler
    def handler(type, value, tb):
        error([ "Unexpected %s error: %s\n" % (str(type), str(value)),
                "Filesystem may be corrupted, run fsck.s3ql as soon as possible!\n",
                "Please report this bug to the program author.\n"
                "Traceback:\n",
                ] + traceback.format_tb(tb))

        fs.mark_damaged()

    # Install
    sys.excepthook = handler

    # Replace ErrnoWrapper, see docstring
    fuse.ErrnoWrapper = ErrnoWrapper


class ErrnoWrapper(object):
    """Dispatches calls to FUSE API methods

    This class is meant to be installed as fuse.ErrnoWrapper instead
    of the class shipped with FUSE Python.

    In contrast to the default one, it catches only those exceptions
    that have been designated for FUSE (type `FUSEError`). All other
    exceptions are propagated and can be handled in the global
    exception hook.

    Moreover, the this ErrnoWrapper logs every request that it
    dispatches with debug priority.
    """

    def __init__(self, func):
        self.func = func

        # Either we are a bound method...
        if hasattr(func, "im_self"):
            inst = func.im_self
            self.name = func.__name__

        # ...or we have open() and create() (I don't quite understand
        # this myself, dig into lowwrap() in fuse.py if you like)
        elif hasattr(func, "__name__") and \
                func.__name__ == "wrap":
            self.name = "file_class.__init__"

        # ..or we have stateful I/O, which is also encapsulated
        # differently
        elif func.__class__.__name__ == "mpx":
            self.name = "mpx"

        else:
            error(["Error: Don't know what object to wrap\n",
                   "Please report this as a bug to the author.\n"])
            self.name = str(func)

    def __call__(self, *a, **kw):
        """Calls the encapsulated function.

        If it raises a FUSEError, the exception is caught and
        the appropriate errno is returned.
        """

        # If we have stateful IO functions, we don't want to
        # print the file class and add the path as an
        # additional information
        if self.name == "mpx":
            ap = a[1:-1]
            pathinfo = " [path='%s']" % a[0]
            name = "file_class." + self.func.name
        else:
            ap = a
            name = self.name
            pathinfo = ""

        # write() is handled special, because we don't want to dump
        # out the whole buffer to the logs.
        if name == "file_class.write":
            ap = ("<data>",) + ap[1:-1]


        # Print request name and parameters
        debug("* %s(%s)%s" %
              (name, ", ".join(map(repr, chain(ap, kw.values()))),
               pathinfo))

        try:
            return self.func(*a, **kw)
        except FUSEError, e:
            return -e.errno


class FUSEError(Exception):
    """Exception representing FUSE Errors to be returned to the kernel.

    This exception can store only an errno. It is meant to return
    error codes to the kernel, which can only be done in this
    limited form.
    """
    def __init__(self, errno):
        self.errno = errno

    def __str__(self):
        return str(self.errno)

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


def io2s3key(inode, offset):
    """Gives s3key corresponding to given inode and starting offset.
    """

    return "s3ql_data_%d-%d" % (inode, offset)



class RevisionError:
    """Raised if the filesystem revision is too new for the program
    """
    def __init__(self, args):
        self.rev_is = args[0]
        self.rev_should = args[1]

    def __str__(self):
        return "Filesystem has revision %d, filesystem tools can only handle " \
                "revisions up %d" % (self.rev_is, self.rev_should)
