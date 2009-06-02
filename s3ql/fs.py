#!/usr/bin/env python
#
#    Copyright (C) 2008  Nikolaus Rath <Nikolaus@rath.org>
#
#    This program can be distributed under the terms of the GNU LGPL.
#

import os
import sys
import apsw
import errno
import stat
from fuse import Fuse, Direntry
import fuse
from time import time
import threading
import s3ql
from s3ql.common import *
import resource
import warnings

# Check fuse version
if not hasattr(fuse, '__version__'):
    raise RuntimeError, \
        "your fuse-py doesn't know of fuse.__version__, probably it's too old."
fuse.fuse_python_api = (0, 2)
fuse.feature_assert('stateful_files', 'has_init')


class fs(Fuse):
    """FUSE filesystem that stores its data on Amazon S3

    Attributes:
    -----------

    :file_class:  Class implementing file access operations
    :local:       Thread-local storage, used for database connections
    :dbfile:      Filename of metadata db
    :cachedir:    Directory for s3 object cache
    :bucket:      Bucket object for datatransfer with S3
    :s3_lock:     Condition object for locking of specific s3 keys
    :noatime:     True if entity access times shouldn't be updated.

    Note: `noatime` does not guarantee that access time will not be
    updated, but only prevents the update where it would adversely
    influence performance.



    Notes on Locking
    ----------------

    It is necessary to prevent simultanous access to the same s3
    object by multiple threads. While read() and write() operations
    could in principle also run unsychronized, cache flushing and
    creation of new objects require complete synchronization (also
    with read() and write()).

    Unfortunately we cannot just use a global lock for all s3 object
    operations, since this would slow down the application
    considerably (even local operations would have to wait for network
    operations to release the lock). Therefore we have to lock on a
    per-object basis.

    While this works fine in principle, we must keep in mind that the
    lack of a global lock means that we must not rely on any
    information associated with an s3 key before we hold the lock on
    this key. An operation involving s3 objects is therefore always of
    the following form:

     1. Look up the s3 key if not yet known

     2. Lock the s3 key

     3. *Update any data associated with the s3 key* (!)

     4. Perform actual operation

     5. Unlock the s3 key


    The locking and unlocking of the s3 keys has to be done with the
    lock_s3key() and unlock_s3key methods.
    """

    def __init__(self, bucket, dbfile, cachedir, noatime=False, cachesize=None):
        """Initializes S3QL fs.
        """
        Fuse.__init__(self)

        # We mess around to pass ourselves to the file class
        class file_class (s3ql.file):
            def __init__(self2, *a, **kw):
                s3ql.file.__init__(self2, self, *a, **kw)
        self.file_class = file_class

        self.local = threading.local()
        self.dbfile = dbfile
        self.cachedir = cachedir
        self.bucket = bucket
        self.noatime = noatime

        # Init Locks
        self.s3_lock = threading.Condition()
        self.s3_lock.locked_keys = set()

        # Get blocksize
        debug("Reading fs parameters...")
        (self.blocksize,) = self.sql("SELECT blocksize FROM parameters").next()

        # Calculate cachesize
        if cachesize is None:
            self.cachesize = self.blocksize * 30
        else:
            self.cachesize = cachesize

        # Check filesystem revision
        (rev,) = self.sql("SELECT version FROM parameters").next()
        if rev < 1:
            raise RevisionError, (rev, 1)

        # Update mount count
        self.sql("UPDATE parameters SET mountcnt = mountcnt + 1")


    def sql_value(self, *a, **kw):
        """Executes a select statement and returns a single value.

        The statement is passed to `self.sql`, and the first element
        of the first result row is returned.
        """

        return self.sql(*a, **kw).next()[0]

    def sql_list(self, *a, **kw):
        """Executes a select statement and returns result list

        The statement is passed to `self.sql`, and the result
        elemented is converted to a list and returned
        """

        return list(self.sql(*a, **kw))

    def sql_row(self, *a, **kw):
        """Executes a select statement and returns first row

        The statement is passed to `self.sql`, and the first result
        row is returned as a tuple.
        """

        return self.sql(*a, **kw).next()

    def sql(self, *a, **kw):
        """Executes given SQL statement with thread-local connection.

        The statement is passed to the cursor.execute() method and
        the resulting object is returned.
        """

        if not hasattr(self.local, "conn"):
            debug("Creating new db connection...")
            self.local.conn = apsw.Connection(self.dbfile)
            self.local.conn.setbusytimeout(5000)

        cursor = self.local.conn.cursor()

        return cursor.execute(*a, **kw)

    def sql_sep(self, *a, **kw):
        """Executes given SQL statement with new cursor

        The statement is passed to the cursor.execute() method and
        the resulting object is returned. The cursor object
        is discarded.

        The ``sep`` in the function name stands for the usage of a
        separate cursor instead of the main one.
        """

        warnings.warn("sql_sep is superseded by sql", DeprecationWarning)
        self.sql(*a,**kw)


    def getattr(self, path):
        """Handles FUSE getattr() requests
        """

        fstat = fuse.Stat()
        try:
            res = self.sql("SELECT mode, refcount, uid, gid, size, inode, rdev, "
                           "atime, mtime, ctime FROM contents_ext WHERE name=? ",
                           (buffer(path),))
            (fstat.st_mode,
             fstat.st_nlink,
             fstat.st_uid,
             fstat.st_gid,
             fstat.st_size,
             fstat.st_ino,
             fstat.st_rdev,
             fstat.st_atime,
             fstat.st_mtime,
             fstat.st_ctime) = res.next()
        except StopIteration:
            # Not truly an error
            raise FUSEError(errno.ENOENT)

        # preferred blocksize for doing IO
        fstat.st_blksize = resource.getpagesize()

        if stat.S_ISREG(fstat.st_mode):
            # determine number of blocks for files
            fstat.st_blocks = self.sql_value("SELECT COUNT(s3key) FROM s3_objects "
                                             "WHERE inode=?", (fstat.st_ino,))
        else:
            # For special nodes, return arbitrary values
            fstat.st_size = 512
            fstat.st_blocks = 1

        # Not applicable and/or overwritten anyway
        fstat.st_dev = 0

        # Device ID = 0 unless we have a device node
        if not stat.S_ISCHR(fstat.st_mode) and not stat.S_ISBLK(fstat.st_mode):
            fstat.st_rdev = 0

        # We can only return the int part, since nanosecond
        # resolution for getattr() is not yet supported by the
        # fuse python api.
        fstat.st_mtime = int(fstat.st_mtime)
        fstat.st_atime = int(fstat.st_atime)
        fstat.st_ctime = int(fstat.st_ctime)

        return fstat

    def readlink(self, path):
        """Handles FUSE readlink() requests.
        """

        (target,inode) = self.sql_row("SELECT target,inode FROM contents_ext "
                                      "WHERE name=?", (buffer(path),))
        self.update_atime(inode)
        return str(target)

    def readdir(self, path, offset):
        """Handles FUSE readdir() requests
        """

        inode = self.get_inode(path)
        self.update_atime(inode)

        # Current directory
        yield Direntry(".", ino=inode, type=stat.S_IFDIR)

        # Parent directory
        if path == "/":
            yield Direntry("..", ino=inode, type=stat.S_IFDIR)
            strip = 1
        else:
            yield Direntry("..", ino=self.get_inode(os.path.dirname(path)),
                           type=stat.S_IFDIR)
            strip = len(path)+1

        # Actual contents
        res = self.sql("SELECT name,inode,mode FROM contents_ext WHERE parent_inode=? "
                       "AND inode != ?", (inode,inode)) # Avoid to get / which is its own parent
        for (name,inode,mode) in res:
            yield Direntry(str(name)[strip:], ino=inode, type=stat.S_IFMT(mode))


    def update_atime(self, inode):
        """Updates the atime of the specified object.

        The objects atime will be set to the current time.
        """

        if self.noatime:
            return

        self.sql("UPDATE inodes SET atime=? WHERE id=?", (time(), inode))

    def update_ctime(self, inode):
        """Updates the ctime of the specified object.

        The objects ctime will be set to the current time.
        """

        self.sql("UPDATE inodes SET ctime=? WHERE id=?", (time(), inode))


    def update_mtime(self, inode):
        """Updates the mtime of the specified object.

        The objects mtime will be set to the current time.
        """

        self.sql("UPDATE inodes SET mtime=? WHERE id=?", (time(), inode))

    def update_mtime_parent(self, path):
        """Updates the mtime of the parent of the specified object.

        The mtime will be set to the current time.
        """

        inode = self.get_inode(os.path.dirname(path))
        self.update_mtime(inode)


    def unlink(self, path):
        """Handles FUSE unlink() requests.

        Implementation depends on the ``hard_remove`` FUSE option
        not being used.
        """

        fstat = self.getattr(path)
        inode = fstat.st_ino

        self.sql("DELETE FROM contents WHERE name=?", (buffer(path),))

        # No more links, remove datablocks
        if fstat.st_nlink == 1:
            res = self.sql("SELECT s3key FROM s3_objects WHERE inode=?",
                           (inode,))
            for (id,) in res:
                # The object may not have been comitted yet
                try:
                    self.bucket.delete_key(id)
                except KeyError:
                    pass

            # Drop cache
            res = self.sql("SELECT fd, cachefile FROM s3_objects WHERE inode=?",
                           (inode,))
            for (fd, cachefile) in res:
                os.close(fd)
                os.unlink(self.cachedir + cachefile)

            self.sql("DELETE FROM s3_objects WHERE inode=?", (inode,))
            self.sql("DELETE FROM inodes WHERE id=?", (inode,))
        else:
            # Also updates ctime
            self.decrease_refcount(inode)

        self.update_mtime_parent(path)

    def get_inode(self, path):
        """Returns inode of object at `path`.
        """

        inode = self.sql_value("SELECT inode FROM contents WHERE name=?",
                               (buffer(path),))
        return inode


    def mark_damaged(self):
        """Marks the filesystem as being damaged and needing fsck.
        """

        self.sql("UPDATE parameters SET needs_fsck=?", (True,))


    def rmdir(self, path):
        """Handles FUSE rmdir() requests.
        """

        inode = self.get_inode(path)
        inode_p = self.get_inode(os.path.dirname(path))


        # Check if directory is empty
        (entries,) = self.sql("SELECT COUNT(name) FROM contents WHERE parent_inode=?",
                           (inode,)).next()
        if entries >= 1:
            debug("Attempted to remove nonempty directory %s" % path)
            raise FUSEError(errno.EINVAL)

        # Delete
        self.sql("BEGIN TRANSACTION")
        try:
            self.sql("DELETE FROM contents WHERE name=?", (buffer(path),))
            self.sql("DELETE FROM inodes WHERE id=?", (inode,))
            self.decrease_refcount(inode_p)
            self.update_mtime(inode_p)
        except:
            self.sql("ROLLBACK")
            raise
        else:
            self.sql("COMMIT")

    def decrease_refcount(self, inode):
        """Decrease reference count for inode by 1.

        Also updates ctime.
        """
        self.sql("UPDATE inodes SET refcount=refcount-1,ctime=? WHERE id=?",
                 (time(), inode))

    def increase_refcount(self, inode):
        """Increase reference count for inode by 1.

        Also updates ctime.
        """
        self.sql("UPDATE inodes SET refcount=refcount+1, ctime=? WHERE id=?",
                 (time(), inode))


    def symlink(self, target, name):
        """Handles FUSE symlink() requests.
        """

        con = self.GetContext()
        inode_p = self.get_inode(os.path.dirname(name))
        self.sql("BEGIN TRANSACTION")
        try:
            self.sql("INSERT INTO inodes (mode,uid,gid,target,mtime,atime,ctime,refcount) "
                                "VALUES(?, ?, ?, ?, ?, ?, ?, 1)",
                                (stat.S_IFLNK, con["uid"], con["gid"], buffer(target),
                                 time(), time(), time()))
            self.sql("INSERT INTO contents(name, inode, parent_inode) VALUES(?,?,?)",
                                (buffer(name), self.local.conn.last_insert_rowid(), inode_p))
            self.update_mtime(inode_p)
        except:
            self.sql("ROLLBACK")
            raise
        else:
            self.sql("COMMIT")

    def rename(self, old, new):
        """Handles FUSE rename() requests.
        """

        self.sql("BEGIN TRANSACTION")
        try:
            self.sql("UPDATE contents SET name=? WHERE name=?", (buffer(new), buffer(old)))
            self.update_mtime_parent(old)
            self.update_mtime_parent(new)
        except:
            self.sql("ROLLBACK")
            raise
        else:
            self.sql("COMMIT")


    def link(self, path, path1):
        """Handles FUSE link() requests.
        """

        # We do not want the getattr() overhead here
        (inode, mode) = self.sql_row("SELECT mode, inode FROM contents_ext WHERE name=?",
                                     (buffer(path),))
        inode_p = self.get_inode(os.path.dirname(path1))

        # Do not allow directory hardlinks
        if stat.S_ISDIR(mode):
            debug("Attempted to hardlink directory %s" % path)
            raise FUSEError(errno.EINVAL)

        self.sql("BEGIN TRANSACTION")
        try:
            self.sql("INSERT INTO contents (name,inode,parent_inode) VALUES(?,?,?)",
                     (buffer(path1), inode, inode_p))
            self.increase_refcount(inode)
            self.update_mtime(inode_p)
        except:
            self.sql("ROLLBACK")
            raise
        else:
            self.sql("COMMIT")

    def chmod(self, path, mode):
        """Handles FUSE chmod() requests.
        """

        self.sql("UPDATE inodes SET mode=?,ctime=? WHERE id=(SELECT inode "
                 "FROM contents WHERE name=?)", (mode, time(), buffer(path)))

    def chown(self, path, user, group):
        """Handles FUSE chown() requests.
        """

        self.sql("UPDATE inodes SET uid=?, gid=?, ctime=? WHERE id=(SELECT inode "
                 "FROM contents WHERE name=?)", (user, group, time(), buffer(path)))

    def mknod(self, path, mode, dev=None):
        """Handles FUSE mknod() requests.
        """

        con = self.GetContext()
        inode_p = self.get_inode(os.path.dirname(path))
        self.sql("BEGIN TRANSACTION")
        try:
            self.sql("INSERT INTO inodes (mtime,ctime,atime,uid,gid,mode,rdev,refcount,size) "
                     "VALUES(?, ?, ?, ?, ?, ?, ?, ?, 0)",
                     (time(), time(), time(), con["uid"], con["gid"], mode, dev, 1))
            self.sql("INSERT INTO contents(name, inode, parent_inode) VALUES(?,?,?)",
                     (buffer(path), self.local.conn.last_insert_rowid(), inode_p))
            self.update_mtime(inode_p)
        except:
            self.sql("ROLLBACK")
            raise
        else:
            self.sql("COMMIT")


    def mkdir(self, path, mode):
        """Handles FUSE mkdir() requests.
        """

        mode |= stat.S_IFDIR # Set type to directory
        inode_p = self.get_inode(os.path.dirname(path))
        con = self.GetContext()
        self.sql("BEGIN TRANSACTION")
        try:
            # refcount is 2 because of "."
            self.sql("INSERT INTO inodes (mtime,atime,ctime,uid,gid,mode,refcount) "
                     "VALUES(?, ?, ?, ?, ?, ?, ?)",
                     (time(), time(), time(), con["uid"], con["gid"], mode, 2))
            inode = self.local.conn.last_insert_rowid()
            self.sql("INSERT INTO contents(name, inode, parent_inode) VALUES(?, ?, ?)",
                (buffer(path), inode, inode_p))
            self.increase_refcount(inode_p)
            self.update_mtime(inode_p)
        except:
            self.sql("ROLLBACK")
            raise
        else:
            self.sql("COMMIT")

    def utimens(self, path, times):
        """Handles FUSE utime() requests.
        """

        (atime, mtime) = times
        self.sql("UPDATE inodes SET atime=?,mtime=?,ctime=? WHERE id=(SELECT inode "
                 "FROM contents WHERE name=?)",
                 (atime.tv_sec + atime.tv_nsec/float(10**9),
                  mtime.tv_sec + mtime.tv_nsec/float(10**9),
                  time(), buffer(path)))


    def statfs(self):
        """Handles FUSE statfs() requests.
        """

        stat = fuse.StatVfs()

        # Blocksize
        stat.f_bsize = resource.getpagesize()
        stat.f_frsize = stat.f_bsize

        # Get number of blocks & inodes blocks
        (blocks,) = self.sql("SELECT COUNT(s3key) FROM s3_objects").next()
        (inodes,) = self.sql("SELECT COUNT(id) FROM inodes").next()

        # Since S3 is unlimited, always return a half-full filesystem
        stat.f_blocks = 2 * blocks
        stat.f_bfree = blocks
        stat.f_bavail = blocks
        stat.f_files = 2 * inodes
        stat.f_ffree = inodes

        return stat


    def truncate(self, bpath, len):
        """Handles FUSE truncate() requests.
        """

        file = self.file_class(bpath, os.O_WRONLY)
        file.ftruncate(len)
        file.release()

    def main(self, mountpoint, fuse_options=None, fg=False, mt=True):
        """Starts the main loop handling FUSE requests.

        fg: stay in foreground
        mt: multithreaded operation
        """

        if mt:
            # Check if apsw supports multithreading
            def test_threading():
                try:
                    self.sql("SELECT 42")
                except apsw.ThreadingViolationError:
                    self.multithreaded = False
                except:
                    self.exc = sys.exc_info()[1]
                else:
                    self.multithreaded = True
            t = threading.Thread(target=test_threading)
            t.start()
            t.join()
            if hasattr(self, "exc"):
                raise self.exc
            if not self.multithreaded:
                warn("WARNING: APSW library is too old, running single threaded only!")
        else:
            self.multithreaded = False

        # Start main event loop
        debug("Starting main event loop...")
        mountoptions =  [ "direct_io",
                          "default_permissions",
                          "use_ino",
                          "fsname=s3qlfs" ] + fuse_options
        args = [ sys.argv[0], "-o" + ",".join(mountoptions), mountpoint]

        if fg:
            args.append("-f")

        Fuse.main(self, args)

        debug("Main event loop terminated.")

    def close(self):
        """Shut down FS instance.

        This method must be called in order to commit the metadata
        of the filesystem to S3 and to release any open locks and
        database connections.
        """

        # Flush file and datacache
        debug("Flushing cache...")
        res = self.sql(
            "SELECT s3key, fd, dirty, cachefile FROM s3_objects WHERE fd IS NOT NULL")
        for (s3key, fd, dirty, cachefile) in res:
            debug("\tCurrent object: " + s3key)
            os.close(fd)
            if dirty:
                error([ "Warning! Object ", s3key, " has not yet been flushed.\n",
                             "Please report this as a bug!\n" ])
                bucket.store_from_file(s3key, self.cachedir + cachefile)
                self.sql("UPDATE s3_objects SET dirty=?, cachefile=?, "
                         "etag=?, fd=? WHERE s3key=?",
                         (False, None, key.etag, None, s3key))
            else:
                self.sql("UPDATE s3_objects SET cachefile=?, fd=? WHERE s3key=?",
                         (None, None, s3key))

            os.unlink(self.cachedir + cachefile)


        self.sql("VACUUM")
        debug("buffers flushed, fs has shut down.")

    def __destroy__(self):
        if hasattr(self, "conn"):
            raise Exception, "s3ql.fs instance was destroyed without calling close()!"

    def lock_s3key(self, s3key):
        """Locks the given s3 key.
        """
        cv = self.s3_lock

        # Lock set of locked s3 keys (global lock)
        cv.acquire()
        try:

            # Wait for given s3 key becoming unused
            while s3key in cv.locked_keys:
                cv.wait()

            # Mark it as used (local lock)
            cv.locked_keys.add(s3key)
        finally:
            # Release global lock
            cv.release()

    def unlock_s3key(self,s3key):
        """Releases lock on given s3key
        """
        cv = self.s3_lock

        # Lock set of locked s3 keys (global lock)
        cv.acquire()
        try:

            # Mark key as free (release local lock)
            cv.locked_keys.remove(s3key)

            # Notify other threads
            cv.notifyAll()

        finally:
            # Release global lock
            cv.release()
