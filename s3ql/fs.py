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
from boto.s3.connection \
              import S3Connection
import s3ql
from s3ql.common import *

# Check fuse version
if not hasattr(fuse, '__version__'):
    raise RuntimeError, \
        "your fuse-py doesn't know of fuse.__version__, probably it's too old."
fuse.fuse_python_api = (0, 2)
fuse.feature_assert('stateful_files', 'has_init')


class fs(Fuse):
    """ FUSE filesystem that stores its data on Amazon S3
    """

    def __init__(self, bucketname, awskey=None, awspass=None):
        """Initializes S3QL fs.
        """
        Fuse.__init__(self)

        # We mess around to pass ourselves to the file class
        class file_class (s3ql.file):
            def __init__(self2, *a, **kw):
                s3ql.file.__init__(self2, self, *a, **kw)
        self.file_class = file_class

        self.db_lock = threading.Lock() # Locking for database connection
        self.local = threading.local() # Thread local variables
        self.dbfile = get_dbfile(bucketname)
        self.cachedir = get_cachedir(bucketname)

        # Connect to S3
        debug("Connecting to S3...")
        self.bucket = S3Connection(awskey, awspass).get_bucket(bucketname)

        # Check consistency
        debug("Checking consistency...")
        key = self.bucket.new_key("dirty")
        if key.get_contents_as_string() != "no":
            # FIXME: Replace this by an exception
            print >> sys.stderr, \
                "Metadata is dirty! Either some changes have not yet propagated\n" \
                "through S3 or the filesystem has not been umounted cleanly. In\n" \
                "the later case you should run s3fsck on the system where the\n" \
                "filesystem has been mounted most recently!\n"
            sys.exit(1)

        # Init cache
        if os.path.exists(self.cachedir):
            # FIXME: Replace this by an exception
            print >> sys.stderr, \
                "Local cache files already exists! Either you are trying to\n" \
                "to mount a filesystem that is already mounted, or the filesystem\n" \
                "has not been umounted cleanly. In the later case you should run\n" \
                "s3fsck.\n"
            sys.exit(1)
        os.mkdir(self.cachedir, 0700)

        # Download metadata
        debug("Downloading metadata...")
        if os.path.exists(self.dbfile):
            # FIXME: Replace this by an exception
            print >> sys.stderr, \
                "Local metadata file already exists! Either you are trying to\n" \
                "to mount a filesystem that is already mounted, or the filesystem\n" \
                "has not been umounted cleanly. In the later case you should run\n" \
                "s3fsck.\n"
            sys.exit(1)
        os.mknod(self.dbfile, 0600 | stat.S_IFREG)
        key = self.bucket.new_key("metadata")
        key.get_contents_to_filename(self.dbfile)


        # Connect to db
        debug("Connecting to db...")
        self.conn = apsw.Connection(self.dbfile)
        self.conn.setbusytimeout(5000)

        # Get blocksize
        debug("Reading fs parameters...")
        (self.blocksize,) = self.sql("SELECT blocksize FROM parameters").next()

        # Check that the fs itself is clean
        (dirty,) = self.sql("SELECT needs_fsck FROM parameters").next()
        if dirty:
            # FIXME: Replace this by an exception
            print >> sys.stderr, "Filesystem damaged, run s3fsk!\n"
            #os.unlink(self.dbfile)
            #os.rmdir(self.cachedir)
            #sys.exit(1)

        # Check filesystem revision
        (rev,) = self.sql("SELECT version FROM parameters").next()
        if rev < 1:
            # FIXME: Replace this by an exception
            print >> sys.stderr, "This version of S3QL is too old for the filesystem!\n"
            #os.unlink(self.dbfile)
            #os.rmdir(self.cachedir)
            #sys.exit(1)


        # Update mount count
        self.sql("UPDATE parameters SET mountcnt = mountcnt + 1")


    def sql(self, *a, **kw):
        """Executes given SQL statement with thread-local cursor.

        The statement is passed to the cursor.execute() method and
        the resulting object is returned.
        """

        if hasattr(self.local, "cursor"):
            cursor = self.local.cursor
        else:
            try:
                self.db_lock.acquire()
                cursor = self.conn.cursor()
            finally:
                self.db_lock.release()
            self.local.cursor = cursor


        return cursor.execute(*a, **kw)

    def sql_n(self, *a, **kw):
        """Executes given SQL statement with new cursor

        The statement is passed to the cursor.execute() method and
        the resulting object is returned. The cursor object
        is discarded.
        """

        try:
            self.db_lock.acquire()
            cursor = self.conn.cursor()
        finally:
            self.db_lock.release()

        return cursor.execute(*a, **kw)


    # This function is also called internally, so we define
    # an unwrapped version
    def getattr_i(self, path):
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
            return -errno.ENOENT

        # FIXME: Preferred blocksize for doing IO
        fstat.st_blksize = 512 * 1024

        if stat.S_ISREG(fstat.st_mode):
            # determine number of blocks for files
            fstat.st_blocks = self.sql("SELECT COUNT(s3key) FROM s3_objects "
                                       "WHERE inode=?", (fstat.st_ino,)).next()[0]
        else:
            # For special nodes, return arbitrary values
            fstat.st_size = 512
            fstat.st_blocks = 1

        # Not applicable and/or overwritten anyway
        fstat.st_dev = 0

        # Device ID = 0 unless we have a device node
        if not stat.S_ISCHR(fstat.st_mode) and not stat.S_ISBLK(fstat.st_mode):
            fstat.st_rdev = 0

        # Make integers
        fstat.st_mtime = int(fstat.st_mtime)
        fstat.st_atime = int(fstat.st_atime)
        fstat.st_ctime = int(fstat.st_ctime)

        return fstat
    getattr = fuse_request_handler(getattr_i)

    @fuse_request_handler
    def readlink(self, path):
        """Handles FUSE readlink() requests.
        """

        (target,inode) = self.sql("SELECT target,inode FROM contents_ext "
                                  "WHERE name=?", (buffer(path),)).next()
        self.update_atime(inode)

        return str(target)

    @fuse_request_handler
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


    @fuse_request_handler
    def unlink(self, path):
        """Handles FUSE unlink() requests.

        Implementation depends on the ``hard_remove`` FUSE option
        not being used.
        """

        fstat = self.getattr_i(path)
        inode = fstat.st_ino

        self.sql("DELETE FROM contents WHERE name=?", (buffer(path),))

        # No more links, remove datablocks
        if fstat.st_nlink == 1:
            res = self.sql("SELECT s3key FROM s3_objects WHERE inode=?",
                           (inode,))
            for (id,) in res:
                self.bucket.delete_key(id)

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

        (inode,) = self.sql("SELECT inode FROM contents WHERE name=?",
                            (buffer(path),)).next()
        return inode


    def mark_damaged(self):
        """Marks the filesystem as being damaged and needing fsck.
        """

        self.sql("UPDATE parameters SET needs_fsck=?", (True,))


    @fuse_request_handler
    def rmdir(self, path):
        """Handles FUSE rmdir() requests.
        """

        inode = self.get_inode(path)
        inode_p = self.get_inode(os.path.dirname(path))


        # Check if directory is empty
        (entries,) = self.sql("SELECT COUNT(name) FROM contents WHERE parent_inode=?",
                           (inode,)).next()
        if entries > 2: # 1 due to parent dir, 1 due to "."
            raise s3qlError(errno=errno.EINVAL,
                            desc="Attempted to remove nonempty directory",
                            path=path, inode=inode)

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


    @fuse_request_handler
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
                                (buffer(name), self.conn.last_insert_rowid(), inode_p))
            self.update_mtime(inode_p)
        except:
            self.sql("ROLLBACK")
            raise
        else:
            self.sql("COMMIT")

    @fuse_request_handler
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


    @fuse_request_handler
    def link(self, path, path1):
        """Handles FUSE link() requests.
        """

        inode = self.get_inode(path)
        inode_p = self.get_inode(os.path.dirname(path1))

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

    @fuse_request_handler
    def chmod(self, path, mode):
        """Handles FUSE chmod() requests.
        """

        self.sql("UPDATE inodes SET mode=?,ctime=? WHERE id=(SELECT inode "
                 "FROM contents WHERE name=?)", (mode, time(), buffer(path)))

    @fuse_request_handler
    def chown(self, path, user, group):
        """Handles FUSE chown() requests.
        """

        self.sql("UPDATE inodes SET uid=?, gid=?, ctime=? WHERE id=(SELECT inode "
                 "FROM contents WHERE name=?)", (user, group, time(), buffer(path)))

    # This function is also called internally, so we define
    # an unwrapped version
    def mknod_i(self, path, mode, dev=None):
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
                     (buffer(path), self.conn.last_insert_rowid(), inode_p))
            self.update_mtime(inode_p)
        except:
            self.sql("ROLLBACK")
            raise
        else:
            self.sql("COMMIT")
    mknod = fuse_request_handler(mknod_i)


    @fuse_request_handler
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
            inode = self.conn.last_insert_rowid()
            self.sql("INSERT INTO contents(name, inode, parent_inode) VALUES(?, ?, ?)",
                (buffer(path), inode, inode_p))
            self.increase_refcount(inode_p)
            self.update_mtime(inode_p)
        except:
            self.sql("ROLLBACK")
            raise
        else:
            self.sql("COMMIT")

    @fuse_request_handler
    def utime(self, path, times):
        """Handles FUSE utime() requests.
        """

        (atime, mtime) = times
        self.sql("UPDATE inodes SET atime=?,mtime=?,ctime=? WHERE id=(SELECT inode "
                 "FROM contents WHERE name=?)", (atime, mtime, time(), buffer(path)))


    @fuse_request_handler
    def statfs(self):
        """Handles FUSE statfs() requests.
        """

        stat = fuse.StatVfs()

        # FIMXME: Blocksize, basically random
        stat.f_bsize = 1024*1024*512 # 512 KB
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


    @fuse_request_handler
    def truncate(self, bpath, len):
        """Handles FUSE truncate() requests.
        """

        file = self.file_class()
        file.opencreate(bpath, os.O_WRONLY)
        file.ftruncate_i(len)
        file.release_i()

    def main(self, mountpoint, fuse_options):
        """Starts the main loop handling FUSE requests.
        """

        # Start main event loop
        debug("Starting main event loop...")
        self.multithreaded = 0
        mountoptions =  [ "direct_io",
                          "default_permissions",
                          "use_ino",
                          "fsname=s3qlfs" ] + fuse_options
        Fuse.main(self, [sys.argv[0], "-f", "-o" + ",".join(mountoptions),
                         mountpoint])



    def close(self):

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
                key = self.bucket.new_key(s3key)
                key.set_contents_from_filename(self.cachedir + cachefile)
                self.sql_n("UPDATE s3_objects SET dirty=?, cachefile=?, "
                         "etag=?, fd=? WHERE s3key=?",
                         (False, None, key.etag, None, s3key))
            else:
                self.sql_n("UPDATE s3_objects SET cachefile=?, fd=? WHERE s3key=?",
                         (None, None, s3key))

            os.unlink(self.cachedir + cachefile)


        # Upload database
        debug("Uploading database..")
        self.sql("VACUUM")
        self.conn.close()
        key = self.bucket.new_key("metadata")
        key.set_contents_from_filename(self.dbfile)

        key = self.bucket.new_key("dirty")
        key.set_contents_from_string("no")

        # Remove database
        debug("Cleaning up...")
        os.unlink(self.dbfile)
        os.rmdir(self.cachedir)
