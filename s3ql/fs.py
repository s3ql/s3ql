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
from boto.s3.connection \
              import S3Connection

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

    def __init__(self, bucketname, mountpoint, awskey=None,
                 awspass=None, debug_on=False, fuse_options=None):
        Fuse.__init__(self)

        # We mess around to pass ourselves to the file class
        class file_class (s3qlFile):
            def __init__(self2, *a, **kw):
                s3qlFile.__init__(self2, self, *a, **kw)
        self.file_class = file_class

        self.fuse_options = fuse_options
        self.bucketname = bucketname
        self.awskey = awskey
        self.awspass = awspass
        self.mountpoint = mountpoint
        self.dbdir = os.environ["HOME"].rstrip("/") + "/.s3qlfs/"
        self.dbfile = self.dbdir + bucketname + ".db"
        self.cachedir = self.dbdir + bucketname + "-cache/"

    # This function is also called internally, so we define
    # an unwrapped version
    def getattr_i(self, path):
        """Handles FUSE getattr() requests
        """

        stat = fuse.Stat()
        try:
            res = self.cursor.execute("SELECT mode, refcount, uid, gid, size, inode, rdev, "
                                      "atime, mtime, ctime FROM contents_ext WHERE name=? ",
                                      (buffer(path),))
            (stat.st_mode,
             stat.st_nlink,
             stat.st_uid,
             stat.st_gid,
             stat.st_size,
             stat.st_ino,
             stat.st_rdev,
             stat.st_atime,
             stat.st_mtime,
             stat.st_ctime) = res.next()
        except StopIteration:
            return -ENOENT

        # FIXME: Preferred blocksize for doing IO
        stat.st_blksize = 512 * 1024

        if S_ISREG(stat.st_mode):
            # determine number of blocks for files
            stat.st_blocks = int(stat.st_size/512)
        else:
            # For special nodes, return arbitrary values
            stat.st_size = 512
            stat.st_blocks = 1

        # Not applicable and/or overwritten anyway
        stat.st_dev = 0

        # Device ID = 0 unless we have a device node
        if not S_ISCHR(stat.st_mode) and not S_ISBLK(stat.st_mode):
            stat.st_rdev = 0

        # Make integers
        stat.st_mtime = int(stat.st_mtime)
        stat.st_atime = int(stat.st_atime)
        stat.st_ctime = int(stat.st_ctime)

        return stat
    getattr = fuse_request_handler(getattr_i)

    @fuse_request_handler
    def readlink(self, path):
        """Handles FUSE readlink() requests.
        """

        (target,inode) = self.cursor.execute\
            ("SELECT target,inode FROM contents_ext WHERE name=?", (buffer(path),)).next()

        self.update_atime(inode=inode)

        return str(target)

    @fuse_request_handler
    def readdir(self, path, offset):
        """Handles FUSE readdir() requests
        """

        stat = self.getattr_i(path)

        self.update_atime(inode=stat.st_ino)

        # Current directory
        yield Direntry(".", ino=stat.st_ino, type=S_IFDIR)

        # Parent directory
        if path == "/":
            yield Direntry("..", ino=stat.st_ino, type=S_IFDIR)
            lookup = "/?*"
            strip = 1
        else:
            parent = path[:path.rindex("/")]
            if parent == "":
                parent = "/"
            yield Direntry("..", ino=self.getattr_i(parent).st_ino,
                           type=S_IFDIR)
            lookup = path + "/?*"
            strip = len(path)+1

        # Actual contents
        res = self.cursor.execute("SELECT name,inode,mode FROM contents_ext WHERE name GLOB ?",
                                  (buffer(lookup),))
        for (name,inode,mode) in res:
            yield Direntry(str(name)[strip:], ino=inode, type=S_IFMT(mode))


    def update_atime(self, inode=None, path=None):
        """Updates the mtime of the specified object.

        Only one of the `inode` and `path` parameters must be given
        to identify the object. The objects atime will be set to the
        current time.
        """

        if (inode and path) or (not inode and not path):
            raise s3qlException("update_mtime must be called with exactly one parameter.")

        if path:
            (inode,) = self.cursor.execute("SELECT inode FROM contents WHERE name=?",
                                           (buffer(path),)).next()

        self.cursor.execute("UPDATE inodes SET atime=? WHERE id=?", (time(), inode))




    def update_parent_mtime(self, path):
        """Updates the mtime of the parent directory of the specified object.

        The mtime of the directory containing the specified object will be set
        to the current time.
        """

        parent = path[:path.rindex("/")]
        if parent == "":
            parent = "/"

        self.cursor.execute("UPDATE inodes SET mtime=? WHERE id=?",
                            (time(), self.getattr_i(parent).st_ino))


    def lock_inode(self, id):
        """ locks the specified inode

        If the entry is already locked, the function waits until
        the lock is released.
        """
        # The python implementation is single threaded
        pass

    def unlock_inode(self, id):
        """ unlocks the specified inode

        This function must only be called if we are actually holding
        a lock on the given entry.
        """
        # The python implementation is single threaded
        pass


    @fuse_request_handler
    def unlink(self, path):
        """Handles FUSE unlink(( requests.

        Implementation depends on the ``hard_remove`` FUSE option
        not being used.
        """

        (inode,refcount) = self.cursor.execute \
            ("SELECT inode,refcount FROM contents_ext WHERE name=?", (buffer(path),)).next()

        self.cursor.execute("DELETE FROM contents WHERE name=?", (buffer(path),))

        # No more links, remove datablocks
        if refcount == 1:
            res = self.cursor.execute("SELECT s3key FROM s3_objects WHERE inode=?", (inode,))
            for (id,) in res:
                bucket.delete_key(id)

            self.cursor.execute("DELETE FROM s3_objects WHERE inode=?", (inode,))
            self.cursor.execute("DELETE FROM inodes WHERE id=?", (inode,))
        else:
            self.cursor.execute("UPDATE inodes SET ctime=? WHERE id=?", (time(), inode))

        self.update_parent_mtime(path)


    def mark_damaged(self):
        """Marks the filesystem as being damaged and needing fsck.
        """

        self.cursor.execute("UPDATE parameters SET needs_fsck=?", (True,))


    @fuse_request_handler
    def rmdir(self, path):
        """Handles FUSE rmdir() requests.
        """

        inode = self.getattr_i(path).st_ino

        # Check if directory is empty
        try:
            self.cursor.execute \
            ("SELECT * FROM contents WHERE name GLOB ? LIMIT 1", (buffer(path + "/*"),)).next()
        except StopIteration:
            pass # That's what we want
        else:
            return -EINVAL

        # Delete
        self.cursor.execute("BEGIN TRANSACTION")
        try:
            self.cursor.execute("DELETE FROM contents WHERE name=?", (buffer(path),))
            self.cursor.execute("DELETE FROM inodes WHERE id=?", (inode,))
            self.update_parent_mtime(path)
        except:
            self.cursor.execute("ROLLBACK")
            raise
        else:
            self.cursor.execute("COMMIT")


    @fuse_request_handler
    def symlink(self, target, name):
        """Handles FUSE symlink() requests.
        """

        con = self.GetContext()
        self.cursor.execute("BEGIN TRANSACTION")
        try:
            self.cursor.execute("INSERT INTO inodes (mode,uid,gid,target,mtime,atime,ctime) "
                                "VALUES(?, ?, ?, ?, ?, ?, ?)",
                                (S_IFLNK, con["uid"], con["gid"], buffer(target), time(), time(), time()))
            self.cursor.execute("INSERT INTO contents(name, inode) VALUES(?, ?)",
                                (buffer(name), self.conn.last_insert_rowid()))
            self.update_parent_mtime(name)
        except:
            self.cursor.execute("ROLLBACK")
            raise
        else:
            self.cursor.execute("COMMIT")

    @fuse_request_handler
    def rename(self, path, path1):
        """Handles FUSE rename() requests.
        """

        self.cursor.execute("UPDATE contents SET name=? WHERE name=?", (buffer(path1), buffer(path)))
        self.update_parent_mtime(path)
        self.update_parent_mtime(path1)


    @fuse_request_handler
    def link(self, path, path1):
        """Handles FUSE link() requests.
        """

        stat = self.getattr_i(path)

        self.cursor.execute("INSERT INTO contents (name,inode) VALUES(?,?)"
                            (buffer(path1), stat.st_ino))
        self.cursor.execute("UPDATE inodes SET ctime=? WHERE id=?", (time(), stat.st_ino))
        self.update_parent_mtime(path1)


    @fuse_request_handler
    def chmod(self, path, mode):
        """Handles FUSE chmod() requests.
        """

        self.cursor.execute("UPDATE inodes SET mode=?,ctime=? WHERE id=(SELECT inode "
                            "FROM contents WHERE name=?)", (mode, time(), buffer(path)))

    @fuse_request_handler
    def chown(self, path, user, group):
        """Handles FUSE chown() requests.
        """

        self.cursor.execute("UPDATE inodes SET uid=?, gid=?, ctime=? WHERE id=(SELECT inode "
                            "FROM contents WHERE name=?)", (user, group, time(), buffer(path)))

    # This function is also called internally, so we define
    # an unwrapped version
    def mknod_i(self, path, mode, dev=None):
        """Handles FUSE mknod() requests.
        """

        con = self.GetContext()
        self.cursor.execute("BEGIN TRANSACTION")
        try:
            self.cursor.execute("INSERT INTO inodes (mtime,ctime,atime,uid, gid, mode, size, rdev) "
                                "VALUES(?, ?, ?, ?, ?, ?, 0, ?)",
                                (time(), time(), time(), con["uid"], con["gid"], mode, dev))
            self.cursor.execute("INSERT INTO contents(name, inode) VALUES(?, ?)",
                                (buffer(path), self.conn.last_insert_rowid()))
            self.update_parent_mtime(path)
        except:
            self.cursor.execute("ROLLBACK")
            raise
        else:
            self.cursor.execute("COMMIT")
    mknod = fuse_request_handler(mknod_i)


    @fuse_request_handler
    def mkdir(self, path, mode):
        """Handles FUSE mkdir() requests.
        """

        mode |= S_IFDIR # Set type to directory
        con = self.GetContext()
        self.cursor.execute("BEGIN TRANSACTION")
        try:
            self.cursor.execute("INSERT INTO inodes (mtime,atime,ctime,uid, gid, mode) "
                                "VALUES(?, ?, ?, ?, ?, ?)",
                                (time(), time(), time(), con["uid"], con["gid"], mode))
            self.cursor.execute("INSERT INTO contents(name, inode) VALUES(?, ?)",
                                (buffer(path), self.conn.last_insert_rowid()))
            self.update_parent_mtime(path)
        except:
            self.cursor.execute("ROLLBACK")
            raise
        else:
            self.cursor.execute("COMMIT")

    @fuse_request_handler
    def utime(self, path, times):
        """Handles FUSE utime() requests.
        """

        (atime, mtime) = times
        self.cursor.execute("UPDATE inodes SET atime=?,mtime=?,ctime=? WHERE id=(SELECT inode "
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
        (blocks,) = self.cursor.execute("SELECT COUNT(s3key) FROM s3_objects").next()
        (inodes,) = self.cursor.execute("SELECT COUNT(id) FROM inodes").next()

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

    def main(self):
        """Starts the main loop handling FUSE requests.

        This implementation only runs single-threaded.
        """

        # Preferences
        if not os.path.exists(self.dbdir):
            os.mkdir(self.dbdir)


        # Connect to S3
        debug("Connecting to S3...")
        self.bucket = S3Connection(self.awskey, self.awspass).\
            get_bucket(self.bucketname)

        # Check consistency
        debug("Checking consistency...")
        key = self.bucket.new_key("dirty")
        if key.get_contents_as_string() != "no":
            print >> sys.stderr, \
                "Metadata is dirty! Either some changes have not yet propagated\n" \
                "through S3 or the filesystem has not been umounted cleanly. In\n" \
                "the later case you should run s3fsck on the system where the\n" \
                "filesystem has been mounted most recently!\n"
            sys.exit(1)

        # Download metadata
        debug("Downloading metadata...")
        if os.path.exists(self.dbfile):
            print >> sys.stderr, \
                "Local metadata file already exists! Either you are trying to\n" \
                "to mount a filesystem that is already mounted, or the filesystem\n" \
                "has not been umounted cleanly. In the later case you should run\n" \
                "s3fsck.\n"
            sys.exit(1)
        os.mknod(self.dbfile, 0600 | S_IFREG)
        key = self.bucket.new_key("metadata")
        key.get_contents_to_filename(self.dbfile)

        # Init cache
        if os.path.exists(self.cachedir):
            print >> sys.stderr, \
                "Local cache files already exists! Either you are trying to\n" \
                "to mount a filesystem that is already mounted, or the filesystem\n" \
                "has not been umounted cleanly. In the later case you should run\n" \
                "s3fsck.\n"
            sys.exit(1)
        os.mkdir(self.cachedir, 0700)

        # Connect to db
        debug("Connecting to db...")
        self.conn = apsw.Connection(self.dbfile)
        self.conn.setbusytimeout(5000)
        self.cursor = self.conn.cursor()

        # Get blocksize
        debug("Reading fs parameters...")
        try:
            (self.blocksize,) = self.cursor.execute("SELECT blocksize FROM parameters").next()
        except StopIteration:
            print >> sys.stderr, "Filesystem damaged, run s3fsk!\n"
            sys.exit(1)

        # Check that the fs itself is clean
        dirty = False
        try:
            (dirty,) = self.cursor.execute("SELECT needs_fsck FROM parameters").next()
        except StopIteration:
            dirty = True
        if dirty:
            print >> sys.stderr, "Filesystem damaged, run s3fsk!\n"
            #os.unlink(self.dbfile)
            #os.rmdir(self.cachedir)
            #sys.exit(1)

        # Update mount count
        self.cursor.execute("UPDATE parameters SET mountcnt = mountcnt + 1")

        # Start main event loop
        debug("Starting main event loop...")
        self.multithreaded = 0
        mountoptions =  [ "direct_io",
                          "default_permissions",
                          "use_ino",
                          "fsname=s3qlfs" ] + self.fuse_options
        Fuse.main(self, [sys.argv[0], "-f", "-o" + ",".join(mountoptions),
                         self.mountpoint])


        # Flush file and datacache
        debug("Flushing cache...")
        res = self.cursor.execute(
            "SELECT s3key, fd, dirty, cachefile FROM s3_objects WHERE fd IS NOT NULL")
        for (s3key, fd, dirty, cachefile) in list(res): # copy list to reuse cursor
            debug("\tCurrent object: " + s3key)
            os.close(fd)
            if dirty:
                error([ "Warning! Object ", s3key, " has not yet been flushed.\n",
                             "Please report this as a bug!\n" ])
                key = self.bucket.new_key(s3key)
                key.set_contents_from_filename(self.cachedir + cachefile)
            self.cursor.execute("UPDATE s3_objects SET dirty=?, cachefile=?, "
                                "etag=?, fd=? WHERE s3key=?",
                                (False, None, key.etag, None, s3key))
            os.unlink(self.cachedir + cachefile)


        # Upload database
        debug("Uploading database..")
        self.cursor.execute("VACUUM")
        self.conn.close()
        key = self.bucket.new_key("metadata")
        key.set_contents_from_filename(self.dbfile)

        key = self.bucket.new_key("dirty")
        key.set_contents_from_string("no")

        # Remove database
        debug("Cleaning up...")
        os.unlink(self.dbfile)
        os.rmdir(self.cachedir)
