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
import fuse
import threading
import traceback
from common import *
from cStringIO import StringIO
import resource
from time import time

__all__ = [ "FUSEError", "server", "RevisionError", "io2s3key" ]

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


class fuse_adaptor(fuse.FUSE):
    """Overwrite some functions
    """

    def readdir(self, path, buf, filler, offset, fi):
        def pyfiller(name, attrs, off):
            if attrs:
                st = fuse.c_stat()
                fuse.set_st_attrs(st, attrs)
            else:
                st = None
            filler(buf, name, st, off)

        self.operations("readdir", path, pyfiller, offset, fi.contents.fh)
        return 0

    def ftruncate(self, path, length, fi):
        return self.operations('ftruncate', path, length, fi.contents.fh)


class server(fuse.Operations):
    """FUSE filesystem that stores its data on Amazon S3

    Attributes:
    -----------

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


    def __call__(self, op, *a):

        # write() is handled specially, because we don't want to dump
        # out the whole buffer to the logs.
        if op == "write":
            ap = [a[0], 
                  "<data, len=%ik>" % int(len(a[1])/1024)] + map(repr, a[2:])
        elif op == "readdir":
            ap = map(repr, a)
            ap[1] = "<filler>"
        else:
            ap = map(repr, a)

        # Print request name and parameters
        debug("* %s(%s)" % (op, ", ".join(ap)))

        try:
            return getattr(self, op)(*a)
        except FUSEError, e:
            # Final error handling is done in fuse.py
            # OSError apparently has to be initialized with a tuple, otherwise
            # errno is not stored correctly
            raise OSError(e.errno, "")
        except:
            (etype, value, tb) = sys.exc_info()

            error([ "Unexpected %s error: %s\n" % (etype.__name__, str(value)), 
                    "Filesystem may be corrupted, run fsck.s3ql as soon as possible!\n", 
                    "Please report this bug on http://code.google.com/p/s3ql/.\n"
                    "Traceback:\n"] + traceback.format_tb(tb))
            self.mark_damaged()
            raise OSError(errno.EIO)


    def __init__(self, bucket, dbfile, cachedir, noatime=False, cachesize=None):
        """Initializes S3QL fs.
        """

        self.local = threading.local()
        self.dbfile = dbfile
        self.cachedir = cachedir
        self.bucket = bucket
        self.noatime = noatime

        # Init Locks
        self.s3_lock = threading.Condition()
        self.s3_lock.locked_keys = set()

        # Check filesystem revision
        debug("Reading fs parameters...")
        cur = self.get_cursor()
        rev = cur.get_val("SELECT version FROM parameters")
        if rev < 1:
            raise RevisionError, (rev, 1)

        # Update mount count
        cur.execute("UPDATE parameters SET mountcnt = mountcnt + 1")

        # Get blocksize
        self.blocksize = cur.get_val("SELECT blocksize FROM parameters")

        # Calculate cachesize
        if cachesize is None:
            self.cachesize = self.blocksize * 30
        else:
            self.cachesize = cachesize



    def get_cursor(self, *a, **kw):
        """Returns a cursor from thread-local connection.

        The cursor is augmented with the convenience functions
        get_row, get_value and get_list.
        """

        if not hasattr(self.local, "conn"):
            debug("Creating new db connection...")
            self.local.conn = apsw.Connection(self.dbfile)
            self.local.conn.setbusytimeout(5000)

        return my_cursor(self.local.conn.cursor())


    def getattr(self, path, inode=None):
        """Handles FUSE getattr() requests
        """

        fstat = dict()
        cur = self.get_cursor()
        if not inode:            
            inode = get_inode(path, cur)
            if not inode: # not found
                raise(FUSEError(errno.ENOENT))
        
        (fstat["st_mode"], 
         fstat["st_nlink"], 
         fstat["st_uid"], 
         fstat["st_gid"], 
         fstat["st_size"], 
         fstat["st_ino"], 
         fstat["st_rdev"], 
         fstat["st_atime"], 
         fstat["st_mtime"], 
         fstat["st_ctime"]) = cur.get_row("SELECT mode, refcount, uid, gid, size, inode, rdev, "
                                          "atime, mtime, ctime FROM contents_ext WHERE inode=? ", 
                                          (inode,))
            
        # preferred blocksize for doing IO
        fstat["st_blksize"] = resource.getpagesize()

        if stat.S_ISREG(fstat["st_mode"]):
            # determine number of blocks for files
            fstat["st_blocks"] = cur.get_val("SELECT COUNT(s3key) FROM s3_objects "
                                             "WHERE inode=?", (inode,))
        else:
            # For special nodes, return arbitrary values
            fstat["st_size"] = 512
            fstat["st_blocks"] = 1

        # Device ID = 0 unless we have a device node
        if not stat.S_ISCHR(fstat["st_mode"]) and not stat.S_ISBLK(fstat["st_mode"]):
            fstat["st_rdev"] = 0

        return fstat

    def readlink(self, path):
        """Handles FUSE readlink() requests.
        """
        cur = self.get_cursor()

        inode = get_inode(path, cur)
        target = cur.get_val("SELECT target FROM inodes WHERE id=?", (inode,))

        if not self.noatime:
            update_atime(inode, cur)
        return str(target)

    def readdir(self, path, filler, offset, fh):
        """Handles FUSE readdir() requests
        """
        cur = self.get_cursor()

        if path == "/":
            inode = get_inode(path, cur)
            inode_p = inode
        else:
            (inode_p, inode) = get_inode(path, cur, trace=True)[-2:]

        if not self.noatime:
            update_atime(inode, cur)

        filler(".", self.getattr(None, inode), 0)
        filler("..", self.getattr(None, inode_p), 0)

        # Actual contents
        res = cur.execute("SELECT name, inode FROM contents WHERE parent_inode=? "
                          "AND inode != parent_inode", (inode,)) # Avoid to get / which is its own parent
        for (name,inode) in res:
            name = str(name)
            filler(name, self.getattr(None, inode), 0)

    def getxattr(self, path, name, position=0):
        raise FUSEError(fuse.ENOTSUP)

    def removexattr(self, path, name):
        raise FUSEError(fuse.ENOTSUP)

    def setxattr(self, path, name, value, options, position=0):
        raise FUSEError(fuse.ENOTSUP)

    def unlink(self, path):
        """Handles FUSE unlink() requests.

        Implementation depends on the ``hard_remove`` FUSE option
        not being used.
        """

        cur = self.get_cursor()
        (inode_p, inode) = get_inode(path, cur, trace=True)[-2:]
        fstat = self.getattr(None, inode)


        cur.execute("DELETE FROM contents WHERE name=? AND parent_inode=?", 
                    (buffer(os.path.basename(path)), inode_p))

        # No more links, remove datablocks
        if fstat["st_nlink"] == 1:
            res = cur.execute("SELECT s3key FROM s3_objects WHERE inode=?", 
                           (inode,))
            for (id,) in res:
                # The object may not have been comitted yet
                try:
                    self.bucket.delete_key(id)
                except KeyError:
                    pass

            # Drop cache
            res = cur.execute("SELECT fd, cachefile FROM s3_objects WHERE inode=?", 
                           (inode,))
            for (fd, cachefile) in res:
                os.close(fd)
                os.unlink(self.cachedir + cachefile)

            cur.execute("DELETE FROM s3_objects WHERE inode=?", (inode,))
            cur.execute("DELETE FROM inodes WHERE id=?", (inode,))
        else:
            # Also updates ctime
            decrease_refcount(inode, cur)

        update_mtime(inode_p, cur)


    def mark_damaged(self):
        """Marks the filesystem as being damaged and needing fsck.
        """

        cur = self.get_cursor()
        cur.execute("UPDATE parameters SET needs_fsck=?", (True,))


    def rmdir(self, path):
        """Handles FUSE rmdir() requests.
        """
        cur = self.get_cursor()

        (inode_p, inode) = get_inode(path, cur, trace=True)[-2:]
        
        # Check if directory is empty
        if cur.get_val("SELECT refcount FROM inodes WHERE id=?", (inode,)) > 2: 
            debug("Attempted to remove nonempty directory %s" % path)
            raise FUSEError(errno.EINVAL)

        # Delete
        cur.execute("BEGIN TRANSACTION")
        try:
            cur.execute("DELETE FROM contents WHERE name=? AND parent_inode=?",
                        (buffer(os.path.basename(path)), inode_p))
            cur.execute("DELETE FROM inodes WHERE id=?", (inode,))
            decrease_refcount(inode_p, cur)
            update_mtime(inode_p, cur)
        except:
            cur.execute("ROLLBACK")
            raise
        else:
            cur.execute("COMMIT")

    def symlink(self, name, target):
        """Handles FUSE symlink() requests.
        """

        cur = self.get_cursor()
        (uid, gid, pid) = fuse.fuse_get_context()
        inode_p = get_inode(os.path.dirname(name), cur)
        cur.execute("BEGIN TRANSACTION")
        try:
            mode = (stat.S_IFLNK | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR |
                     stat.S_IRGRP | stat.S_IWGRP | stat.S_IXGRP |
                     stat.S_IROTH |stat.S_IWOTH | stat.S_IXOTH)
            cur.execute("INSERT INTO inodes (mode,uid,gid,target,mtime,atime,ctime,refcount) "
                                "VALUES(?, ?, ?, ?, ?, ?, ?, 1)", 
                                (mode, uid, gid, buffer(target), 
                                 time(), time(), time()))
            cur.execute("INSERT INTO contents(name, inode, parent_inode) VALUES(?,?,?)", 
                                (buffer(os.path.basename(name)), self.local.conn.last_insert_rowid(), inode_p))
            update_mtime(inode_p, cur)
        except:
            cur.execute("ROLLBACK")
            raise
        else:
            cur.execute("COMMIT")

    def rename(self, old, new):
        """Handles FUSE rename() requests.
        """

        cur = self.get_cursor()
        inode_p_old = get_inode(os.path.dirname(old), cur)
        inode_p_new = get_inode(os.path.dirname(new), cur)
        
        if not inode_p_new:
            warn("rename: path %s does not exist" % new)
            raise FUSEError(errno.EINVAL)
        
        cur.execute("UPDATE contents SET name=?, parent_inode=? WHERE name=? "
                    "AND parent_inode=?", (buffer(os.path.basename(new)), inode_p_new,
                                           buffer(os.path.basename(old)), inode_p_old))
        update_mtime(inode_p_old, cur)
        update_mtime(inode_p_new, cur)

    def link(self, source, target):
        """Handles FUSE link() requests.
        """
        cur = self.get_cursor()

        # We do not want the getattr() overhead here
        fstat = self.getattr(target)
        inode_p = get_inode(os.path.dirname(source), cur)

        # Do not allow directory hardlinks
        if stat.S_ISDIR(fstat["st_mode"]):
            debug("Attempted to hardlink directory %s" % target)
            raise FUSEError(errno.EINVAL)

        cur.execute("BEGIN TRANSACTION")
        try:
            cur.execute("INSERT INTO contents (name,inode,parent_inode) VALUES(?,?,?)", 
                     (buffer(os.path.basename(source)), fstat["st_ino"], inode_p))
            increase_refcount(fstat["st_ino"], cur)
            update_mtime(inode_p, cur)
        except:
            cur.execute("ROLLBACK")
            raise
        else:
            cur.execute("COMMIT")

    def chmod(self, path, mode):
        """Handles FUSE chmod() requests.
        """

        fstat = self.getattr(path)
        if stat.S_IFMT(mode) != stat.S_IFMT(fstat["st_mode"]):
            warn("chmod: attempted to change file mode")
            raise FUSEError(errno.EINVAL)

        cur = self.get_cursor()
        cur.execute("UPDATE inodes SET mode=?,ctime=? WHERE id=?", (mode, time(), fstat["st_ino"]))

    def chown(self, path, user, group):
        """Handles FUSE chown() requests.
        """

        cur = self.get_cursor()
        inode = get_inode(path, cur)
        cur.execute("UPDATE inodes SET uid=?, gid=?, ctime=? WHERE id=?", (user, group, time(), inode))

    def mknod(self, path, mode, dev=None):
        """Handles FUSE mknod() requests.
        """

        # We create only these types (and no hybrids)
        modetype = stat.S_IFMT(mode)
        if not (modetype == stat.S_IFCHR or modetype == stat.S_IFBLK or
                modetype == stat.S_IFIFO):
            log("mknod: invalid mode")
            raise FUSEError(errno.EINVAL)

        cur = self.get_cursor()
        (uid, gid, pid) = fuse.fuse_get_context()
        inode_p = get_inode(os.path.dirname(path), cur)
        cur.execute("BEGIN TRANSACTION")
        try:
            cur.execute("INSERT INTO inodes (mtime,ctime,atime,uid,gid,mode,rdev,refcount,size) "
                     "VALUES(?, ?, ?, ?, ?, ?, ?, ?, 0)", 
                     (time(), time(), time(), uid, gid, mode, dev, 1))
            cur.execute("INSERT INTO contents(name, inode, parent_inode) VALUES(?,?,?)", 
                     (buffer(os.path.basename(path)), cur.last_rowid(), inode_p))
            update_mtime(inode_p, cur)
        except:
            cur.execute("ROLLBACK")
            raise
        else:
            cur.execute("COMMIT")


    def mkdir(self, path, mode):
        """Handles FUSE mkdir() requests.
        """

        # Check mode
        if (stat.S_IFMT(mode) != stat.S_IFDIR and
            stat.S_IFMT(mode) != 0):
            warn("mkdir: invalid mode")
            raise FUSEError(errno.EINVAL)

        # Ensure correct mode
        mode = (mode & ~stat.S_IFMT(mode)) | stat.S_IFDIR

        cur = self.get_cursor()
        inode_p = get_inode(os.path.dirname(path), cur)
        (uid, gid, pid) = fuse.fuse_get_context()
        cur.execute("BEGIN TRANSACTION")
        try:
            # refcount is 2 because of "."
            cur.execute("INSERT INTO inodes (mtime,atime,ctime,uid,gid,mode,refcount) "
                     "VALUES(?, ?, ?, ?, ?, ?, ?)", 
                     (time(), time(), time(), uid, gid, mode, 2))
            inode = cur.last_rowid()
            cur.execute("INSERT INTO contents(name, inode, parent_inode) VALUES(?, ?, ?)", 
                (buffer(os.path.basename(path)), inode, inode_p))
            increase_refcount(inode_p, cur)
            update_mtime(inode_p, cur)
        except:
            cur.execute("ROLLBACK")
            raise
        else:
            cur.execute("COMMIT")

    def utimens(self, path, times):
        """Handles FUSE utime() requests.
        """

        cur = self.get_cursor()
        (atime, mtime) = times
        inode = get_inode(path, cur)
        cur.execute("UPDATE inodes SET atime=?,mtime=?,ctime=? WHERE id=?",
                     (atime, mtime, time(), inode))

    def statfs(self):
        """Handles FUSE statfs() requests.
        """

        cur = self.get_cursor()
        stat = dict()

        # Blocksize
        stat["f_bsize"] = resource.getpagesize()
        stat["f_frsize"] = stat.f_bsize

        # Get number of blocks & inodes blocks
        blocks = cur.get_val("SELECT COUNT(s3key) FROM s3_objects")
        inodes = cur.get_val("SELECT COUNT(id) FROM inodes")

        # Since S3 is unlimited, always return a half-full filesystem
        stat["f_blocks"] = 2 * blocks
        stat["f_bfree"] = blocks
        stat["f_bavail"] = blocks
        stat["f_files"] = 2 * inodes
        stat["f_ffree"] = inodes

        return stat


    def truncate(self, bpath, len):
        """Handles FUSE truncate() requests.
        """

        fh = self.open(bpath, os.O_WRONLY)
        self.ftruncate(bpath, len, fh)
        self.release(bpath, fh)

    def main(self, mountpoint, **kw):
        """Starts the main loop handling FUSE requests.
        """

        # Start main event loop
        debug("Starting main event loop...")
        kw["default_permissions"] = True
        kw["use_ino"] = True
        kw["kernel_cache"] = True
        kw["fsname"] = "s3ql"
        fuse_adaptor(self, mountpoint, **kw)
        debug("Main event loop terminated.")

    def close(self):
        """Shut down FS instance.

        This method must be called in order to commit the metadata
        of the filesystem to S3 and to release any open locks and
        database connections.
        """
        cur = self.get_cursor()
        cur2 = self.get_cursor()

        # Flush file and datacache
        debug("Flushing cache...")
        res = cur.execute(
            "SELECT s3key, fd, dirty, cachefile FROM s3_objects WHERE fd IS NOT NULL")
        for (s3key, fd, dirty, cachefile) in res:
            debug("\tCurrent object: " + s3key)
            os.close(fd)
            if dirty:
                error([ "Warning! Object ", s3key, " has not yet been flushed.\n", 
                             "Please report this as a bug!\n" ])
                etag = self.bucket.store_from_file(s3key, self.cachedir + cachefile)
                cur2.execute("UPDATE s3_objects SET dirty=?, cachefile=?, "
                             "etag=?, fd=? WHERE s3key=?", 
                             (False, None, etag, None, s3key))
            else:
                cur2.execute("UPDATE s3_objects SET cachefile=?, fd=? WHERE s3key=?", 
                             (None, None, s3key))

            os.unlink(self.cachedir + cachefile)


        cur.execute("VACUUM")
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

    def unlock_s3key(self, s3key):
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

    def open(self, path, flags):
        """Opens file `path`.
        
        `flags` is ignored. Returns a file descriptor that is equal to the
        inode of the file, so it is not possible to distinguish between
        different open() and `create()` calls for the same inode.
        """
        
        cur = self.get_cursor()
        return get_inode(path, cur)

    def create(self, path, mode):
        """Creates file `path` with mode `mode`
        
        Returns a file descriptor that is equal to the
        inode of the file, so it is not possible to distinguish between
        different open() and create() calls for the same inode.
        """

        # check mode
        if (stat.S_IFMT(mode) != stat.S_IFREG and
            stat.S_IFMT(mode) != 0):
            warn("create: invalid mode")
            raise FUSEError(errno.EINVAL)

        # Ensure correct mode
        mode = (mode & ~stat.S_IFMT(mode)) | stat.S_IFREG

        cur = self.get_cursor()
        (uid, gid, pid) = fuse.fuse_get_context()
        dir = os.path.dirname(path)
        name = os.path.basename(path)
        inode_p = get_inode(dir, cur)
        cur.execute("BEGIN TRANSACTION")
        try:
            cur.execute("INSERT INTO inodes (mtime,ctime,atime,uid,gid,mode,rdev,refcount,size) "
                     "VALUES(?, ?, ?, ?, ?, ?, ?, ?, 0)", 
                     (time(), time(), time(), uid, gid, mode, None, 1))
            inode = cur.last_rowid()
            cur.execute("INSERT INTO contents(name, inode, parent_inode) VALUES(?,?,?)", 
                     (buffer(name), inode, inode_p))
            update_mtime(inode_p, cur)
        except:
            cur.execute("ROLLBACK")
            raise
        else:
            cur.execute("COMMIT")

        return inode

    def read(self, path, length, offset, inode):
        cur = self.get_cursor()
        buf = StringIO()
        while length > 0:
            if offset >= cur.get_val("SELECT size FROM inodes WHERE id=?", (inode,)):
                break
            tmp = self.read_direct(path, length, offset, inode)
            buf.write(tmp)
            length -= len(tmp)
            offset += len(tmp)
        return buf.getvalue()

    def read_direct(self, path, length, offset, inode):
        """Handles FUSE read() requests.

        May return less than `length` bytes.
        """
        cur = self.get_cursor()

        # Calculate starting offset of next s3 object, we don't
        # read further than that
        offset_f = self.blocksize * (int(offset/self.blocksize)+1)
        if offset + length > offset_f:
            length = offset_f - offset

        # Additionally, we don't read beyond the end of the file
        size = cur.get_val("SELECT size FROM inodes WHERE id=?", (inode,))
        if offset + length > size:
            length = size - offset

        # Obtain required s3 object
        offset_i = self.blocksize * int(offset/self.blocksize)
        s3key = io2s3key(inode, offset_i)

        self.lock_s3key(s3key)
        try:
            fd = self.retrieve_s3(s3key, inode)

            # If the object does not exist, we have a hole and return \0
            if fd is None:
                return "\0" * length

            # If we can't read enough, we have a hole as well
            # (since we already adjusted the length to be within the file size)
            os.lseek(fd, offset - offset_i, os.SEEK_SET)
            buf = StringIO()
            while length > 0:
                tmp = os.read(fd, length)
                if len(tmp) == 0:
                    return buf.getvalue() + "\0" * length
                buf.write(tmp)
                length -= len(tmp)
                
            if not self.noatime:
                update_atime(inode, cur)
                
            return buf.getvalue()
        finally:
            self.unlock_s3key(s3key)


    def retrieve_s3(self, s3key, inode, create=None):
        """Returns fd for s3 object `s3key`.

        If the s3 object is not already cached, it is retrieved from
        Amazon and put into the cache.

        If no such object exists and create is not None, the object is
        created with offset `create`. Otherwise, returns `None.

        The s3 key should already be locked when this function is called.
        """
        cur = self.get_cursor()

        if create is not None:
            offset = int(create)

            if offset % self.blocksize != 0:
                raise Exception, "s3 objects must start at blocksize boundaries"

        cachefile = s3key[1:].replace("~", "~~").replace("/", "~")
        cachepath = self.cachedir + cachefile

        # Check if existing
        res = cur.get_list("SELECT fd, etag FROM s3_objects WHERE s3key=?", (s3key,))

        # Existing Key
        if len(res):
            (fd, etag) = res[0]

        # New key
        else:
            if create is None:
                return None
            fd = os.open(cachepath, os.O_RDWR | os.O_CREAT)
            cur.execute("INSERT INTO s3_objects(s3key,dirty,fd,cachefile,atime,size,inode,offset) "
                     "VALUES(?,?,?,?,?,?,?,?)", 
                     (s3key, True, fd, cachefile, time(), 0, inode, offset))

        # Not yet in cache
        if fd is None:
            self.expire_cache()
            meta = self.bucket.fetch_to_file(s3key, cachepath)

            # Check etag
            if meta.etag != etag:
                warn(["Changes in %s apparently have not yet propagated. Waiting and retrying...\n" % s3key, 
                       "Try to increase the cache size to avoid this.\n"])
                waited = 0
                waittime = 0.01
                while meta.etag != etag and \
                        waited < self.timeout:
                    time.sleep(waittime)
                    waited += waittime
                    waittime *= 1.5
                    meta = self.bucket.lookup_key(s3key)

                # If still not found
                if meta.etag != etag:
                    error(["etag for %s doesn't match metadata!" % s3key, 
                           "Filesystem is probably corrupted (or S3 is having problems), "
                           "run fsck.s3ql as soon as possible.\n"])
                    self.mark_damaged()
                    raise FUSEError(errno.EIO)
                else:
                    meta = self.bucket.fetch_to_file(s3key, cachepath)

            fd = os.open(cachepath, os.O_RDWR)
            cur.execute("UPDATE s3_objects SET dirty=?,fd=?,cachefile=? "
                     "WHERE s3key=?", (False, fd, cachefile, s3key))


        # Update atime
        cur.execute("UPDATE s3_objects SET atime=? WHERE s3key=?", (time(), s3key))

        return fd

    def expire_cache(self):
        """Performs cache expiry.

        If the cache is bigger than `self.cachesize`, the oldest
        entries are flushed until at least `self.blocksize`
        bytes are available.
        """
        cur = self.get_cursor()
        used = cur.get_val("SELECT SUM(size) FROM s3_objects WHERE fd IS NOT NULL")

        while used + self.blocksize > self.cachesize:
            # Find & lock object to flush
            res  = cur.get_list("SELECT s3key FROM s3_objects WHERE fd IS NOT NULL "
                                    "ORDER BY atime ASC LIMIT 1")

            # If there is nothing to flush, we continue anyway
            if not res:
                continue


            s3key = res[0][0]

            self.lock_s3key(s3key)
            try:
                # Information may have changed while we waited for lock
                res = cur.get_list("SELECT dirty,fd,cachefile,size FROM s3_objects "
                                       "WHERE s3key=?", (s3key,))
                if not res:
                    # has been deleted
                    continue

                (dirty, fd, cachefile, size) = res[0]
                if fd is None:
                    # already flushed now
                    continue

                # flush
                os.close(fd)
                etag = self.bucket.store_from_file(s3key, self.cachedir + cachefile)
                cur.execute("UPDATE s3_objects SET dirty=?,fd=?,cachefile=?,etag=? "
                            "WHERE s3key=?", (False, None, None, etag, s3key))
                os.unlink(self.cachedir + cachefile)
            finally:
                self.unlock_s3key(s3key)

            used -= size

    def write(self, path, buf, offset, inode):
        total = len(buf)
        while len(buf) > 0:
            written = self.write_direct(path, buf, offset, inode)
            offset += written
            buf = buf[written:]
        return total


    def write_direct(self, path, buf, offset, inode):
        """Handles FUSE write() requests.

        May write less bytes than given in `buf`.
        """
        cur = self.get_cursor()

        # Obtain required s3 object
        offset_i = self.blocksize * int(offset/self.blocksize)
        s3key = io2s3key(inode, offset_i)

        # We write at most one block
        offset_f = offset_i + self.blocksize
        maxwrite = offset_f - offset

        debug("Writing to s3key " + s3key)

        self.lock_s3key(s3key)
        try:
            fd = self.retrieve_s3(s3key, inode, create=offset_i)

            # Determine number of bytes to write and write
            os.lseek(fd, offset - offset_i, os.SEEK_SET)
            if len(buf) > maxwrite:
                writelen = maxwrite
                writelen = os.write(fd, buf[:maxwrite])
            else:
                writelen = os.write(fd, buf)


            # Update object size
            obj_len = os.lseek(fd, 0, os.SEEK_END)
            cur.execute("UPDATE s3_objects SET size=? WHERE s3key=?", 
                        (obj_len, s3key))

            # Update file size if changed
            res = cur.execute("SELECT s3key FROM s3_objects WHERE inode=? "
                              "AND offset > ?", (inode, offset_i))
            if not list(res):
                cur.execute("UPDATE inodes SET size=?,ctime=? WHERE id=?", 
                            (offset_i + obj_len, time(), inode))

            # Update file mtime
            update_mtime(inode, cur)
            return writelen

        finally:
            self.unlock_s3key(s3key)


    def ftruncate(self, path, len, inode):
        """Handles FUSE ftruncate() requests.
        """
        cur = self.get_cursor()
        cur2 = self.get_cursor()


        # Delete all truncated s3 objects
        # I don't quite see why we are ordering the result, it doesn't
        # seem important - can we omit it?
        res = cur.execute("SELECT s3key FROM s3_objects WHERE "
                          "offset >= ? AND inode=? ORDER BY offset ASC", 
                          (len, inode))
        for (s3key,) in res:
            self.lock_s3key(s3key)
            try:
                (fd, cachefile) = cur2.get_row("SELECT fd,cachefile FROM s3_objects "
                                                  "WHERE s3key=?", (s3key,))

                if fd: # File is in cache
                    os.close(fd)
                    os.unlink(self.cachedir + cachefile)

                # Key may not yet been committed
                try:
                    self.bucket.delete_key(s3key)
                except KeyError:
                    pass

                cur2.execute("DELETE FROM s3_objects WHERE s3key=?", 
                                (s3key,))
            finally:
                self.unlock_s3key(s3key)


        # Get last object before truncation
        offset_i = self.blocksize * int((len-1) / self.blocksize)
        s3key = io2s3key(inode, offset_i)

        self.lock_s3key(s3key)
        try:
            fd = self.retrieve_s3(s3key, inode, create=offset_i)
            cursize = offset_i + os.lseek(fd, 0, os.SEEK_END)

            # If we are actually extending this object, we just write a
            # 0-byte at the last position
            if len > cursize:
                os.lseek(fd, len - 1 - offset_i, os.SEEK_SET)
                os.write(fd, "\0")


            # Otherwise we truncate the file
            else:
                os.ftruncate(fd, len - offset_i)

            # Update file size
            cur.execute("UPDATE inodes SET size=? WHERE id=?", 
                        (len, inode))
            cur.execute("UPDATE s3_objects SET size=?,dirty=? WHERE s3key=?", 
                        (len - offset_i, True, s3key))

            # Update file's mtime
            update_mtime(inode, cur)
        finally:
            self.unlock_s3key(s3key)

    def fsync(self, fdatasync, inode):
        """Handles FUSE fsync() requests.

        We do not lock the s3 objects, because we do not remove them
        from the cache and we mark them as clean before(!) we send
        them to S3. This ensures that if another thread writes
        while we are still sending, the object is correctly marked
        dirty again and will be resent on the next fsync().
        """
        cur = self.get_cursor()
        cur2 = self.get_cursor()

        # Metadata is always synced automatically, so we ignore
        # fdatasync
        res = cur.execute("SELECT s3key, fd, cachefile FROM s3_objects WHERE "
                          "dirty=? AND inode=?", (True, inode))
        for (s3key, fd, cachefile) in res:
            try:
                cur2.execute("UPDATE s3_objects SET dirty=? WHERE s3key=?", 
                             (False, s3key))
                os.fsync(fd)
                etag = self.bucket.store_from_file(s3key, self.cachedir + cachefile)
            except:
                cur2.execute("UPDATE s3_objects SET dirty=? WHERE s3key=?", 
                             (True, s3key))
                raise

            cur2.execute("UPDATE s3_objects SET etag=? WHERE s3key=?", 
                         (etag, s3key))


    # Called for close() calls. Here we sync the data, so that we
    # can still return write errors.
    def flush(self, path, inode):
        """Handles FUSE flush() requests.
        """
        return self.fsync(False, inode)


class RevisionError:
    """Raised if the filesystem revision is too new for the program
    """
    def __init__(self, args):
        self.rev_is = args[0]
        self.rev_should = args[1]

    def __str__(self):
        return "Filesystem has revision %d, filesystem tools can only handle " \
            "revisions up %d" % (self.rev_is, self.rev_should)


def io2s3key(inode, offset):
    """Gives s3key corresponding to given inode and starting offset.
    """

    return "s3ql_data_%d-%d" % (inode, offset)
