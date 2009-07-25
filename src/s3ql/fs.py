#!/usr/bin/env python
#
#    Copyright (C) 2008  Nikolaus Rath <Nikolaus@rath.org>
#
#    This program can be distributed under the terms of the GNU LGPL.
#

import os
import apsw
import errno
import stat
import fuse
import threading
import logging
from s3ql.common import (decrease_refcount, get_inode, update_mtime, get_inodes,
                         increase_refcount, MyCursor, update_atime)
from cStringIO import StringIO
import resource
from time import time


__all__ = [ "FUSEError", "Server", "RevisionError" ]

# standard logger for this module
log = logging.getLogger("fs")

# Logger for low-level fuse
log_fuse = logging.getLogger("fuse")


class FUSEError(Exception):
    """Exception representing FUSE Errors to be returned to the kernel.

    This exception can store only an errno. It is meant to return error codes to
    the kernel, which can only be done in this limited form.
    
    Attributes
    ----------
    :fatal:    If set, the fs will mark the filesystem as needing fsck.
    """
    
    __slots__ = [ 'errno', 'fatal' ]
    
    def __init__(self, errno, fatal=False):
        self.errno = errno
        self.fatal = fatal

    def __str__(self):
        return str(self.errno)


class FuseAdaptor(fuse.FUSE):
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




class Server(fuse.Operations):
    """FUSE filesystem that stores its data on Amazon S3

    Attributes:
    -----------

    :local:       Thread-local storage, used for database connections
    :dbfile:      Filename of metadata db
    :cache:       Holds information about cached s3 objects
    :noatime:     True if entity access times shouldn't be updated.

    Note: `noatime` does not guarantee that access time will not be
    updated, but only prevents the update where it would adversely
    influence performance.

    Notes on database usage
    -----------------------
    
    Database cursors should always be obtained from server.get_cursor(). 
    This ensures that every thread uses a thread-local connection, so
    that one can uniquely retrieve the last inserted rowid and the
    number of rows affected by the last statement.
    
    Since sqlite does not support nested transactions, named 
    savepoints have to be used instead. The name of the savepoint
    should be the name of the class and the invoking method,
    separated by a dot. This should make the name unique.
    
    Keep in mind that, in contrast to `BEGIN ... ROLLBACK`,
    `SAVEPOINT name .. ROLLBACK TO name` does not cancel the
    transaction. Hence the correct logic is:
    
    cursor.execute("SAVEPOINT 'classname.methodname'")
    try:
        # perform transaction statements
    except:
        cursor.execute("ROLLBACK TO 'classname.methodname'")
        raise
    finally:
        cursor.execute("RELEASE 'classname.methodname'")
    
    """


    def __call__(self, op, *a):

        # write() is handled specially, because we don't want to dump
        # out the whole buffer to the logs.
        if op == "write":
            ap = [a[0],
                  "<data, len=%ik>" % int(len(a[1]) / 1024)] + map(repr, a[2:])
        elif op == "readdir":
            ap = map(repr, a)
            ap[1] = "<filler>"
        else:
            ap = map(repr, a)

        # Print request name and parameters
        log_fuse.debug("* %s(%s)", op, ", ".join(ap))

        try:
            return getattr(self, op)(*a)
        except FUSEError, e:
            if e.fatal:
                self.mark_damaged()
            # Final error handling is done in fuse.py
            # OSError apparently has to be initialized with a tuple, otherwise
            # errno is not stored correctly
            raise OSError(e.errno, "")
        except:
            log_fuse.error("Unexpected internal filesystem error."
                           "Filesystem may be corrupted, run fsck.s3ql as soon as possible!" 
                           "Please report this bug on http://code.google.com/p/s3ql/.",
                           exc_info=True)
            self.mark_damaged()
            raise OSError(errno.EIO)


    def __init__(self, cache, dbfile, noatime=True):
        """Initializes S3QL fs.
        """

        self.local = threading.local()
        self.dbfile = dbfile
        self.noatime = noatime
        self.cache = cache
            
        # Check filesystem revision
        log.debug("Reading fs parameters...")
        cur = self.get_cursor()
        rev = cur.get_val("SELECT version FROM parameters")
        if rev < 1:
            raise RevisionError, (rev, 1)

        # Update mount count
        cur.execute("UPDATE parameters SET mountcnt = mountcnt + 1")

        # Get blocksize
        self.blocksize = cur.get_val("SELECT blocksize FROM parameters")


    def get_cursor(self, *a, **kw):
        """Returns a cursor from thread-local connection.

        The cursor is augmented with the convenience functions
        get_row, get_val and get_list.
        """

        if not hasattr(self.local, "conn"):
            log.debug("Creating new db connection...")
            self.local.conn = apsw.Connection(self.dbfile)
            self.local.conn.setbusytimeout(500)
            cur = MyCursor(self.local.conn.cursor())
            
            # For better performance (but risks db corruption if computer crashes)
            cur.execute("PRAGMA temp_store = 2")
            cur.execute("PRAGMA synchronous = off")
            
            return cur

        else:
            return MyCursor(self.local.conn.cursor())

    def getattr(self, path, inode=None):
        """Handles FUSE getattr() requests
        
        We only support the `inode' parameter because fuse.py expects
        this interface. 
        """
        
        if inode is None:
            cur = self.get_cursor()
            try:
                inode = get_inode(path, cur)
            except KeyError: # not found
                raise(FUSEError(errno.ENOENT))

        return self.getattr_ino(inode)

    def getattr_ino(self, inode):
        cur = self.get_cursor()
        fstat = dict()
        
        (fstat["st_mode"],
         fstat["st_nlink"],
         fstat["st_uid"],
         fstat["st_gid"],
         fstat["st_size"],
         fstat["st_ino"],
         fstat["st_rdev"],
         fstat["st_atime"],
         fstat["st_mtime"],
         fstat["st_ctime"]) = cur.get_row("SELECT mode, refcount, uid, gid, size, id, rdev, "
                                          "atime, mtime, ctime FROM inodes WHERE id=? ",
                                          (inode,))
            
        # preferred blocksize for doing IO
        fstat["st_blksize"] = resource.getpagesize()

        if stat.S_ISREG(fstat["st_mode"]):
            # determine number of blocks for files
            # The exact semantics are not clear. For now we just return 1,
            # since the file occupies exactly as much space as it is large.
            fstat["st_blocks"] = 1
            
            # We could also count the number of s3 objects:
            #fstat["st_blocks"] = cur.get_val("SELECT COUNT(s3key) FROM inode_s3key "
            #                                 "WHERE inode=?", (inode,))
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
            (inode_p, inode) = get_inodes(path, cur)[-2:]

        if not self.noatime:
            update_atime(inode, cur)

        filler(".", self.getattr_ino(inode), 0)
        filler("..", self.getattr_ino(inode_p), 0)

        # Actual contents
        res = cur.execute("SELECT name, inode FROM contents WHERE parent_inode=? "
                          "AND inode != parent_inode", (inode,)) # Avoid to get / which is its own parent
        for (name, inode) in res:
            name = str(name)
            filler(name, self.getattr_ino(inode), 0)


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
        (inode_p, inode) = get_inodes(path, cur)[-2:]
        fstat = self.getattr_ino(inode)

        cur.execute("SAVEPOINT 'Server.unlink'")
        try:
            cur.execute("DELETE FROM contents WHERE name=? AND parent_inode=?",
                        (buffer(os.path.basename(path)), inode_p))
    
            # No more links, remove datablocks
            if fstat["st_nlink"] == 1:
                self.cache.remove(inode, cur)                
                cur.execute("DELETE FROM inodes WHERE id=?", (inode,))
            else:
                # Also updates ctime
                decrease_refcount(inode, cur)
    
            update_mtime(inode_p, cur)
        except:
            cur.execute("ROLLBACK TO 'Server.unlink'")
            raise
        finally:
            cur.execute("RELEASE 'Server.unlink'")

    def mark_damaged(self):
        """Marks the filesystem as being damaged and needing fsck.
        """

        cur = self.get_cursor()
        cur.execute("UPDATE parameters SET needs_fsck=?", (True,))


    def rmdir(self, path):
        """Handles FUSE rmdir() requests.
        """
        cur = self.get_cursor()

        (inode_p, inode) = get_inodes(path, cur)[-2:]
        
        # Check if directory is empty
        if cur.get_val("SELECT refcount FROM inodes WHERE id=?", (inode,)) > 2: 
            log.debug("Attempted to remove nonempty directory %s", path)
            raise FUSEError(errno.EINVAL)

        # Delete
        cur.execute("SAVEPOINT 'Server.rmdir'")
        try:
            cur.execute("DELETE FROM contents WHERE name=? AND parent_inode=?",
                        (buffer(os.path.basename(path)), inode_p))
            cur.execute("DELETE FROM inodes WHERE id=?", (inode,))
            decrease_refcount(inode_p, cur)
            update_mtime(inode_p, cur)
        except:
            cur.execute("ROLLBACK TO 'Server.rmdir'")
            raise
        finally:
            cur.execute("RELEASE 'Server.rmdir'")

    def symlink(self, name, target):
        """Handles FUSE symlink() requests.
        """

        cur = self.get_cursor()
        (uid, gid) = fuse.fuse_get_context()[:2]
        inode_p = get_inode(os.path.dirname(name), cur)
        cur.execute("SAVEPOINT 'Server.symlink'")
        try:
            mode = (stat.S_IFLNK | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR | 
                     stat.S_IRGRP | stat.S_IWGRP | stat.S_IXGRP | 
                     stat.S_IROTH | stat.S_IWOTH | stat.S_IXOTH)
            cur.execute("INSERT INTO inodes (mode,uid,gid,target,mtime,atime,ctime,refcount) "
                                "VALUES(?, ?, ?, ?, ?, ?, ?, 1)",
                                (mode, uid, gid, buffer(target),
                                 time(), time(), time()))
            cur.execute("INSERT INTO contents(name, inode, parent_inode) VALUES(?,?,?)",
                                (buffer(os.path.basename(name)), self.local.conn.last_insert_rowid(), inode_p))
            update_mtime(inode_p, cur)
        except:
            cur.execute("ROLLBACK TO 'Server.symlink'")
            raise
        finally:
            cur.execute("RELEASE 'Server.symlink'")

    def rename(self, old, new):
        """Handles FUSE rename() requests.
        """

        cur = self.get_cursor()
        inode_p_old = get_inode(os.path.dirname(old), cur)
        inode_p_new = get_inode(os.path.dirname(new), cur)
        
        if not inode_p_new:
            log.warn("rename: path %s does not exist" % new)
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
            log.info("Attempted to hardlink directory %s", target)
            raise FUSEError(errno.EINVAL)

        cur.execute("SAVEPOINT 'Server.link'")
        try:
            cur.execute("INSERT INTO contents (name,inode,parent_inode) VALUES(?,?,?)",
                     (buffer(os.path.basename(source)), fstat["st_ino"], inode_p))
            increase_refcount(fstat["st_ino"], cur)
            update_mtime(inode_p, cur)
        except:
            cur.execute("ROLLBACK TO 'Server.link'")
            raise
        finally:
            cur.execute("RELEASE 'Server.link'")

    def chmod(self, path, mode):
        """Handles FUSE chmod() requests.
        """

        fstat = self.getattr(path)
        if stat.S_IFMT(mode) != stat.S_IFMT(fstat["st_mode"]):
            log.warn("chmod: attempted to change file mode")
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
        (uid, gid) = fuse.fuse_get_context()[:2]
        inode_p = get_inode(os.path.dirname(path), cur)
        cur.execute("SAVEPOINT 'Server.mknod'")
        try:
            cur.execute("INSERT INTO inodes (mtime,ctime,atime,uid,gid,mode,rdev,refcount,size) "
                     "VALUES(?, ?, ?, ?, ?, ?, ?, ?, 0)",
                     (time(), time(), time(), uid, gid, mode, dev, 1))
            cur.execute("INSERT INTO contents(name, inode, parent_inode) VALUES(?,?,?)",
                     (buffer(os.path.basename(path)), cur.last_rowid(), inode_p))
            update_mtime(inode_p, cur)
        except:
            cur.execute("ROLLBACK TO 'Server.mknod'")
            raise
        finally:
            cur.execute("RELEASE 'Server.mknod'")


    def mkdir(self, path, mode):
        """Handles FUSE mkdir() requests.
        """

        # Check mode
        if (stat.S_IFMT(mode) != stat.S_IFDIR and
            stat.S_IFMT(mode) != 0):
            log.warn("mkdir: invalid mode")
            raise FUSEError(errno.EINVAL)

        # Ensure correct mode
        mode = (mode & ~stat.S_IFMT(mode)) | stat.S_IFDIR

        cur = self.get_cursor()
        inode_p = get_inode(os.path.dirname(path), cur)
        (uid, gid) = fuse.fuse_get_context()[:2]
        cur.execute("SAVEPOINT 'Server.mkdir'")
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
            cur.execute("ROLLBACK TO 'Server.mkdir'")
            raise
        finally:
            cur.execute("RELEASE 'Server.mkdir'")

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
        blocks = cur.get_val("SELECT COUNT(id) FROM s3_objects")
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
        log.debug("Starting main event loop...")
        kw["default_permissions"] = True
        kw["use_ino"] = True
        kw["kernel_cache"] = True
        kw["fsname"] = "s3ql"
        FuseAdaptor(self, mountpoint, **kw)
        log.debug("Main event loop terminated.")


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
            log.warn("create: invalid mode")
            raise FUSEError(errno.EINVAL)

        # Ensure correct mode
        mode = (mode & ~stat.S_IFMT(mode)) | stat.S_IFREG

        cur = self.get_cursor()
        (uid, gid) = fuse.fuse_get_context()[:2]
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
        offset_f = self.blocksize * (int(offset / self.blocksize) + 1)
        if offset + length > offset_f:
            length = offset_f - offset

        # Additionally, we don't read beyond the end of the file
        size = cur.get_val("SELECT size FROM inodes WHERE id=?", (inode,))
        if offset + length > size:
            length = size - offset
            
        # Update access time
        if not self.noatime:
            update_atime(inode, cur)

        # Obtain required s3 object
        offset_i = self.blocksize * int(offset / self.blocksize)
        with self.cache.get(inode, offset_i, cur) as fh:
            fh.seek(offset - offset_i, os.SEEK_SET)
            buf = fh.read(length)
            
            if len(buf) == length:
                return buf
            else:
                # If we can't read enough, we have a hole as well
                # (since we already adjusted the length to be within the file size)
                return buf + "\0" * (length - len(buf))

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
        
        offset_i = self.blocksize * int(offset / self.blocksize)
        
        # We write at most one block
        offset_f = offset_i + self.blocksize
        maxwrite = offset_f - offset
        if len(buf) > maxwrite:
            buf = buf[:maxwrite]

        # Obtain required s3 object
        with self.cache.get(inode, offset_i, cur, markdirty=True) as fh:
            fh.seek(offset - offset_i, os.SEEK_SET)
            fh.write(buf)

        # Update file size if changed
        # Fuse does not ensure that we do not get concurrent write requests,
        # so we have to be careful not to undo a size extension made by
        # a concurrent write.
        minsize = offset + len(buf)
        cur.execute("UPDATE inodes SET size=MAX(size,?), ctime=?, mtime=? WHERE id=?",
                    (minsize, time(), time(), inode))
        if cur.changes() == 0:
            # Still update mtime
            update_mtime(inode, cur)
        
        return len(buf)

    def ftruncate(self, path, len, inode):
        """Handles FUSE ftruncate() requests.
        """
        cur = self.get_cursor()

        # Delete all truncated s3 objects
        self.cache.remove(inode, cur, len)

        # Get last object before truncation
        offset_i = self.blocksize * int((len - 1) / self.blocksize)
        with self.cache.get(inode, offset_i, cur, markdirty=True) as fh:
            fh.truncate(len - offset_i)
                
        # Update file size
        cur.execute("UPDATE inodes SET size=?,mtime=?,ctime=? WHERE id=?",
                     (len, time(), time(), inode))
            

    def fsync(self, fdatasync, inode):
        """Handles FUSE fsync() requests.
        """

        # Metadata is always synced automatically, so we ignore
        # fdatasync
        self.cache.flush(inode, self.get_cursor())


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


