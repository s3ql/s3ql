#
#    Copyright (C) 2008  Nikolaus Rath <Nikolaus@rath.org>
#
#    This program can be distributed under the terms of the GNU LGPL.
#

from __future__ import unicode_literals
import os
import errno
import stat
import fuse
import logging
from s3ql.common import (decrease_refcount, get_inode, update_mtime, get_inodes,
                         increase_refcount, update_atime)
import time
from cStringIO import StringIO
import threading





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
    
    def __init__(self, errno_, fatal=False):
        super(FUSEError, self).__init__()
        self.errno = errno_
        self.fatal = fatal

    def __str__(self):
        return str(self.errno)

    
class Server(object):
    """FUSE filesystem that stores its data on Amazon S3

    Attributes:
    -----------

    :dbcm:        DBConnectionManager instance
    :cache:       Holds information about cached s3 objects
    :noatime:     True if entity access times shouldn't be updated.
    :in_fuse_loop: Is the FUSE main loop running?
    :encountered_errors: Is set to true if a request handler raised an exception
    :size_cmtime_cache: Caches size, ctime and mtime for currently open files
    :writelock:  Lock for synchronizing updates to size_cmtime_cache

    Note: `noatime` does not guarantee that access time will not be
    updated, but only prevents the update where it would adversely
    influence performance.   
    
    Notes
    -----
    
    Normally, we would just update the ctime, mtime and size of a file
    in the _write() method. However, it turns out that this SQL query
    is responsible for 90% of the CPU time when copying large files. This
    is especially grave, because FUSE currently only calls write with
    4k buffers. For that reason we omit updating these attributes
    and store them in the size_cmtime_cache dict instead. This dict
    is has to be taken into account by all other methods that read
    or write the attributes and is flushed when the file is closed.
    """

    
    def __call__(self, op, *a):

        # write() is handled specially, because we don't want to dump
        # out the whole buffer to the logs.
        if op == "write":
            ap = ["<data, len=%ik>" % int(len(a[0]) / 1024)] + [ repr(x) for x in a[1:] ]
        elif op == "readdir":
            ap = [ repr(x) for x in a ]
            ap[0] = "<filler>"
        else:
            ap = [ repr(x) for x in a ]

        # Print request name and parameters
        log_fuse.debug("* %s(%s)", op, ", ".join(ap))

        try:
            return getattr(self, op)(*a)
        except FUSEError as exc:
            if exc.fatal:
                self.mark_damaged()
            # Final error handling is done in fuse.py
            # OSError apparently has to be initialized with a tuple, otherwise
            # errno is not stored correctly
            raise OSError(exc.errno, "")
        except:
            log.error("Unexpected internal filesystem error.\n"
                      "Filesystem may be corrupted, run fsck.s3ql as soon as possible!\n" 
                      "Please report this bug on http://code.google.com/p/s3ql/.",
                      exc_info=True)
            self.mark_damaged()
            self.encountered_errors = True
            raise OSError(errno.EIO)


    def __init__(self, cache, dbcm, noatime=False):
        """Initializes S3QL fs.
        """

        self.dbcm = dbcm
        self.noatime = noatime
        self.cache = cache
        self.encountered_errors = False
        self.in_fuse_loop = False
        self.size_cmtime_cache = dict()
        self.writelock = threading.Lock()
            
        # Check filesystem revision
        log.debug("Reading fs parameters...")
        rev = dbcm.get_val("SELECT version FROM parameters")
        if rev < 1:
            raise RevisionError(rev, 1)

        # Update mount count
        dbcm.execute("UPDATE parameters SET mountcnt = mountcnt + 1")

        # Get blocksize
        self.blocksize = dbcm.get_val("SELECT blocksize FROM parameters")

    def init(self):
        '''Called when fuse has been initialized
        '''
        self.cache.init()
        
        
    def getattr(self, path):
        """Handles FUSE getattr() requests
        
        """
        
        with self.dbcm() as conn:
            try:
                inode = get_inode(path, conn)
            except KeyError: # not found
                raise(FUSEError(errno.ENOENT))

        return self.fgetattr(inode)

    def fgetattr(self, inode):
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
         fstat["st_ctime"]) = self.dbcm.get_row("SELECT mode, refcount, uid, gid, size, id, rdev, "
                                                "atime, mtime, ctime FROM inodes WHERE id=? ",
                                                (inode,))
         
        # Take into account uncommitted changes
        try:
            st = self.size_cmtime_cache[inode]
        except KeyError:
            pass
        else:
            (fstat['st_size'], fstat['st_ctime'], fstat['st_mtime']) = st
        
        # Convert to local time
        fstat['st_mtime'] += time.timezone
        fstat['st_atime'] += time.timezone
        fstat['st_ctime'] += time.timezone
        
        if stat.S_ISREG(fstat["st_mode"]):
            # This is the number of 512 blocks allocated for the file
            fstat["st_blocks"] = int( fstat['st_size'] / 512 )
                                 
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

        with self.dbcm() as conn:
            inode = get_inode(path, conn)
            target = conn.get_val("SELECT target FROM inodes WHERE id=?", (inode,))
    
            if not self.noatime:
                update_atime(inode, conn)
        return target                           
            
    def opendir(self, path):
        """Returns a numerical file handle."""
        
        with self.dbcm() as conn:
            return get_inode(path, conn)

    def readdir(self, filler, offset, inode):
        """Handles FUSE readdir() requests
        """
        # We don't support offset (yet)
        #pylint: disable-msg=W0613

        with self.dbcm() as conn:        
            if not self.noatime:
                update_atime(inode, conn)
                
            for (name, inode) in conn.query("SELECT name, inode FROM contents WHERE parent_inode=?",
                                            (inode,)):
                filler(name, self.fgetattr(inode), 0)

    def getxattr(self, path, name, position=0):
        # Yes, could be a function
        #pylint: disable-msg=R0201
        raise FUSEError(fuse.ENOTSUP)


    def removexattr(self, path, name):
        # Yes, could be a function
        #pylint: disable-msg=R0201
        raise FUSEError(fuse.ENOTSUP)


    def setxattr(self, path, name, value, options, position=0):
        # Yes, could be a function
        #pylint: disable-msg=R0201
        raise FUSEError(fuse.ENOTSUP)


    def unlink(self, path):
        """Handles FUSE unlink() requests.

        Implementation depends on the ``hard_remove`` FUSE option
        not being used, otherwise we would have to keep track
        of open file handles to unlinked files.
        """

        with self.dbcm() as conn:
            (inode_p, inode) = get_inodes(path, conn)[-2:]
            fstat = self.fgetattr(inode)

            with conn.transaction():
                conn.execute("DELETE FROM contents WHERE name=? AND parent_inode=?",
                            (os.path.basename(path), inode_p))
        
                # No more links, remove datablocks
                if fstat["st_nlink"] == 1:
                    self.cache.remove(inode)                
                    conn.execute("DELETE FROM inodes WHERE id=?", (inode,))
                else:
                    # Also updates ctime
                    decrease_refcount(inode, conn)
        
                update_mtime(inode_p, conn)


    def mark_damaged(self):
        """Marks the filesystem as being damaged and needing fsck.
        """

        self.dbcm.execute("UPDATE parameters SET needs_fsck=?", (True,))


    def rmdir(self, path):
        """Handles FUSE rmdir() requests.
        """

        with self.dbcm() as conn:
            (inode_p, inode) = get_inodes(path, conn)[-2:]
            
            # Check if directory is empty
            if conn.get_val("SELECT COUNT(name) FROM contents WHERE parent_inode=?",
                            (inode,)) > 2: 
                log.debug("Attempted to remove nonempty directory %s", path)
                raise FUSEError(errno.EINVAL)
    
            # Delete
            with conn.transaction():
                conn.execute("DELETE FROM contents WHERE name=? AND parent_inode=?",
                            (b'.', inode))
                conn.execute("DELETE FROM contents WHERE name=? AND parent_inode=?",
                            (b'..', inode))                        
                conn.execute("DELETE FROM contents WHERE name=? AND parent_inode=?",
                            (os.path.basename(path), inode_p))
                conn.execute("DELETE FROM inodes WHERE id=?", (inode,))
                decrease_refcount(inode_p, conn)
                update_mtime(inode_p, conn)


    def symlink(self, name, target):
        """Handles FUSE symlink() requests.
        """

        (uid, gid) = self.get_uid_pid()
        
        with self.dbcm() as conn:
            inode_p = get_inode(os.path.dirname(name), conn)
            with conn.transaction():
                mode = (stat.S_IFLNK | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR | 
                        stat.S_IRGRP | stat.S_IWGRP | stat.S_IXGRP | 
                        stat.S_IROTH | stat.S_IWOTH | stat.S_IXOTH)
                timestamp = time.time() - time.timezone
                inode = conn.rowid("INSERT INTO inodes (mode,uid,gid,target,mtime,atime,ctime,refcount) "
                            "VALUES(?, ?, ?, ?, ?, ?, ?, ?)",
                            (mode, uid, gid, target, timestamp, timestamp, timestamp, 1))
                conn.execute("INSERT INTO contents(name, inode, parent_inode) VALUES(?,?,?)",
                            (os.path.basename(name), inode, inode_p))
                update_mtime(inode_p, conn)


    def rename(self, old, new):
        """Handles FUSE rename() requests.
        """

        with self.dbcm() as conn:
    
            inode = get_inode(old, conn)
            fstat = self.fgetattr(inode)
            inode_p_old = get_inode(os.path.dirname(old), conn)
            inode_p_new = get_inode(os.path.dirname(new), conn)
            
            try:
                inode_repl = get_inode(new, conn)
            except KeyError:
                # Target does not exist
                conn.execute("UPDATE contents SET name=?, parent_inode=? WHERE name=? "
                            "AND parent_inode=?", (os.path.basename(new), inode_p_new,
                                                   os.path.basename(old), inode_p_old))
                
                if stat.S_ISDIR(fstat['st_mode']):
                    conn.execute('UPDATE contents SET inode=? WHERE name=? AND parent_inode=?',
                                (inode_p_new, b'..', inode))
            
                update_mtime(inode_p_old, conn)
                update_mtime(inode_p_new, conn)
                return
                
            else:
                # Target exists, overwrite
                fstat_repl = self.fgetattr(inode_repl)
                
                # Both directories
                if stat.S_ISDIR(fstat_repl['st_mode']) and stat.S_ISDIR(fstat['st_mode']):
                    with conn.transaction():
                        if conn.get_val("SELECT COUNT(name) FROM contents WHERE parent_inode=?",
                                        (inode_repl,)) > 2: 
                            log.debug("Attempted to overwrite nonempty directory %s", new)
                            raise FUSEError(errno.EINVAL)
                    
                        # Replace target
                        conn.execute("UPDATE contents SET inode=? WHERE name=? AND parent_inode=?",
                                    (inode, os.path.basename(new), inode_p_new))
                        conn.execute("UPDATE contents SET inode=? WHERE name=? AND parent_inode=?",
                                    (inode_p_new, b'..', inode))
                        
                        # Delete old name
                        conn.execute('DELETE FROM contents WHERE name=? AND parent_inode=?',
                                    (os.path.basename(old), inode_p_old))
                        decrease_refcount(inode_p_old, conn)
                        
                        # Delete overwritten directory
                        conn.execute("DELETE FROM contents WHERE name=? AND parent_inode=?",
                                    (b'.', inode_repl))
                        conn.execute("DELETE FROM contents WHERE name=? AND parent_inode=?",
                                    (b'..', inode_repl))
                        conn.execute("DELETE FROM inodes WHERE id=?", (inode_repl,))
                        
                        update_mtime(inode_p_old, conn)
                        update_mtime(inode_p_new, conn)
                        
                    return
                            
                # Only one is a directory
                elif stat.S_ISDIR(fstat['st_mode']) or stat.S_ISDIR(fstat_repl['st_mode']):
                    log.debug('Cannot rename file to directory (or vice versa).')
                    raise FUSEError(errno.EINVAL) 
                
                # Both files
                else:
                    with conn.transaction():
                        conn.execute("UPDATE contents SET inode=? WHERE name=? AND parent_inode=?",
                                    (inode, os.path.basename(new), inode_p_new))
                        conn.execute('DELETE FROM contents WHERE name=? AND parent_inode=?',
                                    (os.path.basename(old), inode_p_old))
                
                        # No more links, remove datablocks
                        if fstat_repl["st_nlink"] == 1:
                            self.cache.remove(inode_repl)                
                            conn.execute("DELETE FROM inodes WHERE id=?", (inode_repl,))
                        else:
                            # Also updates ctime
                            decrease_refcount(inode_repl, conn)
            
                    update_mtime(inode_p_old, conn)
                    update_mtime(inode_p_new, conn)
                    return

    def link(self, source, target):
        """Handles FUSE link() requests.
        """

        with self.dbcm() as conn:
            fstat = self.getattr(target)
            inode_p = get_inode(os.path.dirname(source), conn)
    
            with conn.transaction():
                conn.execute("INSERT INTO contents (name,inode,parent_inode) VALUES(?,?,?)",
                         (os.path.basename(source), fstat["st_ino"], inode_p))
                increase_refcount(fstat["st_ino"], conn)
                update_mtime(inode_p, conn)


    def chmod(self, path, mode):
        """Handles FUSE chmod() requests.
        """

        fstat = self.getattr(path)
        if stat.S_IFMT(mode) != stat.S_IFMT(fstat["st_mode"]):
            log.warn("chmod: attempted to change file mode")
            raise FUSEError(errno.EINVAL)

        self.dbcm.execute("UPDATE inodes SET mode=?,ctime=? WHERE id=?", 
                        (mode, time.time() - time.timezone, fstat["st_ino"]))

    def chown(self, path, user, group):
        """Handles FUSE chown() requests.
        """

        with self.dbcm() as conn:
            inode = get_inode(path, conn)
            conn.execute("UPDATE inodes SET uid=?, gid=?, ctime=? WHERE id=?",
                         (user, group, time.time() - time.timezone, inode))

    def mknod(self, path, mode, dev=None):
        """Handles FUSE mknod() requests.
        """

        # We create only these types (and no hybrids)
        if not (stat.S_ISCHR(mode) or stat.S_ISBLK(mode) or stat.S_ISFIFO(mode)
                or stat.S_ISSOCK(mode) ):
            log("mknod: invalid mode")
            raise FUSEError(errno.EINVAL)

        (uid, gid) = self.get_uid_pid()
        with self.dbcm() as conn:
            inode_p = get_inode(os.path.dirname(path), conn)
            with conn.transaction():
                timestamp = time.time() - time.timezone
                inode = conn.rowid('INSERT INTO inodes (mtime,ctime,atime,uid,gid,mode,rdev, '
                                   'refcount) VALUES(?, ?, ?, ?, ?, ?, ?, ?)',
                                   (timestamp, timestamp, timestamp, uid, gid, mode, dev, 1))
                conn.execute("INSERT INTO contents(name, inode, parent_inode) VALUES(?,?,?)",
                         (os.path.basename(path), inode, inode_p))
                update_mtime(inode_p, conn)


    def mkdir(self, path, mode):
        """Handles FUSE mkdir() requests.
        """

        # FUSE does not pass type information, so we set S_IFDIR manually
        # We still fail if a different type information is received.
        if (stat.S_IFMT(mode) != stat.S_IFDIR and
            stat.S_IFMT(mode) != 0):
            log.warn("mkdir: invalid mode")
            raise FUSEError(errno.EINVAL)
        mode = (mode & ~stat.S_IFMT(mode)) | stat.S_IFDIR

        with self.dbcm() as conn:
            inode_p = get_inode(os.path.dirname(path), conn)
            (uid, gid) = self.get_uid_pid()
            with conn.transaction():
                timestamp = time.time() - time.timezone
                inode = conn.rowid("INSERT INTO inodes (mtime,atime,ctime,uid,gid,mode,refcount) "
                                   "VALUES(?, ?, ?, ?, ?, ?, ?)",
                                   (timestamp, timestamp, timestamp, uid, gid, mode, 2))
                conn.execute("INSERT INTO contents(name, inode, parent_inode) VALUES(?, ?, ?)",
                             (os.path.basename(path), inode, inode_p))
                conn.execute("INSERT INTO contents(name, inode, parent_inode) VALUES(?, ?, ?)",
                             (b'.', inode, inode))
                conn.execute("INSERT INTO contents(name, inode, parent_inode) VALUES(?, ?, ?)",
                             (b'..', inode_p, inode))     
                increase_refcount(inode_p, conn)
                update_mtime(inode_p, conn)


    def utimens(self, path, times=None):
        """Handles FUSE utime() requests.
        """

        
        timestamp = time.time() - time.timezone
        if times is None:
            (atime, mtime) = time.time()
        else:
            (atime, mtime) = times
            
        with self.dbcm() as conn:
            inode = get_inode(path, conn)
            conn.execute("UPDATE inodes SET atime=?,mtime=?,ctime=? WHERE id=?",
                         (atime - time.timezone, mtime - time.timezone, 
                          timestamp, inode))
            
        with self.writelock:
            try:
                st = self.size_cmtime_cache[inode]
            except KeyError:
                pass
            else:
                self.size_cmtime_cache[inode] = (st[0], timestamp, mtime - time.timezone)

    def statfs(self, path):
        """Handles FUSE statfs() requests.
        """
        # Result is independent of path
        #pylint: disable-msg=W0613

        stat_ = dict()

        # Get number of blocks & inodes
        with self.dbcm() as conn: 
            blocks = conn.get_val("SELECT COUNT(key) FROM s3_objects")
            inodes = conn.get_val("SELECT COUNT(id) FROM inodes")
            size = conn.get_val('SELECT SUM(size) FROM s3_objects')
            
        if size is None: 
            size = 0
        
        # file system block size,
        # It would be more appropriate to switch f_bsize and f_frsize,
        # but since df and stat ignore f_frsize, this way we can
        # export more information  
        stat_["f_bsize"] = int( size / blocks ) if blocks != 0 else self.blocksize
        stat_['f_frsize'] = self.blocksize     
        
        # size of fs in f_frsize units 
        # (since S3 is unlimited, always return a half-full filesystem,
        # but at least 50 GB)
        total_blocks = max(2*blocks, 50 * 1024**3 / stat_['f_bsize'])
        stat_["f_blocks"] = total_blocks
        stat_["f_bfree"] = total_blocks - blocks
        stat_["f_bavail"] = total_blocks - blocks # free for non-root
        
        total_inodes = max(2 * inodes, 50000)
        stat_["f_files"] = total_inodes
        stat_["f_ffree"] = total_inodes - inodes
        stat_["f_favail"] = total_inodes - inodes # free for non-root

        return stat_


    def truncate(self, bpath, len_):
        """Handles FUSE truncate() requests.
        """
        # We have different arguments from base class since we overwrote the
        # truncate call in FuseAdaptor
        #pylint: disable-msg=W0221
        
        # TODO: We should only call release() if this is really
        # the last open fd to this file.
        fh = self.open(bpath, os.O_WRONLY)
        self.ftruncate(len_, fh)
        self.flush(fh)
        self.release(fh)

    def main(self, mountpoint, **kw):
        """Starts the main loop handling FUSE requests.
        
        Returns False if any errors occonned in the main loop.
        Note that we cannot throw an exception, because the request handler
        are called from within C code.
        """

        # Start main event loop
        log.debug("Starting main event loop...")
        kw[b"default_permissions"] = True
        kw[b"use_ino"] = True
        kw[b"kernel_cache"] = True
        kw[b"fsname"] = "s3ql"
        self.encountered_errors = False
        self.in_fuse_loop = True
        try:
            fuse.FUSE(self, mountpoint, **kw)
        finally:
            self.in_fuse_loop = False
        log.debug("Main event loop terminated.")

        return not self.encountered_errors

    def open(self, path, flags):
        """Opens file `path`.
        
        `flags` is ignored. Returns a file descriptor that is equal to the
        inode of the file, so it is not possible to distinguish between
        different open() and `create()` calls for the same inode.
        """
        # We don't use open flags for anything
        #pylint: disable-msg=W0613
        
        with self.dbcm() as conn:
            return get_inode(path, conn)

    def get_uid_pid(self):
        '''If running in FUSE loop, return uid and pid of requesting process.
        
        Otherwise return (0,0).
        '''
        
        if self.in_fuse_loop:
            return fuse.fuse_get_context()[:2]
        else: 
            return (0, 0)
        
        
    def create(self, path, mode, fi=None):
        """Creates file `path` with mode `mode`
        
        Returns a file descriptor that is equal to the
        inode of the file, so it is not possible to distinguish between
        different open() and create() calls for the same inode.
        """
        assert fi is None

        # Type has to be regular file or not specified at all
        if (stat.S_IFMT(mode) != stat.S_IFREG and
            stat.S_IFMT(mode) != 0):
            log.warn("create: invalid mode")
            raise FUSEError(errno.EINVAL)
        mode = (mode & ~stat.S_IFMT(mode)) | stat.S_IFREG

        (uid, gid) = self.get_uid_pid()
        dirname = os.path.dirname(path)
        name = os.path.basename(path)
        with self.dbcm() as conn:
            inode_p = get_inode(dirname, conn)
            with conn.transaction():
                timestamp = time.time() - time.timezone
                inode = conn.rowid("INSERT INTO inodes (mtime,ctime,atime,uid,gid,mode, "
                                   "rdev,refcount,size) VALUES(?, ?, ?, ?, ?, ?, ?, ?, 0)",
                                   (timestamp, timestamp, timestamp, uid, gid, mode, None, 1))
                conn.execute("INSERT INTO contents(name, inode, parent_inode) VALUES(?,?,?)",
                            (name, inode, inode_p))
                update_mtime(inode_p, conn)

        return inode

    def read(self, length, offset, inode):
        '''Handles fuse read() requests.
        '''
        buf = StringIO()
        
        # Make sure that we don't read beyond the file size. This
        # should not happen unless direct_io is activated, but it's
        # cheap and nice for testing.
        size = self.fgetattr(inode)['st_size']
        length = min(size - offset, length)
        
        while length > 0:
            tmp = self._read(length, offset, inode)
            buf.write(tmp)
            length -= len(tmp)
            offset += len(tmp)
            
        return buf.getvalue()

    def _read(self, length, offset, inode):
        """Reads at the specified position until the end of the block

        This method may return less than `length` bytes if a blocksize
        boundary is encountered. It may also read beyond the end of
        the file, filling the buffer with additional null bytes.
        """
                
        # Update access time
        if not self.noatime:
            with self.dbcm() as conn:
                update_atime(inode, conn)

        # Calculate required block
        blockno = offset // self.blocksize
        offset_rel = offset - blockno * self.blocksize
        
        # Don't try to read into the next block
        if offset_rel + length > self.blocksize:
            length = self.blocksize - offset_rel
         
        with self.cache.get(inode, blockno) as fh:
            fh.seek(offset_rel)
            buf = fh.read(length)
            
            if len(buf) == length:
                return buf
            else:
                # If we can't read enough, add nullbytes
                return buf + b"\0" * (length - len(buf))

    def write(self, buf, offset, inode):
        """Handles FUSE write() requests.
     
        """

        total = len(buf)
        while len(buf) > 0:
            written = self._write(buf, offset, inode)
            offset += written
            buf = buf[written:]
        return total


    def _write(self, buf, offset, inode):
        """Write as much as we can.

        May write less bytes than given in `buf`, returns
        the number of bytes written.
        """
        
        # Calculate required block
        blockno = offset // self.blocksize
        offset_rel = offset - blockno * self.blocksize
        
        # Don't try to write into the next block
        if offset_rel + len(buf) > self.blocksize:
            buf = buf[:self.blocksize - offset_rel]

        with self.cache.get(inode, blockno) as fh:
            fh.seek(offset_rel)
            fh.write(buf)

        # Update file size if changed
        # Fuse does not ensure that we do not get concurrent write requests,
        # so we have to be careful not to undo a size extension made by
        # a concurrent write.
        minsize = offset + len(buf)
        timestamp = time.time() - time.timezone
        
        with self.writelock:
            try:
                st = self.size_cmtime_cache[inode]
            except KeyError:
                st = (self.dbcm.get_val('SELECT size FROM inodes WHERE id=?', (inode,)),
                      timestamp)
                
            if minsize > st[0]:
                self.size_cmtime_cache[inode] = (minsize, timestamp, timestamp)
            else:
                self.size_cmtime_cache[inode] = (st[0], st[1], timestamp)
      
        return len(buf)

    def ftruncate(self, len_, inode):
        """Handles FUSE ftruncate() requests.
        """

        # Delete all truncated s3 objects
        blockno = len_ // self.blocksize
        self.cache.remove(inode, blockno + 1)

        # Get last object before truncation
        if len != 0:
            with self.cache.get(inode, blockno) as fh:
                fh.truncate(len_ - self.blocksize * blockno)
                
        # Update file size
        timestamp = time.time() - time.timezone
        self.dbcm.execute("UPDATE inodes SET size=?,mtime=?,ctime=? WHERE id=?",
                          (len_, timestamp, timestamp, inode))
        

        self.size_cmtime_cache[inode] = (len_, timestamp, timestamp)
            

    def fsync(self, fdatasync, inode):
        """Handles FUSE fsync() requests.
        """
        # Metadata is always synced automatically, so we ignore
        # fdatasync
        #pylint: disable-msg=W0613
        self.cache.flush(inode)

    def releasedir(self, inode):
        pass

    def release(self, inode):    
        with self.writelock:
            try:
                st = self.size_cmtime_cache.pop(inode)
            except KeyError:
                pass
            else:
                self.dbcm.execute('UPDATE inodes SET size=?, ctime=?, mtime=? WHERE id=?',
                                  (st[0], st[1], st[2], inode))
                                        
    
    # Called for close() calls. 
    def flush(self, inode):
        """Handles FUSE flush() requests.
        """
        pass
      
    def bmap(self, path, blocksize, idx):
        # Yes, could be a function
        #pylint: disable-msg=R0201
        raise FUSEError(fuse.ENOTSUP)
    
    def fsyncdir(self, datasync, fip):
        pass
        

class RevisionError(Exception):
    """Raised if the filesystem revision is too new for the program
    """
    def __init__(self, args):
        super(RevisionError, self).__init__()
        self.rev_is = args[0]
        self.rev_should = args[1]

    def __str__(self):
        return "Filesystem has revision %d, filesystem tools can only handle " \
            "revisions up %d" % (self.rev_is, self.rev_should)


