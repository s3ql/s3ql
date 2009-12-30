'''
$Id$

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import unicode_literals, division, print_function

import os
import errno 
import stat
import llfuse
import logging
from s3ql.common import (decrease_refcount, get_path, update_mtime, 
                         increase_refcount, update_atime, CTRL_NAME, CTRL_INODE)
import time
from cStringIO import StringIO
import threading
#import psyco

__all__ = [ "Server", "RevisionError" ]

# standard logger for this module
log = logging.getLogger("fs")

    
class Operations(llfuse.Operations):
    """FUSE filesystem that stores its data on Amazon S3

    This class implements low-level FUSE operations and is meant to be
    passed to llfuse.init().
    
    The ``access`` method of this class always gives full access, independent
    of file permissions. If the FUSE library is initialized with ``allow_other``
    or ``allow_root``, the ``default_permissions`` option should therefore always
    be pased as well. 
    
    
    Attributes:
    -----------

    :dbcm:        DBConnectionManager instance
    :cache:       Holds information about cached s3 objects
    :noatime:     True if entity access times shouldn't be updated.
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

    
    def handle_exc(self, exc):
        '''Handle exceptions that occured during request processing. 
                
        This method marks the file system as needing fsck and logs the
        error.
        '''

        log.error("Unexpected internal filesystem error.\n"
                  "Filesystem may be corrupted, run fsck.s3ql as soon as possible!\n" 
                  "Please report this bug on http://code.google.com/p/s3ql/.")
        self.mark_damaged()
        self.encountered_errors = True


    def __init__(self, cache, dbcm, noatime=False):
        super(Operations, self).__init__()

        self.dbcm = dbcm
        self.noatime = noatime
        self.cache = cache
        self.encountered_errors = False
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

        
    def lookup(self, parent_inode, name):        
        if name == CTRL_NAME:
            fstat = self.getattr_all(CTRL_INODE)
            # Make sure the control file is only writable by the user
            # who mounted the file system
            fstat["st_uid"] = os.getuid()
            fstat["st_gid"] = os.getgid()
        
        else:
            with self.dbcm() as conn:
                try:
                    inode = conn.get_val("SELECT inode FROM contents WHERE name=? AND parent_inode=?",
                                    (name, parent_inode))
                except KeyError: # not found
                    raise(llfuse.FUSEError(errno.ENOENT))
    
                fstat = self.getattr_all(inode)

        return fstat
    
    def getattr(self, inode):
        fstat = self.getattr_all(inode)
        del fstat['generation']
        del fstat['entry_timeout']
        return fstat
        
    def getattr_all(self, inode):
        '''Get entry attributes for `inode`
        
        This includes the elements of ``struct stat` as well as ``attr_timeout``,
        ``entry_timeout`` and ``generation``
        '''
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
            
        # Timeout
        fstat['attr_timeout'] = 3600
        fstat['entry_timeout'] = 3600
        
        # TODO: Add generation no to db
        fstat['generation'] = 1

        return fstat

    def readlink(self, inode):
        with self.dbcm() as conn:
            target = conn.get_val("SELECT target FROM inodes WHERE id=?", (inode,))
    
            if not self.noatime:
                update_atime(inode, conn)
        return target                           
            
    def opendir(self, inode):
        """Open directory 
        
        `flags` is ignored. Returns the inode as file handle, so it is not
        possible to distinguish between different open() and `create()` calls
        for the same inode.
        """        
        return inode

    def readdir(self, fh, off):
        
        if off > 0:
            return
        
        inode = fh
        with self.dbcm() as conn:        
            if not self.noatime:
                update_atime(inode, conn)
                
            # FIXME: Add suppoprt for `off`
            for (name, inode) in conn.query("SELECT name, inode FROM contents WHERE parent_inode=?",
                                            (inode,)):
                fstat = self.getattr(inode)
                del fstat['attr_timeout']
                
                # FIXME: Are we in trouble if iteration stops before
                # the query is exhausted?
                yield (name, fstat)


    def getxattr(self, inode, name):
        # Handle S3QL commands
        if inode == CTRL_INODE:
            if name == b's3ql_errors?':
                if self.encountered_errors:
                    return b'errors encountered'
                else:
                    return b'no errors'
            elif name == b's3ql_pid?':
                return bytes(os.getpid())
            
            return llfuse.FUSEError(errno.EINVAL)
        
        raise llfuse.FUSEError(llfuse.ENOTSUP)

    def setxattr(self, inode, name, value):
        
        # Handle S3QL commands
        if inode == CTRL_INODE:
            if name == b's3ql_flushcache!':
                # Force all entries out of the cache
                bak = self.cache.maxsize
                try:
                    self.cache.maxsize = 0
                    self.cache.expire()
                finally:
                    self.cache.maxsize = bak
                return
            
            return llfuse.FUSEError(errno.EINVAL)
        
        raise llfuse.FUSEError(llfuse.ENOTSUP)
    
    def unlink(self, inode_p, name):
        """Handles FUSE unlink() requests.

        Implementation depends on the ``hard_remove`` FUSE option
        not being used, otherwise we would have to keep track
        of open file handles to unlinked files.
        """

        with self.dbcm.transaction() as conn:
            attr = self.lookup(inode_p, name) 
            conn.execute("DELETE FROM contents WHERE name=? AND parent_inode=?",
                        (name, inode_p))
    
            # No more links, remove datablocks
            if attr["st_nlink"] == 1:
                self.cache.remove(attr['st_ino'])
                conn.execute("DELETE FROM inodes WHERE id=?", (attr['st_ino'],))
            else:
                # Also updates ctime
                decrease_refcount(attr['st_ino'], conn)
    
            update_mtime(inode_p, conn)


    def mark_damaged(self):
        """Mark the filesystem as being damaged and needing fsck"""

        self.dbcm.execute("UPDATE parameters SET needs_fsck=?", (True,))


    def rmdir(self, inode_p, name):
        """Handles FUSE rmdir() requests.
        """

        with self.dbcm.transaction() as conn:
            # Raises error if directory doesn't exist
            attr = self.lookup(inode_p, name)
            inode = attr['st_ino']
            
            # Check if directory is empty
            if conn.get_val("SELECT COUNT(name) FROM contents WHERE parent_inode=?",
                            (inode,)) > 2: 
                log.debug("Attempted to remove nonempty directory %s",
                          get_path(name, inode_p, conn))
                raise llfuse.FUSEError(errno.EINVAL)
    
            # Delete
            conn.execute("DELETE FROM contents WHERE name=? AND parent_inode=?",
                        (b'.', inode))
            conn.execute("DELETE FROM contents WHERE name=? AND parent_inode=?",
                        (b'..', inode))                        
            conn.execute("DELETE FROM contents WHERE name=? AND parent_inode=?",
                        (name, inode_p))
            conn.execute("DELETE FROM inodes WHERE id=?", (inode,))
            decrease_refcount(inode_p, conn)
            update_mtime(inode_p, conn)


    def symlink(self, inode_p, name, target, ctx):
        if name == CTRL_NAME:
            with self.dbcm() as conn:
                log.error('Attempted to create s3ql control file at %s',
                          get_path(name, inode_p, conn))
            raise llfuse.FUSEError(errno.EACCES)
        
        with self.dbcm.transaction() as conn:
            mode = (stat.S_IFLNK | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR | 
                    stat.S_IRGRP | stat.S_IWGRP | stat.S_IXGRP | 
                    stat.S_IROTH | stat.S_IWOTH | stat.S_IXOTH)
            timestamp = time.time() - time.timezone
            inode = conn.rowid("INSERT INTO inodes (mode,uid,gid,target,mtime,atime,ctime,refcount) "
                        "VALUES(?, ?, ?, ?, ?, ?, ?, ?)",
                        (mode, ctx.uid, ctx.gid, target, timestamp, timestamp, timestamp, 1))
            conn.execute("INSERT INTO contents(name, inode, parent_inode) VALUES(?,?,?)",
                        (name, inode, inode_p))
            update_mtime(inode_p, conn)
            
        return self.getattr_all(inode)


    def rename(self, inode_p_old, name_old, inode_p_new, name_new):  
        if name_new == CTRL_NAME or name_old == CTRL_NAME: 
            with self.dbcm() as conn:
                log.error('Attempted to rename s3ql control file (%s -> %s)',
                          get_path(name_old, inode_p_old, conn),
                          get_path(name_new, inode_p_new, conn))
            raise llfuse.FUSEError(errno.EACCES)

        with self.dbcm.transaction() as conn:
            fstat = self.lookup(inode_p_old, name_old)
            inode = fstat['st_ino']
            
            try:
                inode_repl = conn.get_val("SELECT inode FROM contents WHERE name=? AND parent_inode=?",
                                          (name_new, inode_p_new))
            except KeyError:
                # Target does not exist
                conn.execute("UPDATE contents SET name=?, parent_inode=? WHERE name=? "
                            "AND parent_inode=?", (name_new, inode_p_new,
                                                   name_old, inode_p_old))
                
                if stat.S_ISDIR(fstat['st_mode']):
                    conn.execute('UPDATE contents SET inode=? WHERE name=? AND parent_inode=?',
                                (inode_p_new, b'..', inode))
            
                update_mtime(inode_p_old, conn)
                update_mtime(inode_p_new, conn)
                return
                
            else:
                # Target exists, overwrite
                fstat_repl = self.getattr_all(inode_repl)
                
                # Both directories
                if stat.S_ISDIR(fstat_repl['st_mode']) and stat.S_ISDIR(fstat['st_mode']):
                    if conn.get_val("SELECT COUNT(name) FROM contents WHERE parent_inode=?",
                                    (inode_repl,)) > 2: 
                        log.debug("Attempted to overwrite nonempty directory %s",
                                  get_path(name_new, inode_p_new, conn))
                        raise llfuse.FUSEError(errno.EINVAL)
                
                    # Replace target
                    conn.execute("UPDATE contents SET inode=? WHERE name=? AND parent_inode=?",
                                (inode, name_new, inode_p_new))
                    conn.execute("UPDATE contents SET inode=? WHERE name=? AND parent_inode=?",
                                (inode_p_new, b'..', inode))
                    
                    # Delete old name
                    conn.execute('DELETE FROM contents WHERE name=? AND parent_inode=?',
                                (name_old, inode_p_old))
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
                    raise llfuse.FUSEError(errno.EINVAL) 
                
                # Both files
                else:
                    conn.execute("UPDATE contents SET inode=? WHERE name=? AND parent_inode=?",
                                (inode, name_new, inode_p_new))
                    conn.execute('DELETE FROM contents WHERE name=? AND parent_inode=?',
                                (name_old, inode_p_old))
            
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

    def link(self, inode, new_inode_p, new_name):
        if new_name == CTRL_NAME or inode == CTRL_INODE:
            with self.dbcm() as conn:
                log.error('Attempted to create s3ql control file at %s',
                          get_path(new_name, new_inode_p, conn))
            raise llfuse.FUSEError(errno.EACCES)

        with self.dbcm.transaction() as conn:
            conn.execute("INSERT INTO contents (name,inode,parent_inode) VALUES(?,?,?)",
                     (new_name, inode, new_inode_p))
            increase_refcount(inode, conn)
            update_mtime(new_inode_p, conn)
            
        return self.getattr_all(inode)


    def setattr(self, inode, attr):
        """Handles FUSE setattr() requests.
        """

        with self.dbcm.transaction() as conn:
            timestamp = time.time() - time.timezone
            
            if 'st_mode' in attr:
                mode = attr.pop('st_mode')
                fstat = self.getattr_all(inode)
            
                if stat.S_IFMT(mode) != stat.S_IFMT(fstat["st_mode"]):
                    log.warn("setattr: attempted to change file mode")
                    raise llfuse.FUSEError(errno.EINVAL)

                conn.execute("UPDATE inodes SET mode=?,ctime=? WHERE id=?", 
                             (mode, timestamp, inode))

            if 'st_uid' in attr:
                uid = attr.pop('st_uid')
                conn.execute("UPDATE inodes SET uid=?, ctime=? WHERE id=?",
                             (uid, timestamp, inode))
                
            if 'st_gid' in attr:
                gid = attr.pop('st_gid')
                conn.execute("UPDATE inodes SET gid=?, ctime=? WHERE id=?",
                             (gid, timestamp, inode))

            if 'st_atime' in attr:
                atime = attr.pop('st_atime')
                conn.execute("UPDATE inodes SET atime=?,ctime=? WHERE id=?",
                             (atime - time.timezone, timestamp, inode))

            if 'st_mtime' in attr:
                mtime = attr.pop('st_mtime')
                conn.execute("UPDATE inodes SET mtime=?,ctime=? WHERE id=?",
                             (mtime - time.timezone, timestamp, inode))
            
                with self.writelock:
                    try:
                        st = self.size_cmtime_cache[inode]
                    except KeyError:
                        pass
                    else:
                        self.size_cmtime_cache[inode] = (st[0], timestamp, mtime - time.timezone)

        # We must not hold a database lock when calling s3cache.get
        if 'st_size' in attr:
            size = attr.pop('st_size')
            self._truncate(inode, size)
            
        if len(attr) > 0:
            log.warn("setattr: attempted to change immutable attribute(s): %s" % repr(attr))
            raise llfuse.FUSEError(errno.EINVAL)
        
        return self.getattr(inode)
            

    def mknod(self, inode_p, name, mode, rdev, ctx):
        if name == CTRL_NAME: 
            with self.dbcm() as conn:
                log.error('Attempted to mknod s3ql control file at %s',
                          get_path(name, inode_p, conn))
            raise llfuse.FUSEError(errno.EACCES)
        
        # We create only these types (and no hybrids)
        if not (stat.S_ISCHR(mode) or stat.S_ISBLK(mode) or stat.S_ISFIFO(mode)
                or stat.S_ISSOCK(mode) ):
            log("mknod: invalid mode")
            raise llfuse.FUSEError(errno.EINVAL)

        timestamp = time.time() - time.timezone
        with self.dbcm.transaction() as conn:
            inode = conn.rowid('INSERT INTO inodes (mtime,ctime,atime,uid,gid,mode,rdev, '
                               'refcount) VALUES(?, ?, ?, ?, ?, ?, ?, ?)',
                               (timestamp, timestamp, timestamp, ctx.uid, ctx.gid, mode, rdev, 1))
            conn.execute("INSERT INTO contents(name, inode, parent_inode) VALUES(?,?,?)",
                     (name, inode, inode_p))
            update_mtime(inode_p, conn) 
        
        return self.getattr_all(inode)


    def mkdir(self, inode_p, name, mode, ctx):
        if name == CTRL_NAME: 
            with self.dbcm() as conn:
                log.error('Attempted to mkdir s3ql control file at %s',
                          get_path(name, inode_p, conn))
            raise llfuse.FUSEError(errno.EACCES)

        # FUSE does not pass type information, so we set S_IFDIR manually
        # We still fail if a different type information is received.
        if (stat.S_IFMT(mode) != stat.S_IFDIR and
            stat.S_IFMT(mode) != 0):
            log.warn("mkdir: invalid mode")
            raise llfuse.FUSEError(errno.EINVAL)
        mode = (mode & ~stat.S_IFMT(mode)) | stat.S_IFDIR
        
        timestamp = time.time() - time.timezone
        with self.dbcm.transaction() as conn:
            inode = conn.rowid("INSERT INTO inodes (mtime,atime,ctime,uid,gid,mode,refcount) "
                               "VALUES(?, ?, ?, ?, ?, ?, ?)",
                               (timestamp, timestamp, timestamp, ctx.uid, ctx.gid, mode, 2))
            conn.execute("INSERT INTO contents(name, inode, parent_inode) VALUES(?, ?, ?)",
                         (name, inode, inode_p))
            conn.execute("INSERT INTO contents(name, inode, parent_inode) VALUES(?, ?, ?)",
                         (b'.', inode, inode))
            conn.execute("INSERT INTO contents(name, inode, parent_inode) VALUES(?, ?, ?)",
                         (b'..', inode_p, inode))     
            increase_refcount(inode_p, conn)
            update_mtime(inode_p, conn)
            
        return self.getattr_all(inode)


    def statfs(self):
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
        if stat_['f_bsize'] != 0:
            total_blocks = max(2*blocks, 50 * 1024**3 / stat_['f_bsize'])
        else:
            total_blocks = 2*blocks
            
        stat_["f_blocks"] = total_blocks
        stat_["f_bfree"] = total_blocks - blocks
        stat_["f_bavail"] = total_blocks - blocks # free for non-root
        
        total_inodes = max(2 * inodes, 50000)
        stat_["f_files"] = total_inodes
        stat_["f_ffree"] = total_inodes - inodes
        stat_["f_favail"] = total_inodes - inodes # free for non-root

        return stat_

    def open(self, inode, flags):
        """Open file 
        
        `flags` is ignored. Returns the inode as file handle, so it is not
        possible to distinguish between different open() and `create()` calls
        for the same inode.
        """        
        return inode

     
    def access(self, inode, mode, ctx, get_sup_gids):
        '''Check if requesting process has `mode` rights on `inode`.
        
        This method always returns true, since it should only be called
        when permission checking is disabled (if permission checking is
        enabled, the `default_permissions` FUSE option should be set).
        '''
        # Yeah, could be a function
        #pylint: disable-msg=R0201 
        
        return True
        
    def create(self, inode_p, name, mode, ctx):   
        '''Create a file and open it
                
        `ctx` must be a context object that contains pid, uid and 
        primary gid of the requesting process.

        Returns a tuple of the form ``(fh, attr)``. `fh` is
        integer file handle that is used to identify the open file and
        `attr` is a dict similar to the one returned by `lookup`. The
        file handle is actually equal to the inode of the file,   
        so it is not possible to distinguish between different open() and `create()` calls
        for the same inode.
        '''
            
        if name == CTRL_NAME:
            with self.dbcm() as conn:
                log.error('Attempted to create s3ql control file at %s',
                          get_path(name, inode_p, conn))
            raise llfuse.FUSEError(errno.EACCES)

        # Type has to be regular file or not specified at all
        if (stat.S_IFMT(mode) != stat.S_IFREG and
            stat.S_IFMT(mode) != 0):
            log.warn("create: invalid mode")
            raise llfuse.FUSEError(errno.EINVAL)
        mode = (mode & ~stat.S_IFMT(mode)) | stat.S_IFREG

        timestamp = time.time() - time.timezone
        with self.dbcm.transaction() as conn:
            inode = conn.rowid("INSERT INTO inodes (mtime,ctime,atime,uid,gid,mode, "
                               "rdev,refcount,size) VALUES(?, ?, ?, ?, ?, ?, ?, ?, 0)",
                               (timestamp, timestamp, timestamp, ctx.uid, ctx.gid, mode, None, 1))
            conn.execute("INSERT INTO contents(name, inode, parent_inode) VALUES(?,?,?)",
                        (name, inode, inode_p))
            update_mtime(inode_p, conn)

        return (inode, self.getattr_all(inode))

    def read(self, fh, offset, length):
        '''Read `size` bytes from `fh` at position `off`
        
        Unless EOF is reached, returns exactly `size` bytes. 
        '''
        buf = StringIO()
        inode = fh
        
        # Make sure that we don't read beyond the file size. This
        # should not happen unless direct_io is activated, but it's
        # cheap and nice for testing.
        size = self.getattr_all(inode)['st_size']
        length = min(size - offset, length)
        
        while length > 0:
            tmp = self._read(fh, offset, length)
            buf.write(tmp)
            length -= len(tmp)
            offset += len(tmp)
            
        return buf.getvalue()

    def _read(self, inode, offset, length):
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

    def write(self, fh, offset, buf):
        total = len(buf)
        while len(buf) > 0:
            written = self._write(fh, offset, buf)
            offset += written
            buf = buf[written:]
        return total


    def _write(self, inode, offset, buf):
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

    def _truncate(self, inode, len_):
        """Truncate `inode` to `len_`
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
            

    def fsync(self, fh, datasync):
        # Metadata is always synced automatically, so we ignore
        # fdatasync
        self.cache.flush(fh) # fh is an inode

    def releasedir(self, fh):
        pass

    def release(self, fh):    
        inode = fh
        with self.writelock:
            try:
                st = self.size_cmtime_cache.pop(inode)
            except KeyError:
                pass
            else:
                self.dbcm.execute('UPDATE inodes SET size=?, ctime=?, mtime=? WHERE id=?',
                                  (st[0], st[1], st[2], inode))
                                        
    
    # Called for close() calls. 
    def flush(self, fh):
        pass
      
    def fsyncdir(self, fh, datasync):
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


#psyco.bind(Operations)