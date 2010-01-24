'''
$Id$

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function

import os
import errno 
import stat
import llfuse
import logging
from s3ql.common import (get_path, CTRL_NAME, CTRL_INODE)
import time
from cStringIO import StringIO
import psyco
import struct
from s3ql.multi_lock import MultiLock

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

    :dbcm:        `DBConnectionManager` instance
    :cache:       Holds information about cached s3 objects
    :noatime:     Do not update directory access time
    :encountered_errors: Is set to true if a request handler raised an exception
    :inode_lock:    MultiLock for synchronizing updates to `open_files`
    :open_files: A dict of the currently opened file inodes. The value is another
                dict with keys ``cached_attrs`` and ``open count``

 
    Notes
    -----
    
    Normally, we could update the ctime, mtime and size of a file
    in the _write() method. However, it turns out that this SQL query
    is responsible for 90% of the CPU time when copying large files. 
    Therefore we cache these values instead.
    
    Threads may block when acquiring a Python lock and when trying to
    access the database. To prevent deadlocks, a function must not
    try to acquire any Python lock when it holds a database lock (i.e.,
    is in the middle of a transaction). This has also to be taken
    into account when calling other functions, especially from e.g.
    S3Cache.
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


    def __init__(self, cache, dbcm, noatime=True):
        super(Operations, self).__init__()

        self.dbcm = dbcm
        self.noatime = noatime
        self.cache = cache
        self.encountered_errors = False
        self.open_files = dict()
        
        self.inode_lock = MultiLock()
            
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
        
        # Check cache first
        if inode in self.open_files:
            try:
                return self.open_files[inode]['cached_attrs'].copy()
            except KeyError:
                # File has just been closed, continue with fetching
                pass
            
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
         fstat["st_ctime"]) = self.dbcm.get_row("SELECT mode, refcount, uid, gid, size, id, "
                                                "rdev, atime, mtime, ctime FROM inodes WHERE id=? ",
                                                (inode,))
                
        # Convert to local time
        fstat['st_mtime'] += time.timezone
        fstat['st_atime'] += time.timezone
        fstat['st_ctime'] += time.timezone
        
        if stat.S_ISREG(fstat["st_mode"]):
            # This is the number of 512 blocks allocated for the file
            fstat["st_blocks"] =  fstat['st_size'] // 512 
                                 
        else:
            # For special nodes, return arbitrary values
            fstat["st_size"] = 512
            fstat["st_blocks"] = 1
            
        # Timeout, can effectively be infinite since attribute changes
        # are only triggered by the kernel's own requests
        fstat['attr_timeout'] = 3600
        fstat['entry_timeout'] = 3600

        # We want our blocksize for IO as large as possible to get large
        # write requests
        fstat['st_blksize'] = 128 * 1024
        
        # Our inodes are already unique
        fstat['generation'] = 1
        
        return fstat

    def readlink(self, inode):
        target = self.dbcm.get_val("SELECT target FROM inodes WHERE id=?", (inode,))
    
        if not self.noatime:
            timestamp = time.time() 
            with self.inode_lock(inode):
                if inode in self.open_files:
                    self.open_files[inode]['cached_attrs']['st_atime'] = timestamp
                    self.open_files[inode]['cached_attrs']['st_ctime'] = timestamp
                else:
                    timestamp -= time.timezone
                    self.dbcm.execute("UPDATE inodes SET atime=?, ctime=? WHERE id=?",
                                      (timestamp, timestamp, inode))
            
        return target                           
            
    def opendir(self, inode):
        """Open directory 
        
        `flags` is ignored. Returns the inode as file handle, so it is not
        possible to distinguish between different open() and `create()` calls
        for the same inode.
        """        
       
        return inode

    def check_args(self, args):
        '''Check and/or supplement fuse mount options'''
        
        if llfuse.fuse_version() >= 28:
            log.debug('Using big_writes')
            args.append(b'big_writes')
            args.append('max_write=131072')  
            args.append('no_remote_lock')

    def readdir(self, fh, off):
        
        inode = fh
        with self.dbcm() as conn:   
            if not self.noatime:
                timestamp = time - time.timezone
                conn.execute("UPDATE inodes SET atime=?, ctime=? WHERE id=?",
                                  (timestamp, timestamp, fh))     
               
            # The ResultSet is automatically deleted
            # when yield raises GeneratorExit.
            for (name, inode) in conn.query("SELECT name, inode FROM contents WHERE parent_inode=? "
                                            "LIMIT -1 OFFSET ?", (inode, off)):
                fstat = self.getattr(inode)
                del fstat['attr_timeout']

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
            
            elif name == b'stat.s3ql':
                return self.extstat()
            
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
            
            elif name == 'copy':
                self.copy_tree(*struct.unpack('II', value))
            
            return llfuse.FUSEError(errno.EINVAL)
        
        raise llfuse.FUSEError(llfuse.ENOTSUP)
    
    def copy_tree(self, src_ino, target_ino):
        '''Efficiently copy directory tree'''
        
        # First we have to flush the cache
        self.cache.flush_all()
        
        queue = [ (src_ino, target_ino) ]
        stamp = time.time()
        while queue:
            (src_ino, target_ino) = queue.pop()
            self._copy_tree(src_ino, target_ino, queue)
            
            # Give other threads a chance to access the db
            if time.time() - stamp > 5:
                time.sleep(1)
                stamp = time.time()
        
    def _copy_tree(self, src_ino, target_ino, queue):
        
        with self.dbcm.transaction() as conn:
            for (name_, ino) in conn.query('SELECT name, inode FROM contents WHERE parent_inode=?',
                                          (src_ino,)):
                ino_new = conn.rowid('INSERT INTO inodes SELECT * FROM inodes WHERE id=?',
                                     (ino,))
                conn.execute('INSERT INTO contents (name, inode, parent_inode) VALUES(?, ?, ?)',
                             (name_, ino_new, target_ino))
                
                mode = conn.get_val('SELECT mode FROM inodes WHERE id=?', ino)
                if stat.S_ISDIR(mode):
                    queue.append((ino, ino_new))
                       
                for (s3key, blockno) in conn.query('SELECT s3key, blockno FROM blocks WHERE inode=?',
                                                   (ino,)):
                    conn.execute('INSERT INTO blocks (inode, blockno, s3key) VALUES(?, ?, ?)',
                                 (ino_new, blockno, s3key))
                    
        
    def unlink(self, inode_p, name):
        
        timestamp = time.time() - time.timezone
        attr = self.lookup(inode_p, name) 
        inode = attr['st_ino']
           
        with self.inode_lock(inode):    
            with self.dbcm.transaction() as conn:        
                conn.execute("DELETE FROM contents WHERE name=? AND parent_inode=?",
                             (name, inode_p))
    
                # No more links and not open
                if attr["st_nlink"] == 1 and inode not in self.open_files:
                    remove = True
                elif inode in self.open_files:
                    self.open_files[inode]['cached_attrs']['st_nlink'] -= 1
                    self.open_files[inode]['cached_attrs']['st_ctime'] = time.time()
                    remove = False
                else:
                    conn.execute("UPDATE inodes SET refcount=refcount-1, ctime=? WHERE id=?",
                                 (timestamp, inode))
                    remove = False
    
                conn.execute("UPDATE inodes SET mtime=?, ctime=? WHERE id=?",
                             (timestamp, timestamp, inode_p))
            
            if remove:
                self.cache.remove(inode)
                self.dbcm.execute("DELETE FROM inodes WHERE id=?", (inode,))

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
            timestamp = time.time() - time.timezone
            conn.execute("UPDATE inodes SET mtime=?, ctime=?, refcount=refcount-1 WHERE id=?",
                         (timestamp, timestamp, inode_p))


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
            conn.execute("UPDATE inodes SET mtime=?, ctime=? WHERE id=?",
                         (timestamp, timestamp, inode_p))
            
        return self.getattr_all(inode)


    def rename(self, inode_p_old, name_old, inode_p_new, name_new):  
        if name_new == CTRL_NAME or name_old == CTRL_NAME: 
            with self.dbcm() as conn:
                log.error('Attempted to rename s3ql control file (%s -> %s)',
                          get_path(name_old, inode_p_old, conn),
                          get_path(name_new, inode_p_new, conn))
            raise llfuse.FUSEError(errno.EACCES)

        fstat_old = self.lookup(inode_p_old, name_old)
        
        try:
            fstat_new = self.lookup(inode_p_new, name_new)
        except llfuse.FUSEError as exc:
            if exc.errno != errno.ENOENT:
                raise
            else:
                target_exists = False
        else:
            target_exists = True
            
        if not target_exists:
            if stat.S_ISDIR(fstat_old['st_mode']):
                self._rename_dir(inode_p_old, name_old, inode_p_new, name_new,
                                 fstat_old['st_ino'])
            else:
                self._rename_file(inode_p_old, name_old, inode_p_new, name_new)
        else:
            if stat.S_IFMT(fstat_old['st_mode']) != stat.S_IFMT(fstat_new['st_mode']):
                log.warn('Cannot rename file to directory (or vice versa).')
                raise llfuse.FUSEError(errno.EINVAL) 
            
            if stat.S_ISDIR(fstat_old['st_mode']):
                self._replace_dir(inode_p_old, name_old, inode_p_new, name_new,
                                  fstat_old['st_ino'], fstat_new['st_ino'])
            else:
                self._replace_file(inode_p_old, name_old, inode_p_new, name_new,
                                   fstat_old['st_ino'], fstat_new['st_ino'])
        
        
    def _rename_dir(self, inode_p_old, name_old, inode_p_new, name_new, inode):
        '''Rename a directory'''

        timestamp = time.time() - time.timezone
             
        with self.dbcm.transaction() as conn:
            conn.execute("UPDATE contents SET name=?, parent_inode=? WHERE name=? "
                         "AND parent_inode=?", (name_new, inode_p_new,
                                                name_old, inode_p_old))
            conn.execute('UPDATE contents SET inode=? WHERE name=? AND parent_inode=?',
                         (inode_p_new, b'..', inode))
            conn.execute("UPDATE inodes SET mtime=?, ctime=?, refcount=refcount-1 WHERE id=?",
                                 (timestamp, timestamp, inode_p_old))
            conn.execute("UPDATE inodes SET mtime=?, ctime=?, refcount=refcount+1 WHERE id=?",
                                 (timestamp, timestamp, inode_p_new))
            
    def _rename_file(self, inode_p_old, name_old, inode_p_new, name_new):
        '''Rename a file'''

        timestamp = time.time() - time.timezone
             
        with self.dbcm.transaction() as conn:
            conn.execute("UPDATE contents SET name=?, parent_inode=? WHERE name=? "
                         "AND parent_inode=?", (name_new, inode_p_new,
                                                name_old, inode_p_old))
            conn.execute("UPDATE inodes SET mtime=?, ctime=? WHERE id=?",
                                 (timestamp, timestamp, inode_p_old))
            conn.execute("UPDATE inodes SET mtime=?, ctime=? WHERE id=?",
                                 (timestamp, timestamp, inode_p_new))
            
    def _replace_dir(self, inode_p_old, name_old, inode_p_new, name_new, 
                 inode_old, inode_new):
        '''Replace a directory'''
        
        timestamp = time.time() - time.timezone
             
        with self.dbcm.transaction() as conn:
            if conn.get_val("SELECT COUNT(name) FROM contents WHERE parent_inode=?",
                            (inode_new,)) > 2: 
                log.warn("Attempted to overwrite nonempty directory %s",
                          get_path(name_new, inode_p_new, conn))
                raise llfuse.FUSEError(errno.EINVAL)
                
            # Replace target
            conn.execute("UPDATE contents SET inode=? WHERE name=? AND parent_inode=?",
                        (inode_old, name_new, inode_p_new))
            conn.execute("UPDATE contents SET inode=? WHERE name=? AND parent_inode=?",
                        (inode_p_new, b'..', inode_old))
            
            # Delete old name
            conn.execute('DELETE FROM contents WHERE name=? AND parent_inode=?',
                        (name_old, inode_p_old))
            conn.execute("UPDATE inodes SET refcount=refcount-1, ctime=? WHERE id=?",
                         (timestamp, inode_p_old))
            
            # Delete overwritten directory
            conn.execute("DELETE FROM contents WHERE name=? AND parent_inode=?",
                        (b'.', inode_new))
            conn.execute("DELETE FROM contents WHERE name=? AND parent_inode=?",
                        (b'..', inode_new))
            conn.execute("DELETE FROM inodes WHERE id=?", (inode_new,))
            
            conn.execute("UPDATE inodes SET mtime=?, ctime=? WHERE id=?",
                         (timestamp, timestamp, inode_p_old))
            conn.execute("UPDATE inodes SET mtime=?, ctime=? WHERE id=?",
                         (timestamp, timestamp, inode_p_new))
                    
                            
    def _replace_file(self, inode_p_old, name_old, inode_p_new, name_new, 
                 inode_old, inode_new):
        '''Replace a file'''
        
        timestamp = time.time() - time.timezone
             
        with self.inode_lock(inode_new):      
            with self.dbcm.transaction() as conn:       
                conn.execute("UPDATE contents SET inode=? WHERE name=? AND parent_inode=?",
                            (inode_old, name_new, inode_p_new))
                conn.execute('DELETE FROM contents WHERE name=? AND parent_inode=?',
                            (name_old, inode_p_old))
            
                # We need to get up-to-date information after having
                # started the transaction
                nlink =  conn.get_val('SELECT refcount FROM inodes WHERE id=?', (inode_new,))
                
                # No more links and not open
                if nlink == 1 and inode_new not in self.open_files:
                    remove = True
                elif inode_new in self.open_files:
                    self.open_files[inode_new]['cached_attrs']['st_nlink'] -= 1
                    self.open_files[inode_new]['cached_attrs']['st_ctime'] = time.time()
                    remove = False
                else:
                    conn.execute("UPDATE inodes SET refcount=refcount-1, ctime=? WHERE id=?",
                                 (timestamp, inode_new))
                    remove = False
                    
                        
                conn.execute("UPDATE inodes SET mtime=?, ctime=? WHERE id=?",
                             (timestamp, timestamp, inode_p_old))
                conn.execute("UPDATE inodes SET mtime=?, ctime=? WHERE id=?",
                             (timestamp, timestamp, inode_p_new))
                
            # Must release transaction first
            if remove:
                self.cache.remove(inode_new)
                self.dbcm.execute("DELETE FROM inodes WHERE id=?", (inode_new,))
            

    def link(self, inode, new_inode_p, new_name):
        if new_name == CTRL_NAME or inode == CTRL_INODE:
            with self.dbcm() as conn:
                log.error('Attempted to create s3ql control file at %s',
                          get_path(new_name, new_inode_p, conn))
            raise llfuse.FUSEError(errno.EACCES)

        with self.inode_lock(inode):
            with self.dbcm.transaction() as conn:
                conn.execute("INSERT INTO contents (name,inode,parent_inode) VALUES(?,?,?)",
                         (new_name, inode, new_inode_p))
                  
                if inode in self.open_files:
                    self.open_files[inode]['cached_attrs']['st_nlink'] += 1
                    self.open_files[inode]['cached_attrs']['st_ctime'] = time.time()
                else:
                    conn.execute("UPDATE inodes SET refcount=refcount+1, ctime=? WHERE id=?",
                                 (time.time() - time.timezone, inode))
                        
                conn.execute("UPDATE inodes SET mtime=? WHERE id=?",
                             (time.time() - time.timezone, new_inode_p))
            
        return self.getattr_all(inode)

    def setattr(self, inode, attr):
        """Handles FUSE setattr() requests"""
            
        if 'st_size' in attr:
            len_ = attr['st_size']
            
            # Delete all truncated s3 objects
            blockno = len_ // self.blocksize
            self.cache.remove(inode, blockno + 1)
    
            # Get last object before truncation
            if len != 0:
                with self.cache.get(inode, blockno) as fh:
                    fh.truncate(len_ - self.blocksize * blockno)
                 
           
        with self.inode_lock(inode):   
            # Update metadata in cache if possible
            if inode in self.open_files:
                self.open_files[inode]['cached_attrs'].update(attr)
                return self.open_files[inode]['cached_attrs'].copy()
            
            # Otherwise write metadata update to db
            else:
                attr_old = self.getattr(inode)
                attr_old.update(attr)
                self._setattr(inode, attr_old)
                
                return attr_old

    def _setattr(self, inode, attr):
        """Write changed inode attributes to db"""

        timestamp = time.time() - time.timezone
        self.dbcm.execute("UPDATE inodes SET mode=?, refcount=?, uid=?, gid=?, size=?, "
                          "rdev=?, atime=?, mtime=?, ctime=? WHERE id=?", 
                          [ attr[x] for x in ('st_mode', 'st_nlink', 'st_uid', 'st_gid', 'st_size',
                                              'st_rdev',) ] + 
                          [ attr['st_atime'] -  time.timezone, attr['st_mtime'] -  time.timezone,
                           timestamp, inode ])
    

    def mknod(self, inode_p, name, mode, rdev, ctx):
        if name == CTRL_NAME: 
            with self.dbcm() as conn:
                log.error('Attempted to mknod s3ql control file at %s',
                          get_path(name, inode_p, conn))
            raise llfuse.FUSEError(errno.EACCES)
        
        # Type can be everything except for directory
        if stat.S_ISDIR(mode):
            log("mknod: can't create directory")
            raise llfuse.FUSEError(errno.EINVAL)

        timestamp = time.time() - time.timezone
        with self.dbcm.transaction() as conn:
            inode = conn.rowid('INSERT INTO inodes (mtime,ctime,atime,uid,gid,mode,rdev, '
                               'refcount) VALUES(?, ?, ?, ?, ?, ?, ?, ?)',
                               (timestamp, timestamp, timestamp, ctx.uid, ctx.gid, mode, rdev, 1))
            conn.execute("INSERT INTO contents(name, inode, parent_inode) VALUES(?,?,?)",
                     (name, inode, inode_p))
            conn.execute("UPDATE inodes SET mtime=? WHERE id=?", (timestamp, inode_p))
        
        return self.getattr_all(inode)


    def mkdir(self, inode_p, name, mode, ctx):
        if name == CTRL_NAME: 
            with self.dbcm() as conn:
                log.error('Attempted to mkdir s3ql control file at %s',
                          get_path(name, inode_p, conn))
            raise llfuse.FUSEError(errno.EACCES)

        # Type must be directory or unspecified
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
            conn.execute("UPDATE inodes SET mtime=?, refcount=refcount+1 WHERE id=?",
                         (timestamp, inode_p))
            
        return self.getattr_all(inode)

    def extstat(self):
        '''Return extended file system statistics'''
        
        with self.dbcm() as conn: 
            entries = conn.get_val("SELECT COUNT(rowid) FROM contents")
            blocks = conn.get_val("SELECT COUNT(id) FROM s3_objects")
            inodes = conn.get_val("SELECT COUNT(id) FROM inodes")
            size_1 = conn.get_val('SELECT SUM(size) FROM inodes')
            size_2 = conn.get_val('SELECT SUM(size) FROM s3_objects')

        if not size_1:
            size_1 = 1
        if not size_2:
            size_2 = 1

        return struct.pack('LLLLLLL', entries, blocks, inodes, size_1, size_2, 
                           self.cache.get_bucket_size(),
                           self.dbcm.get_db_size())
            
    def statfs(self):
        stat_ = dict()

        # Get number of blocks & inodes
        with self.dbcm() as conn: 
            blocks = conn.get_val("SELECT COUNT(id) FROM s3_objects")
            inodes = conn.get_val("SELECT COUNT(id) FROM inodes")
            size = conn.get_val('SELECT SUM(size) FROM s3_objects')
            
        if size is None: 
            size = 0
        
        # file system block size,
        # It would be more appropriate to switch f_bsize and f_frsize,
        # but since df and stat ignore f_frsize, this way we can
        # export more information  
        stat_["f_bsize"] =  int(size // blocks) if blocks != 0 else self.blocksize
        stat_['f_frsize'] = self.blocksize     
        
        # size of fs in f_frsize units 
        # (since S3 is unlimited, always return a half-full filesystem,
        # but at least 50 GB)
        if stat_['f_bsize'] != 0:
            total_blocks = int(max(2*blocks, 50 * 1024**3 // stat_['f_bsize']))
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
        with self.inode_lock(inode):
            if inode in self.open_files:
                self.open_files[inode]['open_count'] += 1
            else:
                self.open_files[inode] = { 'open_count': 1, 
                                          'cached_attrs': self.getattr_all(inode) }
                
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
            conn.execute("UPDATE inodes SET mtime=? WHERE id=?",
                         (timestamp, inode_p))
            
        attrs = self.getattr_all(inode)
        with self.inode_lock(inode):
            self.open_files[inode] = { 'open_count': 1, 
                                       'cached_attrs': attrs.copy() }
                
        return (inode, attrs)

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
           
        with self.inode_lock(inode):
            self.open_files[fh]['cached_attrs']['st_atime'] = time.time()
             
        return buf.getvalue()

    def _read(self, inode, offset, length):
        """Reads at the specified position until the end of the block

        This method may return less than `length` bytes if a blocksize
        boundary is encountered. It may also read beyond the end of
        the file, filling the buffer with additional null bytes.
        """

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
        minsize = offset + total
        while buf:
            written = self._write(fh, offset, buf)
            offset += written
            buf = buf[written:]
            
        
        # Update file size if changed
        # Fuse does not ensure that we do not get concurrent write requests,
        # so we have to be careful not to undo a size extension made by
        # a concurrent write.
        timestamp = time.time()
        with self.inode_lock(fh):
            tmp = self.open_files[fh]['cached_attrs']
            tmp['st_size'] = max(tmp['st_size'], minsize)
            tmp['st_mtime'] = timestamp
            tmp['st_atime'] = timestamp
            
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
      
        return len(buf)
            
    def fsync(self, fh, datasync):
        if not datasync:
            self._setattr(fh, self.open_files[fh]['cached_attrs'])
            
        self.cache.flush(fh) 

    def releasedir(self, inode):
        pass
       
    def release(self, fh):  
        
        with self.inode_lock(fh):
            tmp =  self.open_files[fh]
            tmp['open_count'] -= 1
             
            if tmp['open_count'] == 0:
                if tmp['cached_attrs']['st_nlink'] == 0:
                    self.cache.remove(fh)
                    self.dbcm.execute("DELETE FROM inodes WHERE id=?", (fh,))
                else:
                    self._setattr(fh, tmp['cached_attrs'])
                
                # We must delete the cache only after having
                # committed to db to prevent a race condition with
                # getattr_all().  
                del self.open_files[fh]   
                                        
    # Called for close() calls. 
    def flush(self, fh):
        pass
      
    def fsyncdir(self, fh, datasync):
        if not datasync:
            self._setattr(fh,  self.open_files[fh]['cached_attrs'])  
    
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

psyco.bind(Operations)