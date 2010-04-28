'''
fs.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function

import os
import errno
import stat
import llfuse
import collections
import logging
from .inode_cache import InodeCache
from .common import (get_path, CTRL_NAME, CTRL_INODE, ROOT_INODE)
import time
from cStringIO import StringIO
import struct

__all__ = [ "Server" ]

# standard logger for this module
log = logging.getLogger("fs")


class Operations(llfuse.Operations):
    """A full-featured file system for online data storage

    This class implements low-level FUSE operations and is meant to be
    passed to llfuse.init().
    
    The ``access`` method of this class always gives full access, independent
    of file permissions. If the FUSE library is initialized with ``allow_other``
    or ``allow_root``, the ``default_permissions`` option should therefore always
    be passed as well. 
    
    
    Attributes:
    -----------

    :dbcm:        `DBConnectionManager` instance
    :cache:       Holds information about cached blocks
    :lock:        Global lock to synchronize request processing
    :encountered_errors: Is set to true if a request handler raised an exception
    :inode_cache: A cache for the attributes of the currently opened inodes.

 
    Multithreading
    --------------
    
    This class is not thread safe. Methods must only be called when the caller
    holds the global lock `lock` that is also passed to the constructor. 
    
    However, some handlers do release the global lock while they are running. So
    is nevertheless possible for multiple handlers to run at the same time, as long
    as the concurrency is orchestrated by the instance itself.
    
    Since  threads may block both when (re-)acquiring the global lock and when trying to access the
    database, it is important that these two operations are always carried out in the same order. The
    convention is that if a method needs both a database lock and the global lock, the
    global lock is always acquired first. In other words, no method will ever try to obtain
    the global lock during an active database transaction.
    
 
    Directory Entry Types
    ----------------------
    
    S3QL is quite agnostic when it comes to directory entry types. Every directory entry can contain
    other entries *and* have a associated data, size, link target and device number.
    
    This philosophy only fails for the 'st_nlink' attribute because S3QL does not have a concept of '.'
    and '..' entries (introducing them would add complexity and waste space in the database). This
    problem is handled empirically: for each inode, S3QL manages a correction term `nlink_off`
    that is added to the number of references to that inode to generate the 'st_nlink' attribute.
    `nlink_off` is managed as follows:
    - Inodes created by mkdir() start with `nlink_off=1`
    - Inodes created with mknod(), create() and symlink() calls start with `nlink_off=0`
    - mkdir() increases `nlink_off` for the parent inode by 1
    - rmdir() decreases `nlink_off` for the parent inode by 1
    - rename() of an inode with `nlink_off != 0` decreases `nlink_off` for the old
      and increases it for the new parent inode.
    
    Finally, S3QL makes some provisions for users relying on 
    unlink()/rmdir() to fail for a directory/file. For that, it explicitly
    checks the st_mode attribute.
    """

    def handle_exc(self, exc):
        '''Handle exceptions that occurred during request processing. 
                
        This method marks the file system as needing fsck and logs the
        error.
        '''

        log.error("Unexpected internal filesystem error.\n"
                  "Filesystem may be corrupted, run fsck.s3ql as soon as possible!\n"
                  "Please report this bug on http://code.google.com/p/s3ql/.")
        self.encountered_errors = True


    def __init__(self, dbcm, cache, lock, blocksize):
        super(Operations, self).__init__()

        self.dbcm = dbcm
        self.cache = cache
        self.encountered_errors = False
        self.inodes = InodeCache(dbcm)
        self.lock = lock
        self.open_inodes = collections.defaultdict(lambda: 0)
        self.blocksize = blocksize

        # Make sure the control file is only writable by the user
        # who mounted the file system
        self.inodes[CTRL_INODE].uid = os.getuid()
        self.inodes[CTRL_INODE].gid = os.getgid()
          
    def init(self):
        self.cache.init()
        self.inodes.init()

    def destroy(self):
        self.inodes.close()

    def lookup(self, id_p, name):
        with self.dbcm() as conn:
            if name == CTRL_NAME:
                return self.inodes[CTRL_INODE]

            try:
                id_ = conn.get_val("SELECT inode FROM contents WHERE name=? AND parent_inode=?",
                                   (name, id_p))
            except KeyError:
                raise(llfuse.FUSEError(errno.ENOENT))
            return self.inodes[id_]

    def getattr(self, id_):
        return self.inodes[id_]

    def readlink(self, id_):
        timestamp = time.time()
        inode = self.inodes[id_]
        inode.atime = timestamp
        inode.ctime = timestamp
        return inode.target


    def opendir(self, id_):
        return self.open(id_, None)

    def check_args(self, args):
        '''Check and/or supplement fuse mount options'''

        args.append(b'big_writes')
        args.append('max_write=131072')
        args.append('no_remote_lock')

    def readdir(self, id_, off):
        timestamp = time.time()

        # The inode cache may need to write to the database 
        # while our SELECT query is running
        with self.dbcm.transaction() as conn:
            inode = self.inodes[id_]
            inode.atime = timestamp

            # .
            if off == 0:
                yield ('.', inode)

            # ..
            if off <= 1:
                if id_ == ROOT_INODE:
                    yield ('..', inode)
                else:
                    for (id_p,) in conn.query('SELECT parent_inode FROM contents '
                                              'WHERE inode=?', (id_,)):
                        yield ('..', self.inodes[id_p])

            # The ResultSet is automatically deleted
            # when yield raises GeneratorExit.
            for (name, cid_) in conn.query("SELECT name, inode FROM contents WHERE parent_inode=? "
                                           "LIMIT -1 OFFSET ?", (id_, off - 2)):
                yield (name, self.inodes[cid_])

    def getxattr(self, id_, name):
        # Handle S3QL commands
        if id_ == CTRL_INODE:
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

        else:
            try:
                value = self.dbcm.get_val('SELECT value FROM ext_attributes WHERE inode=? AND name=?',
                                          (id_, name))
            except KeyError:
                raise llfuse.FUSEError(llfuse.ENOATTR)
            return value

    def listxattr(self, id_):
        names = list()
        with self.dbcm() as conn:
            for (name,) in conn.query('SELECT name FROM ext_attributes WHERE inode=?', (id_,)):
                names.append(name)
        return names

    def setxattr(self, id_, name, value):

        # Handle S3QL commands
        if id_ == CTRL_INODE:
            if name == b's3ql_flushcache!':
                # Force all entries out of the cache
                self.lock.release()
                try:
                    self.cache.clear()
                finally:
                    self.lock.acquire()
                return

            elif name == 'copy':
                self.copy_tree(*struct.unpack('II', value))

            return llfuse.FUSEError(errno.EINVAL)
        else:
            self.dbcm.execute('INSERT OR REPLACE INTO ext_attributes (inode, name, value) '
                              'VALUES(?, ?, ?)', (id_, name, value))
            self.inodes[id_].ctime = time.time()

    def removexattr(self, id_, name):
        changes = self.dbcm.execute('DELETE FROM ext_attributes WHERE inode=? AND name=?',
                                    (id_, name))
        if changes == 0:
            raise llfuse.FUSEError(llfuse.ENOATTR)
        self.inodes[id_].ctime = time.time()

    def copy_tree(self, src_id, target_id):
        '''Efficiently copy directory tree'''

        # First we have to flush the cache
        self.cache.flush_all()

        old_target_id = target_id
        queue = [ (src_id, target_id) ]
        id_cache = dict()
        processed = 0
        while queue:
            (src_id, target_id) = queue.pop()
            processed += self._copy_tree(src_id, target_id, queue, id_cache)

            # Give other threads a chance to access the db
            processed += 1
            if processed > 5000:
                self.lock.release()
                time.sleep(0.2)
                self.lock.acquire()
                processed = 0

        llfuse.invalidate_inode(old_target_id)

    def _copy_tree(self, src_id, target_id, queue, id_cache):

        processed = 0
        adj_nlink = self.inodes[target_id].nlink_off != 0
        with self.dbcm.transaction() as conn:
            for (name, id_) in conn.query('SELECT name, inode FROM contents WHERE parent_inode=?',
                                           (src_id,)):

                if id_ not in id_cache:
                    inode = self.inodes[id_]
                    if inode.nlink_off != 0:
                        nlink_off = 1
                        if adj_nlink:
                            self.inodes[target_id].nlink_off += 1
                    else:
                        nlink_off = 0

                    inode_new = self.inodes.create_inode(refcount=1, mode=inode.mode, size=inode.size,
                                                         uid=inode.uid, gid=inode.gid,
                                                         mtime=inode.mtime, atime=inode.atime,
                                                         ctime=inode.ctime, target=inode.target,
                                                         rdev=inode.rdev, nlink_off=nlink_off)
                    id_new = inode_new.id

                    if inode.refcount != 1:
                        id_cache[id_] = id_new

                    for (obj_id, blockno) in conn.query('SELECT obj_id, blockno FROM blocks '
                                                        'WHERE inode=?', (id_,)):
                        conn.execute('INSERT INTO blocks (inode, blockno, obj_id) VALUES(?, ?, ?)',
                                     (id_new, blockno, obj_id))
                        conn.execute('UPDATE objects SET refcount=refcount+1 WHERE id=?', (obj_id,))

                    if conn.has_val('SELECT 1 FROM contents WHERE parent_inode=?', (id_,)):
                        queue.append((id_, id_new))
                else:
                    id_new = id_cache[id_]
                    self.inodes[id_new].refcount += 1

                conn.execute('INSERT INTO contents (name, inode, parent_inode) VALUES(?, ?, ?)',
                             (name, id_new, target_id))
                processed += 1

            return processed

    def unlink(self, id_p, name):
        inode = self.lookup(id_p, name)

        if stat.S_ISDIR(inode.mode):
            raise llfuse.FUSEError(errno.EISDIR)

        self._remove(id_p, name, inode.id)

    def rmdir(self, id_p, name):
        inode = self.lookup(id_p, name)

        if not stat.S_ISDIR(inode.mode):
            raise llfuse.FUSEError(errno.ENOTDIR)

        self._remove(id_p, name, inode.id, adj_nlink_off=True)


    def _remove(self, id_p, name, id_, adj_nlink_off=False):
        '''Remove entry `name`with parent inode `inode_p` 
        
        `attr` must be the result of lookup(inode_p, name).
        
        If `adj_nlink_off` is set, the `nlink_off` attribute of the
        parent directory is decreased by one as well.
        '''

        timestamp = time.time()

        with self.dbcm() as conn:

            # Check that there are no child entries
            if conn.has_val("SELECT 1 FROM contents WHERE parent_inode=?", (id_,)):
                log.debug("Attempted to remove entry with children: %s",
                          get_path(id_p, conn, name))
                raise llfuse.FUSEError(errno.ENOTEMPTY)

            conn.execute("DELETE FROM contents WHERE name=? AND parent_inode=?",
                         (name, id_p))
            inode = self.inodes[id_]
            inode.refcount -= 1
            inode.ctime -= timestamp

            if inode.refcount == 0 and self.open_inodes[id_] == 0:
                self.cache.remove(id_, self.lock, 0, inode.size // self.blocksize + 1)
                # Since the inode is not open, it's not possible that new blocks
                # get created at this point and we can safely delete the inode
                conn.execute('DELETE FROM ext_attributes WHERE inode=?', (id_,))
                del self.inodes[id_]

            inode_p = self.inodes[id_p]
            inode_p.mtime = timestamp
            inode_p.ctime = timestamp
            if adj_nlink_off and inode_p.nlink_off != 0:
                inode_p.nlink_off -= 1

    def symlink(self, id_p, name, target, ctx):
        mode = (stat.S_IFLNK | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR |
                    stat.S_IRGRP | stat.S_IWGRP | stat.S_IXGRP |
                    stat.S_IROTH | stat.S_IWOTH | stat.S_IXOTH)
        return self._create(id_p, name, mode, ctx, target=target)

    def rename(self, id_p_old, name_old, id_p_new, name_new):
        if name_new == CTRL_NAME or name_old == CTRL_NAME:
            with self.dbcm() as conn:
                log.warn('Attempted to rename s3ql control file (%s -> %s)',
                          get_path(id_p_old, conn, name_old),
                          get_path(id_p_new, conn, name_new))
            raise llfuse.FUSEError(errno.EACCES)
        elif name_old in ('.', '..'):
            log.warn('Attempted to rename . or ..')
            raise llfuse.FUSEError(errno.EACCES)
        elif name_new in ('.', '..'):
            log.warn('Attempted to rename . or ..')
            raise llfuse.FUSEError(errno.EACCES)

        inode_old = self.lookup(id_p_old, name_old)

        try:
            inode_new = self.lookup(id_p_new, name_new)
        except llfuse.FUSEError as exc:
            if exc.errno != errno.ENOENT:
                raise
            else:
                target_exists = False
        else:
            target_exists = True

        if target_exists:
            self._replace(id_p_old, name_old, id_p_new, name_new,
                          inode_old.id, inode_new.id)
        else:
            self._rename(id_p_old, name_old, id_p_new, name_new, inode_old.id)


    def _rename(self, id_p_old, name_old, id_p_new, name_new, id_):
        timestamp = time.time()

        with self.dbcm.transaction() as conn:
            conn.execute("UPDATE contents SET name=?, parent_inode=? WHERE name=? "
                         "AND parent_inode=?", (name_new, id_p_new,
                                                name_old, id_p_old))

            inode_p_old = self.inodes[id_p_old]
            inode_p_new = self.inodes[id_p_new]
            inode_p_old.mtime = timestamp
            inode_p_new.mtime = timestamp
            inode_p_old.ctime = timestamp
            inode_p_new.ctime = timestamp

            if self.inodes[id_].nlink_off != 0:
                if inode_p_old.nlink_off != 0:
                    inode_p_old.nlink_off -= 1
                if inode_p_new.nlink_off != 0:
                    inode_p_new.nlink_off += 1

    def _replace(self, id_p_old, name_old, id_p_new, name_new,
                 id_old, id_new):

        timestamp = time.time()

        with self.dbcm() as conn:
            if conn.has_val("SELECT 1 FROM contents WHERE parent_inode=?", (id_new,)):
                log.info("Attempted to overwrite entry with children: %s",
                          get_path(id_p_new, conn, name_new))
                raise llfuse.FUSEError(errno.EINVAL)

            inode_p_old = self.inodes[id_p_old]
            inode_p_new = self.inodes[id_p_new]

            # Replace target
            conn.execute("UPDATE contents SET inode=? WHERE name=? AND parent_inode=?",
                        (id_old, name_new, id_p_new))
            inode_new = self.inodes[id_new]
            inode_old = self.inodes[id_old]
            if inode_p_new.nlink_off != 0:
                if inode_new.nlink_off != 0 and inode_old.nlink_off == 0:
                    inode_p_new.nlink_off -= 1
                elif inode_new.nlink_off == 0 and inode_old.nlink_off != 0:
                    inode_p_new.nlink_off += 1

            # Delete old name
            conn.execute('DELETE FROM contents WHERE name=? AND parent_inode=?',
                        (name_old, id_p_old))
            if inode_old.nlink_off != 0 and inode_p_old.nlink_off != 0:
                inode_p_old.nlink_off -= 1

            inode_new.refcount -= 1
            inode_new.ctime = timestamp

            inode_p_old.ctime = timestamp
            inode_p_old.mtime = timestamp
            inode_p_new.ctime = timestamp
            inode_p_new.mtime = timestamp
            
            if inode_new.refcount == 0 and self.open_inodes[id_new] == 0:
                self.cache.remove(id_new, self.lock, 0, inode_new.size // self.blocksize + 1)
                # Since the inode is not open, it's not possible that new blocks
                # get created at this point and we can safely delete the inode
                conn.execute('DELETE FROM ext_attributes WHERE inode=?', (id_new,))
                del self.inodes[id_new]


    def link(self, id_, new_id_p, new_name):
        if new_name == CTRL_NAME or id_ == CTRL_INODE:
            with self.dbcm() as conn:
                log.error('Attempted to create s3ql control file at %s',
                          get_path(new_id_p, conn, new_name))
            raise llfuse.FUSEError(errno.EACCES)
        elif new_name in ('.', '..'):
            raise llfuse.FUSEError(errno.EEXIST)

        timestamp = time.time()
        with self.dbcm.transaction() as conn:
            conn.execute("INSERT INTO contents (name, inode, parent_inode) VALUES(?,?,?)",
                         (new_name, id_, new_id_p))

            inode = self.inodes[id_]
            inode.refcount += 1
            inode.ctime = timestamp

            inode_p = self.inodes[new_id_p]
            inode_p.ctime = timestamp
            inode_p.mtime = timestamp

            if inode.nlink_off != 0 and inode_p.nlink_off != 0:
                inode_p.nlink_off += 1

        return inode

    def setattr(self, id_, attr):
        """Handles FUSE setattr() requests
        
        This method may release the global lock while it is running.
        """

        inode = self.inodes[id_]
        timestamp = time.time()

        if 'st_size' in attr:
            len_ = attr['st_size']

            # Delete all truncated blocks
            last_block = len_ // self.blocksize
            total_blocks = inode.size // self.blocksize + 1
            self.cache.remove(id_, self.lock, last_block+1, total_blocks)

            # Get last object before truncation
            if len_ != 0:
                with self.cache.get(id_, last_block, self.lock) as fh:
                    fh.truncate(len_ - self.blocksize * last_block)
                    
            # Inode may have expired from cache 
            inode = self.inodes[id_]
            inode.size = len_

        if 'st_mode' in attr:
            inode.mode = attr['st_mode']

        if 'st_uid' in attr:
            inode.uid = attr['st_uid']

        if 'st_gid' in attr:
            inode.gid = attr['st_gid']

        if 'st_rdev' in attr:
            inode.rdev = attr['st_rdev']

        if 'st_atime' in attr:
            inode.atime = attr['st_atime']
            inode.ctime = timestamp

        if 'st_mtime' in attr:
            inode.mtime = attr['st_mtime']
            inode.ctime = timestamp

        if 'st_ctime' in attr:
            inode.ctime = attr['st_ctime']

        return inode

    def mknod(self, id_p, name, mode, rdev, ctx):
        return self._create(id_p, name, mode, ctx, rdev=rdev)

    def mkdir(self, id_p, name, mode, ctx):
        return self._create(id_p, name, mode, ctx, nlink_off=1)

    def extstat(self):
        '''Return extended file system statistics'''

        self.inodes.flush()
        self.lock.release()
        try:
            with self.dbcm() as conn:
                entries = conn.get_val("SELECT COUNT(rowid) FROM contents")
                blocks = conn.get_val("SELECT COUNT(id) FROM objects")
                inodes = conn.get_val("SELECT COUNT(id) FROM inodes")
                size_1 = conn.get_val('SELECT SUM(size) FROM inodes')
                size_2 = conn.get_val('SELECT SUM(size) FROM objects')

            if not size_1:
                size_1 = 1
            if not size_2:
                size_2 = 1

            return struct.pack('QQQQQQQ', entries, blocks, inodes, size_1, size_2,
                               self.cache.get_bucket_size(),
                               self.dbcm.get_db_size())
        finally:
            self.lock.acquire()

    def statfs(self):
        stat_ = dict()

        # Get number of blocks & inodes
        with self.dbcm() as conn:
            blocks = conn.get_val("SELECT COUNT(id) FROM objects")
            inodes = conn.get_val("SELECT COUNT(id) FROM inodes")
            size = conn.get_val('SELECT SUM(size) FROM objects')

        if size is None:
            size = 0

        # file system block size,
        # It would be more appropriate to switch f_bsize and f_frsize,
        # but since df and stat ignore f_frsize, this way we can
        # export more information  
        stat_["f_bsize"] = int(size // blocks) if blocks != 0 else self.blocksize
        stat_['f_frsize'] = self.blocksize

        # size of fs in f_frsize units 
        # (since backend is supposed to be unlimited, always return a half-full filesystem,
        # but at least 50 GB)
        if stat_['f_bsize'] != 0:
            total_blocks = int(max(2 * blocks, 50 * 1024 ** 3 // stat_['f_bsize']))
        else:
            total_blocks = 2 * blocks

        stat_["f_blocks"] = total_blocks
        stat_["f_bfree"] = total_blocks - blocks
        stat_["f_bavail"] = total_blocks - blocks # free for non-root

        total_inodes = max(2 * inodes, 50000)
        stat_["f_files"] = total_inodes
        stat_["f_ffree"] = total_inodes - inodes
        stat_["f_favail"] = total_inodes - inodes # free for non-root

        return stat_

    def open(self, id_, flags):
        self.open_inodes[id_] += 1
        return id_

    def access(self, id_, mode, ctx, get_sup_gids):
        '''Check if requesting process has `mode` rights on `inode`.
        
        This method always returns true, since it should only be called
        when permission checking is disabled (if permission checking is
        enabled, the `default_permissions` FUSE option should be set).
        '''
        # Yeah, could be a function
        #pylint: disable-msg=R0201 

        return True

    def create(self, id_p, name, mode, ctx):
        inode = self._create(id_p, name, mode, ctx)
        self.open_inodes[inode.id] += 1
        return (inode.id, inode)

    def _create(self, id_p, name, mode, ctx, refcount=1, nlink_off=0, rdev=0,
                target=None):
        if name == CTRL_NAME:
            with self.dbcm() as conn:
                log.error('Attempted to create s3ql control file at %s',
                          get_path(id_p, conn, name))
            raise llfuse.FUSEError(errno.EACCES)
        elif name in ('.', '..'):
            raise llfuse.FUSEError(errno.EEXIST)

        timestamp = time.time()
        with self.dbcm.transaction() as conn:
            inode = self.inodes.create_inode(mtime=timestamp, ctime=timestamp, atime=timestamp,
                                             uid=ctx.uid, gid=ctx.gid, mode=mode, refcount=refcount,
                                             nlink_off=nlink_off, rdev=rdev, target=target)
            conn.execute("INSERT INTO contents(name, inode, parent_inode) VALUES(?,?,?)",
                         (name, inode.id, id_p))
            inode_p = self.inodes[id_p]
            inode_p.mtime = timestamp
            inode_p.ctime = timestamp

            if nlink_off != 0 and inode_p.nlink_off != 0:
                inode_p.nlink_off += 1

        return inode


    def read(self, fh, offset, length):
        '''Read `size` bytes from `fh` at position `off`
        
        Unless EOF is reached, returns exactly `size` bytes. 
        
        This method releases the global lock while it is running.
        '''
        buf = StringIO()
        inode = self.inodes[fh]

        # Make sure that we don't read beyond the file size. This
        # should not happen unless direct_io is activated, but it's
        # cheap and nice for testing.
        size = inode.size
        length = min(size - offset, length)

        while length > 0:
            tmp = self._read(fh, offset, length)
            buf.write(tmp)
            length -= len(tmp)
            offset += len(tmp)

        # Inode may have expired from cache 
        inode = self.inodes[fh]
        
        timestamp = time.time()
        inode.atime = timestamp
        inode.ctime = timestamp

        return buf.getvalue()

    def _read(self, id_, offset, length):
        """Reads at the specified position until the end of the block

        This method may return less than `length` bytes if a blocksize
        boundary is encountered. It may also read beyond the end of
        the file, filling the buffer with additional null bytes.
        
        This method releases the global lock while it is running.
        """

        # Calculate required block
        blockno = offset // self.blocksize
        offset_rel = offset - blockno * self.blocksize

        # Don't try to read into the next block
        if offset_rel + length > self.blocksize:
            length = self.blocksize - offset_rel

        with self.cache.get(id_, blockno, self.lock) as fh:
            fh.seek(offset_rel)
            buf = fh.read(length)

            if len(buf) == length:
                return buf
            else:
                # If we can't read enough, add null bytes
                return buf + b"\0" * (length - len(buf))

    def write(self, fh, offset, buf):
        '''Handle FUSE write requests.
        
        This method releases the global lock while it is running.
        '''
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
        inode = self.inodes[fh]
        inode.size = max(inode.size, minsize)
        inode.mtime = timestamp
        inode.ctime = timestamp

        return total


    def _write(self, id_, offset, buf):
        """Write as much as we can.

        May write less bytes than given in `buf`, returns
        the number of bytes written.
        
        This method releases the global lock while it is running.
        """

        # Calculate required block
        blockno = offset // self.blocksize
        offset_rel = offset - blockno * self.blocksize

        # Don't try to write into the next block
        if offset_rel + len(buf) > self.blocksize:
            buf = buf[:self.blocksize - offset_rel]

        with self.cache.get(id_, blockno, self.lock) as fh:
            fh.seek(offset_rel)
            fh.write(buf)

        return len(buf)

    def fsync(self, fh, datasync):
        if not datasync:
            self.inodes.flush_id(fh)

        self.cache.flush(fh)

    def releasedir(self, fh):
        return self.release(fh)

    def release(self, fh):
        self.open_inodes[fh] -= 1

        if self.open_inodes[fh] == 0:
            del self.open_inodes[fh]

            inode = self.inodes[fh]
            if inode.refcount == 0:
                self.cache.remove(inode.id, self.lock, 0, inode.size // self.blocksize + 1)
                # Since the inode is not open, it's not possible that new blocks
                # get created at this point and we can safely delete the in
                del self.inodes[fh]


    # Called for close() calls. 
    def flush(self, fh):
        pass

    def fsyncdir(self, fh, datasync):
        if not datasync:
            self.inodes.flush_id(fh)

