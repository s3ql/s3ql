'''
fs.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function, absolute_import

import os
import errno
import stat
import llfuse
import collections
import logging
from .inode_cache import InodeCache, OutOfInodesError
from .common import (get_path, CTRL_NAME, CTRL_INODE, without,
                     EmbeddedException, ExceptionStoringThread)
import time
from .block_cache import BlockCache
from cStringIO import StringIO
from .database import NoSuchRowError
from .backends.common import NoSuchObject, ChecksumError
import struct
import cPickle as pickle
import threading
from llfuse.interface import FUSEError
from global_lock import lock

__all__ = [ "Server" ]

# standard logger for this module
log = logging.getLogger("fs")

class Operations(llfuse.Operations):
    """A full-featured file system for online data storage

    This class implements low-level FUSE operations and is meant to be passed to
    llfuse.init().
    
    The ``access`` method of this class always gives full access, independent of
    file permissions. If the FUSE library is initialized with ``allow_other`` or
    ``allow_root``, the ``default_permissions`` option should therefore always
    be passed as well.
    
    
    Attributes:
    -----------

    :cache:       Holds information about cached blocks
    :encountered_errors: Is set to true if a request handler raised an exception
    :inode_cache: A cache for the attributes of the currently opened inodes.
    :open_inodes: dict of currently opened inodes. This is used to not remove
                  the blocks of unlinked inodes that are still open.
 
    Multithreading
    --------------
    
    This class uses the `global_lock` module. All methods are reentrant
    and may release the global lock while they are running.
    
 
    Directory Entry Types
    ----------------------
    
    S3QL is quite agnostic when it comes to directory entry types. Every
    directory entry can contain other entries *and* have a associated data,
    size, link target and device number. However, S3QL makes some provisions for
    users relying on unlink()/rmdir() to fail for a directory/file. For that, it
    explicitly checks the st_mode attribute.
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


    def __init__(self, bucket, db, cachedir, blocksize, cache_size,
                 cache_entries=768):
        super(Operations, self).__init__()

        self.encountered_errors = False
        self.inodes = InodeCache(db)
        self.db = db
        self.inode_flush_thread = None
        self.open_inodes = collections.defaultdict(lambda: 0)
        self.blocksize = blocksize
        self.cache = BlockCache(bucket, db, cachedir, cache_size, cache_entries)

        # Make sure the control file is only writable by the user
        # who mounted the file system
        self.inodes[CTRL_INODE].uid = os.getuid()
        self.inodes[CTRL_INODE].gid = os.getgid()

    def init(self):
        self.cache.init()
        self.inode_flush_thread = InodeFlushThread(self.inodes)
        self.inode_flush_thread.start()

    def destroy(self):
        try:
            self.inode_flush_thread.stop()
        except EmbeddedException:
            log.error('FlushThread terminated with exception.')
            self.encountered_errors = True
            
        self.inodes.destroy()
        self.cache.destroy()
        
        if self.cache.encountered_errors:
            self.encountered_errors = True

    def lookup(self, id_p, name):
        if name == CTRL_NAME:
            return self.inodes[CTRL_INODE]

        if name == '.':
            return self.inodes[id_p]

        if name == '..':
            id_ = self.db.get_val("SELECT parent_inode FROM contents WHERE inode=?",
                                  (id_p,))
            return self.inodes[id_]

        try:
            id_ = self.db.get_val("SELECT inode FROM contents WHERE name=? AND parent_inode=?",
                                  (name, id_p))
        except NoSuchRowError:
            raise(llfuse.FUSEError(errno.ENOENT))
        return self.inodes[id_]

    def getattr(self, id_):
        try:
            return self.inodes[id_]
        except KeyError:
            # It is possible to get getattr() for an inode that
            # has just been unlinked()
            raise FUSEError(errno.ENOENT)

    def readlink(self, id_):
        timestamp = time.time()
        inode = self.inodes[id_]
        if inode.atime < inode.ctime or inode.atime < inode.mtime:
            inode.atime = timestamp 
        return inode.target

    def opendir(self, id_):
        return id_

    def check_args(self, args):
        '''Check and/or supplement fuse mount options'''

        args.append(b'big_writes')
        args.append('max_write=131072')
        args.append('no_remote_lock')

    def readdir(self, id_, off):
        if off == 0:
            off = -1
            
        inode = self.inodes[id_]
        if inode.atime < inode.ctime or inode.atime < inode.mtime:
            inode.atime = time.time() 

        # The ResultSet is automatically deleted
        # when yield raises GeneratorExit.  
        res = self.db.query("SELECT rowid, name, inode FROM contents WHERE parent_inode=? "
                         'AND rowid > ? ORDER BY rowid', (id_, off))
        for (next_, name, cid_) in res:
            yield (name, self.inodes[cid_], next_)

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

            elif name == b's3qlstat':
                return self.extstat()

            raise llfuse.FUSEError(errno.EINVAL)

        else:
            try:
                value = self.db.get_val('SELECT value FROM ext_attributes WHERE inode=? AND name=?',
                                          (id_, name))
            except NoSuchRowError:
                raise llfuse.FUSEError(llfuse.ENOATTR)
            return value

    def listxattr(self, id_):
        names = list()
        for (name,) in self.db.query('SELECT name FROM ext_attributes WHERE inode=?', (id_,)):
            names.append(name)
        return names

    def setxattr(self, id_, name, value):
        
        # Handle S3QL commands
        if id_ == CTRL_INODE:
            if name == b's3ql_flushcache!':
                self.cache.clear()
                self.cache.upload_manager.join_all()
            elif name == 'copy':
                self.copy_tree(*struct.unpack('II', value))
            elif name == 'lock':
                self.lock_tree(*pickle.loads(value))  
            elif name == 'rmtree':
                self.remove_tree(*pickle.loads(value))
            elif name == 'cachesize':
                self.cache.max_size = pickle.loads(value)      
            else:
                raise llfuse.FUSEError(errno.EINVAL)
        else:
            if self.inodes[id_].locked:
                raise FUSEError(errno.EPERM)
                    
            self.db.execute('INSERT OR REPLACE INTO ext_attributes (inode, name, value) '
                            'VALUES(?, ?, ?)', (id_, name, value))
            self.inodes[id_].ctime = time.time()

    def removexattr(self, id_, name):
        
        if self.inodes[id_].locked:
            raise FUSEError(errno.EPERM)
            
        changes = self.db.execute('DELETE FROM ext_attributes WHERE inode=? AND name=?',
                                  (id_, name))
        if changes == 0:
            raise llfuse.FUSEError(llfuse.ENOATTR)
        self.inodes[id_].ctime = time.time()

    def lock_tree(self, id0):
        '''Lock directory tree'''
        
        log.debug('lock_tree(%d): start', id0)
        queue = [ id0 ]  
        self.inodes[id0].locked = True
        processed = 0
        while True:    
            id_p = queue.pop()
            for (id_,) in self.db.query('SELECT inode FROM contents WHERE parent_inode=?',
                                        (id_p,)):
                self.inodes[id_].locked = True
                processed += 1
                
                if self.db.has_val('SELECT 1 FROM contents WHERE parent_inode=?', (id_,)):
                    queue.append(id_)
                
            if not queue:
                break
                            
            if processed > 1000:
                processed = 0
                self.yield_lock()

        log.debug('lock_tree(%d): end', id0)

    def remove_tree(self, id_p0, name0):
        '''Remove directory tree'''
               
        log.debug('remove_tree(%d, %s): start', id_p0, name0)
         
        if self.inodes[id_p0].locked:
            raise FUSEError(errno.EPERM)
            
        id0 = self.lookup(id_p0, name0).id
        queue = [ id0 ]
        processed = 0
        while True:
            found_subdirs = False
            id_p = queue.pop()  
            for (name, id_) in self.db.query('SELECT name, inode FROM contents WHERE '
                                             'parent_inode=?', (id_p,)):
                   
                if self.db.has_val('SELECT 1 FROM contents WHERE parent_inode=?', 
                                   (id_,)):
                    if not found_subdirs:
                        found_subdirs = True
                        queue.append(id_p)
                    queue.append(id_)
                    
                else:
                    llfuse.invalidate_entry(id_p, name)
                    self._remove(id_p, name, id_, force=True)
                
                processed += 1   
                if processed > 500:
                    processed = 0           
                    if not found_subdirs:
                        found_subdirs = True
                        queue.append(id_p)
                    break
                
            if not queue:
                llfuse.invalidate_entry(id_p0, name0)
                self._remove(id_p0, name0, id0, force=True)
                break              
            
            if processed == 0:
                self.yield_lock()        
        
        log.debug('remove_tree(%d, %s): end', id_p0, name0)


    def yield_lock(self):
        '''Temporarily release global lock'''
        
        log.debug('yield_lock(): releasing global lock')
        with without(lock):
            time.sleep(0.01)
        
    
    def copy_tree(self, src_id, target_id):
        '''Efficiently copy directory tree'''

        log.debug('copy_tree(%d, %d): start', src_id, target_id)
        
        # First we make sure that all blocks are in the database
        self.cache.commit()
        log.debug('copy_tree(%d, %d): commited cache', src_id, target_id)

        # Copy target attributes
        src_inode = self.inodes[src_id]
        target_inode = self.inodes[target_id]
        for attr in ('atime', 'ctime', 'mtime', 'mode', 'uid', 'gid'):
            setattr(target_inode, attr, getattr(src_inode, attr))

        # We first replicate into a dummy inode 
        timestamp = time.time()
        tmp = self.inodes.create_inode(mtime=timestamp, ctime=timestamp, atime=timestamp,
                                       uid=0, gid=0, mode=0, refcount=0)
        
        queue = [ (src_id, tmp.id) ]
        id_cache = dict()
        processed = 0
        in_transit = set()
        while True:
            (t1, t2) = self._copy_tree(queue, id_cache)
            processed += t1
            in_transit.update(t2)

            if not queue:
                break
            
            log.debug('copy_tree(%d, %d): processed %d entries', 
                      src_id, target_id, processed)
            
            # Give other threads a chance to access the db
            if processed > 250:
                processed = 0
                self.yield_lock()
  

        # If we replicated blocks whose associated objects where still in
        # transit, we have to wait for the transit to complete before we make
        # the replicated tree visible to the user. Otherwise access to the newly
        # created blocks will raise a NoSuchObject exception.
        while in_transit:
            log.debug('copy_tree(%d, %d): in_transit: %s', src_id, target_id,
                      in_transit)
            in_transit = [ x for x in in_transit 
                           if x in self.cache.upload_manager.in_transit ]
            if in_transit:
                self.cache.upload_manager.join_one()

            
        # Make replication visible
        self.db.execute('UPDATE contents SET parent_inode=? WHERE parent_inode=?',
                     (target_inode.id, tmp.id))
        del self.inodes[tmp.id]
        llfuse.invalidate_inode(target_inode.id)
        
        log.debug('copy_tree(%d, %d): start', src_id, target_id)

    def _copy_tree(self, queue, id_cache):

        # To avoid lookups and make code tidier
        make_inode = self.inodes.create_inode

        processed = 0
        in_transit = set()
        (src_id, target_id) = queue.pop()
                
        for (name, id_) in self.db.query('SELECT name, inode FROM contents WHERE parent_inode=?',
                                         (src_id,)):
            if id_ not in id_cache:
                inode = self.inodes[id_]

                try:
                    inode_new = make_inode(refcount=1, mode=inode.mode, size=inode.size,
                                           uid=inode.uid, gid=inode.gid,
                                           mtime=inode.mtime, atime=inode.atime,
                                           ctime=inode.ctime, target=inode.target,
                                           rdev=inode.rdev)
                except OutOfInodesError:
                    log.warn('Could not find a free inode')
                    raise FUSEError(errno.ENOSPC)

                id_new = inode_new.id

                if inode.refcount != 1:
                    id_cache[id_] = id_new

                for (obj_id, blockno) in self.db.query('SELECT obj_id, blockno FROM blocks '
                                                    'WHERE inode=?', (id_,)):
                    self.db.execute('INSERT INTO blocks (inode, blockno, obj_id) VALUES(?, ?, ?)',
                                 (id_new, blockno, obj_id))
                    self.db.execute('UPDATE objects SET refcount=refcount+1 WHERE id=?', (obj_id,))
                    
                    if (id_, blockno) in self.cache.upload_manager.in_transit:
                        in_transit.add((id_, blockno))

                if self.db.has_val('SELECT 1 FROM contents WHERE parent_inode=?', (id_,)):
                    queue.append((id_, id_new))
            else:
                id_new = id_cache[id_]
                self.inodes[id_new].refcount += 1

            self.db.execute('INSERT INTO contents (name, inode, parent_inode) VALUES(?, ?, ?)',
                         (name, id_new, target_id))
            processed += 1

        return (processed, in_transit)

    def unlink(self, id_p, name):
        inode = self.lookup(id_p, name)

        if stat.S_ISDIR(inode.mode):
            raise llfuse.FUSEError(errno.EISDIR)

        self._remove(id_p, name, inode.id)

    def rmdir(self, id_p, name):
        inode = self.lookup(id_p, name)

        if self.inodes[id_p].locked:
            raise FUSEError(errno.EPERM)
            
        if not stat.S_ISDIR(inode.mode):
            raise llfuse.FUSEError(errno.ENOTDIR)

        self._remove(id_p, name, inode.id)


    def _remove(self, id_p, name, id_, force=False):
        '''Remove entry `name` with parent inode `id_p` 
        
        `id_` must be the inode of `name`. If `force` is True, then
        the `locked` attribute is ignored.
        
        This method releases the global lock.
        '''

        timestamp = time.time()

        # Check that there are no child entries
        if self.db.has_val("SELECT 1 FROM contents WHERE parent_inode=?", (id_,)):
            log.debug("Attempted to remove entry with children: %s",
                      get_path(id_p, self.db, name))
            raise llfuse.FUSEError(errno.ENOTEMPTY)

        if self.inodes[id_p].locked and not force:
            raise FUSEError(errno.EPERM)
        
        self.db.execute("DELETE FROM contents WHERE name=? AND parent_inode=?",
                        (name, id_p))
        inode = self.inodes[id_]
        inode.refcount -= 1
        inode.ctime = timestamp
        
        inode_p = self.inodes[id_p]
        inode_p.mtime = timestamp
        inode_p.ctime = timestamp

        if inode.refcount == 0 and id_ not in self.open_inodes:
            self.cache.remove(id_, 0, inode.size // self.blocksize + 1)
            # Since the inode is not open, it's not possible that new blocks
            # get created at this point and we can safely delete the inode
            self.db.execute('DELETE FROM ext_attributes WHERE inode=?', (id_,))
            del self.inodes[id_]

    def symlink(self, id_p, name, target, ctx):
        mode = (stat.S_IFLNK | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR | 
                    stat.S_IRGRP | stat.S_IWGRP | stat.S_IXGRP | 
                    stat.S_IROTH | stat.S_IWOTH | stat.S_IXOTH)
        return self._create(id_p, name, mode, ctx, target=target)

    def rename(self, id_p_old, name_old, id_p_new, name_new):
        if name_new == CTRL_NAME or name_old == CTRL_NAME:
            log.warn('Attempted to rename s3ql control file (%s -> %s)',
                      get_path(id_p_old, self.db, name_old),
                      get_path(id_p_new, self.db, name_new))
            raise llfuse.FUSEError(errno.EACCES)

        if (self.inodes[id_p_old].locked
            or self.inodes[id_p_new].locked):
            raise FUSEError(errno.EPERM) 
            
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
            self._rename(id_p_old, name_old, id_p_new, name_new)


    def _rename(self, id_p_old, name_old, id_p_new, name_new):
        timestamp = time.time()

        self.db.execute("UPDATE contents SET name=?, parent_inode=? WHERE name=? "
                     "AND parent_inode=?", (name_new, id_p_new,
                                            name_old, id_p_old))

        inode_p_old = self.inodes[id_p_old]
        inode_p_new = self.inodes[id_p_new]
        inode_p_old.mtime = timestamp
        inode_p_new.mtime = timestamp
        inode_p_old.ctime = timestamp
        inode_p_new.ctime = timestamp

    def _replace(self, id_p_old, name_old, id_p_new, name_new,
                 id_old, id_new):

        timestamp = time.time()

        if self.db.has_val("SELECT 1 FROM contents WHERE parent_inode=?", (id_new,)):
            log.info("Attempted to overwrite entry with children: %s",
                      get_path(id_p_new, self.db, name_new))
            raise llfuse.FUSEError(errno.EINVAL)

        # Replace target
        self.db.execute("UPDATE contents SET inode=? WHERE name=? AND parent_inode=?",
                    (id_old, name_new, id_p_new))
        

        # Delete old name
        self.db.execute('DELETE FROM contents WHERE name=? AND parent_inode=?',
                    (name_old, id_p_old))

        inode_new = self.inodes[id_new]
        inode_new.refcount -= 1
        inode_new.ctime = timestamp

        inode_p_old = self.inodes[id_p_old]
        inode_p_old.ctime = timestamp
        inode_p_old.mtime = timestamp
        
        inode_p_new = self.inodes[id_p_new]
        inode_p_new.ctime = timestamp
        inode_p_new.mtime = timestamp

        if inode_new.refcount == 0 and id_new not in self.open_inodes:
            self.cache.remove(id_new, 0, inode_new.size // self.blocksize + 1)
            # Since the inode is not open, it's not possible that new blocks
            # get created at this point and we can safely delete the inode
            self.db.execute('DELETE FROM ext_attributes WHERE inode=?', (id_new,))
            del self.inodes[id_new]


    def link(self, id_, new_id_p, new_name):
        if new_name == CTRL_NAME or id_ == CTRL_INODE:
            log.warn('Attempted to create s3ql control file at %s',
                      get_path(new_id_p, self.db, new_name))
            raise llfuse.FUSEError(errno.EACCES)

        timestamp = time.time()
        inode_p = self.inodes[new_id_p]
        
        if inode_p.refcount == 0:
            log.warn('Attempted to create entry %s with unlinked parent %d',
                     new_name, new_id_p)
            raise FUSEError(errno.EINVAL)
        
        if inode_p.locked:
            raise FUSEError(errno.EPERM)
        
        inode_p.ctime = timestamp
        inode_p.mtime = timestamp

        self.db.execute("INSERT INTO contents (name, inode, parent_inode) VALUES(?,?,?)",
                     (new_name, id_, new_id_p))
        inode = self.inodes[id_]
        inode.refcount += 1
        inode.ctime = timestamp

        return inode

    def setattr(self, id_, attr):
        """Handles FUSE setattr() requests"""

        inode = self.inodes[id_]
        timestamp = time.time()

        if inode.locked:
            raise FUSEError(errno.EPERM)
        
        if 'st_size' in attr:
            len_ = attr['st_size']

            # Delete all truncated blocks
            last_block = len_ // self.blocksize
            total_blocks = inode.size // self.blocksize + 1
            self.cache.remove(id_, last_block + 1, total_blocks)

            # Get last object before truncation
            if len_ != 0:
                try:
                    with self.cache.get(id_, last_block) as fh:
                        fh.truncate(len_ - self.blocksize * last_block)
                        
                except NoSuchObject as exc:
                    log.warn('Backend lost block %d of inode %d (id %s)!', 
                             last_block, id_, exc.key)
                    self.encountered_errors = True
                    raise FUSEError(errno.EIO)
                
                except ChecksumError as exc:
                    log.warn('Backend returned malformed data for block %d of inode %d (%s)',
                             last_block, id_, exc)
                    raise FUSEError(errno.EIO)
        
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

        if 'st_mtime' in attr:
            inode.mtime = attr['st_mtime']

        if 'st_ctime' in attr:
            inode.ctime = attr['st_ctime']
        else:
            inode.ctime = timestamp
            
        return inode

    def mknod(self, id_p, name, mode, rdev, ctx):
        return self._create(id_p, name, mode, ctx, rdev=rdev)

    def mkdir(self, id_p, name, mode, ctx):
        return self._create(id_p, name, mode, ctx)

    def extstat(self):
        '''Return extended file system statistics'''

        entries = self.db.get_val("SELECT COUNT(rowid) FROM contents")
        blocks = self.db.get_val("SELECT COUNT(id) FROM objects")
        inodes = self.db.get_val("SELECT COUNT(id) FROM inodes")
        fs_size = self.db.get_val('SELECT SUM(size) FROM inodes') or 0
        dedup_size = self.db.get_val('SELECT SUM(size) FROM objects') or 0
        compr_size = self.db.get_val('SELECT SUM(compr_size) FROM objects') or 0

        return struct.pack('QQQQQQQ', entries, blocks, inodes, fs_size, dedup_size,
                           compr_size, self.db.get_size())


    def statfs(self):
        stat_ = dict()

        # Get number of blocks & inodes
        blocks = self.db.get_val("SELECT COUNT(id) FROM objects")
        inodes = self.db.get_val("SELECT COUNT(id) FROM inodes")
        size = self.db.get_val('SELECT SUM(size) FROM objects')

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
        if (self.inodes[id_].locked and
            (flags & os.O_RDWR or flags & os.O_WRONLY)):
            raise FUSEError(errno.EPERM)
        
        self.open_inodes[id_] += 1
        return id_

    def access(self, id_, mode, ctx, get_sup_gids):
        '''Check if requesting process has `mode` rights on `inode`.
        
        This method always returns true, since it should only be called
        when permission checking is disabled (if permission checking is
        enabled, the `default_permissions` FUSE option should be set).
        '''
        # Yeah, could be a function and has unused arguments
        #pylint: disable=R0201,W0613

        return True

    def create(self, id_p, name, mode, ctx):
        inode = self._create(id_p, name, mode, ctx)
        self.open_inodes[inode.id] += 1
        return (inode.id, inode)

    def _create(self, id_p, name, mode, ctx, rdev=0, target=None):
        if name == CTRL_NAME:
            log.warn('Attempted to create s3ql control file at %s',
                     get_path(id_p, self.db, name))
            raise llfuse.FUSEError(errno.EACCES)

        timestamp = time.time()
        inode_p = self.inodes[id_p]
        
        if inode_p.locked:
            raise FUSEError(errno.EPERM)
                    
        if inode_p.refcount == 0:
            log.warn('Attempted to create entry %s with unlinked parent %d',
                     name, id_p)
            raise FUSEError(errno.EINVAL)
        inode_p.mtime = timestamp
        inode_p.ctime = timestamp

        try:
            inode = self.inodes.create_inode(mtime=timestamp, ctime=timestamp, atime=timestamp,
                                             uid=ctx.uid, gid=ctx.gid, mode=mode, refcount=1,
                                             rdev=rdev, target=target)
        except OutOfInodesError:
            log.warn('Could not find a free inode')
            raise FUSEError(errno.ENOSPC)

        self.db.execute("INSERT INTO contents(name, inode, parent_inode) VALUES(?,?,?)",
                     (name, inode.id, id_p))

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

        if inode.atime < inode.ctime or inode.atime < inode.mtime:
            inode.atime = time.time()

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

        try:
            with self.cache.get(id_, blockno) as fh:
                fh.seek(offset_rel)
                buf = fh.read(length)
                
        except NoSuchObject as exc:
            log.warn('Backend lost block %d of inode %d (id %s)!', 
                     blockno, id_, exc.key)
            self.encountered_errors = True
            raise FUSEError(errno.EIO)
        
        except ChecksumError as exc:
            log.warn('Backend returned malformed data for block %d of inode %d (%s)',
                     blockno, id_, exc)
            raise FUSEError(errno.EIO)

        if len(buf) == length:
            return buf
        else:
            # If we can't read enough, add null bytes
            return buf + b"\0" * (length - len(buf))
                
    def write(self, fh, offset, buf):
        '''Handle FUSE write requests.
        
        This method releases the global lock while it is running.
        '''
        
        if self.inodes[fh].locked:
            raise FUSEError(errno.EPERM)
            
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

        try:
            with self.cache.get(id_, blockno) as fh:
                fh.seek(offset_rel)
                fh.write(buf)
                
        except NoSuchObject as exc:
            log.warn('Backend lost block %d of inode %d (id %s)!', 
                     blockno, id_, exc.key)
            self.encountered_errors = True
            raise FUSEError(errno.EIO)
                            
        except ChecksumError as exc:
            log.warn('Backend returned malformed data for block %d of inode %d (%s)',
                     blockno, id_, exc)
            raise FUSEError(errno.EIO)
                
        return len(buf)

    def fsync(self, fh, datasync):
        if not datasync:
            self.inodes.flush_id(fh)

        self.cache.flush(fh)

    def releasedir(self, fh):
        return

    def release(self, fh):
        self.open_inodes[fh] -= 1

        if self.open_inodes[fh] == 0:
            del self.open_inodes[fh]

            inode = self.inodes[fh]
            if inode.refcount == 0:
                self.cache.remove(inode.id, 0, inode.size // self.blocksize + 1)
                # Since the inode is not open, it's not possible that new blocks
                # get created at this point and we can safely delete the in
                del self.inodes[fh]



    # Called for close() calls. 
    def flush(self, fh):
        pass

    def fsyncdir(self, fh, datasync):
        if not datasync:
            self.inodes.flush_id(fh)


class InodeFlushThread(ExceptionStoringThread):
    '''
    Periodically commit dirty inodes.
    
    This class uses the `global_lock` module. When calling objects
    passed in the constructor, the global lock is acquired first.
    '''    
    
    def __init__(self, cache):
        super(InodeFlushThread, self).__init__()
        self.cache = cache
        self.stop_event = threading.Event()
        self.name = 'Inode Flush Thread'
        self.daemon = True 
                
    def run_protected(self):
        log.debug('FlushThread: start')

        while not self.stop_event.is_set():
            with lock:
                self.cache.flush()
            self.stop_event.wait(5)
        
        log.debug('FlushThread: end')    
        
    def stop(self):
        '''Wait for thread to finish, raise any occurred exceptions.
        
        This  method releases the global lock.
        '''
        
        self.stop_event.set()
        lock.release()
        try:
            self.join_and_raise()
        finally:
            lock.acquire()
            
        
        