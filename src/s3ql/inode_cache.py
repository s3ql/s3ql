'''
inode_cache.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2010 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function

import time
import logging
import threading
import sys
import database as dbcm
from random import randint
import apsw
from .common import ExceptionStoringThread
from .database import NoSuchRowError
from s3ql.common import EmbeddedException

__all__ = [ 'InodeCache', 'OutOfInodesError' ]
log = logging.getLogger('inode_cache')

CACHE_SIZE = 100
ATTRIBUTES = ('mode', 'refcount', 'uid', 'gid', 'size', 'locked',
              'rdev', 'target', 'atime', 'mtime', 'ctime', 'id')
ATTRIBUTE_STR = ', '.join(ATTRIBUTES)
UPDATE_ATTRS = ('mode', 'refcount', 'uid', 'gid', 'size', 'locked',
              'rdev', 'target', 'atime', 'mtime', 'ctime')
UPDATE_STR = ', '.join('%s=?' % x for x in UPDATE_ATTRS)
TIMEZONE = time.timezone

class _Inode(object):
    '''An inode with its attributes'''

    __slots__ = ATTRIBUTES + ('dirty',)

    def __init__(self):
        super(_Inode, self).__init__()
        self.dirty = False

    # This allows access to all st_* attributes, even if they're
    # not defined in the table
    def __getattr__(self, key):
        if key == 'st_nlink':
            return self.refcount

        elif key == 'st_blocks':
            return self.size // 512

        elif key == 'st_ino':
            return self.id

        # Timeout, can effectively be infinite since attribute changes
        # are only triggered by the kernel's own requests
        elif key == 'attr_timeout' or key == 'entry_timeout':
            return 3600

        # We want our blocksize for IO as large as possible to get large
        # write requests
        elif key == 'st_blksize':
            return 128 * 1024

        # Our inodes are already unique
        elif key == 'generation':
            return 1

        elif key.startswith('st_'):
            return getattr(self, key[3:])

    def __eq__(self, other):
        if not isinstance(other, _Inode):
            return NotImplemented

        for attr in ATTRIBUTES:
            if getattr(self, attr) != getattr(other, attr):
                return False

        return True


    def copy(self):
        copy = _Inode()

        for attr in ATTRIBUTES:
            setattr(copy, attr, getattr(self, attr))

        return copy

    def __setattr__(self, name, value):
        if name != 'dirty':
            object.__setattr__(self, 'dirty', True)
        object.__setattr__(self, name, value)


class InodeCache(object):
    '''
    This class maps the `inode` SQL table to a dict, caching the rows.
    
    If the cache is full and a row is not in the cache, the least-recently
    retrieved row is deleted from the cache. This means that accessing
    cached rows will *not* change the order of their expiration. 
    
    Attributes: 
    -----------
    :attrs:   inode indexed dict holding the attributes
    :cached_rows: list of the inodes that are in cache
    :pos:    position of the most recently retrieved inode in 
             'cached_rows'.
             
    Notes
    -----
    
    This class is not thread-safe.
    
    Callers should keep in mind that the changes of the returned inode
    object will only be written to the database if the inode is still
    in the cache when its attributes are updated: it is possible for 
    the caller to keep a reference to an inode when that
    inode has already been expired from the InodeCache. Modifications
    to this inode object will be lost(!).
    
    Callers should therefore use the returned inode objects only
    as long as they can guarantee that no other calls to InodeCache
    are made that may result in expiration of inodes from the cache.  
    
    Moreover, the caller must make sure that he does not call
    InodeCache methods while a database transaction is active that
    may be rolled back. This would rollback database updates
    performed by InodeCache, which are generally for inodes that
    are expired from the cache and therefore *not* directly related
    to the effects of the current method call.
    '''

    def __init__(self):
        self.attrs = dict()
        self.cached_rows = list()

        # Fill the cache with dummy data, so that we don't have to
        # check if the cache is full or not (it will always be full)        
        for _ in xrange(CACHE_SIZE):
            self.cached_rows.append(None)

        self.pos = 0
        self.flush_thread = None

    def init(self):
        '''Initialize background flush thread'''

        self.flush_thread = FlushThread(self)
        self.flush_thread.start()

    def __delitem__(self, inode):
        if dbcm.execute('DELETE FROM inodes WHERE id=?', (inode,)) != 1:
            raise KeyError('No such inode')
        try:
            del self.attrs[inode]
        except KeyError:
            pass

    def __getitem__(self, id_):
        try:
            return self.attrs[id_]
        except KeyError:
            try:
                inode = self.getattr(id_)
            except NoSuchRowError:
                raise KeyError('No such inode: %d' % id_)
            
            old_id = self.cached_rows[self.pos]
            self.cached_rows[self.pos] = id_
            self.pos = (self.pos + 1) % CACHE_SIZE
            if old_id is not None:
                try:
                    old_inode = self.attrs[old_id]
                except KeyError:
                    # We may have deleted that inode
                    pass
                else:
                    del self.attrs[old_id]
                    self.setattr(old_inode)
            self.attrs[id_] = inode
            return inode

    def getattr(self, id_):
        attrs = dbcm.get_row("SELECT %s FROM inodes WHERE id=? " % ATTRIBUTE_STR,
                                  (id_,))
        inode = _Inode()

        for (i, id_) in enumerate(ATTRIBUTES):
            setattr(inode, id_, attrs[i])

        # Convert to local time
        inode.atime += TIMEZONE
        inode.mtime += TIMEZONE
        inode.ctime += TIMEZONE

        inode.dirty = False

        return inode

    def create_inode(self, **kw):

        inode = _Inode()

        for (key, val) in kw.iteritems():
            setattr(inode, key, val)

        for i in ('atime', 'ctime', 'mtime'):
            kw[i] -= TIMEZONE

        init_attrs = [ x for x in ATTRIBUTES if x in kw ]

        # We want to restrict inodes to 2^32, and we do not want to immediately
        # reuse deleted inodes (so that the lack of generation numbers isn't too
        # likely to cause problems with NFS)
        sql = ('INSERT INTO inodes (id, %s) VALUES(?, %s)'
               % (', '.join(init_attrs), ','.join('?' for _ in init_attrs)))
        bindings = [ kw[x] for x in init_attrs ]
        for _ in range(100):
            # _Inode.id is not explicitly defined
            #pylint: disable-msg=W0201
            inode.id = randint(0, 2 ** 32 - 1)
            try:
                dbcm.execute(sql, [inode.id] + bindings)
            except apsw.ConstraintError:
                pass
            else:
                break
        else:
            raise OutOfInodesError()


        return self[inode.id]


    def setattr(self, inode):
        if not inode.dirty:
            return
        inode.dirty = False
        inode = inode.copy()

        inode.atime -= TIMEZONE
        inode.mtime -= TIMEZONE
        inode.ctime -= TIMEZONE

        dbcm.execute("UPDATE inodes SET %s WHERE id=?" % UPDATE_STR,
                          [ getattr(inode, x) for x in UPDATE_ATTRS ] + [inode.id])

    def flush_id(self, id_):
        if id_ in self.attrs:
            self.setattr(self.attrs[id_])

    def destroy(self):
        '''Finalize cache'''

        try:
            self.flush_thread.stop()
        except EmbeddedException:
            log.error('FlushThread terminated with exception.')

        for i in xrange(len(self.cached_rows)):
            id_ = self.cached_rows[i]
            self.cached_rows[i] = None
            if id_ is not None:
                try:
                    inode = self.attrs[id_]
                except KeyError:
                    # We may have deleted that inode
                    pass
                else:
                    del self.attrs[id_]
                    self.setattr(inode)

    def flush(self):
        '''Flush all entries to database'''

        # We don't want to use dict.itervalues() since
        # the dict may change while we iterate
        for i in xrange(len(self.cached_rows)):
            id_ = self.cached_rows[i]
            if id_ is not None:
                try:
                    inode = self.attrs[id_]
                except KeyError:
                    # We may have deleted that inode
                    pass
                else:
                    self.setattr(inode)

    def __del__(self):
        if self.attrs:
            raise RuntimeError('InodeCache instance was destroyed without calling close()')


class FlushThread(ExceptionStoringThread):

    def __init__(self, cache):
        super(FlushThread, self).__init__()
        self.cache = cache
        self.stop_event = threading.Event()
        self.name = 'Inode Flush Thread'
        self.daemon = True 
                
    def run_protected(self):
        log.debug('FlushThread: start')

        while not self.stop_event.is_set():
            self.cache.flush()
            self.stop_event.wait(5)
        
        log.debug('FlushThread: end')    
        
    def stop(self):
        '''Wait for thread to finish, raise any occurred exceptions'''
        
        self.stop_event.set()           
        self.join_and_raise()
        
        
class OutOfInodesError(Exception):

    def __str__(self):
        return 'Could not find free rowid in inode table'
