'''
inode_cache.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2010 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function

import time
import common
import logging
import threading

__all__ = [ 'InodeCache' ]
log = logging.getLogger('inode_cache')

CACHE_SIZE = 100
ATTRIBUTES = ('mode', 'refcount', 'nlink_off', 'uid', 'gid', 'size',
              'rdev', 'target', 'atime', 'mtime', 'ctime', 'id')
ATTRIBUTE_STR = ', '.join(ATTRIBUTES)
UPDATE_ATTRS = ('mode', 'refcount', 'nlink_off', 'uid', 'gid', 'size',
              'rdev', 'target', 'atime', 'mtime', 'ctime')
UPDATE_STR = ', '.join('%s=?' % x for x in UPDATE_ATTRS)
TIMEZONE = time.timezone

class _Inode(object):
    '''An inode with its attributes'''

    __slots__ = ATTRIBUTES

    # This allows access to all st_* attributes, even if they're
    # not defined in the table
    def __getattr__(self, key):
        if key == 'st_nlink':
            return self.refcount + self.nlink_off

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
    '''

    def __init__(self, dbcm):

        self.dbcm = dbcm
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

        self.flush_thread = common.ExceptionStoringThread(self.flush_loop, log, pass_self=True)
        self.flush_thread.setName('inode flush thread')
        self.flush_thread.daemon = True
        self.flush_thread.stop_event = threading.Event()
        self.flush_thread.start()

    def __delitem__(self, inode):
        if self.dbcm.execute('DELETE FROM inodes WHERE id=?', (inode,)) != 1:
            raise KeyError('No such inode')
        try:
            del self.attrs[inode]
        except KeyError:
            pass

    def __getitem__(self, id_):
        try:
            return self.attrs[id_]
        except KeyError:
            inode = self.getattr(id_)
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
        attrs = self.dbcm.get_row("SELECT %s FROM inodes WHERE id=? " % ATTRIBUTE_STR,
                                  (id_,))
        inode = _Inode()

        for (i, id_) in enumerate(ATTRIBUTES):
            setattr(inode, id_, attrs[i])

        # Convert to local time
        inode.atime += TIMEZONE
        inode.mtime += TIMEZONE
        inode.ctime += TIMEZONE

        return inode

    def create_inode(self, **kw):

        inode = _Inode()

        for (key, val) in kw.iteritems():
            setattr(inode, key, val)

        for i in ('atime', 'ctime', 'mtime'):
            kw[i] -= TIMEZONE

        init_attrs = [ x for x in ATTRIBUTES if x in kw ]

        # _Inode.id is not explicitly defined
        #pylint: disable-msg=W0201
        inode.id = self.dbcm.rowid('INSERT INTO inodes (%s) VALUES(%s)'
                                   % (', '.join(init_attrs),
                                      ','.join('?' for _ in init_attrs)),
                                   (kw[x] for x in init_attrs))
        return self[inode.id]


    def setattr(self, inode):
        inode = inode.copy()

        inode.atime -= TIMEZONE
        inode.mtime -= TIMEZONE
        inode.ctime -= TIMEZONE

        self.dbcm.execute("UPDATE inodes SET %s WHERE id=?" % UPDATE_STR,
                          [ getattr(inode, x) for x in UPDATE_ATTRS ] + [inode.id])

    def flush_id(self, id_):
        if id_ in self.attrs:
            self.setattr(self.attrs[id_])

    def close(self):
        '''Finalize cache'''

        if self.flush_thread:
            self.flush_thread.stop_event.set()
            self.flush_thread.join_and_raise()

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

    def flush_loop(self, self_t):
        while not self_t.stop_event.is_set():
            self.flush()
            self_t.stop_event.wait(5)

    def __del__(self):
        if self.attrs:
            raise RuntimeError('InodeCache instance was destroyed without calling close()')

