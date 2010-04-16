'''
inode_cache.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2010 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function

import threading
import time


__all__ = [ 'InodeCache' ]

CACHE_SIZE = 100
ATTRIBUTES = ('mode', 'refcount', 'nlink_off', 'uid', 'gid', 'size',
              'rdev', 'target', 'atime', 'mtime', 'ctime', 'id')
ATTRIBUTE_STR = ', '.join(ATTRIBUTES)
SET_ATTRS = tuple(x for x in ATTRIBUTES if x != 'id')
SET_ATTRS_STR = ', '.join(SET_ATTRS)
ASSIGN_STR = ', '.join('%s=?' % x for x in SET_ATTRS)

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
    '''

    def __init__(self, dbcm):

        self.dbcm = dbcm
        self.attrs = dict()
        self.cached_rows = list()
        self.lock = threading.Lock()

        # Fill the cache with dummy data, so that we don't have to
        # check if the cache is full or not (it will always be full)        
        for _ in xrange(CACHE_SIZE):
            self.cached_rows.append(None)

        self.pos = 0

    def __delitem__(self, inode):
        if self.dbcm.execute('DELETE FROM inodes WHERE id=?', (inode,)) != 1:
            raise KeyError('No such inode')
        try:
            del self.attrs[inode]
        except KeyError:
            pass

    def __getitem__(self, inode):
        with self.lock:
            try:
                return self.attrs[inode]
            except KeyError:
                attr = self.getattr(inode)
                old_inode = self.cached_rows[self.pos]
                self.cached_rows[self.pos] = inode
                self.pos = (self.pos + 1) % CACHE_SIZE
                if old_inode is not None:
                    self.setattr(old_inode)
                    del self.attrs[old_inode]
                self.attrs[inode] = attr
                return attr

    def getattr(self, id_):
        attrs = self.dbcm.get_row("SELECT %s FROM inodes WHERE id=? " % ATTRIBUTE_STR,
                                  (id_,))
        inode = _Inode()

        for (i, id_) in enumerate(ATTRIBUTES):
            setattr(inode, id_, attrs[i])

        # Convert to local time
        inode.atime += time.timezone
        inode.mtime += time.timezone
        inode.ctime += time.timezone

        return inode

    def create_inode(self, **kw):

        inode = _Inode()

        for (key, val) in kw.iteritems():
            setattr(inode, key, val)

        for i in ('atime', 'ctime', 'mtime'):
            kw[i] -= time.timezone

        # _Inode.id is not explicitly defined
        #pylint: disable-msg=W0201
        inode.id = self.dbcm.rowid('INSERT INTO inodes (%s) VALUES(%s)'
                                   % (SET_ATTRS_STR, ','.join('?' for _ in SET_ATTRS)),
                                   (kw[x] for x in SET_ATTRS))
        return inode


    def setattr(self, id_):
        inode = self.attrs[id_]
        self.dbcm.execute("UPDATE inodes SET %s WHERE id=?" % ASSIGN_STR,
                          [getattr(inode, x) for x in SET_ATTRS] + [id_])

    def flush(self):
        '''Flush all cached entries to database'''

        with self.lock:
            for i in xrange(len(self.cached_rows)):
                inode = self.cached_rows[i]
                if inode is not None:
                    self.setattr(inode)
                    del self.attrs[inode]
                self.cached_rows[i] = None

    def __del__(self):
        if self.attrs:
            raise RuntimeError('AttrCache instance was destroyed without flushing all entries')

