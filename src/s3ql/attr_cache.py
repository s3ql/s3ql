'''
attr_cache.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2010 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function

import threading
import time

CACHE_SIZE = 50

class AttrCache(object):
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
        for i in ('dummy_%d' % x for x in xrange(CACHE_SIZE)):
            self.attrs[i] = None
            self.cached_rows.append(i)

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
                if isinstance(old_inode, int):
                    self.setattr(old_inode)
                del self.attrs[old_inode]
                self.attrs[inode] = attr
                return attr

    def getattr(self, inode):
        fstat = dict()

        (fstat["st_mode"],
         fstat['refcount'],
         fstat['nlink_off'],
         fstat["st_uid"],
         fstat["st_gid"],
         fstat["st_size"],
         fstat["st_ino"],
         fstat["st_rdev"],
         fstat["target"],
         fstat["st_atime"],
         fstat["st_mtime"],
         fstat["st_ctime"]) = self.dbcm.get_row("SELECT mode, refcount, nlink_off, uid, gid, size, id, "
                                                "rdev, target, atime, mtime, ctime FROM inodes "
                                                "WHERE id=? ", (inode,))

        # Convert to local time
        fstat['st_mtime'] += time.timezone
        fstat['st_atime'] += time.timezone
        fstat['st_ctime'] += time.timezone

        return fstat

    def create_inode(self, st_mode, refcount, nlink_off, st_uid, st_gid, st_size,
                     st_rdev, st_atime, st_mtime, target):

        timestamp = time.time()
        inode = self.dbcm.rowid('INSERT INTO inodes (mode, refcount, nlink_off, uid, gid,'
                                'size, rdev, atime, mtime, ctime, target) '
                                'VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                                (st_mode, refcount, nlink_off, st_uid, st_gid, st_size,
                                 st_rdev, st_atime - time.timezone, st_mtime - time.timezone,
                                 timestamp - time.timezone, target))

        return {'st_ino': inode,
                'st_mode': st_mode,
                'refcount': refcount,
                'nlink_off': nlink_off,
                'st_uid': st_uid,
                'st_gid': st_gid,
                'st_size': st_size,
                'st_rdev': st_rdev,
                'st_atime': st_atime,
                'st_mtime': st_mtime,
                'target': target,
                'st_ctime': timestamp}


    def setattr(self, inode):
        fstat = self.attrs[inode]
        self.dbcm.execute("UPDATE inodes SET mode=?, refcount=?, nlink_off=?, uid=?, gid=?, size=?, "
                          "rdev=?, atime=?, mtime=?, ctime=?, target=? WHERE id=?",
                           (fstat["st_mode"],
                            fstat['refcount'],
                            fstat['nlink_off'],
                            fstat["st_uid"],
                            fstat["st_gid"],
                            fstat["st_size"],
                            fstat["st_rdev"],
                            fstat["st_atime"] - time.timezone,
                            fstat["st_mtime"] - time.timezone,
                            fstat["st_ctime"] - time.timezone,
                            fstat['target'],
                            fstat["st_ino"]))

    def flush(self):
        '''Flush all cached entries to database'''

        with self.lock:
            for i in xrange(len(self.cached_rows)):
                inode = self.cached_rows[i]
                if isinstance(inode, int):
                    self.setattr(inode)
                del self.attrs[inode]
                self.attrs['dummy_%d' % i] = None
                self.cached_rows[i] = 'dummy_%d' % i

    def __del__(self):
        for i in self.attrs.itervalues():
            if i is not None:
                raise RuntimeError('AttrCache instance was destroyed without flushing all entries')

