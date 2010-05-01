#!/usr/bin/env python
'''
llfuse_example.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2010 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
''' 

from __future__ import division, print_function, absolute_import

import llfuse
import errno
import stat
import sys

class Operations(llfuse.Operations):
    '''A very simple example filesystem'''
    
    def __init__(self):      
        super(Operations, self).__init__()
        self.entries = [
                   # name, attr
                   (b'.', { 'st_ino': 1,
                           'st_mode': stat.S_IFDIR | 0755,
                           'st_nlink': 2}),
                  (b'..', { 'st_ino': 1,
                           'st_mode': stat.S_IFDIR | 0755,
                           'st_nlink': 2}),
                  (b'file1', { 'st_ino': 2, 'st_nlink': 1,
                              'st_mode': stat.S_IFREG | 0644 }),
                  (b'file2', { 'st_ino': 3, 'st_nlink': 1,
                              'st_mode': stat.S_IFREG | 0644 }) ]
        
        self.contents = { # Inode: Contents
                         2: b'Hello, World\n',
                         3: b'Some more file contents\n'
        }
        
        self.by_inode = dict()
        self.by_name = dict()
        
        for entry in self.entries:
            (name, attr) = entry        
            if attr['st_ino'] in self.contents: 
                attr['st_size'] = len(self.contents[attr['st_ino']])
 
                
            self.by_inode[attr['st_ino']] = attr
            self.by_name[name] = attr

                           
        
    def lookup(self, parent_inode, name):
        try:
            attr = self.by_name[name].copy()
        except KeyError:
            raise llfuse.FUSEError(errno.ENOENT)
        
        attr['attr_timeout'] = 1
        attr['entry_timeout'] = 1
        attr['generation'] = 1
        
        return attr
 
    
    def getattr(self, inode):
        attr = self.by_inode[inode].copy()
        attr['attr_timeout'] = 1
        return attr
    
    def readdir(self, fh, off):    
        for entry in self.entries:
            if off > 0:
                off -= 1
                continue
            
            yield entry
    
        
    def read(self, fh, off, size):
        return self.contents[fh][off:off+size]  
    
    def open(self, inode, flags):
        if inode in self.contents:
            return inode
        else:
            raise RuntimeError('Attempted to open() a directory')
    
    def opendir(self, inode):
        return inode
 
    def access(self, inode, mode, ctx, get_sup_gids):
        return True



if __name__ == '__main__':
    
    if len(sys.argv) != 2:
        raise SystemExit('Usage: %s <mountpoint>' % sys.argv[0])
    
    mountpoint = sys.argv[1]
    operations = Operations()
    
    llfuse.init(operations, mountpoint, [ b"nonempty", b'fsname=llfuses_xmp' ])
    llfuse.main()
    