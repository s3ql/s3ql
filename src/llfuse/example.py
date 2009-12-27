'''
example.py

Copyright (C) 2008  Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL. 
'''

from interface import FUSEError
from operations import Operations
import errno
import stat


class XmpOperations(Operations):
    '''A very simple example filesystem'''
    
    def __init__(self):      
        super(XmpOperations, self).__init__()
        self.entries = [
                   # name, attr
                   (b'.', { 'st_ino': 1,
                           'st_mode': stat.S_IFDIR | 0755,
                           'st_nlink': 2}),
                  (b'..', { 'st_ino': 1,
                           'st_mode': stat.S_IFDIR | 0755,
                           'st_nlink': 2}),
                  (b'file', { 'st_ino': 2, 'st_nlink': 1,
                              'st_mode': stat.S_IFREG | 0644 }) ]
        
        # Try to overflow readdir() bufer
        file_name_len = 40
        files_required = (2 * 4096) // file_name_len
        for i in range(files_required):
            self.entries.append(
                  ('%0*d' % (file_name_len, i),
                   { 'st_ino': 3, 
                     'st_nlink': files_required,
                     'st_mode': stat.S_IFREG | 0644 }))
            
        
        self.contents = { # Inode: Contents
                         2: b'Hello, World\n',
                         3: b'Stupid file contents\n'
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
            raise FUSEError(errno.ENOENT)
        
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

