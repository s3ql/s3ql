'''
local.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import division, print_function, absolute_import

from .common import AbstractBucket, NoSuchBucket, NoSuchObject
import shutil
import logging
import cPickle as pickle
import os
import errno
import thread

log = logging.getLogger("backend.local")


class Bucket(AbstractBucket):
    '''
    A bucket that is stored on the local hard disk
    '''

    def __init__(self, name):
        super(Bucket, self).__init__()
        self.name = name
        
        if not os.path.exists(name):
            raise NoSuchBucket(name)

    def __str__(self):
        return '<local bucket, name=%r>' % self.name

    def lookup(self, key):
        """Return metadata for given key.

        If the key does not exist, `NoSuchObject` is raised.
        """
        
        path = self._key_to_path(key)
        try:
            with open(path, 'rb') as src:
                return pickle.load(src)
        except IOError as exc:
            if exc.errno == errno.ENOENT:
                raise NoSuchObject(key)
            else:
                raise

    def open_read(self, key):
        """Open object for reading

        Return a tuple of a file-like object and metadata. Bucket
        contents can be read from the file-like object. 
        """
        
        path = self._key_to_path(key)
        try:
            src = open(path, 'rb') 
        except IOError as exc:
            if exc.errno == errno.ENOENT:
                raise NoSuchObject(key)
            else:
                raise
            
        metadata = pickle.load(src)
        
        return (src, metadata)
    
    def open_write(self, key, metadata=None):
        """Open object for writing

        `metadata` can be a dict of additional attributes to store with the
        object. Returns a file-like object.
        """
        
        # By renaming, we make sure that there are no
        # conflicts between parallel reads, the last one wins
        
        path = self._key_to_path(key)
        tmpname = '%s.%d-%d' % (path, os.getpid(), thread.get_ident()) 
        
        try:
            dest = open(tmpname, 'wb')
        except IOError as exc:
            if exc.errno != errno.ENOENT:
                raise
            os.makedirs(os.path.dirname(path))
            dest = open(tmpname, 'wb')
            
        os.rename(tmpname, path)
        pickle.dump(metadata, dest, 2)
        return dest    
            
    def read_after_create_consistent(self):
        '''Does this backend provide read-after-create consistency?'''
        return True
    
    def read_after_write_consistent(self):
        '''Does this backend provide read-after-write consistency?'''
        return True
        
    def read_after_delete_consistent(self):
        '''Does this backend provide read-after-delete consistency?'''
        return True

    def list_after_delete_consistent(self):
        '''Does this backend provide list-after-delete consistency?'''
        return True
        
    def list_after_create_consistent(self):
        '''Does this backend provide list-after-create consistency?'''
        return True
    
    def clear(self):
        """Delete all objects in bucket"""
        
        for name in os.listdir(self.name):
            path = os.path.join(self.name, name)
            if os.path.isdir(path):
                shutil.rmtree(path)
            else:
                os.unlink(path)

    def contains(self, key):
        '''Check if `key` is in bucket'''
        
        path = self._key_to_path(key)
        try:
            os.lstat(path)
        except OSError as exc:
            if exc.errno == errno.ENOENT:
                return False
            raise
        return True

    def delete(self, key, force=False):
        """Delete object stored under `key`

        ``bucket.delete(key)`` can also be written as ``del bucket[key]``.
        If `force` is true, do not return an error if the key does not exist.
        """
        path = self._key_to_path(key)
        try:
            os.unlink(path)
        except OSError as exc:
            if exc.errno == errno.ENOENT:
                if force:
                    pass
                else:
                    raise NoSuchObject(key)
            else:
                raise

    def list(self, prefix=''):
        '''List keys in bucket

        Returns an iterator over all keys in the bucket.
        '''
        if prefix:
            base = os.path.dirname(self._key_to_path(prefix))     
        else:
            base = self.name
            
        for (path, dirnames, filenames) in os.walk(base, topdown=True):
            
            # Do not look in wrong directories
            if prefix:
                rpath = path[len(self.name):] # path relative to base
                prefix_l = ''.join(rpath.split('/'))
                
                dirs_to_walk = list()
                for name in dirnames:
                    prefix_ll = unescape(prefix_l + name)
                    if prefix_ll.startswith(prefix[:len(prefix_ll)]):
                        dirs_to_walk.append(name)
                dirnames[:] = dirs_to_walk
                                            
            for name in filenames:
                key = unescape(name)
                
                if not prefix or key.startswith(prefix):
                    yield key

    def copy(self, src, dest):
        """Copy data stored under key `src` to key `dest`
        
        If `dest` already exists, it will be overwritten. The copying
        is done on the remote side. If the backend does not support
        this operation, raises `UnsupportedError`.
        """
        
        path_src = self._key_to_path(src)
        path_dest = self._key_to_path(dest)

        # Can't use shutil.copyfile() here, need to make
        # sure destination path exists
        try:
            dest = open(path_dest, 'wb')
        except IOError as exc:
            if exc.errno != errno.ENOENT:
                raise
            os.makedirs(os.path.dirname(path_dest))
            dest = open(path_dest, 'wb')
        
        try:
            with open(path_src, 'rb') as src:
                shutil.copyfileobj(src, dest)
        except IOError as exc:
            if exc.errno == errno.ENOENT:
                raise NoSuchObject(src)
            else:
                raise
        finally:
            dest.close()

    def rename(self, src, dest):
        """Rename key `src` to `dest`
        
        If `dest` already exists, it will be overwritten. The rename
        is done on the remote side. If the backend does not support
        this operation, raises `UnsupportedError`.
        """
        src_path = self._key_to_path(src)
        dest_path = self._key_to_path(dest)
        if not os.path.exists(src_path):
            raise NoSuchObject(src)
           
        try: 
            os.rename(src_path, dest_path)
        except OSError as exc:
            if exc.errno != errno.ENOENT:
                raise
            os.makedirs(os.path.dirname(dest_path))   
            os.rename(src_path, dest_path)  
        
    def _key_to_path(self, key):
        '''Return path for given key'''
        
        # NOTE: We must not split the path in the middle of an
        # escape sequence, or list() will fail to work.
        
        key = escape(key)
        
        if not key.startswith('s3ql_data_'):
            return os.path.join(self.name, key)
        
        no = key[10:]
        path = [ self.name, 's3ql_data_']
        for i in range(0, len(no), 3):
            path.append(no[:i])
        path.append(key + '.dat')
        
        return os.path.join(*path)

def escape(s):
    '''Escape '/', '=' and '\0' in s'''

    s = s.replace('=', '=3D')
    s = s.replace('/', '=2F')
    s = s.replace('\0', '=00')

    return s

def unescape(s):
    '''Un-Escape '/', '=' and '\0' in s'''

    s = s.replace('=2F', '/')
    s = s.replace('=00', '\0')
    s = s.replace('=3D', '=')

    return s


