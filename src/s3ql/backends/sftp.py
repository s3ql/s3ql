'''
__init__.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2010 Nikolaus Rath <Nikolaus@rath.org>
Copyright (C) 2010 Ron Knapp <ron.siesta@gmail.com>

This program can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import division, print_function, absolute_import

from .common import AbstractConnection, AbstractBucket, NoSuchBucket, NoSuchObject
import logging
import errno
import shutil
import cPickle as pickle
import os
import stat
import paramiko
import threading

log = logging.getLogger("backend.sftp")


class Connection(AbstractConnection):
    '''
    Provides a connection to an SFTP server.
    
    This class is threadsafe. All methods (except for internal methods
    starting with underscore) may be called concurrently by different
    threads.
    '''   
     
    def __init__(self, host, port, login, password):
        super(Connection, self).__init__()

        self.port = port or 22
        self.host = host
        self.login = login
        self.password = password

        self._client = None
        self.sftp = None
        
        self._setup_ssh_connection()
        
        self.lock = threading.RLock()

    def _setup_ssh_connection(self):

        self._client = paramiko.SSHClient()
        # Probably not a good idea to do this by default
        #self._client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self._client.load_host_keys(os.path.expanduser('~/.ssh/known_hosts'))
        self._client.connect(self.host, port=self.port, username=self.login, password=self.password)
        self.sftp = self._client.open_sftp()
        
        # We don't want the connection to time out
        self._client.get_transport().set_keepalive(300)

    def __contains__(self, entry):
        with self.lock:
            try:
                self.sftp.stat(entry)
            except IOError as exc:
                if exc.errno == errno.ENOENT:
                    return False
                else:
                    raise
            else:
                return True

    def delete_bucket(self, name, recursive=False):
        """Delete bucket"""

        with self.lock:
            if name not in self:
                raise NoSuchBucket(name)
    
            if recursive:
                self._rmtree(name)
    
            self.sftp.rmdir(name)
        
    def _rmtree(self, path):
        '''Recursively delete contents of remote path'''
        
        for attr in self.sftp.listdir_attr(path):
            fullname = '%s/%s' % (path, attr.filename)
            if stat.S_ISDIR(attr.st_mode):
                self._rmtree(fullname)
                self.sftp.rmdir(fullname)
            else:
                self.sftp.remove(fullname)                

        
    def create_bucket(self, name, passphrase=None, compression='lzma'):
        """Create and return bucket"""

        with self.lock:
            self.sftp.mkdir(name)
            return self.get_bucket(name, passphrase, compression)

    def get_bucket(self, name, passphrase=None, compression='lzma'):
        """Return Bucket instance for the bucket `name`"""
        
        with self.lock:
            if name not in self:
                raise NoSuchBucket(name)
    
            return Bucket(self, name, passphrase, compression)

    def close(self):
        with self.lock:
            self._client.close()        

    def prepare_fork(self):
        with self.lock:
            self._client.close()

    def finish_fork(self):
        with self.lock:
            self._setup_ssh_connection()

class Bucket(AbstractBucket):
    '''
    Stores data remotely on an SFTP server.
    
    This class is threadsafe. All methods (except for internal methods
    starting with underscore) may be called concurrently by different
    threads.
    '''    
    
    def __init__(self, conn, name, passphrase, compression):
        super(Bucket, self).__init__(passphrase, compression)
        self.conn = conn
        self.name = name

    def _key_to_path(self, key):
        '''Return path for given key'''
        
        key = _escape(key)
        
        if not key.startswith('s3ql_data_'):
            return os.path.join(self.name, key)
        
        no = key[10:]
        path = [ self.name, 's3ql_data']
        for i in range(0, len(no), 3):
            path.append(no[:i])
        path.append(key)
        
        return os.path.join(*path)
    
    def __str__(self):
        return '<sftp bucket, name=%r>' % self.name

    def read_after_create_consistent(self):
        return True
    
    def read_after_write_consistent(self):
        return True

    def read_after_delete_consistent(self):
        return True
    
    def clear(self):
        # Access to protected member ok
        #pylint: disable=W0212
        with self.conn.lock:
            self.conn._rmtree(self.name)

    def contains(self, key):
        with self.conn.lock:
            return (self._key_to_path(key) + '.dat') in self.conn

    def raw_lookup(self, key):
        with self.conn.lock:
            path = self._key_to_path(key)
            try:
                src = self.conn.sftp.open(path + '.meta', 'rb')
                return pickle.load(src)
            except IOError as exc:
                if exc.errno == errno.ENOENT:
                    raise NoSuchObject(key)
                else:
                    raise

    def delete(self, key, force=False):
        with self.conn.lock:
            path = self._key_to_path(key)
    
            try:
                self.conn.sftp.remove(path + '.dat')
                self.conn.sftp.remove(path + '.meta')
            except IOError as exc:
                if exc.errno == errno.ENOENT:
                    if force:
                        pass
                    else:
                        raise NoSuchObject(key)
                else:
                    raise

    def list(self, prefix=''):
        with self.conn.lock:
            if prefix:
                base = os.path.dirname(self._key_to_path(prefix))     
            else:
                base = self.name
                
            for (path, dirnames, filenames) in self._walk(base):

                # Do not look in wrong directories
                if prefix:
                    rpath = path[len(self.name):] # path relative to base
                    prefix_l = ''.join(rpath.split('/'))
                    
                    dirs_to_walk = list()
                    for name in dirnames:
                        prefix_ll = _unescape(prefix_l + name)
                        if prefix_ll.startswith(prefix[:len(prefix_ll)]):
                            dirs_to_walk.append(name)
                    dirnames[:] = dirs_to_walk
                                    
                for name in filenames:
                    if not name.endswith('.dat'):
                        continue
                    key = _unescape(name[:-4])
                    
                    if not prefix or key.startswith(prefix):
                        yield key
           
    def _walk(self, base):
        '''Iterate recursively over directories, like os.walk'''
                         
        to_visit = [ base ]
        while to_visit: 
            base = to_visit.pop()
            files = list()
            for attr in self.conn.sftp.listdir_attr(base):
                if stat.S_ISDIR(attr.st_mode):
                    to_visit.append('%s/%s' % (base, attr.filename))
                else:
                    files.append(attr.filename) 
            yield (base, to_visit, files)
    
    def _makedirs(self, path):
        '''Like os.makedirs, but over sftp'''
        
        cur = '/'
        done = False
        for el in path.split('/'):
            cur = '%s/%s' % (cur, el)
            if cur not in self.conn:
                self.conn.sftp.mkdir(cur)
                done = True
        
        if not done:
            err = OSError('Entry already exists: %s' % cur)
            err.errno = errno.EEXIST
            raise err

    def raw_fetch(self, key, fh):
        with self.conn.lock:       
            path = self._key_to_path(key)
            try:
                src = self.conn.sftp.open(path + '.dat', 'r')
                src.prefetch()
                fh.seek(0)
                shutil.copyfileobj(src, fh)
                src.close()
    
                src = self.conn.sftp.open(path + '.meta', 'r')
                src.prefetch()
                metadata = pickle.load(src)
                src.close()
    
            except IOError as exc:
                if exc.errno == errno.ENOENT:
                    raise NoSuchObject(key)
                else:
                    raise
    
            return metadata

    def raw_store(self, key, fh, metadata):
        with self.conn.lock:
            path = self._key_to_path(key)
            fh.seek(0)
            
            try:
                dest = self.conn.sftp.open(path + '.dat', 'w')
                dest.set_pipelined(True)
            except IOError as exc:
                if exc.errno != errno.ENOENT:
                    raise
                self._makedirs(os.path.dirname(path))
                dest = self.conn.sftp.open(path + '.dat', 'w')
                dest.set_pipelined(True)
                
            shutil.copyfileobj(fh, dest)
            dest.close()
                    
            dest = self.conn.sftp.open(path + '.meta', 'w')
            dest.set_pipelined(True)
            pickle.dump(metadata, dest, 2)
            dest.close()

    def rename(self, src, dest):
        with self.conn.lock:
            src_path = self._key_to_path(src)
            dest_path = self._key_to_path(dest)
            
            try:
                self.conn.sftp.lstat(src_path + '.dat')
            except IOError as exc:
                if exc.errno == errno.ENOENT:
                    raise NoSuchObject(src)
                else:
                    raise
               
            try: 
                self.conn.sftp.rename(src_path + '.dat', dest_path + '.dat')
                self.conn.sftp.rename(src_path + '.meta', dest_path + '.meta')
            except IOError as exc:
                if exc.errno != errno.ENOENT:
                    raise
                self._makedirs(os.path.dirname(dest_path))
                self.conn.sftp.rename(src_path + '.dat', dest_path + '.dat')
                self.conn.sftp.rename(src_path + '.meta', dest_path + '.meta')

def _escape(s):
    '''Escape '/', '=' and '\0' in s'''

    s = s.replace('=', '=3D')
    s = s.replace('/', '=2F')
    s = s.replace('\0', '=00')

    return s

def _unescape(s):
    '''Un-Escape '/', '=' and '\0' in s'''

    s = s.replace('=2F', '/')
    s = s.replace('=00', '\0')
    s = s.replace('=3D', '=')

    return s
