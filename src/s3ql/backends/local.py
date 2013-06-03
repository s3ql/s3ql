'''
local.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from ..logging import logging # Ensure use of custom logger class
from ..common import BUFSIZE, PICKLE_PROTOCOL
from .common import AbstractBackend, DanglingStorageURLError, NoSuchObject, ChecksumError
import _thread
import io
import os
import pickle
import shutil

log = logging.getLogger(__name__)

class Backend(AbstractBackend):
    '''
    A backend that stores data on the local hard disk
    '''

    needs_login = False

    def __init__(self, storage_url, backend_login, backend_pw, ssl_context=None):
        '''Initialize local backend
        
        Login and password are ignored.
        '''
        # Unused argument
        #pylint: disable=W0613

        super().__init__()
        name = storage_url[len('local://'):]
        self.name = name

        if not os.path.exists(name):
            raise DanglingStorageURLError(name)

    def __str__(self):
        return 'local://%s' % self.name

    def is_temp_failure(self, exc): #IGNORE:W0613
        '''Return true if exc indicates a temporary error'''

        return False

    def lookup(self, key):
        """Return metadata for given key.

        If the key does not exist, `NoSuchObject` is raised.
        """

        path = self._key_to_path(key)
        try:
            with open(path, 'rb') as src:
                return pickle.load(src)
        except FileNotFoundError:
            raise NoSuchObject(key) from None
        except pickle.UnpicklingError as exc:
            if (isinstance(exc.args[0], str)
                and exc.args[0].startswith('invalid load key')):
                raise ChecksumError('Invalid metadata') from None
            raise

    def get_size(self, key):
        '''Return size of object stored under *key*'''

        return os.path.getsize(self._key_to_path(key))

    def open_read(self, key):
        """Open object for reading

        Return a file-like object. Data can be read using the `read` method. metadata is stored in
        its *metadata* attribute and can be modified by the caller at will. The object must be
        closed explicitly.
        """

        path = self._key_to_path(key)
        try:
            fh = ObjectR(path)
        except FileNotFoundError:
            raise NoSuchObject(key) from None
        
        try:
            fh.metadata = pickle.load(fh)
        except pickle.UnpicklingError as exc:
            if (isinstance(exc.args[0], str)
                and exc.args[0].startswith('invalid load key')):
                raise ChecksumError('Invalid metadata') from None
            raise
        return fh

    def open_write(self, key, metadata=None, is_compressed=False):
        """Open object for writing

        `metadata` can be a dict of additional attributes to store with the object. Returns a file-
        like object. The object must be closed explicitly. After closing, the *get_obj_size* may be
        used to retrieve the size of the stored object (which may differ from the size of the
        written data).
        
        The *is_compressed* parameter indicates that the caller is going to write compressed data,
        and may be used to avoid recompression by the backend.
        """

        path = self._key_to_path(key)

        # By renaming, we make sure that there are no
        # conflicts between parallel reads, the last one wins
        tmpname = '%s#%d-%d' % (path, os.getpid(), _thread.get_ident())

        try:
            dest = ObjectW(tmpname)
        except FileNotFoundError:
            try:
                os.makedirs(os.path.dirname(path))
            except FileExistsError:
                # Another thread may have created the directory already
                pass
            dest = ObjectW(tmpname)

        os.rename(tmpname, path)
        pickle.dump(metadata, dest, PICKLE_PROTOCOL)
        return dest

    def clear(self):
        """Delete all objects in backend"""

        for name in os.listdir(self.name):
            path = os.path.join(self.name, name)
            if os.path.isdir(path):
                shutil.rmtree(path)
            else:
                os.unlink(path)

    def contains(self, key):
        '''Check if `key` is in backend'''

        path = self._key_to_path(key)
        try:
            os.lstat(path)
        except FileNotFoundError:
            return False
        return True

    def delete(self, key, force=False):
        """Delete object stored under `key`

        ``backend.delete(key)`` can also be written as ``del backend[key]``.
        If `force` is true, do not return an error if the key does not exist.
        """
        path = self._key_to_path(key)
        try:
            os.unlink(path)
        except FileNotFoundError:
            if force:
                pass
            else:
                raise NoSuchObject(key) from None

    def list(self, prefix=''):
        '''List keys in backend

        Returns an iterator over all keys in the backend.
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
        
        If `dest` already exists, it will be overwritten.
        """

        path_src = self._key_to_path(src)
        path_dest = self._key_to_path(dest)

        # Can't use shutil.copyfile() here, need to make
        # sure destination path exists
        try:
            dest = open(path_dest, 'wb')
        except FileNotFoundError:
            try:
                os.makedirs(os.path.dirname(path_dest))
            except FileExistsError:
                # Another thread may have created the directory already
                pass
            dest = open(path_dest, 'wb')

        try:
            with open(path_src, 'rb') as src:
                shutil.copyfileobj(src, dest, BUFSIZE)
        except FileNotFoundError:
            raise NoSuchObject(src) from None
        finally:
            dest.close()

    def rename(self, src, dest):
        """Rename key `src` to `dest`
        
        If `dest` already exists, it will be overwritten.
        """
        src_path = self._key_to_path(src)
        dest_path = self._key_to_path(dest)
        if not os.path.exists(src_path):
            raise NoSuchObject(src)

        try:
            os.rename(src_path, dest_path)
        except FileNotFoundError:
            try:
                os.makedirs(os.path.dirname(dest_path))
            except FileExistsError:
                # Another thread may have created the directory already
                pass
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
        path.append(key)

        return os.path.join(*path)

def escape(s):
    '''Escape '/', '=' and '.' in s'''

    s = s.replace('=', '=3D')
    s = s.replace('/', '=2F')
    s = s.replace('#', '=23')

    return s

def unescape(s):
    '''Un-Escape '/', '=' and '.' in s'''

    s = s.replace('=2F', '/')
    s = s.replace('=23', '#')
    s = s.replace('=3D', '=')

    return s


# Inherit from io.FileIO rather than io.BufferedReader to disable buffering. Default buffer size is
# ~8 kB (http://docs.python.org/3/library/functions.html#open), but backends are almost always only
# accessed by block_cache and stream_read_bz2/stream_write_bz2, which all use the much larger
# s3ql.common.BUFSIZE
class ObjectR(io.FileIO):
    '''A local storage object opened for reading'''


    def __init__(self, name, metadata=None):
        super().__init__(name)
        self.metadata = metadata

class ObjectW(object):
    '''A local storage object opened for writing'''

    def __init__(self, name):
        super().__init__()

        # Inherit from io.FileIO rather than io.BufferedReader to disable buffering. Default buffer
        # size is ~8 kB (http://docs.python.org/3/library/functions.html#open), but backends are
        # almost always only accessed by block_cache and stream_read_bz2/stream_write_bz2, which all
        # use the much larger s3ql.common.BUFSIZE
        self.fh = open(name, 'wb', buffering=0)
        
        self.obj_size = 0
        self.closed = False

    def write(self, buf):
        '''Write object data'''

        self.fh.write(buf)
        self.obj_size += len(buf)

    def close(self):
        '''Close object and upload data'''

        self.fh.close()
        self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()
        return False

    def get_obj_size(self):
        if not self.closed:
            raise RuntimeError('Object must be closed first.')
        return self.obj_size
