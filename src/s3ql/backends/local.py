'''
local.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from ..logging import logging # Ensure use of custom logger class
from ..common import BUFSIZE, PICKLE_PROTOCOL
from ..inherit_docstrings import (copy_ancestor_docstring, ABCDocstMeta)
from .common import AbstractBackend, DanglingStorageURLError, NoSuchObject, ChecksumError
import _thread
import io
import os
import pickle
import shutil

log = logging.getLogger(__name__)

class Backend(AbstractBackend, metaclass=ABCDocstMeta):
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
        return 'local directory %s' % self.name

    @copy_ancestor_docstring
    def is_temp_failure(self, exc): #IGNORE:W0613
        return False

    @copy_ancestor_docstring
    def lookup(self, key):
        path = self._key_to_path(key)
        try:
            with open(path, 'rb') as src:
                return pickle.load(src)
        except FileNotFoundError:
            raise NoSuchObject(key)
        except pickle.UnpicklingError as exc:
            if (isinstance(exc.args[0], str)
                and exc.args[0].startswith('invalid load key')):
                raise ChecksumError('Invalid metadata')
            raise

    @copy_ancestor_docstring
    def get_size(self, key):
        return os.path.getsize(self._key_to_path(key))

    @copy_ancestor_docstring
    def open_read(self, key):
        path = self._key_to_path(key)
        try:
            fh = ObjectR(path)
        except FileNotFoundError:
            raise NoSuchObject(key)
        
        try:
            fh.metadata = pickle.load(fh)
        except pickle.UnpicklingError as exc:
            if (isinstance(exc.args[0], str)
                and exc.args[0].startswith('invalid load key')):
                raise ChecksumError('Invalid metadata')
            raise
        return fh

    @copy_ancestor_docstring
    def open_write(self, key, metadata=None, is_compressed=False):
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

    @copy_ancestor_docstring
    def clear(self):
        for name in os.listdir(self.name):
            path = os.path.join(self.name, name)
            if os.path.isdir(path):
                shutil.rmtree(path)
            else:
                os.unlink(path)

    @copy_ancestor_docstring
    def contains(self, key):
        path = self._key_to_path(key)
        try:
            os.lstat(path)
        except FileNotFoundError:
            return False
        return True

    @copy_ancestor_docstring
    def delete(self, key, force=False):
        path = self._key_to_path(key)
        try:
            os.unlink(path)
        except FileNotFoundError:
            if force:
                pass
            else:
                raise NoSuchObject(key)

    @copy_ancestor_docstring
    def list(self, prefix=''):

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

    @copy_ancestor_docstring
    def copy(self, src, dest):
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
            raise NoSuchObject(src)
        finally:
            dest.close()

    @copy_ancestor_docstring
    def rename(self, src, dest):
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
