'''
local.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

import _thread
import io
import logging
import os
import struct

from ..common import ThawError, freeze_basic_mapping, thaw_basic_mapping
from .common import AbstractBackend, CorruptedObjectError, DanglingStorageURLError, NoSuchObject

log = logging.getLogger(__name__)


class Backend(AbstractBackend):
    '''
    A backend that stores data on the local hard disk
    '''

    needs_login = False
    known_options = set()

    def __init__(self, options):
        '''Initialize local backend'''

        # Unused argument
        # pylint: disable=W0613

        super().__init__()
        self.prefix = options.storage_url[len('local://') :].rstrip('/')

        if not os.path.exists(self.prefix):
            raise DanglingStorageURLError(self.prefix)

    @property
    def has_delete_multi(self):
        return True

    def __str__(self):
        return 'local directory %s' % self.prefix

    def is_temp_failure(self, exc):  # IGNORE:W0613
        return False

    def lookup(self, key):
        path = self._key_to_path(key)
        try:
            with open(path, 'rb') as src:
                return _read_meta(src)
        except FileNotFoundError:
            raise NoSuchObject(key)

    def get_size(self, key):
        return os.path.getsize(self._key_to_path(key))

    def open_read(self, key):
        path = self._key_to_path(key)
        try:
            fh = ObjectR(path)
        except FileNotFoundError:
            raise NoSuchObject(key)

        try:
            fh.metadata = _read_meta(fh)
        except ThawError:
            fh.close()
            raise CorruptedObjectError('Invalid metadata')
        return fh

    def open_write(self, key, metadata=None, is_compressed=False):
        if metadata is None:
            metadata = dict()
        elif not isinstance(metadata, dict):
            raise TypeError('*metadata*: expected dict or None, got %s' % type(metadata))

        path = self._key_to_path(key)
        buf = freeze_basic_mapping(metadata)
        if len(buf).bit_length() > 16:
            raise ValueError('Metadata too large')

        # By renaming, we make sure that there are no
        # conflicts between parallel reads, the last one wins
        tmpname = '%s#%d-%d.tmp' % (path, os.getpid(), _thread.get_ident())

        dest = ObjectW(tmpname)
        os.rename(tmpname, path)

        dest.write(b's3ql_1\n')
        dest.write(struct.pack('<H', len(buf)))
        dest.write(buf)

        return dest

    def contains(self, key):
        path = self._key_to_path(key)
        try:
            os.lstat(path)
        except FileNotFoundError:
            return False
        return True

    def delete_multi(self, keys, force=False):
        for (i, key) in enumerate(keys):
            try:
                self.delete(key, force=force)
            except:
                del keys[:i]
                raise

        del keys[:]

    def delete(self, key, force=False):
        path = self._key_to_path(key)
        try:
            os.unlink(path)
        except FileNotFoundError:
            if force:
                pass
            else:
                raise NoSuchObject(key)

    def list(self, prefix=''):

        if prefix:
            base = os.path.dirname(self._key_to_path(prefix))
        else:
            base = self.prefix

        for (path, dirnames, filenames) in os.walk(base, topdown=True):

            # Do not look in wrong directories
            if prefix:
                rpath = path[len(self.prefix) :]  # path relative to base
                prefix_l = ''.join(rpath.split('/'))

                dirs_to_walk = list()
                for name in dirnames:
                    prefix_ll = unescape(prefix_l + name)
                    if prefix_ll.startswith(prefix[: len(prefix_ll)]):
                        dirs_to_walk.append(name)
                dirnames[:] = dirs_to_walk

            for name in filenames:
                # Skip temporary files
                if '#' in name:
                    continue

                key = unescape(name)

                if not prefix or key.startswith(prefix):
                    yield key

    def _key_to_path(self, key):
        '''Return path for given key'''

        # NOTE: We must not split the path in the middle of an
        # escape sequence, or list() will fail to work.

        key = escape(key)

        if not key.startswith('s3ql_data_'):
            return os.path.join(self.prefix, key)

        no = key[10:]
        path = [self.prefix, 's3ql_data_']
        for i in range(0, len(no), 3):
            path.append(no[:i])
        path.append(key)

        return os.path.join(*path)


def _read_meta(fh):
    buf = fh.read(9)
    if not buf.startswith(b's3ql_1\n'):
        raise CorruptedObjectError('Invalid object header: %r' % buf)

    len_ = struct.unpack('<H', buf[-2:])[0]
    try:
        return thaw_basic_mapping(fh.read(len_))
    except ThawError:
        raise CorruptedObjectError('Invalid metadata')


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

    def close(self, checksum_warning=True):
        '''Close object

        The *checksum_warning* parameter is ignored.
        '''
        super().close()


class ObjectW:
    '''A local storage object opened for writing'''

    def __init__(self, name):
        super().__init__()

        # Default buffer size is ~8 kB
        # (http://docs.python.org/3/library/functions.html#open), but backends
        # are almost always only accessed by block_cache and
        # stream_read_bz2/stream_write_bz2, which all use the much larger
        # s3ql.common.BUFSIZE - so we may just as well disable buffering.

        # Create parent directories as needed
        try:
            self.fh = open(name, 'wb', buffering=0)
        except FileNotFoundError:
            try:
                os.makedirs(os.path.dirname(name))
            except FileExistsError:
                # Another thread may have created the directory already
                pass
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
