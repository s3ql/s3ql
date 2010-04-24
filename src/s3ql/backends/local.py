'''
local.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function, absolute_import

from .common import AbstractConnection, AbstractBucket, COMPRESS_ZLIB
import shutil
import logging
import cPickle as pickle
import os
import errno

log = logging.getLogger("backend.local")

class Connection(AbstractConnection):
    """A connection that stores buckets on the local disk"""

    def delete_bucket(self, name, recursive=False):
        """Delete bucket"""

        if not os.path.exists(name):
            raise KeyError('Directory of local bucket does not exist')

        if recursive:
            shutil.rmtree(name)
        else:
            os.rmdir(name)

    def create_bucket(self, name, passphrase=None):
        """Create and return a bucket"""

        if os.path.exists(name):
            raise RuntimeError('Bucket already exists')
        os.mkdir(name)

        return self.get_bucket(name, passphrase)

    def get_bucket(self, name, passphrase=None, compression=COMPRESS_ZLIB):
        """Return a bucket instance for the bucket `name`
        
        Raises `KeyError` if the bucket does not exist.
        """

        if not os.path.exists(name):
            raise KeyError('Local bucket directory %s does not exist' % name)
        return Bucket(name, passphrase, compression)


class Bucket(AbstractBucket):
    '''A bucket that is stored on the local hard disk'''

    def __init__(self, name, passphrase, compression):
        super(Bucket, self).__init__(passphrase, compression)
        self.name = name

    def __str__(self):
        if self.passphrase:
            return '<encrypted local bucket, name=%r>' % self.name
        else:
            return '<local bucket, name=%r>' % self.name


    def clear(self):
        """Delete all objects in bucket"""

        for name in os.listdir(self.name):
            if not name.endswith('.dat'):
                continue
            os.unlink(os.path.join(self.name, name))
            os.unlink(os.path.join(self.name, name[:-4]) + '.meta')

    def contains(self, key):
        filename = escape(key) + '.dat'
        return filename in os.listdir(self.name)

    def raw_lookup(self, key):
        filename = os.path.join(self.name, escape(key))
        try:
            with open(filename + '.meta', 'rb') as src:
                return pickle.load(src)
        except IOError as exc:
            if exc.errno == errno.ENOENT:
                raise KeyError('Key %r not in bucket' % key)
            else:
                raise

    def delete(self, key, force=False):
        """Deletes the specified key

        ``bucket.delete(key)`` can also be written as ``del bucket[key]``.
        If `force` is true, do not return an error if the key does not exist.
        """

        filename = os.path.join(self.name, escape(key))
        try:
            os.unlink(filename + '.dat')
            os.unlink(filename + '.meta')
        except IOError as exc:
            if exc.errno == errno.ENOENT:
                if force:
                    pass
                else:
                    raise KeyError('Key %r not in bucket' % key)
            else:
                raise


    def list(self, prefix=''):
        """List keys in bucket

        Returns an iterator over all keys in the bucket.
        """

        for name in os.listdir(self.name):
            if not name.endswith('.dat'):
                continue
            key = unescape(name[:-len('.dat')])
            if not key.startswith(prefix):
                continue
            yield key

    def get_size(self):
        """Get total size of bucket"""

        size = 0
        for name in os.listdir(self.name):
            if not name.endswith('.dat'):
                continue
            size += os.path.getsize(os.path.join(self.name, name))

        return size

    def raw_fetch(self, key, fh):
        filename = os.path.join(self.name, escape(key))
        try:
            with open(filename + '.dat', 'rb') as src:
                fh.seek(0)
                shutil.copyfileobj(src, fh)
            with open(filename + '.meta', 'rb') as src:
                metadata = pickle.load(src)
        except IOError as exc:
            if exc.errno == errno.ENOENT:
                raise KeyError('Key %r not in bucket' % key)
            else:
                raise

        return metadata

    def raw_store(self, key, fh, metadata):
        filename = os.path.join(self.name, escape(key))
        fh.seek(0)
        with open(filename + '.dat', 'wb') as dest:
            shutil.copyfileobj(fh, dest)
        with open(filename + '.meta', 'wb') as dest:
            pickle.dump(metadata, dest, 2)

    def copy(self, src, dest):
        """Copy data stored under `src` to `dest`"""

        if not isinstance(src, str):
            raise TypeError('key must be of type str')

        if not isinstance(dest, str):
            raise TypeError('key must be of type str')

        filename_src = os.path.join(self.name, escape(src))
        filename_dest = os.path.join(self.name, escape(dest))

        if not os.path.exists(filename_src + '.dat'):
            raise KeyError('source key does not exist')

        shutil.copyfile(filename_src + '.dat', filename_dest + '.dat')
        shutil.copyfile(filename_src + '.meta', filename_dest + '.meta')


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

