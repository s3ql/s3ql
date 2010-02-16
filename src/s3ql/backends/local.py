'''
$Id$

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function, absolute_import

from .common import AbstractConnection
from . import s3
from time import sleep
from contextlib import contextmanager
import shutil
import logging
import threading
import errno
import cPickle as pickle
import os

log = logging.getLogger("backend.local")

# For testing 
# Don't change randomly, these values are fine tuned
# for the tests to work without too much time.
LOCAL_TX_DELAY = 0.1
LOCAL_PROP_DELAY = 0.4

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
        """Create and return an S3 bucket"""

        if os.path.exists(name):
            raise RuntimeError('Bucket already exists')
        os.mkdir(name)

        return self.get_bucket(name, passphrase)

    def get_bucket(self, name, passphrase=None):
        """Return a bucket instance for the bucket `name`
        
        Raises `KeyError` if the bucket does not exist.
        """

        if not os.path.exists(name):
            raise KeyError('Local bucket directory %s does not exist' % name)
        return Bucket(self, name, passphrase)


class Bucket(s3.Bucket):
    '''A bucket that is stored on the local hard disk'''

    def __init__(self, conn, name, passphrase):
        super(Bucket, self).__init__(conn, name, passphrase)
        self.bbucket = LocalBotoBucket(name)

    @contextmanager
    def _get_boto(self):
        '''Provide boto bucket object'''

        yield self.bbucket

class LocalBotoKey(dict):
    '''
    Pretends to be a boto S3 key.
    '''

    def __init__(self, bucket, name, meta):
        super(LocalBotoKey, self).__init__()
        self.bucket = bucket
        self.name = name
        self.metadata = meta

    def get_contents_to_file(self, fh):
        log.debug("LocalBotoKey: get_contents_to_file() for %s", self.name)

        if self.name in self.bucket.in_transmit:
            raise ConcurrencyError()

        self.bucket.in_transmit.add(self.name)
        sleep(LOCAL_TX_DELAY)
        self.bucket.in_transmit.remove(self.name)

        filename = os.path.join(self.bucket.name, escape(self.name))
        with open(filename + '.dat', 'rb') as src:
            fh.seek(0)
            shutil.copyfileobj(src, fh)
        with open(filename + '.meta', 'rb') as src:
            self.metadata = pickle.load(src)

    def set_contents_from_file(self, fh):
        log.debug("LocalBotoKey: set_contents_from_file() for %s", self.name)

        if self.name in self.bucket.in_transmit:
            raise ConcurrencyError()

        self.bucket.in_transmit.add(self.name)
        sleep(LOCAL_TX_DELAY)
        self.bucket.in_transmit.remove(self.name)

        filename = os.path.join(self.bucket.name, escape(self.name))
        fh.seek(0)
        with open(filename + '.tmp', 'wb') as dest:
            shutil.copyfileobj(fh, dest)
        with open(filename + '.mtmp', 'wb') as dest:
            pickle.dump(self.metadata, dest, 2)

        def set_():
            sleep(LOCAL_PROP_DELAY)
            log.debug("LocalBotoKey: Committing store for %s", self.name)
            try:
                os.rename(filename + '.tmp', filename + '.dat')
                os.rename(filename + '.mtmp', filename + '.meta')
            except OSError as e:
                # Quick successive calls of store may fail, because they
                # overwrite an existing .tmp file, which is already
                # renamed by an earlier thread when the current thread tries
                # to rename.
                if e.errno == errno.ENOENT:
                    pass
                else:
                    raise

        t = threading.Thread(target=set_)
        t.start()

    def set_metadata(self, key, val):
        self.metadata[key] = val

    def get_metadata(self, key):
        return self.metadata[key]


class LocalBotoBucket(object):
    """
    Represents a bucket stored on a local directory and emulates an artificial propagation delay and
    transmit time.

    It tries to raise ConcurrencyError if several threads try to write or read the same object at a time
    (but it cannot guarantee to catch these cases).
    
    The class relies on the keys not including '/' and not ending in .dat or .meta, otherwise strange and
    dangerous things will happen.
    """

    def __init__(self, name):
        super(LocalBotoBucket, self).__init__()
        self.name = name
        self.in_transmit = set()

    def delete_key(self, key):
        log.debug("LocalBotoBucket: Handling delete_key(%s)", key)
        if key in self.in_transmit:
            raise ConcurrencyError
        self.in_transmit.add(key)
        sleep(LOCAL_TX_DELAY)
        self.in_transmit.remove(key)

        filename = os.path.join(self.name, escape(key))
        if not os.path.exists(filename + '.dat'):
            raise KeyError('Key does not exist in bucket')

        def set_():
            sleep(LOCAL_PROP_DELAY)
            log.debug("LocalBotoBucket: Committing delete_key(%s)", key)
            os.unlink(filename + '.dat')
            os.unlink(filename + '.meta')

        threading.Thread(target=set_).start()

    def list(self, prefix=''):
        # We add the size attribute outside init
        #pylint: disable-msg=W0201
        log.debug("LocalBotoBucket: Handling list()")
        for name in os.listdir(self.name):
            if not name.endswith('.dat'):
                continue
            key = unescape(name[:-len('.dat')])
            if not key.startswith(prefix):
                continue
            el = LocalBotoKey(self, key, dict())
            el.size = os.path.getsize(os.path.join(self.name, name))
            yield el

    def get_key(self, key):
        log.debug("LocalBotoBucket: Handling get_key(%s)", key)
        if key in self.in_transmit:
            raise ConcurrencyError
        self.in_transmit.add(key)
        sleep(LOCAL_TX_DELAY)
        self.in_transmit.remove(key)
        filename = os.path.join(self.name, escape(key))
        if os.path.exists(filename + '.dat'):
            with open(filename + '.meta', 'rb') as src:
                metadata = pickle.load(src)
            return LocalBotoKey(self, key, metadata)
        else:
            return None

    def new_key(self, key):
        return LocalBotoKey(self, key, dict())

    def copy_key(self, dest, src_bucket, src):
        log.debug("LocalBotoBucket: Received copy from %s to %s", src, dest)

        if dest in self.in_transmit or src in self.in_transmit:
            raise ConcurrencyError
        self.in_transmit.add(src)
        self.in_transmit.add(dest)
        sleep(LOCAL_TX_DELAY)

        filename_src = os.path.join(src_bucket, escape(src))
        filename_dest = os.path.join(self.name, escape(dest))

        if not os.path.exists(filename_src + '.dat'):
            raise KeyError('source key does not exist')

        def set_():
            sleep(LOCAL_PROP_DELAY)
            log.debug("LocalBotoBucket: Committing copy from %s to %s", src, dest)
            shutil.copyfile(filename_src + '.dat', filename_dest + '.dat')
            shutil.copyfile(filename_src + '.meta', filename_dest + '.meta')

        threading.Thread(target=set_).start()
        self.in_transmit.remove(dest)
        self.in_transmit.remove(src)
        log.debug("LocalBotoBucket: Returning from copy %s to %s", src, dest)


class ConcurrencyError(Exception):
    """Raised if several threads try to access the same s3 object
    """
    pass


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

