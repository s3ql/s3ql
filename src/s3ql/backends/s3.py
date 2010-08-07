'''
s3.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function, absolute_import

# Python boto uses several deprecated modules, deactivate warnings for them
import warnings
warnings.filterwarnings("ignore", "", DeprecationWarning, "boto")

from .common import AbstractConnection, AbstractBucket, NoSuchBucket, NoSuchObject
from time import sleep
from .boto.s3.connection import S3Connection
from contextlib import contextmanager
from .boto import exception
from s3ql.common import (TimeoutError, ExceptionStoringThread)
import logging
import errno
import httplib
import re
import time
import threading

log = logging.getLogger("backend.s3")

class Connection(AbstractConnection):
    """Represents a connection to Amazon S3

    This class just dispatches everything to boto. It uses a separate boto
    connection object for each thread.
    
    This class is threadsafe. All methods (except for internal methods
    starting with underscore) may be called concurrently by different
    threads.    
    """

    def __init__(self, awskey, awspass, reduced_redundancy=False):
        super(Connection, self).__init__()
        self.awskey = awskey
        self.awspass = awspass
        self.pool = list()
        self.lock = threading.RLock()
        self.conn_cnt = 0
        self.reduced_redundancy = reduced_redundancy

    def _pop_conn(self):
        '''Get boto connection object from the pool'''

        with self.lock:
            try:
                conn = self.pool.pop()
            except IndexError:
                # Need to create a new connection
                log.debug("Creating new boto connection (active conns: %d)...",
                          self.conn_cnt)
                conn = S3Connection(self.awskey, self.awspass)
                self.conn_cnt += 1
    
            return conn

    def _push_conn(self, conn):
        '''Return boto connection object to pool'''
        with self.lock:
            self.pool.append(conn)

    def delete_bucket(self, name, recursive=False):
        """Delete bucket"""

        if not recursive:
            with self._get_boto() as boto:
                boto.delete_bucket(name)
                return

        # Delete recursively
        with self._get_boto() as boto:
            step = 1
            waited = 0
            while waited < 600:
                try:
                    boto.delete_bucket(name)
                except exception.S3ResponseError as exc:
                    if exc.code != 'BucketNotEmpty':
                        raise
                else:
                    return
                self.get_bucket(name, passphrase=None).clear()
                time.sleep(step)
                waited += step
                step *= 2

            raise RuntimeError('Bucket does not seem to get empty')


    @contextmanager
    def _get_boto(self):
        """Provide boto connection object"""

        conn = self._pop_conn()
        try:
            yield conn
        finally:
            self._push_conn(conn)

    def create_bucket(self, name, location, passphrase=None,
                      compression='lzma'):
        """Create and return an S3 bucket
        
        Note that a call to `get_bucket` right after creation may fail,
        since the changes do not propagate instantaneously through AWS.
        """
        # Argument number deliberately differs from base class
        #pylint: disable-msg=W0221
        
        if not re.match('^[a-z][a-z0-9.-]{1,60}[a-z0-9]$', name):
            raise InvalidBucketNameError()

        with self._get_boto() as boto:
            try:
                boto.create_bucket(name, location=location)
            except exception.S3ResponseError as exc:
                if exc.code == 'InvalidBucketName':
                    raise InvalidBucketNameError()
                else:
                    raise

        return Bucket(self, name, passphrase, compression)

    def get_bucket(self, name, passphrase=None, compression='lzma'):
        """Return a bucket instance for the bucket `name`"""

        if not re.match('^[a-z][a-z0-9.-]{1,60}[a-z0-9]$', name):
            raise InvalidBucketNameError()
        
        with self._get_boto() as boto:
            try:
                boto.get_bucket(name)
            except exception.S3ResponseError as e:
                if e.status == 404:
                    raise NoSuchBucket(name)
                elif e.code == 'InvalidBucketName':
                    raise InvalidBucketNameError()
                else:
                    raise
        return Bucket(self, name, passphrase, compression)

class Bucket(AbstractBucket):
    """Represents a bucket stored in Amazon S3.

    This class should not be instantiated directly, but using
    `Connection.get_bucket()`.

    Due to AWS' eventual propagation model, we may receive e.g. a 'unknown
    bucket' error when we try to upload a key into a newly created bucket. For
    this reason, many boto calls are wrapped with `retry_boto`. Note that this
    assumes that no one else is messing with the bucket at the same time.
    
    This class is threadsafe. All methods (except for internal methods
    starting with underscore) may be called concurrently by different
    threads.    
    """

    @contextmanager
    def _get_boto(self):
        '''Provide boto bucket object'''
        # Access to protected methods ok
        #pylint: disable-msg=W0212

        boto_conn = self.conn._pop_conn()
        try:
            yield retry_boto(boto_conn.get_bucket, self.name)
        finally:
            self.conn._push_conn(boto_conn)

    def __init__(self, conn, name, passphrase, compression):
        super(Bucket, self).__init__(passphrase, compression)
        self.conn = conn
        self.name = name
        with self._get_boto() as boto:
            self.rac_consistent = (boto.get_location() != '')

    def clear(self):
        """Delete all objects in bucket
        
        This function starts multiple threads."""

        threads = list()
        for (no, s3key) in enumerate(self):
            if no != 0 and no % 1000 == 0:
                log.info('Deleted %d objects so far..', no)

            log.debug('Deleting key %s', s3key)

            # Ignore missing objects when clearing bucket
            t = ExceptionStoringThread(self.delete, s3key, True)
            t.start()
            threads.append(t)

            if len(threads) > 50:
                log.debug('50 threads reached, waiting..')
                threads.pop(0).join_and_raise()

        log.debug('Waiting for removal threads')
        for t in threads:
            t.join_and_raise()

    def __str__(self):
        if self.passphrase:
            return '<encrypted s3 bucket, name=%r>' % self.name
        else:
            return '<s3 bucket, name=%r>' % self.name

    def contains(self, key):
        with self._get_boto() as boto:
            bkey = retry_boto(boto.get_key, key)

        return bkey is not None

    def read_after_create_consistent(self):
        return self.rac_consistent

    def read_after_write_consistent(self):
        return False
             

                        
    
    def raw_lookup(self, key):
        '''Retrieve metadata for `key`
        
        If the key has been lost (S3 returns 405), it is automatically
        deleted so that it will no longer be returned by list_keys.
        '''
        with self._get_boto() as boto:
            bkey = _get_boto_key(boto, key)

        if bkey is None:
            raise NoSuchObject(key)

        return bkey.metadata

    def delete(self, key, force=False):
        """Deletes the specified key

        ``bucket.delete(key)`` can also be written as ``del bucket[key]``.
        If `force` is true, do not return an error if the key does not exist.
        """

        if not isinstance(key, str):
            raise TypeError('key must be of type str')

        with self._get_boto() as boto:
            if not force and retry_boto(boto.get_key, key) is None:
                raise NoSuchObject(key)

            retry_boto(boto.delete_key, key)

    def list(self, prefix=''):
        with self._get_boto() as boto:
            for bkey in boto.list(prefix):
                yield bkey.name

    def raw_fetch(self, key, fh):
        '''Fetch `key` and store in `fh`
        
        If the key has been lost (S3 returns 405), it is automatically
        deleted so that it will no longer be returned by list_keys.
        '''        
        
        with self._get_boto() as boto:
            bkey = _get_boto_key(boto, key)
                    
            if bkey is None:
                raise NoSuchObject(key)
            fh.seek(0)
            retry_boto(bkey.get_contents_to_file, fh)

        return bkey.metadata

    def raw_store(self, key, fh, metadata):
        with self._get_boto() as boto:
            bkey = boto.new_key(key)
            bkey.metadata.update(metadata)
            retry_boto(bkey.set_contents_from_file, fh,
                       reduced_redundancy=self.conn.reduced_redundancy)


    def copy(self, src, dest):
        if not isinstance(src, str):
            raise TypeError('key must be of type str')

        if not isinstance(dest, str):
            raise TypeError('key must be of type str')

        with self._get_boto() as boto:
            retry_boto(boto.copy_key, dest, self.name, src)

def _get_boto_key(boto, key):
    '''Get boto key object for `key`
    
    If the key has been lost (S3 returns 405), it is automatically
    deleted so that it will no longer be returned by list_keys.
    '''
    
    try:
        return retry_boto(boto.get_key, key)
    except exception.S3ResponseError as exc:
        if exc.error_code != 'MethodNotAllowed':
            raise
        
        # Object was lost
        log.warn('Object %s has been lost by Amazon, deleting..', key)
        retry_boto(boto.delete_key, key)
        return None
        
def retry_boto(fn, *a, **kw):
    """Wait for fn(*a, **kw) to succeed
    
    If `fn(*a, **kw)` raises any of
    
     - `boto.exception.S3ResponseError` with errorcode in
       (`NoSuchBucket`, `RequestTimeout`)
     - `IOError` with errno 104
     - `httplib.IncompleteRead` 
     
    the function is called again. If the timeout is reached, `TimeoutError` is raised.
    """

    step = 0.2
    timeout = 300
    waited = 0
    while waited < timeout:
        try:
            return fn(*a, **kw)
        except exception.S3ResponseError as exc:
            if exc.error_code in ('NoSuchBucket', 'RequestTimeout', 'InternalError'):
                log.warn('Encountered %s error when calling %s, retrying...',
                         exc.error_code, fn.__name__)
            else:
                raise
        except IOError as exc:
            if exc.errno == errno.ECONNRESET:
                pass
            else:
                raise
        except exception.S3CopyError as exc:
            if exc.error_code in ('RequestTimeout', 'InternalError'):
                log.warn('Encountered %s error when calling %s, retrying...',
                         exc.error_code, fn.__name__)
            else:
                raise
        except httplib.IncompleteRead as exc:
            log.warn('Encountered %s error when calling %s, retrying...',
                     exc.error_code, fn.__name__)
            
        sleep(step)
        waited += step
        if step < timeout / 30:
            step *= 2

    raise TimeoutError()

class InvalidBucketNameError(Exception):

    def __str__(self):
        return 'Bucket name contains invalid characters.'
