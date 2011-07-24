'''
s3etal.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import division, print_function, absolute_import

from .common import AbstractConnection, AbstractBucket, NoSuchBucket, NoSuchObject
from contextlib import contextmanager
from s3ql.common import (TimeoutError, AsyncFn)
import logging
import errno
import httplib
import re
import time
import threading

log = logging.getLogger("backend.s3etal")

class Connection(AbstractConnection):
    """A connection to Amazon S3 compatible services

    This class is threadsafe. All methods (except for internal methods
    starting with underscore) may be called concurrently by different
    threads.    
    """

    def __init__(self, login, password, use_ssl):
        super(Connection, self).__init__()
        self.use_ssl = use_ssl


    def delete_bucket(self, name, recursive=False):
        """Delete bucket"""

        pass

    def create_bucket(self, name, location, passphrase=None,
                      compression='lzma'):
        """Create and return an S3 bucket
        
        Note that a call to `get_bucket` right after creation may fail,
        since the changes do not propagate instantaneously through AWS.
        """
        # Argument number deliberately differs from base class
        #pylint: disable-msg=W0221
        
        return Bucket(self, name, passphrase, compression)

    def check_name(self, name):
        '''Check if bucket name conforms to requirements
        
        Raises `InvalidBucketName` for invalid names.
        '''
        
        if (not re.match('^[a-z0-9][a-z0-9.-]{1,60}[a-z0-9]$', name) 
            or '..' in name
            or '.-' in name
            or '-.' in name
            or re.match('^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$', name)):
            raise InvalidBucketNameError()
        
    def get_bucket(self, name, passphrase=None, compression='lzma'):
        """Return a bucket instance for the bucket `name`"""

        self.check_name(name)
        return Bucket(self, name, passphrase, compression)

class Bucket(AbstractBucket):
    """A bucket stored in Amazon S3 and compatible services

    This class should not be instantiated directly, but using
    `Connection.get_bucket()`.
 
    This class is threadsafe. All methods (except for internal methods
    starting with underscore) may be called concurrently by different
    threads.    
    """

    def __init__(self, conn, name, passphrase, compression):
        super(Bucket, self).__init__(passphrase, compression)
        self.conn = conn
        self.name = name

    def clear(self):
        """Delete all objects in bucket
        
        This function starts multiple threads."""

        threads = list()
        for (no, s3key) in enumerate(self):
            if no != 0 and no % 1000 == 0:
                log.info('Deleted %d objects so far..', no)

            log.debug('Deleting key %s', s3key)

            # Ignore missing objects when clearing bucket
            t = AsyncFn(self.delete, s3key, True)
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
            return '<encrypted bucket, name=%r>' % self.name
        else:
            return '<bucket, name=%r>' % self.name

class InvalidBucketNameError(Exception):

    def __str__(self):
        return 'Bucket name contains invalid characters.'
