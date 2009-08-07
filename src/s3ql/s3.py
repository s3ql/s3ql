#
#    Copyright (C) 2008  Nikolaus Rath <Nikolaus@rath.org>
#
#    This program can be distributed under the terms of the GNU LGPL.
#

# pylint has serios trouble with the boto object
#pylint: disable-msg=E1103


from __future__ import unicode_literals
import hashlib
from time import sleep
from datetime import datetime
import isodate
from boto.s3.connection import S3Connection
import boto.exception as bex
import threading
import copy
from s3ql.common import (waitfor) 
import logging

__all__ = [ "Connection", "ConcurrencyError", "Bucket", "LocalBucket", "Metadata" ]

log = logging.getLogger("s3")
 
class Connection(object):
    """Represents a connection to Amazon S3

    Currently, this just dispatches everything to boto. Note
    that boto is not threadsafe, so we need to create a
    separate s3 connection for each thread.
    """

    def __init__(self, awskey, awspass, encrypt=None):
        self.awskey = awskey
        self.awspass = awspass
        self.encrypt = encrypt

        self.tlocal = threading.local()

        self.tlocal.boto = S3Connection(awskey, awspass)

    def delete_bucket(self, name, recursive=False):
        """Deletes a bucket from S3.

        Fails if there are objects in the bucket and `recursive` is
        `False`.
        """
        boto = self.get_boto()


        if recursive:
            bucket = boto.get_bucket(name)
            for s3key in bucket.list():
                log.debug('Deleting key %s', s3key)
                bucket.delete_key(s3key)

        boto.delete_bucket(name)

    def get_boto(self):
        """Returns boto s3 connection local to current thread.
        """

        if not hasattr(self.tlocal, "boto"):
            self.tlocal.boto = S3Connection(self.awskey, self.awspass)

        return self.tlocal.boto


    def create_bucket(self, name):
        """Creates an S3 bucket
        """
        boto = self.get_boto()
        boto.create_bucket(name)


    def get_bucket(self, name):
        """Returns a bucket instance for the bucket `name`

        If the bucket doesn't exist, it is created first.
        """
        if not self.bucket_exists(name):
            self.create_bucket(name)
            
            # S3 needs some time before we can fetch the bucket
            waitfor(10, self.bucket_exists, name)

        return Bucket(self, name)

    def bucket_exists(self, name):
        """Checks if the bucket `name` exists.
        """
        boto = self.get_boto()
        try:
            boto.get_bucket(name)
        except bex.S3ResponseError, e:
            if e.status == 404:
                return False
            else:
                raise
        else:
            return True



class Bucket(object):
    """Represents a bucket stored in Amazon S3.

    This class should not be instantiated directly, but using
    `Connection.get_bucket()`.

    The class behaves more or less like a dict. It raises the
    same exceptions, can be iterated over and indexed into.

    """

    def __init__(self, conn, name):
        self.name = name
        self.conn = conn

        self.tlocal = threading.local()
        self.tlocal.bucket = conn.get_boto().get_bucket(name)


    def get_boto(self):
        """Returns boto bucket object for current thread.
        """

        if not hasattr(self.tlocal, "boto"):
            self.tlocal.boto = self.conn.get_boto().get_bucket(self.name)

        return self.tlocal.boto


    def __str__(self):
        return "<bucket: %s>" % self.name

    def __getitem__(self, key):
        return self.fetch(key)[0]

    def __setitem__(self, key, value):
        self.store(key, value)

    def __delitem__(self, key):
        self.delete_key(key)

    def __iter__(self):
        return self.keys()

    def  __contains__(self, key):
        return self.has_key(key)

    def has_key(self, key):
        return self.lookup_key(key) is not None

    def iteritems(self):
        for key in self.keys():
            yield (key, self[key])

    def keys(self):
        for pair in self.list_keys():
            yield pair[0]

    def lookup_key(self, key):
        """Return metadata for given key.

        If the key does not exist, `None` is returned. Otherwise a
        `Metadata` instance is returned.
        """
 
        bkey = self.get_boto().get_key(key)

        if bkey:
            return self.boto_key_to_metadata(bkey)
        else:
            return None
  
    def delete_key(self, key, force=False):
        """Deletes the specified key

        ``bucket.delete_key(key)`` can also be written as ``del bucket[key]``.
        If `force` is true, do not return an error if the key does not exist.

        """
 
        boto = self.get_boto()
        if not force and not boto.get_key(key):
            raise KeyError
        boto.delete_key(key)


    def list_keys(self):
        """List keys in bucket

        Returns an iterator over all keys in the bucket. The iterator
        generates tuples of the form (key, metadata), where metadata is
        of the form returned by `lookup_key`.

        This function is also used if the bucket itself is used in an
        iterative context: ``for key in bucket`` is the same as ``for
        (key,metadata) in bucket.list_keys()`` (except that the former
        doesn't define the `metadata` variable).
        """

        for key in self.get_boto().list():
            yield (unicode(key.name), self.boto_key_to_metadata(key))


    def fetch(self, key):
        """Return data stored under `key`.

        Returns a tuple with the data and metadata. If only the data
        itself is required, ``bucket[key]`` is a more concise notation
        for ``bucket.fetch(key)[0]``.
        """
 
        bkey = self.get_boto().get_key(key)
        if not bkey:
            raise KeyError

        val = bkey.get_contents_as_string()
        metadata = self.boto_key_to_metadata(bkey)            
  
        return (val, metadata)

    def store(self, key, val):
        """Store data under `key`.

        Returns the etag of the data.

        If the metadata is not required, one can simply assign to the
        subscripted bucket instead of using this function:
        ``bucket[key] = val`` is equivalent to ``bucket.store(key,
        val)``.
        """
 
        bkey = self.get_boto().new_key(key)
        bkey.set_contents_from_string(val)

        return bkey.etag.rstrip('"').lstrip('"').encode('us-ascii')

            
    def fetch_to_file(self, key, file_):
        """Fetches data stored under `key` and writes them to `file_`.

        `file_` has to be a file name. Returns the metadata in the
        format used by `lookup_key`.
        """
 
        bkey = self.get_boto().get_key(key)
        if not bkey.exists():
            raise KeyError

        bkey.get_contents_to_filename(file_)
        metadata = self.boto_key_to_metadata(bkey)            


        return metadata

    def store_from_file(self, key, file_):
        """Reads `file` and stores the data under `key`

        `file` has to be a file name. Returns the etag.
        """

        bkey = self.get_boto().new_key(key)
        bkey.set_contents_from_filename(file_)

        return bkey.etag.rstrip('"').lstrip('"').encode('us-ascii')
 

    def copy(self, src, dest):
        """Copies data stored under `src` to `dest`
        """

        self.get_boto().copy_key(dest, self.name, src)

    @staticmethod
    def boto_key_to_metadata(bkey):
        """Extracts metadata from boto key object.
        """
        #pylint: disable-msg=W0201
        meta = Metadata(bkey.metadata)
        meta.etag = bkey.etag.rstrip('"').lstrip('"').encode('us-ascii')
        meta.key = bkey.name
        
        if bkey.last_modified is not None:
            # The format depends on how the data has been retrieved (fetch or list)
            try:
                meta.last_modified = datetime.strptime(bkey.last_modified, "%a, %d %b %Y %H:%M:%S %Z")
            except ValueError:
                meta.last_modified = isodate.parse_datetime(bkey.last_modified)
                
            # Convert to UTC if timezone aware
            if meta.last_modified.utcoffset():
                meta.last_modified = (meta.last_modified - meta.last_modified.utcoffset()) \
                    .replace(tzinfo=None)
                                                 
        else:
            meta.last_modified = None
        meta.size = int(bkey.size)

        return meta



class LocalBucket(Bucket):
    """Represents a bucket stored in Amazon S3.

    This class doesn't actually connect but holds all data in memory.
    It is meant only for testing purposes. It emulates an artificial
    propagation delay and transmit time.

    It tries to raise ConcurrencyError if several threads try to write or read
    the same object at a time (but it cannot guarantee to catch these cases).
    The reason for this is not that
    concurrent accesses to the same object were in itself harmful, but
    that they should not occur because the write(), sync() etc.
    systemcalls are synchronized. So if the s3 accesses occur
    concurrent, something went wrong with the syscall synchronization.
    """

    def __init__(self, name="local"):
        # We deliberately do not call the superclass constructor
        #pylint: disable-msg=W0231
        self.keystore = {}
        self.name = name
        self.in_transmit = set()
        self.tx_delay = 0
        self.prop_delay = 0


    def lookup_key(self, key):
        log.debug("LocalBucket: Received lookup for %s", key)
        if key in self.in_transmit:
            raise ConcurrencyError
        self.in_transmit.add(key)
        sleep(self.tx_delay)
        self.in_transmit.remove(key)
        if not self.keystore.has_key(key):
            return None
        else:
            return self.keystore[key][1]

    def list_keys(self):
        for key in self.keystore:
            yield (key, self.keystore[key][1])

    def delete_key(self, key, force=False):
        if key in self.in_transmit:
            raise ConcurrencyError
        log.debug("LocalBucket: Received delete for %s", key)
        self.in_transmit.add(key)
        sleep(self.tx_delay)
        self.in_transmit.remove(key)

        # Make sure the key exists, otherwise we get an error
        # in a different thread
        if not force and not self.keystore.has_key(key):
            raise KeyError

        def set_():
            sleep(self.prop_delay)
            log.debug("LocalBucket: Committing delete for %s", key)
            # Don't bother if some other thread already deleted it
            try:
                del self.keystore[key]
            except KeyError:
                pass

        threading.Thread(target=set_).start()

    def fetch(self, key):
        log.debug("LocalBucket: Received fetch for %s", key)
        if key in self.in_transmit:
            raise ConcurrencyError
        self.in_transmit.add(key)
        sleep(self.tx_delay)
        self.in_transmit.remove(key)
        return self.keystore[key]

    def store(self, key, val):
        log.debug("LocalBucket: Received store for %s", key)
        if key in self.in_transmit:
            raise ConcurrencyError
        self.in_transmit.add(key)
        sleep(self.tx_delay)
        metadata = Metadata()
        metadata.key = key
        metadata.size = len(val)
        metadata.last_modified = datetime.utcnow()
        metadata.etag =  hashlib.md5(val).hexdigest()
        def set_():
            sleep(self.prop_delay)
            log.debug("LocalBucket: Committing store for %s, etag %s", key, metadata.etag)
            self.keystore[key] = (val, metadata)
        t = threading.Thread(target=set_)
        t.start()
        self.in_transmit.remove(key)
        log.debug("LocalBucket: Returning from store for %s" % key)
        return metadata.etag


    def fetch_to_file(self, key, file_):
        (data, metadata) = self.fetch(key)
        file_ = open(file_, "wb")
        file_.write(data)
        file_.close()
        return metadata

    def store_from_file(self, key, file_):
        file_ = open(file_, "rb")
        value = file_.read()
        file_.close()

        return self.store(key, value)

    def copy(self, src, dest):
        """Copies data stored under `src` to `dest`
        """
        log.debug("LocalBucket: Received copy from %s to %s", src, dest)
        if dest in self.in_transmit or src in self.in_transmit:
            raise ConcurrencyError
        self.in_transmit.add(src)
        self.in_transmit.add(dest)
        sleep(self.tx_delay)
        def set_():
            sleep(self.prop_delay)
            log.debug("LocalBucket: Committing copy from %s to %s", src, dest)
            self.keystore[dest] = copy.deepcopy(self.keystore[src])
        threading.Thread(target=set_).start()
        self.in_transmit.remove(dest)
        self.in_transmit.remove(src)
        log.debug("LocalBucket: Returning from copy %s to %s", src, dest)


class Metadata(dict):
    """Represents the metadata associated with an S3 object.

    The "hardcoded" meta-information etag, size and last-modified are
    accessible as instance attributes. For access to user defined
    metadata, the instance has to be subscripted.

    Note that the last-modified attribute is a datetime object.
    """
    
    pass
        

class ConcurrencyError(Exception):
    """Raised if several threads try to access the same s3 object
    """
    pass
