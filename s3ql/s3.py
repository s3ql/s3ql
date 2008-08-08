#!/usr/bin/env python
#
#    Copyright (C) 2008  Nikolaus Rath <Nikolaus@rath.org>
#
#    This program can be distributed under the terms of the GNU LGPL.
#
import hashlib
from time import time, sleep
from datetime import datetime
from boto.s3.connection import S3Connection
import boto.exception
import threading

class Connection(object):
    """Represents a connection to Amazon S3

    Currently, this just dispatches everything to boto.
    """

    def __init__(self, awskey, awspass, encrypt=None):
        self.awskey = awskey
        self.awspass = awspass
        self.encrypt = encrypt

        self.boto = S3Connection(awskey, awspass)

    def delete_bucket(self, name, recursive=False):
        """Deletes a bucket from S3.

        Fails if there are objects in the bucket and `recursive` is
        `False`.
        """
        if recursive:
            bucket = self.boto.get_bucket(bucketname)
            for s3key in bucket.list():
                bucket.delete_key(s3key)

        boto.delete_bucket(bucketname)

    def create_bucket(self, name):
        """Creates an S3 bucket
        """
        boto.create_bucket(bucketname)


    def get_bucket(self, name):
        """Returns a bucket instance for the bucket `name`

        If the bucket doesn't exist, it is created first.
        """
        if not self.bucket_exists(name):
            self.create_bucket(name)
            # S3 needs some time before we can fetch the bucket
            sleep(5)
        else:
            # FIXME: Check that bucket encryption password is correct
            pass

        return Bucket(self, name)

    def bucket_exists(self, name):
        """Checks if the bucket `name` exists.
        """
        try:
            self.boto.get_bucket(name)
        except boto.exception.S3ResponseError, e:
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
        self.bucket = conn.boto.get_bucket(name)


    def __str__(self):
        return "<bucket: %s>" % self.name

    def __getitem__(self, key):
        return self.fetch(key)[0]

    def __setitem__(self, key, value):
        self.store(key, value)

    def __delitem__(self, key):
        self.delete_key(key)

    def __iter__(self):
        for (key,metadata) in self.list_keys():
            yield key

    def lookup_key(self, key):
        """Return metadata for given key.

        If the key does not exist, `None` is returned. Otherwise a
        `Metadata` instance is returned.
        """
        bkey = self.bucket.get_key(key)

        if bkey:
            return self.boto_key_to_metadata(bkey)
        else:
            return None

    def delete_key(self, key):
        """Deletes the specified key

        ``bucket.delete_key(key)`` can also be written as ``del bucket[key]``.

        """
        self.bucket.delete_key(key)

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

        return self.bucket.list()


    def fetch(self, key):
        """Return data stored under `key`.

        Returns a tuple with the data and metadata. If only the data
        itself is required, ``bucket[key]`` is a more concise notation
        for ``bucket.fetch(key)[0]``.
        """

        bkey = self.bucket.new_key(key)
        val = bkey.get_contents_as_string()
        metadata = self.boto_key_to_metadata(bkey)

        return (val, metadata)

    def store(self, key, val):
        """Store data under `key`.

        Returns the newly associated metadata as returned by
        `lookup_key`.

        If the metadata is not required, one can simply assign to the
        subscripted bucket instead of using this function:
        ``bucket[key] = val`` is equivalent to ``bucket.store(key,
        val)``.
        """

        bkey = self.bucket.new_key(key)
        bkey.set_contents_from_string(val)

        return self.boto_key_to_metadata(bkey)

    def fetch_to_file(self, key, file):
        """Fetches data stored under `key` and writes them to `file`.

        `file` has to be a file name. Returns the metadata in the
        format used by `lookup_key`.
        """
        bkey = self.bucket.new_key(key)
        bkey.get_contents_to_filename(file)

        return self.boto_key_to_metadata(bkey)

    def store_from_file(self, key, file):
        """Reads `file` and stores the data under `key`

        `file` has to be a file name. Returns the metadata in the
        format used by `lookup_key`.
        """
        bkey = self.bucket.new_key(key)
        bkey.set_contents_from_filename(file)

        return self.boto_key_to_metadata(bkey)

    def boto_key_to_metadata(self, bkey):
        """Extracts metadata from boto key object.
        """

        meta = Metadata(bkey.metadata)
        meta.etag = bkey.etag
        if bkey.last_modified is not None:
            meta.last_modified = datetime.strptime(
                bkey.last_modified, "%a, %d %b %Y %H:%M:%S %Z")
        else:
            meta.last_modified = None
        meta.size = bkey.size

        return meta



class LocalBucket(Bucket):
    """Represents a bucket stored in Amazon S3.


    This class doesn't actually connect but holds all data in
    memory. It is meant only for testing purposes. It emulates a
    propagation delay of 5 seconds and a transmit time of 5 seconds.
    It raises ConcurrencyError if several threads try to write or read
    the same object at a time.
    """

    def __init__(self, name="local"):
        self.keys = {}
        self.name = name
        self.in_transmit = set()
        self.tx_delay = 5
        self.prop_delay = 5


    def lookup_key(self, key):
        if key in self.in_transmit:
            raise ConcurrencyError
        self.in_transmit.add(key)
        sleep(self.tx_delay)
        self.in_transmit.remove(key)
        if not self.keys.has_key(key):
            return None
        else:
            return self.keys[key][0]

    def list_keys(self):
        for key in self.keys:
            yield self.keys[key]

    def delete_key(self, key):
        if key in self.in_transmit:
            raise ConcurrencyError
        self.in_transmit.add(key)
        sleep(self.tx_delay)
        del self.keys[key]
        self.in_transmit.remove(key)

    def fetch(self, key):
        if key in self.in_transmit:
            raise ConcurrencyError
        self.in_transmit.add(key)
        sleep(self.tx_delay)
        self.in_transmit.remove(key)
        return self.keys[key]

    def store(self, key, val):
        if key in self.in_transmit:
            raise ConcurrencyError
        self.in_transmit.add(key)
        sleep(self.tx_delay)
        metadata = Metadata()
        metadata.key = key
        metadata.size = len(val)
        metadata.last_modified = datetime.now()
        metadata.etag =  hashlib.md5(val).hexdigest()
        def set():
            sleep(self.prop_delay)
            self.keys[key] = (val, metadata)
        t = threading.Thread(target=set)
        t.start()
        self.in_transmit.remove(key)
        return metadata


    def fetch_to_file(self, key, file):
        (data, metadata) = self.fetch(key)
        file = open(file, "wb")
        file.write(data)
        file.close()
        return metadata

    def store_from_file(self, key, file):
        file = open(file, "rb")
        value = file.read()
        file.close()

        return self.store(key, value)


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
