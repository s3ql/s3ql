#!/usr/bin/env python
#
#    Copyright (C) 2008  Nikolaus Rath <Nikolaus@rath.org>
#
#    This program can be distributed under the terms of the GNU LGPL.
#

import os
import sys
import errno
from time import time
from s3ql.common import *

# Check fuse version
import fuse
if not hasattr(fuse, '__version__'):
    raise RuntimeError, \
        "your fuse-py doesn't know of fuse.__version__, probably it's too old."
fuse.fuse_python_api = (0, 2)
fuse.feature_assert('stateful_files', 'has_init')

class file(object):
    """Class representing open files in s3qlfs.

    Attributes
    ----------

    :fs:     s3qlfs instance belonging to this file
    """

    def __init__(self, fs, *a, **kw):
        """Handles FUSE open() and create() requests.
        """

        self.fs = fs

        # FIXME: Apparenty required, even though passed as parameter to fuse
        self.direct_io = True
        self.keep_cache = None

        if len(a) == 0 and len(kw) == 0: # internal call
            return
        else:
            self.opencreate(*a, **kw)

    def opencreate(self, path, flags, mode=None):
        self.path = path
        self.flags = flags

        # Create if not existing
        if mode:
            self.fs.mknod_i(path, mode)

        self.inode = self.fs.get_inode(path)


    @fuse_request_handler
    def read(self, length, offset):
        """Handles FUSE read() requests.

        May return less than `length` bytes, to the ``direct_io`` FUSE
        option has to be enabled.
        """

        (s3key, offset_i, fd) = self.get_s3(offset)

        # If we do not reach the desired position, then
        # we have a hole and return \0
        if os.lseek(fd,offset - offset_i, os.SEEK_SET) != offset - offset_i:
            return "\0" * length

        self.fs.update_atime(self.inode)
        return os.read(fd,length)


    def get_s3(self, offset, create=False, offset_min=0):
        """Returns s3 object containing given file offset.

        Searches for an S3 object with minimum offset `offset_min` and maximum
        offset 'offset'. If the s3 object is not already cached, it is retrieved
        from Amazon and put into the cache.

        If no such object exists and create=True, the object is
        created with offset 'offset_min'.

        The return value is a tuple (s3key, offset, fd) or None if the object
        does not exist.
        """

        # FIXME: We need to lock the s3 object here

        try:
            # Try to find key
            (s3key, offset_i, fd, etag) = self.fs.sql(
                "SELECT s3key, offset, fd, etag FROM s3_objects WHERE inode = ? "
                "AND offset <= ? AND offset >= ? ORDER BY offset DESC LIMIT 1",
                (self.inode, offset, offset_min)).next()

            if fd is None:
                # FIXME: Check if there is space available in
                # the cache.
                fd = self.retrieve_s3(s3key, etag)

            self.fs.sql("UPDATE s3_objects SET atime=? WHERE s3key=?",
                        (time(), s3key))
            return (s3key, offset_i, fd)

        except StopIteration:
            if not create:
                return None
            else:
                # Create object with minimum offset
                offset_i = offset_min
                (s3key, fd) = self.create_s3(offset_i)
                return (s3key, offset_i, fd)
        finally:
            pass
            # FIXME: Here we should unlock


    def retrieve_s3(self, s3key, etag):
        """Retrieves an S3 object from Amazon and stores it in cache.

        Returns fd of the cachefile. No locking is done. If the etag
        doesn't match the retrieved data, the retrieval is repeated
        after a short interval (to cope with S3 propagation delays).
        In case of repeated failure, an error is returned.
        """

        cachefile = s3key[1:].replace("/", "_")
        cachepath = self.fs.cachedir + cachefile
        key = self.fs.bucket.new_key(s3key)
        key.get_contents_to_filename(cachepath)

        # Check etag
        if key.etag != etag:
            debug("etag mismatch, trying to refetch..")
            waited = 0
            waittime = 0.01
            while key.etag != etag and \
                    waited < self.fs.timeout:
                time.sleep(waittime)
                waited += waittime
                waittime *= 1.5
                key = self.bucket.get_key(s3key) # Updates metadata

            # If still not found
            if key.etag != etag:
                raise S3fsError("Object etag doesn't match metadata!",
                                s3key=s3key, inode=self.inode, path=self.path,
                                fatal=True)
            else:
                self.fs.error("etag mismatch for "+ s3key + ". Try increasing the cache size... ")
                key.get_contents_to_filename(cachepath)

        fd = os.open(cachepath, os.O_RDWR)
        self.fs.sql("UPDATE s3_objects SET dirty=?, fd=?, cachefile=? "
                    "WHERE s3key=?", (False, fd, cachefile, s3key))

        return fd

    def create_s3(self, offset):
        """Creates an S3 object with given file offset.

        Returns (s3key,fd). No locking is done and no checks are
        performed if the object already exists.
        """

        # Ensure that s3key is ASCII
        # FIXME: If encryption is activated, the key shouldn't correspond
        # to the path but to e.g. inode_offset (unique)
        s3key = repr(self.path)[1:-1] + "__" + ("%016d" % offset)
        cachefile = s3key[1:].replace("/", "_")
        cachepath = self.fs.cachedir + cachefile
        fd = os.open(cachepath, os.O_RDWR | os.O_CREAT)
        self.fs.sql(
            "INSERT INTO s3_objects(inode,offset,dirty,fd,s3key,cachefile, atime) "
            "VALUES(?,?,?,?,?,?,?)", (self.inode, offset, True, fd, s3key, cachefile, time()))

        k = self.fs.bucket.new_key(s3key)
        k.set_contents_from_string("dirty")
        return (s3key,fd)


    def write_i(self, buf, offset):
        """Handles FUSE write() requests.

        May write less byets than given in `buf`, to the ``direct_io`` FUSE
        option has to be enabled.
        """

        # Lookup the S3 object into we have to write. The object
        # should start not earlier than the last block boundary
        # before offset, because otherwise we would extend the
        # object beyound the desired blocksize
        (s3key, offset_i, fd) = self.get_s3(
            offset, create = True,
            offset_min = self.fs.blocksize * int(offset/self.fs.blocksize))

        try:
            # Offset of the next s3 object
            offset_f = self.fs.sql(
                "SELECT s3key FROM s3_objects WHERE inode = ? "
                "AND offset > ? ORDER BY offset ASC LIMIT 1",
                (self.inode, offset)).next()[0]
            # Filesize will not change with this write
            adjsize = False
        except StopIteration:
            # If there is no next s3 object, we also need to
            # update the filesize
            offset_f = None
            adjsize = True

        # We write at most one block
        if offset_f is None or offset_f > offset_i + self.fs.blocksize:
            offset_f = offset_i + self.fs.blocksize
        maxwrite = offset_f - offset_i

        # Determine number of bytes to write and write
        os.lseek(fd, offset - offset_i, os.SEEK_SET)
        if len(buf) > maxwrite:
            writelen = maxwrite
            writelen = os.write(fd, buf[:maxwrite])
        else:
            writelen = os.write(fd,buf)

        # Update filesize if we are working at the last chunk
        if adjsize:
            obj_len = os.lseek(fd, 0, os.SEEK_END)
            self.fs.sql("UPDATE inodes SET size=?,ctime=? WHERE id=?",
            (offset_i + obj_len, time(), self.inode))

        # Update file mtime
        self.fs.sql("UPDATE inodes SET mtime=? WHERE id = ?",
                            (time(), self.inode))
        return writelen
    write = fuse_request_handler(write_i)


    def ftruncate_i(self, len):
        """Handles FUSE ftruncate() requests.
        """

        # Delete all truncated s3 objects
        res = self.fs.sql("SELECT s3key,fd,cachefile FROM s3_objects WHERE "
                          "offset >= ? AND inode=?", (len,inode))

        for (s3key, fd, cachefile) in res:
            if fd: # File is in cache
                os.close(fd,0)
                os.unlink(self.fs.cachedir + cachefile)
            self.fs.bucket.delete_key(s3key)
        self.fs.sql("DELETE FROM s3_objects WHERE "
                            "offset >= ? AND inode=?", (len,inode))

        # Get last object before truncation
        (s3key, offset_i, fd) = self.get_s3(offset)
        cursize = offset_i + os.lseek(fd, 0, os.SEEK_END)

        # If we are actually extending the file, we just write a
        # 0-byte at the last position
        if len > cursize:
            self.write_i("\0", len-1)

        # Otherwise we truncate the file and update
        # the file size
        else:
            os.ftruncate(fd, len - offset_i)
            self.fs.sql("UPDATE inodes SET size=? WHERE id=?",
                                (len, self.inode))

        # Update file's mtime
        self.fs.sql("UPDATE inodes SET mtime=?,ctime=? WHERE id = ?",
                            (time(), time(), self.inode))
    ftruncate = fuse_request_handler(ftruncate_i)

    def release_i(self, flags):
        """Handles FUSE release() requests.
        """
        pass
    release = fuse_request_handler(release_i)


    def fsync_i(self, fdatasync):
        """Handles FUSE fsync() requests.
        """

        # Metadata is always synced automatically, so we ignore
        # fdatasync
        res = self.fs.sql(
            "SELECT s3key, fd, cachefile FROM s3_objects WHERE "
            "dirty=? AND inode=?", (True, self.inode))
        for (s3key, fd, cachefile) in res:
            # We need to mark as clean *before* we write the changes,
            # because otherwise we would loose changes that occured
            # while we wrote to S3
            self.fs.sql("UPDATE s3_objects SET dirty=? WHERE s3key=?",
                                (False, s3key))
            os.fsync(fd)
            key = self.fs.bucket.new_key(s3key)
            key.set_contents_from_filename(self.fs.cachedir + cachefile)
            self.fs.sql("UPDATE s3_objects SET etag=? WHERE s3key=?",
                        (key.etag, s3key))
    fsync = fuse_request_handler(fsync_i)


    # Called for close() calls. Here we sync the data, so that we
    # can still return write errors.
    @fuse_request_handler
    def flush(self):
        """Handles FUSE flush() requests.
        """
        return self.fsync_i(False)

    @fuse_request_handler
    def fgetattr(self):
        """Handles FUSE fgetattr() requests.
        """
        return self.fs.getattr_i(self.path)
