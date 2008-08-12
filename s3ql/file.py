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
    :path:   path of the opened file
    :inode:  inode of the opened file
    :timeout: Maximum time to wait for s3 propagation

    Attributes copied from fs instance for faster access:

    bucket, cachesize, blocksize
    """

    def __init__(self, fs, path, flags, mode=None):
        """Handles FUSE open() and create() requests.
        """

        self.fs = fs
        self.bucket = fs.bucket
        self.cachesize = fs.cachesize
        self.blocksize = fs.blocksize
        self.path = path
        self.timeout = 30

        # Create if not existing
        if mode:
            self.fs.mknod(path, mode)

        self.inode = self.fs.get_inode(path)
        assert self.inode > 0


        # FIXME: Apparenty required, even though passed as parameter to fuse
        self.direct_io = True
        self.keep_cache = None


    def read(self, length, offset):
        """Handles FUSE read() requests.

        May return less than `length` bytes, to the ``direct_io`` FUSE
        option has to be enabled.
        """

        # Calculate starting offset of next s3 object, we don't
        # read further than that
        offset_f = self.blocksize * (int(offset/self.blocksize)+1)
        if offset + length > offset_f:
            length = offset_f - offset

        # Obtain required s3 object
        offset_i = self.blocksize * int(offset/self.blocksize)
        s3key = io2s3key(self.inode, offset_i)

        self.fs.lock_s3key(s3key)
        try:
            fd = self.retrieve_s3(s3key)

            # If the object does not exist, we have a hole and return \0
            if fd is None:
                return "\0" * length

            # If we do not reach the desired position, then
            # we have a hole as well
            if os.lseek(fd,offset - offset_i, os.SEEK_SET) != offset - offset_i:
                return "\0" * length

            self.fs.update_atime(self.inode)
            return os.read(fd,length)
        finally:
            self.fs.unlock_s3key(s3key)


    def retrieve_s3(self, s3key, create=None):
        """Returns fd for s3 object `s3key`.

        If the s3 object is not already cached, it is retrieved from
        Amazon and put into the cache.

        If no such object exists and create is not None, the object is
        created with offset `create`. Otherwise, returns `None.

        The s3 key should already be locked when this function is called.
        """

        if create is not None:
            offset = int(create)

            if offset % self.blocksize != 0:
                raise Exception, "s3 objects must start at blocksize boundaries"

        cachefile = s3key[1:].replace("~", "~~").replace("/", "~")
        cachepath = self.fs.cachedir + cachefile

        # Check if existing
        res = self.fs.sql_list("SELECT fd, etag FROM s3_objects WHERE s3key=?",
                              (s3key,))

        # Existing Key
        if len(res):
            (fd, etag) = res[0]

        # New key
        else:
            if create is None:
                return None
            fd = os.open(cachepath, os.O_RDWR | os.O_CREAT)
            self.fs.sql("INSERT INTO s3_objects(s3key,dirty,fd,cachefile,atime,size,inode,offset) "
                        "VALUES(?,?,?,?,?,?,?,?)",
                        (s3key, True, fd, cachefile, time(), 0, self.inode, offset))

        # Not yet in cache
        if fd is None:
            self.expire_cache()
            meta = self.bucket.fetch_to_file(s3key, cachepath)

            # Check etag
            if meta.etag != etag:
                warn(["Changes in %s apparently have not yet propagated. Waiting and retrying...\n" % s3key,
                       "Try to increase the cache size to avoid this.\n"])
                waited = 0
                waittime = 0.01
                while meta.etag != etag and \
                        waited < self.timeout:
                    time.sleep(waittime)
                    waited += waittime
                    waittime *= 1.5
                    meta = self.bucket.lookup_key(s3key)

                # If still not found
                if meta.etag != etag:
                    error(["etag for %s doesn't match metadata!" % s3key,
                           "Filesystem is probably corrupted (or S3 is having problems), "
                           "run fsck.s3ql as soon as possible.\n"])
                    self.fs.mark_damaged()
                    raise FUSEError(errno.EIO)
                else:
                    meta = self.bucket.fetch_to_file(s3key, cachepath)

            fd = os.open(cachepath, os.O_RDWR)
            self.fs.sql("UPDATE s3_objects SET dirty=?,fd=?,cachefile=? "
                        "WHERE s3key=?", (False, fd, cachefile, s3key))


        # Update atime
        self.fs.sql("UPDATE s3_objects SET atime=? WHERE s3key=?", (time(), s3key))

        return fd

    def expire_cache(self):
        """Performs cache expiry.

        If the cache is bigger than `self.cachesize`, the oldest
        entries are flushed until at least `self.fs.blocksize`
        bytes are available.
        """

        used = self.fs.sql("SELECT SUM(size) FROM s3_objects WHERE fd IS NOT NULL") .next()[0]

        while used + self.fs.blocksize > self.cachesize:
            # Find & lock object to flush
            res  = self.fs.sql_list("SELECT s3key FROM s3_objects WHERE fd IS NOT NULL "
                                    "ORDER BY atime ASC LIMIT 1")

            # If there is nothing to flush, we continue anyway
            if not res:
                continue


            s3key = res[0][0]

            self.fs.lock_s3key(s3key)
            try:
                # Information may have changed while we waited for lock
                res = self.fs.sql_list("SELECT dirty,fd,cachefile,size FROM s3_objects "
                                       "WHERE s3key=?", (s3key,))
                if not res:
                    # has been deleted
                    continue

                (dirty,fd,cachefile,size) = res[0]
                if fd is None:
                    # already flushed now
                    continue

                # flush
                os.close(fd)
                meta = self.bucket.store_from_file(s3key, self.fs.cachedir + cachefile)
                self.fs.sql("UPDATE s3_objects SET dirty=?,fd=?,cachefile=?,etag=? "
                            "WHERE s3key=?", (False, None, None, meta.etag, s3key))
                os.unlink(self.cachedir + cachefile)
            finally:
                self.fs.unlock_s3key(s3key)

            used -= size


    def write(self, buf, offset):
        """Handles FUSE write() requests.

        May write less byets than given in `buf`, to the ``direct_io`` FUSE
        option has to be enabled.
        """

        # Obtain required s3 object
        offset_i = self.blocksize * int(offset/self.blocksize)
        s3key = io2s3key(self.inode, offset_i)

        # We write at most one block
        offset_f = offset_i + self.blocksize
        maxwrite = offset_f - offset

        self.fs.lock_s3key(s3key)
        try:
            fd = self.retrieve_s3(s3key, create=offset_i)

            # Determine number of bytes to write and write
            os.lseek(fd, offset - offset_i, os.SEEK_SET)
            if len(buf) > maxwrite:
                writelen = maxwrite
                writelen = os.write(fd, buf[:maxwrite])
            else:
                writelen = os.write(fd,buf)


            # Update object size
            obj_len = os.lseek(fd, 0, os.SEEK_END)
            self.fs.sql("UPDATE s3_objects SET size=? WHERE s3key=?",
                        (obj_len, s3key))

            # Update file size if changed
            res = self.fs.sql("SELECT s3key FROM s3_objects WHERE inode=? "
                              "AND offset > ?", (self.inode, offset_i))
            if not list(res):
                self.fs.sql("UPDATE inodes SET size=?,ctime=? WHERE id=?",
                            (offset_i + obj_len, time(), self.inode))

            # Update file mtime
            self.fs.update_mtime(self.inode)
            return writelen

        finally:
            self.fs.unlock_s3key(s3key)


    def ftruncate(self, len):
        """Handles FUSE ftruncate() requests.
        """


        # Delete all truncated s3 objects
        res = self.fs.sql("SELECT s3key FROM s3_objects WHERE "
                          "offset >= ? AND inode=? ORDER BY offset INC",
                          (len, self.inode))
        for (s3key,) in res:
            self.fs.lock_s3key(s3key)
            try:
                (fd, cachefile) = self.fs.sql_row("SELECT fd,cachefile FROM s3_objects "
                                                  "WHERE s3key=?", (s3key,))

                if fd: # File is in cache
                    os.close(fd)
                    os.unlink(self.fs.cachedir + cachefile)

                # Key may not yet been committed
                try:
                    self.bucket.delete_key(s3key)
                except KeyError:
                    pass

                self.fs.sql_sep("DELETE FROM s3_objects WHERE s3key=?",
                                (s3key,))
            finally:
                self.fs.unlock_s3key(s3key)


        # Get last object before truncation
        offset_i = self.blocksize * int( (len-1) / self.blocksize)
        s3key = io2s3key(self.inode, offset_i)

        self.fs.lock_s3key(s3key)
        try:
            fd = self.retrieve_s3(s3key, create=offset_i)
            cursize = offset_i + os.lseek(fd, 0, os.SEEK_END)

            # If we are actually extending this object, we just write a
            # 0-byte at the last position
            if len > cursize:
                os.lseek(fd, len - 1 - offset_i, os.SEEK_SET)
                os.write(fd, "\0")


            # Otherwise we truncate the file
            else:
                os.ftruncate(fd, len - offset_i)

            # Update file size
            self.fs.sql("UPDATE inodes SET size=? WHERE id=?",
                        (len, self.inode))
            self.fs.sql("UPDATE s3_objects SET size=?,dirty=? WHERE s3key=?",
                        (len - offset_i, True, s3key))

            # Update file's mtime
            self.fs.update_mtime(self.inode)
        finally:
            self.fs.unlock_s3key(s3key)

    def release(self, flags):
        """Handles FUSE release() requests.
        """
        pass


    def fsync(self, fdatasync):
        """Handles FUSE fsync() requests.

        We do not lock the s3 objects, because we do not remove them
        from the cache and we mark them as clean before(!) we send
        them to S3. This ensures that if another thread writes
        while we are still sending, the object is correctly marked
        dirty again and will be resent on the next fsync().
        """

        # Metadata is always synced automatically, so we ignore
        # fdatasync
        res = self.fs.sql("SELECT s3key, fd, cachefile FROM s3_objects WHERE "
                          "dirty=? AND inode=?", (True, self.inode))
        for (s3key, fd, cachefile) in res:
            self.fs.sql_sep("UPDATE s3_objects SET dirty=? WHERE s3key=?",
                        (False, s3key))
            os.fsync(fd)
            meta = self.bucket.store_from_file(s3key, self.fs.cachedir + cachefile)
            self.fs.sql_sep("UPDATE s3_objects SET etag=? WHERE s3key=?",
                        (meta.etag, s3key))


    # Called for close() calls. Here we sync the data, so that we
    # can still return write errors.
    def flush(self):
        """Handles FUSE flush() requests.
        """
        return self.fsync(False)

    def fgetattr(self):
        """Handles FUSE fgetattr() requests.
        """
        return self.fs.getattr(self.path)
