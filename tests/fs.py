#!/usr/bin/env python
#
#    Copyright (C) 2008  Nikolaus Rath <Nikolaus@rath.org>
#
#    This program can be distributed under the terms of the GNU LGPL.
#

from tests import TestCase, assert_true, assert_equals, assert_raises
import tempfile
import s3ql


class fs(TestCase):
    """Tests s3ql.fs and s3ql.file modules
    """

    def __init__(self, cb):
        self.cb = cb

        self.bucket = s3ql.s3.LocalBucket()
        self.bucket.tx_delay = 0
        self.bucket.prop_delay = 0

        self.dbfile = tempfile.mktemp()
        self.cachedir = tempfile.mkdtemp() + "/"
        self.blocksize = 1024

        self.data =

        s3ql.setup_db(self.dbfile, self.blocksize)
        s3ql.setup_bucket(self.bucket, self.dbfile)

        self.server = s3ql.fs(self.bucket, self.dbfile, self.cachedir)



    def test_mt(self):
        """ Checks multithreading
        """



    # Check that s3 object locking works when retrieving

    # Check that s3 object locking works when creating

    # Check that s3 objects are committed after fsync

    def destroy(self):

        self.server.close()
        os.unlink(self.dbfile)
        os.rmdir(self.cachedir)
