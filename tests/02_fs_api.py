#!/usr/bin/env python
#
#    Copyright (C) 2008  Nikolaus Rath <Nikolaus@rath.org>
#
#    This program can be distributed under the terms of the GNU LGPL.
#

import tempfile
import unittest
import s3ql
import os

class fs_api_tests(unittest.TestCase):

    def setUp(self):
        self.bucket = s3ql.s3.LocalBucket()
        self.bucket.tx_delay = 0
        self.bucket.prop_delay = 0

        self.dbfile = tempfile.mktemp()
        self.cachedir = tempfile.mkdtemp() + "/"
        self.blocksize = 1024

        s3ql.setup_db(self.dbfile, self.blocksize)
        s3ql.setup_bucket(self.bucket, self.dbfile)

        self.server = s3ql.fs(self.bucket, self.dbfile, self.cachedir)


    def testMt(self):
        """ Checks multithreading
        """



    # Check that s3 object locking works when retrieving

    # Check that s3 object locking works when creating

    # Check that s3 objects are committed after fsync

    def destroy(self):

        self.server.close()
        os.unlink(self.dbfile)
        os.rmdir(self.cachedir)


# Somehow important according to pyunit documentation
def suite():
    return unittest.makeSuite(fs_api_tests)


# Allow calling from command line
if __name__ == "__main__":
            unittest.main()
