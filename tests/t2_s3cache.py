#!/usr/bin/env python
#
#    Copyright (C) 2008  Nikolaus Rath <Nikolaus@rath.org>
#
#    This program can be distributed under the terms of the GNU LGPL.
#

import unittest
import tempfile
import os
from s3ql import mkfs, s3, fs
from random import randrange


class s3cache_tests(unittest.TestCase):

    def setUp(self):
        self.bucket = s3.LocalBucket()
        self.bucket.tx_delay = 0
        self.bucket.prop_delay = 0

        self.dbfile = tempfile.NamedTemporaryFile()
        self.cachedir = tempfile.mkdtemp() + "/"
        self.blocksize = 1024

        mkfs.setup_db(self.dbfile.name, self.blocksize)
        mkfs.setup_bucket(self.bucket, self.dbfile.name)

        self.server = fs.Server(self.bucket, self.dbfile.name, self.cachedir)


    def tearDown(self):
        # May not have been called if a test failed
        if hasattr(self, "server"):
            self.server.close()
        self.dbfile.close()
        os.rmdir(self.cachedir)

    @staticmethod
    def random_name(prefix=""):
        return "s3ql" + prefix + str(randrange(100,999,1))
    
    @staticmethod   
    def random_data(len):
        fd = open("/dev/urandom", "rb")
        return fd.read(len)
      
    # Check creation and reading
    # Check download and reading 
    # Check flushing
    
    
    # Check that s3 object locking works when retrieving

    # Check that s3 object locking works when creating

    # Check that s3 objects are committed after fsync


def suite():
    return unittest.makeSuite(s3cache_tests)

if __name__ == "__main__":
    unittest.main()
