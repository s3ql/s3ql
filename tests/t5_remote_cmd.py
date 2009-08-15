#!/usr/bin/env python
#
#    Copyright (C) 2008  Nikolaus Rath <Nikolaus@rath.org>
#
#    This program can be distributed under the terms of the GNU LGPL.
#

from __future__ import unicode_literals
import os
from random import randrange
from s3ql.common import waitfor
from s3ql import s3
import tempfile
import time
import posixpath
import unittest
import _awscred
import sys

# Allow invocation without runall.py
main = sys.modules['__main__']
if not hasattr(main, 'aws_credentials'):
    main.aws_credentials = _awscred.get()
    
@unittest.skipUnless(main.aws_credentials, 'remote tests disabled')
class RemoteCmdTests(unittest.TestCase): 
    
    def setUp(self):
        self.base = tempfile.mkdtemp()

        # Find unused bucket
        (awskey, awspass) = main.aws_credentials
        self.conn = s3.Connection(awskey, awspass)
        
        self.bucketname = self.random_name()
        tries = 10
        while self.conn.bucket_exists(self.bucketname) and tries > 10:
            self.bucketname = self.random_name()
            tries -= 1
            
        if tries == 0:
            raise RuntimeError("Failed to find an unused bucket name.")


    def tearDown(self):
        self.conn.delete_bucket(self.bucketname, recursive=True)   
            
    @staticmethod
    def random_name():
        return "s3ql" + str(randrange(10000, 99999, 1))

    
    def test_mount(self):
     
        cmd = os.path.join(os.path.dirname(__file__), "..", "mkfs.s3ql")
        ret = os.spawnl(os.P_WAIT, cmd, "mkfs.s3ql", "--blocksize", "1",
                        '--quiet', self.bucketname)       
        self.assertTrue(ret == 0)

        cmd = os.path.join(os.path.dirname(__file__), "..", "mount.s3ql")
        pid = os.spawnl(os.P_NOWAIT, cmd, "mount.s3ql", "--cachesize", "1",
                        '--quiet', self.bucketname, self.base)       
               
        # Wait for mountpoint to come up
        self.assertTrue(waitfor(10, posixpath.ismount, self.base))

        # Umount
        time.sleep(2)
        
        self.assertEquals(os.spawnlp(os.P_WAIT, "fusermount",
                                     "fusermount", "-u", self.base), 0)
        
        (dummy, status) = os.waitpid(pid, 0)

        self.assertTrue(os.WIFEXITED(status))
        self.assertEquals(os.WEXITSTATUS(status), 0)
        self.assertFalse(posixpath.ismount(self.base))
        os.rmdir(self.base)
        
        cmd = os.path.join(os.path.dirname(__file__), "..", "fsck.s3ql")
        ret = os.spawnl(os.P_WAIT, cmd, "fsck.s3ql", "--quiet", self.bucketname)       
        self.assertTrue(ret == 0) 
        


# Somehow important according to pyunit documentation
def suite():
    return unittest.makeSuite(RemoteCmdTests)


# Allow calling from command line
if __name__ == "__main__":
    unittest.main()
