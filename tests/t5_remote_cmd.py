'''
$Id$

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import unicode_literals, division, print_function 

import os
from random import randrange
from s3ql.common import waitfor
from s3ql import s3
import tempfile
import posixpath
import unittest
import _awscred
import sys
import subprocess

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
        passphrase = 'blafasel'
        
        cmd =  os.path.join(os.path.dirname(__file__), "..", "bin", "mkfs.s3ql")
        child = subprocess.Popen([cmd, "--blocksize", "1", '--quiet',
                                           '--encrypt', self.bucketname], stdin=subprocess.PIPE)
        child.communicate('%s\n%s\n' % (passphrase, passphrase))
        self.assertEquals(child.returncode, 0)
               
        cmd = os.path.join(os.path.dirname(__file__), "..", "bin", "mount.s3ql")
        child = subprocess.Popen([cmd, "--cachesize", "1", '--quiet', self.bucketname, self.base],
                                 stdin=subprocess.PIPE)
        child.stdin.write('%s\n' % passphrase)  
        self.assertEqual(child.wait(), 0)     
               
        # Wait for mountpoint to come up
        self.assertTrue(waitfor(10, posixpath.ismount, self.base))

        # Umount as soon as mountpoint is no longer in use
        self.assertTrue(waitfor(5, lambda : 
                                subprocess.call(['fuser', '-m', '-s', self.base]) == 1))
        path = os.path.join(os.path.dirname(__file__), "..", "bin", "umount.s3ql")            
        self.assertEquals(subprocess.call([path, '--quiet', self.base]), 0)
        
        # Now wait for server process
        self.assertEquals(child.wait(), 0)
        self.assertFalse(posixpath.ismount(self.base))
            
        os.rmdir(self.base)
        
        cmd = os.path.join(os.path.dirname(__file__), "..", "bin", "fsck.s3ql")
        child = subprocess.Popen([cmd, "--quiet", self.bucketname], stdin=subprocess.PIPE)
        child.stdin.write('%s\n' % passphrase)  
        self.assertEqual(child.wait(), 0)     


# Somehow important according to pyunit documentation
def suite():
    return unittest.makeSuite(RemoteCmdTests)


# Allow calling from command line
if __name__ == "__main__":
    unittest.main()
