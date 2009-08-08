#!/usr/bin/env python
#
#    Copyright (C) 2008  Nikolaus Rath <Nikolaus@rath.org>
#
#    This program can be distributed under the terms of the GNU LGPL.
#

from __future__ import unicode_literals
import unittest
from s3ql import mkfs, s3,  fsck
from s3ql.database import WrappedConnection
from s3ql.common import ROOT_INODE
import os
import stat
import tempfile
import apsw
import time

# TODO: Find a way to suppress the fsck log warnings
# when we expect errors
class fsck_tests(unittest.TestCase):

    def setUp(self):
        self.bucket = s3.LocalBucket()
        self.bucket.tx_delay = 0
        self.bucket.prop_delay = 0

        self.dbfile = tempfile.NamedTemporaryFile()
        self.cachedir = tempfile.mkdtemp() + "/"
        self.blocksize = 1024

        self.conn = WrappedConnection(apsw.Connection(self.dbfile.name), retrytime=0)
        mkfs.setup_db(self.conn, self.blocksize)
        self.checker = fsck.Checker(self.conn, self.cachedir, self.bucket, checkonly=False)
        
    def tearDown(self):
        self.dbfile.close()
        os.rmdir(self.cachedir)        
        

    def test_parameters(self):
        self.conn.execute('DELETE FROM parameters')
        self.assertRaises(fsck.FatalFsckError, self.checker.check_parameters)
        
    def test_cache(self):
        fh = open(self.cachedir + 'testkey', 'wb')
        fh.write('somedata')
        fh.close()
        
        self.assertFalse(self.checker.check_cache())
        self.assertTrue(self.checker.check_cache())
        
        self.assertEquals(self.bucket['testkey'], 'somedata')
        
    def test_dirs(self):
        inode = 42
        self.conn.execute("INSERT INTO inodes (id, mode,uid,gid,mtime,atime,ctime,refcount) "
                   "VALUES (?,?,?,?,?,?,?,?)", 
                   (inode, stat.S_IFDIR | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR
                   | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH,
                    os.getuid(), os.getgid(), time.time(), time.time(), time.time(), 1))
        
        # Create a new directory without . and ..
        self.conn.execute('INSERT INTO contents (name, inode, parent_inode) VALUES(?,?,?)',
                        (b'testdir', inode, ROOT_INODE))
        
        self.assertFalse(self.checker.check_dirs())
        self.assertTrue(self.checker.check_dirs())
        
        # and another with wrong entries
        self.conn.execute('UPDATE contents SET inode=? WHERE name=? AND parent_inode=?',
                        (ROOT_INODE, b'.', inode))
        self.assertFalse(self.checker.check_dirs())
        self.assertTrue(self.checker.check_dirs())
        
        
        self.conn.execute('UPDATE contents SET inode=? WHERE name=? AND parent_inode=?',
                        (inode, b'..', inode))
        
        self.assertFalse(self.checker.check_dirs())
        self.assertTrue(self.checker.check_dirs())
               
    def test_lof1(self):
        
        # Make lost+found a file
        inode = self.conn.get_val("SELECT inode FROM contents WHERE name=? AND parent_inode=?", 
                                (b"lost+found", ROOT_INODE))
        self.conn.execute('DELETE FROM contents WHERE parent_inode=?', (inode,))
        self.conn.execute('UPDATE inodes SET mode=?, size=? WHERE id=?',
                        (stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR, 0, inode))
        
        self.assertFalse(self.checker.check_lof())
        self.assertTrue(self.checker.check_lof())
    
    def test_lof2(self):    
        # Remove lost+found
        self.conn.execute('DELETE FROM contents WHERE name=? and parent_inode=?',
                        (b'lost+found', ROOT_INODE))
         
        self.assertFalse(self.checker.check_lof())
        self.assertTrue(self.checker.check_lof())
        

# Somehow important according to pyunit documentation
def suite():
    return unittest.makeSuite(fsck_tests)


# Allow calling from command line
if __name__ == "__main__":
    unittest.main()
