'''
$Id$

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function

import tempfile
import unittest 
from time import time
from s3ql import mkfs, s3
from s3ql.common import ROOT_INODE
from _common import TestCase 
from s3ql.database import WrappedConnection
import apsw
import stat
import os


class sqlite_tests(TestCase): 

    def setUp(self):
        self.bucket =  s3.LocalConnection().create_bucket('foobar', 'brazl')

        self.dbfile = tempfile.NamedTemporaryFile()
        self.cachedir = tempfile.mkdtemp() + "/"
        self.blocksize = 1024

        self.conn = WrappedConnection(apsw.Connection(self.dbfile.name),
                                      retrytime=0)
        mkfs.setup_db(self.conn, self.blocksize)
        

    def tearDown(self):
        self.dbfile.close()
        os.rmdir(self.cachedir)
        
    def test_contents_parent_inode(self):
        """Check that parent_inode can only be a directory
        """
        conn = self.conn

        # Try to insert a fishy name
        self.assertRaises(apsw.ConstraintError, conn.execute, 
                          "INSERT INTO contents (name, inode, parent_inode) VALUES(?,?,?)",
                          (b"foo/bar", ROOT_INODE, ROOT_INODE))
                
        # Create a file
        inode = conn.rowid("INSERT INTO inodes (mode,uid,gid,mtime,atime,ctime,refcount,size) "
                           "VALUES (?,?,?,?,?,?,?,?)",
                           (stat.S_IFREG, os.getuid(), os.getgid(), time(), time(), time(), 1, 0))
        conn.execute("INSERT INTO contents (name, inode, parent_inode) VALUES(?,?,?)",
                   (b"testfile", inode, ROOT_INODE))
                           
        # Try to create a file with a file as parent
        inode2 = conn.rowid("INSERT INTO inodes (mode,uid,gid,mtime,atime,ctime,refcount,size) "
                            "VALUES (?,?,?,?,?,?,?,0)",
                            (stat.S_IFREG, os.getuid(), os.getgid(), time(), time(), time(), 1))
        self.assertRaises(apsw.ConstraintError, conn.execute, 
                          "INSERT INTO contents (name, inode, parent_inode) VALUES(?,?,?)",
                          (b"testfile2", inode, inode2))
        
        # Try to change the type of an inode
        self.assertRaises(apsw.ConstraintError, conn.execute, 
                          "UPDATE inodes SET mode=? WHERE id=?",
                          (stat.S_IFREG, ROOT_INODE))               
        
              
# Somehow important according to pyunit documentation
def suite():
    return unittest.makeSuite(sqlite_tests)

# Allow calling from command line
if __name__ == "__main__":
    unittest.main()