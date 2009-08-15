#!/usr/bin/env python
#
#    Copyright (C) 2008  Nikolaus Rath <Nikolaus@rath.org>
#
#    This program can be distributed under the terms of the GNU LGPL.
#

from __future__ import unicode_literals    
import tempfile
import unittest
from time import time
from s3ql import mkfs, s3
from s3ql.common import ROOT_INODE
from s3ql.database import WrappedConnection
import apsw
import stat
import os


class sqlite_tests(unittest.TestCase): 

    def setUp(self):
        self.bucket = s3.LocalBucket()
        self.bucket.tx_delay = 0
        self.bucket.prop_delay = 0

        self.dbfile = tempfile.NamedTemporaryFile()
        self.cachedir = tempfile.mkdtemp() + "/"
        self.blocksize = 1024

        self.conn = WrappedConnection(apsw.Connection(self.dbfile.name).cursor(),
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
        
                   
    def test_inodes_mode(self):
        """Check that inodes can only have one type.
        
        Note that not all combinations are forbidden, for example
        ``S_IFREG|S_IFDIR == S_IFSOCK``.
        """
        conn = self.conn
        
        # Create a file
        self.assertRaises(apsw.ConstraintError,
                          conn.execute,
                          "INSERT INTO inodes (mode,uid,gid,mtime,atime,ctime,refcount) "
                          "VALUES (?,?,?,?,?,?,?)",
                          (stat.S_IFCHR | stat.S_IFIFO, 
                           os.getuid(), os.getgid(), time(), time(), time(), 1))
        
        
    def test_blocks(self):
        """Check that inodes with associated blocks must be regular files.
        
        """
        conn = self.conn
        s3key = 'foo'
        
        # Create an s3 object
        conn.execute('INSERT INTO s3_objects (key, refcount) VALUES(?, ?)',
                    (s3key, 1))
                    
        # Create a file
        inode = conn.rowid("INSERT INTO inodes (mode,uid,gid,mtime,atime,ctime,refcount,size) "
                           "VALUES (?,?,?,?,?,?,?,?)",
                           (stat.S_IFREG, os.getuid(), os.getgid(), time(), time(), time(), 1, 0))
        
        # Insert  the datablock
        conn.execute('INSERT INTO blocks (inode, blockno, s3key) VALUES(?, ?, ?)',
                    (inode, 0, s3key))
        
        # Try to change the file type
        self.assertRaises(apsw.ConstraintError, conn.execute, 
                          "UPDATE inodes SET mode=?, size=? WHERE id=?",
                          (stat.S_IFIFO, None, inode))  
        
        # Remove the link, then it should work
        conn.execute('DELETE FROM blocks WHERE inode=?', (inode,))
        conn.execute("UPDATE inodes SET mode=?, size=? WHERE id=?",
                    (stat.S_IFIFO, None, inode)) 
        
        # Try to insert a link to a directory
        self.assertRaises(apsw.ConstraintError,
                          conn.execute,
                          "INSERT INTO blocks (inode, blockno, s3key) VALUES(?, ?, ?)",
                          (ROOT_INODE, 0, s3key))
              
              
# Somehow important according to pyunit documentation
def suite():
    return unittest.makeSuite(sqlite_tests)

# Allow calling from command line
if __name__ == "__main__":
    unittest.main()