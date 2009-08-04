#!/usr/bin/env python
#
#    Copyright (C) 2008  Nikolaus Rath <Nikolaus@rath.org>
#
#    This program can be distributed under the terms of the GNU LGPL.
#

import unittest
from s3ql import mkfs, s3, fs, fsck
from s3ql.s3cache import S3Cache
from s3ql.cursor_manager import CursorManager
from s3ql.common import ROOT_INODE
import os
import stat
import tempfile
import time

class fsck_tests(unittest.TestCase):

    def setUp(self):
        self.bucket = s3.LocalBucket()
        self.bucket.tx_delay = 0
        self.bucket.prop_delay = 0

        self.dbfile = tempfile.NamedTemporaryFile()
        self.cachedir = tempfile.mkdtemp() + "/"
        self.blocksize = 1024

        self.cm = CursorManager(self.dbfile.name)
        mkfs.setup_db(self.cm, self.blocksize)

        self.cache = S3Cache(self.bucket, self.cachedir, self.blocksize * 5, 
                             self.blocksize, self.cm)
        self.cache.timeout = 1
        self.server = fs.Server(self.cache, self.cm)        
        
        self.checker = fsck.Checker(self.cm, self.cachedir, self.bucket, checkonly=False)
        
    def tearDown(self):
        self.cache.close()
        self.dbfile.close()
        os.rmdir(self.cachedir)        
        

    def test_parameters(self):
        self.cm.execute('DELETE FROM parameters')
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
        self.cm.execute("INSERT INTO inodes (id, mode,uid,gid,mtime,atime,ctime,refcount) "
                   "VALUES (?,?,?,?,?,?,?,?)", 
                   (inode, stat.S_IFDIR | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR
                   | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH,
                    os.getuid(), os.getgid(), time.time(), time.time(), time.time(), 1))
        
        # Create a new directory without . and ..
        self.cm.execute('INSERT INTO contents (name, inode, parent_inode) VALUES(?,?,?)',
                        ('testdir', inode, ROOT_INODE))
        
        self.assertFalse(self.checker.check_dirs())
        self.assertTrue(self.checker.check_dirs())
        
        # and another with wrong entries
        self.cm.execute('UPDATE contents SET inode=? WHERE name=? AND parent_inode=?',
                        (ROOT_INODE, '.', inode))
        self.assertFalse(self.checker.check_dirs())
        self.assertTrue(self.checker.check_dirs())
        
        
        self.cm.execute('UPDATE contents SET inode=? WHERE name=? AND parent_inode=?',
                        (inode, '..', inode))
        
        self.assertFalse(self.checker.check_dirs())
        self.assertTrue(self.checker.check_dirs())
               
    def test_lof1(self):
        
        # Make lost+found a file
        inode = self.cm.get_val("SELECT inode FROM contents WHERE name=? AND parent_inode=?", 
                                ("lost+found", ROOT_INODE))
        self.cm.execute('DELETE FROM contents WHERE parent_inode=?', (inode,))
        self.cm.execute('UPDATE inodes SET mode=?, size=? WHERE id=?',
                        (stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR, 0, inode))
        
        self.assertFalse(self.checker.check_lof())
        self.assertTrue(self.checker.check_lof())
    
    def test_lof2(self):    
        # Remove lost+found
        self.cm.execute('DELETE FROM contents WHERE name=? and parent_inode=?',
                        ('lost+found', ROOT_INODE))
         
        self.assertFalse(self.checker.check_lof())
        self.assertTrue(self.checker.check_lof())
        

# Somehow important according to pyunit documentation
def suite():
    return unittest.makeSuite(fsck_tests)


# Allow calling from command line
if __name__ == "__main__":
    unittest.main()
