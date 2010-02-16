'''
$Id$

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function

import unittest
from s3ql import mkfs, fsck
from s3ql.backends import local
from s3ql.database import WrappedConnection
import apsw
from s3ql.common import ROOT_INODE
from _common import TestCase
import os
import stat
import tempfile
import time
import shutil

# TODO: Rewrite this test case

# Each test should correspond to exactly one function in the tested
# module, and testing should be done under the assumption that any
# other functions that are called by the tested function work perfectly.
class fsck_tests(TestCase):

    def setUp(self):
        self.bucket_dir = tempfile.mkdtemp()
        self.passphrase = 'schnupp'
        self.bucket = local.Connection().get_bucket(self.bucket_dir, self.passphrase)
        self.dbfile = tempfile.NamedTemporaryFile()
        self.cachedir = tempfile.mkdtemp() + "/"
        self.blocksize = 1024

        self.conn = WrappedConnection(apsw.Connection(self.dbfile.name),
                                      retrytime=0)
        mkfs.setup_db(self.conn, self.blocksize)

        fsck.conn = self.conn
        fsck.cachedir = self.cachedir
        fsck.bucket = self.bucket
        fsck.checkonly = False
        fsck.expect_errors = True
        fsck.found_errors = False

    def tearDown(self):
        self.dbfile.close()
        shutil.rmtree(self.cachedir)
        shutil.rmtree(self.bucket_dir)

    def test_detect(self):
        self.conn.execute('DELETE FROM parameters')
        self.assertRaises(fsck.FatalFsckError, fsck.detect_fs)

    def assert_fsck(self, fn):
        '''Check that fn detects and corrects an error'''

        fsck.found_errors = False
        fn()
        self.assertTrue(fsck.found_errors)
        fsck.found_errors = False
        fn()
        self.assertFalse(fsck.found_errors)

    def test_cache(self):
        inode = 6
        self.conn.execute("INSERT INTO inodes (id, mode,uid,gid,mtime,atime,ctime,refcount) "
                   "VALUES (?,?,?,?,?,?,?,?)",
                   (inode, stat.S_IFDIR | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR
                   | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH,
                    os.getuid(), os.getgid(), time.time(), time.time(), time.time(), 1))

        fh = open(self.cachedir + 'inode_%d_block_1' % inode, 'wb')
        fh.write('somedata')
        fh.close()

        self.assert_fsck(fsck.check_cache)
        self.assertEquals(self.bucket['s3ql_data_1'], 'somedata')

    def test_lof1(self):

        # Make lost+found a file
        inode = self.conn.get_val("SELECT inode FROM contents WHERE name=? AND parent_inode=?",
                                (b"lost+found", ROOT_INODE))
        self.conn.execute('DELETE FROM contents WHERE parent_inode=?', (inode,))
        self.conn.execute('DROP TRIGGER inodes_check_parent_inode_update')
        self.conn.execute('UPDATE inodes SET mode=?, size=? WHERE id=?',
                        (stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR, 0, inode))

        self.assert_fsck(fsck.check_lof)

    def test_lof2(self):
        # Remove lost+found
        self.conn.execute('DELETE FROM contents WHERE name=? and parent_inode=?',
                        (b'lost+found', ROOT_INODE))

        self.assert_fsck(fsck.check_lof)

    def test_inode_refcount(self):

        conn = self.conn

        # Create an orphaned inode
        conn.execute("INSERT INTO inodes (mode,uid,gid,mtime,atime,ctime,refcount,size) "
                     "VALUES (?,?,?,?,?,?,?,?)",
                     (stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR,
                      0, 0, time.time(), time.time(), time.time(), 2, 0))

        self.assert_fsck(fsck.check_inode_refcount)

        # Create an inode with wrong refcount
        inode = conn.rowid("INSERT INTO inodes (mode,uid,gid,mtime,atime,ctime,refcount,size) "
                           "VALUES (?,?,?,?,?,?,?,?)",
                           (stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR,
                            0, 0, time.time(), time.time(), time.time(), 1, 0))
        conn.execute('INSERT INTO contents (name, inode, parent_inode) VALUES(?, ?, ?)',
                     (b'name1', inode, ROOT_INODE))
        conn.execute('INSERT INTO contents (name, inode, parent_inode) VALUES(?, ?, ?)',
                     (b'name2', inode, ROOT_INODE))

        self.assert_fsck(fsck.check_inode_refcount)


    def test_keylist(self):
        # Create an object that only exists in s3
        self.bucket['s3ql_data_4364'] = 'Testdata'
        self.assert_fsck(fsck.check_keylist)

        # Create an object that does not exist in S3
        self.conn.execute('INSERT INTO s3_objects (id, refcount, size) VALUES(?, ?, ?)',
                          (34, 1, 0))
        self.assert_fsck(fsck.check_keylist)

    @staticmethod
    def random_data(len_):
        with open("/dev/urandom", "rb") as fd:
            return fd.read(len_)

    def test_loops(self):
        conn = self.conn

        # Create some directory inodes  
        inodes = [ conn.rowid("INSERT INTO inodes (mode,uid,gid,mtime,atime,ctime,refcount) "
                              "VALUES (?,?,?,?,?,?,?)",
                              (stat.S_IFDIR | stat.S_IRUSR | stat.S_IWUSR,
                               0, 0, time.time(), time.time(), time.time(), 3))
                   for dummy in range(3) ]

        inodes.append(inodes[0])
        last = inodes[0]
        for inode in inodes[1:]:
            conn.execute('INSERT INTO contents (name, inode, parent_inode) VALUES(?, ?, ?)',
                         (bytes(inode), inode, last))
            last = inode


        fsck.found_errors = False
        fsck.check_inode_refcount()
        self.assertFalse(fsck.found_errors)
        fsck.check_loops()
        self.assertTrue(fsck.found_errors)
        # We can't fix loops yet


    def test_s3_refcounts(self):
        conn = self.conn
        s3key = 42
        inode = 42
        conn.execute("INSERT INTO inodes (id, mode,uid,gid,mtime,atime,ctime,refcount,size) "
                     "VALUES (?,?,?,?,?,?,?,?,?)",
                     (inode, stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR,
                      os.getuid(), os.getgid(), time.time(), time.time(), time.time(), 1, 0))

        conn.execute('INSERT INTO s3_objects (id, refcount, size) VALUES(?, ?, ?)',
                     (s3key, 2, 0))
        conn.execute('INSERT INTO blocks (inode, blockno, s3key) VALUES(?, ?, ?)',
                     (inode, 1, s3key))
        conn.execute('INSERT INTO blocks (inode, blockno, s3key) VALUES(?, ?, ?)',
                     (inode, 2, s3key))

        fsck.found_errors = False
        fsck.check_s3_refcounts()
        self.assertFalse(fsck.found_errors)

        conn.execute('INSERT INTO blocks (inode, blockno, s3key) VALUES(?, ?, ?)',
                     (inode, 3, s3key))
        self.assert_fsck(fsck.check_s3_refcounts)

# Somehow important according to pyunit documentation
def suite():
    return unittest.makeSuite(fsck_tests)

if __name__ == "__main__":
    unittest.main()
