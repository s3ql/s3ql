'''
t3_fsck.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function

import unittest2 as unittest
from s3ql import mkfs, fsck
from s3ql.backends import local
from s3ql import database as dbcm
from s3ql.common import ROOT_INODE
from _common import TestCase
import os
import stat
import tempfile
import time
import shutil

class fsck_tests(TestCase):

    def setUp(self):
        self.bucket_dir = tempfile.mkdtemp()
        self.passphrase = 'schnupp'
        self.bucket = local.Connection().get_bucket(self.bucket_dir, self.passphrase)
        self.cachedir = tempfile.mkdtemp() + "/"
        self.blocksize = 1024

        self.dbfile = tempfile.NamedTemporaryFile()
        dbcm.init(self.dbfile.name)
        mkfs.setup_tables(dbcm)
        mkfs.init_tables(dbcm)

        fsck.conn = dbcm._pop_conn()
        fsck.cachedir = self.cachedir
        fsck.bucket = self.bucket
        fsck.checkonly = False
        fsck.expect_errors = True
        fsck.blocksize = self.blocksize
        fsck.found_errors = False

    def tearDown(self):
        shutil.rmtree(self.cachedir)
        shutil.rmtree(self.bucket_dir)

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
        dbcm.execute("INSERT INTO inodes (id, mode,uid,gid,mtime,atime,ctime,refcount) "
                   "VALUES (?,?,?,?,?,?,?,?)",
                   (inode, stat.S_IFDIR | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR
                   | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH,
                    os.getuid(), os.getgid(), time.time(), time.time(), time.time(), 1))

        fh = open(self.cachedir + 'inode_%d_block_1.d' % inode, 'wb')
        fh.write('somedata')
        fh.close()

        self.assert_fsck(fsck.check_cache)
        self.assertEquals(self.bucket['s3ql_data_1'], 'somedata')

        fh = open(self.cachedir + 'inode_%d_block_1' % inode, 'wb')
        fh.write('otherdata')
        fh.close()

        self.assert_fsck(fsck.check_cache)
        self.assertEquals(self.bucket['s3ql_data_1'], 'somedata')


    def test_lof1(self):

        # Make lost+found a file
        inode = dbcm.get_val("SELECT inode FROM contents WHERE name=? AND parent_inode=?",
                                (b"lost+found", ROOT_INODE))
        dbcm.execute('DELETE FROM contents WHERE parent_inode=?', (inode,))
        dbcm.execute('UPDATE inodes SET mode=?, size=? WHERE id=?',
                        (stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR, 0, inode))

        self.assert_fsck(fsck.check_lof)

    def test_lof2(self):
        # Remove lost+found
        dbcm.execute('DELETE FROM contents WHERE name=? and parent_inode=?',
                        (b'lost+found', ROOT_INODE))

        self.assert_fsck(fsck.check_lof)

    def test_inode_refcount(self):

        # Create an orphaned inode
        dbcm.execute("INSERT INTO inodes (mode,uid,gid,mtime,atime,ctime,refcount,size) "
                     "VALUES (?,?,?,?,?,?,?,?)",
                     (stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR,
                      0, 0, time.time(), time.time(), time.time(), 2, 0))

        self.assert_fsck(fsck.check_inode_refcount)

        # Create an inode with wrong refcount
        inode = dbcm.rowid("INSERT INTO inodes (mode,uid,gid,mtime,atime,ctime,refcount,size) "
                           "VALUES (?,?,?,?,?,?,?,?)",
                           (stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR,
                            0, 0, time.time(), time.time(), time.time(), 1, 0))
        dbcm.execute('INSERT INTO contents (name, inode, parent_inode) VALUES(?, ?, ?)',
                     (b'name1', inode, ROOT_INODE))
        dbcm.execute('INSERT INTO contents (name, inode, parent_inode) VALUES(?, ?, ?)',
                     (b'name2', inode, ROOT_INODE))

        self.assert_fsck(fsck.check_inode_refcount)

    def test_inode_sizes(self):

        id_ = dbcm.rowid("INSERT INTO inodes (mode,uid,gid,mtime,atime,ctime,refcount,size) "
                         "VALUES (?,?,?,?,?,?,?,?)",
                         (stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR,
                          0, 0, time.time(), time.time(), time.time(), 2, 0))

        dbcm.execute('INSERT INTO contents (name, inode, parent_inode) VALUES(?,?,?)',
                     ('test-entry', id_, ROOT_INODE))

        # Create a block
        obj_id = dbcm.rowid('INSERT INTO objects (refcount, size) VALUES(?, ?)',
                            (1, 500))
        dbcm.execute('INSERT INTO blocks (inode, blockno, obj_id) VALUES(?, ?, ?)',
                     (id_, 0, obj_id))


        self.assert_fsck(fsck.check_inode_sizes)



    def test_keylist(self):
        # Create an object that only exists in the bucket
        self.bucket['s3ql_data_4364'] = 'Testdata'
        self.assert_fsck(fsck.check_keylist)

        # Create an object that does not exist in the bucket
        dbcm.execute('INSERT INTO objects (id, refcount, size) VALUES(?, ?, ?)',
                          (34, 1, 0))
        self.assert_fsck(fsck.check_keylist)

    @staticmethod
    def random_data(len_):
        with open("/dev/urandom", "rb") as fd:
            return fd.read(len_)

    def test_loops(self):

        # Create some directory inodes  
        inodes = [ dbcm.rowid("INSERT INTO inodes (mode,uid,gid,mtime,atime,ctime,refcount) "
                              "VALUES (?,?,?,?,?,?,?)",
                              (stat.S_IFDIR | stat.S_IRUSR | stat.S_IWUSR,
                               0, 0, time.time(), time.time(), time.time(), 1))
                   for dummy in range(3) ]

        inodes.append(inodes[0])
        last = inodes[0]
        for inode in inodes[1:]:
            dbcm.execute('INSERT INTO contents (name, inode, parent_inode) VALUES(?, ?, ?)',
                         (bytes(inode), inode, last))
            last = inode

        fsck.found_errors = False
        fsck.check_inode_refcount()
        self.assertFalse(fsck.found_errors)
        fsck.check_loops()
        self.assertTrue(fsck.found_errors)
        # We can't fix loops yet

    def test_obj_refcounts(self):

        obj_id = 42
        inode = 42
        dbcm.execute("INSERT INTO inodes (id, mode,uid,gid,mtime,atime,ctime,refcount,size) "
                     "VALUES (?,?,?,?,?,?,?,?,?)",
                     (inode, stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR,
                      os.getuid(), os.getgid(), time.time(), time.time(), time.time(), 1, 0))

        dbcm.execute('INSERT INTO objects (id, refcount, size) VALUES(?, ?, ?)',
                     (obj_id, 2, 0))
        dbcm.execute('INSERT INTO blocks (inode, blockno, obj_id) VALUES(?, ?, ?)',
                     (inode, 1, obj_id))
        dbcm.execute('INSERT INTO blocks (inode, blockno, obj_id) VALUES(?, ?, ?)',
                     (inode, 2, obj_id))

        fsck.found_errors = False
        fsck.check_obj_refcounts()
        self.assertFalse(fsck.found_errors)

        dbcm.execute('INSERT INTO blocks (inode, blockno, obj_id) VALUES(?, ?, ?)',
                     (inode, 3, obj_id))
        self.assert_fsck(fsck.check_obj_refcounts)

        dbcm.execute('DELETE FROM blocks WHERE obj_id=?', (obj_id,))
        self.assert_fsck(fsck.check_obj_refcounts)

    def test_unix_size(self):

        inode = 42
        dbcm.execute("INSERT INTO inodes (id, mode,uid,gid,mtime,atime,ctime,refcount,size) "
                     "VALUES (?,?,?,?,?,?,?,?,?)",
                     (inode, stat.S_IFLNK | stat.S_IRUSR | stat.S_IWUSR,
                      os.getuid(), os.getgid(), time.time(), time.time(), time.time(), 1, 0))

        dbcm.execute('INSERT INTO contents (name, inode, parent_inode) VALUES(?,?,?)',
                     ('test-entry', inode, ROOT_INODE))

        fsck.found_errors = False
        fsck.check_inode_unix()
        self.assertFalse(fsck.found_errors)

        dbcm.execute('UPDATE inodes SET size = 1 WHERE id=?', (inode,))
        fsck.check_inode_unix()
        self.assertTrue(fsck.found_errors)

    def test_unix_target(self):

        inode = 42
        dbcm.execute("INSERT INTO inodes (id, mode,uid,gid,mtime,atime,ctime,refcount) "
                     "VALUES (?,?,?,?,?,?,?,?)",
                     (inode, stat.S_IFCHR | stat.S_IRUSR | stat.S_IWUSR,
                      os.getuid(), os.getgid(), time.time(), time.time(), time.time(), 1))

        dbcm.execute('INSERT INTO contents (name, inode, parent_inode) VALUES(?,?,?)',
                     ('test-entry', inode, ROOT_INODE))

        fsck.found_errors = False
        fsck.check_inode_unix()
        self.assertFalse(fsck.found_errors)

        dbcm.execute('UPDATE inodes SET target = ? WHERE id=?', ('foo', inode))
        fsck.check_inode_unix()
        self.assertTrue(fsck.found_errors)

    def test_unix_rdev(self):

        inode = 42
        dbcm.execute("INSERT INTO inodes (id, mode,uid,gid,mtime,atime,ctime,refcount) "
                     "VALUES (?,?,?,?,?,?,?,?)",
                     (inode, stat.S_IFIFO | stat.S_IRUSR | stat.S_IWUSR,
                      os.getuid(), os.getgid(), time.time(), time.time(), time.time(), 1))
        dbcm.execute('INSERT INTO contents (name, inode, parent_inode) VALUES(?,?,?)',
                     ('test-entry', inode, ROOT_INODE))

        fsck.found_errors = False
        fsck.check_inode_unix()
        self.assertFalse(fsck.found_errors)

        dbcm.execute('UPDATE inodes SET rdev=? WHERE id=?', (42, inode))
        fsck.check_inode_unix()
        self.assertTrue(fsck.found_errors)

    def test_unix_child(self):

        inode = dbcm.rowid("INSERT INTO inodes (mode,uid,gid,mtime,atime,ctime,refcount) "
                   "VALUES (?,?,?,?,?,?,?)",
                   (stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR,
                    os.getuid(), os.getgid(), time.time(), time.time(), time.time(), 1))

        dbcm.execute('INSERT INTO contents (name, inode, parent_inode) VALUES(?,?,?)',
                     ('test-entry', inode, ROOT_INODE))

        fsck.found_errors = False
        fsck.check_inode_unix()
        self.assertFalse(fsck.found_errors)
        dbcm.execute('INSERT INTO contents (name, inode, parent_inode) VALUES(?,?,?)',
                     ('foo', ROOT_INODE, inode))
        fsck.check_inode_unix()
        self.assertTrue(fsck.found_errors)

    def test_unix_blocks(self):

        obj_id = 87
        inode = dbcm.rowid("INSERT INTO inodes (mode,uid,gid,mtime,atime,ctime,refcount) "
                   "VALUES (?,?,?,?,?,?,?)",
                   (stat.S_IFSOCK | stat.S_IRUSR | stat.S_IWUSR,
                    os.getuid(), os.getgid(), time.time(), time.time(), time.time(), 1))

        dbcm.execute('INSERT INTO contents (name, inode, parent_inode) VALUES(?,?,?)',
                     ('test-entry', inode, ROOT_INODE))

        fsck.found_errors = False
        fsck.check_inode_unix()
        self.assertFalse(fsck.found_errors)

        dbcm.execute('INSERT INTO objects (id, refcount, size) VALUES(?, ?, ?)',
                     (obj_id, 2, 0))
        dbcm.execute('INSERT INTO blocks (inode, blockno, obj_id) VALUES(?, ?, ?)',
                     (inode, 1, obj_id))
        fsck.check_inode_unix()
        self.assertTrue(fsck.found_errors)


# Somehow important according to pyunit documentation
def suite():
    return unittest.makeSuite(fsck_tests)

if __name__ == "__main__":
    unittest.main()
