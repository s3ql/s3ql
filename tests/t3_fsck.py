'''
t3_fsck.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function

import unittest2 as unittest
from s3ql.fsck import Fsck
from s3ql.backends import local
from s3ql.database import Connection
from s3ql.common import ROOT_INODE, create_tables, init_tables
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
        self.db = Connection(self.dbfile.name)
        create_tables(self.db)
        init_tables(self.db)

        self.fsck = Fsck(self.cachedir, self.bucket,
                  { 'blocksize': self.blocksize }, self.db)
        self.fsck.expect_errors = True

    def tearDown(self):
        shutil.rmtree(self.cachedir)
        shutil.rmtree(self.bucket_dir)

    def assert_fsck(self, fn):
        '''Check that fn detects and corrects an error'''

        
        self.fsck.found_errors = False
        fn()
        self.assertTrue(self.fsck.found_errors)
        self.fsck.found_errors = False
        fn()
        self.assertFalse(self.fsck.found_errors)

    def test_cache(self):
        inode = 6
        self.db.execute("INSERT INTO inodes (id, mode,uid,gid,mtime,atime,ctime,refcount) "
                   "VALUES (?,?,?,?,?,?,?,?)",
                   (inode, stat.S_IFDIR | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR
                   | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH,
                    os.getuid(), os.getgid(), time.time(), time.time(), time.time(), 1))

        fh = open(self.cachedir + 'inode_%d_block_1.d' % inode, 'wb')
        fh.write('somedata')
        fh.close()

        self.assert_fsck(self.fsck.check_cache)
        self.assertEquals(self.bucket['s3ql_data_1'], 'somedata')

        fh = open(self.cachedir + 'inode_%d_block_1' % inode, 'wb')
        fh.write('otherdata')
        fh.close()

        self.assert_fsck(self.fsck.check_cache)
        self.assertEquals(self.bucket['s3ql_data_1'], 'somedata')


    def test_lof1(self):

        # Make lost+found a file
        inode = self.db.get_val("SELECT inode FROM contents WHERE name=? AND parent_inode=?",
                                (b"lost+found", ROOT_INODE))
        self.db.execute('DELETE FROM contents WHERE parent_inode=?', (inode,))
        self.db.execute('UPDATE inodes SET mode=?, size=? WHERE id=?',
                        (stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR, 0, inode))

        self.assert_fsck(self.fsck.check_lof)

    def test_lof2(self):
        # Remove lost+found
        self.db.execute('DELETE FROM contents WHERE name=? and parent_inode=?',
                        (b'lost+found', ROOT_INODE))

        self.assert_fsck(self.fsck.check_lof)

    def test_inode_refcount(self):

        # Create an orphaned inode
        self.db.execute("INSERT INTO inodes (mode,uid,gid,mtime,atime,ctime,refcount,size) "
                     "VALUES (?,?,?,?,?,?,?,?)",
                     (stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR,
                      0, 0, time.time(), time.time(), time.time(), 2, 0))

        self.assert_fsck(self.fsck.check_inode_refcount)

        # Create an inode with wrong refcount
        inode = self.db.rowid("INSERT INTO inodes (mode,uid,gid,mtime,atime,ctime,refcount,size) "
                           "VALUES (?,?,?,?,?,?,?,?)",
                           (stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR,
                            0, 0, time.time(), time.time(), time.time(), 1, 0))
        self.db.execute('INSERT INTO contents (name, inode, parent_inode) VALUES(?, ?, ?)',
                     (b'name1', inode, ROOT_INODE))
        self.db.execute('INSERT INTO contents (name, inode, parent_inode) VALUES(?, ?, ?)',
                     (b'name2', inode, ROOT_INODE))

        self.assert_fsck(self.fsck.check_inode_refcount)

    def test_inode_sizes(self):

        id_ = self.db.rowid("INSERT INTO inodes (mode,uid,gid,mtime,atime,ctime,refcount,size) "
                         "VALUES (?,?,?,?,?,?,?,?)",
                         (stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR,
                          0, 0, time.time(), time.time(), time.time(), 2, 0))

        self.db.execute('INSERT INTO contents (name, inode, parent_inode) VALUES(?,?,?)',
                     ('test-entry', id_, ROOT_INODE))

        # Create a block
        obj_id = self.db.rowid('INSERT INTO objects (refcount, size) VALUES(?, ?)',
                            (1, 500))
        self.db.execute('INSERT INTO blocks (inode, blockno, obj_id) VALUES(?, ?, ?)',
                     (id_, 0, obj_id))


        self.assert_fsck(self.fsck.check_inode_sizes)



    def test_keylist(self):
        # Create an object that only exists in the bucket
        self.bucket['s3ql_data_4364'] = 'Testdata'
        self.assert_fsck(self.fsck.check_keylist)

        # Create an object that does not exist in the bucket
        self.db.execute('INSERT INTO objects (id, refcount, size) VALUES(?, ?, ?)',
                          (34, 1, 0))
        self.assert_fsck(self.fsck.check_keylist)

    @staticmethod
    def random_data(len_):
        with open("/dev/urandom", "rb") as fd:
            return fd.read(len_)

    def test_loops(self):

        # Create some directory inodes  
        inodes = [ self.db.rowid("INSERT INTO inodes (mode,uid,gid,mtime,atime,ctime,refcount) "
                              "VALUES (?,?,?,?,?,?,?)",
                              (stat.S_IFDIR | stat.S_IRUSR | stat.S_IWUSR,
                               0, 0, time.time(), time.time(), time.time(), 1))
                   for dummy in range(3) ]

        inodes.append(inodes[0])
        last = inodes[0]
        for inode in inodes[1:]:
            self.db.execute('INSERT INTO contents (name, inode, parent_inode) VALUES(?, ?, ?)',
                         (bytes(inode), inode, last))
            last = inode

        self.fsck.found_errors = False
        self.fsck.check_inode_refcount()
        self.assertFalse(self.fsck.found_errors)
        self.fsck.check_loops()
        self.assertTrue(self.fsck.found_errors)
        # We can't fix loops yet

    def test_obj_refcounts(self):

        obj_id = 42
        inode = 42
        self.db.execute("INSERT INTO inodes (id, mode,uid,gid,mtime,atime,ctime,refcount,size) "
                     "VALUES (?,?,?,?,?,?,?,?,?)",
                     (inode, stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR,
                      os.getuid(), os.getgid(), time.time(), time.time(), time.time(), 1, 0))

        self.db.execute('INSERT INTO objects (id, refcount, size) VALUES(?, ?, ?)',
                     (obj_id, 2, 0))
        self.db.execute('INSERT INTO blocks (inode, blockno, obj_id) VALUES(?, ?, ?)',
                     (inode, 1, obj_id))
        self.db.execute('INSERT INTO blocks (inode, blockno, obj_id) VALUES(?, ?, ?)',
                     (inode, 2, obj_id))

        self.fsck.found_errors = False
        self.fsck.check_obj_refcounts()
        self.assertFalse(self.fsck.found_errors)

        self.db.execute('INSERT INTO blocks (inode, blockno, obj_id) VALUES(?, ?, ?)',
                     (inode, 3, obj_id))
        self.assert_fsck(self.fsck.check_obj_refcounts)

        self.db.execute('DELETE FROM blocks WHERE obj_id=?', (obj_id,))
        self.assert_fsck(self.fsck.check_obj_refcounts)

    def test_unix_size(self):

        inode = 42
        self.db.execute("INSERT INTO inodes (id, mode,uid,gid,mtime,atime,ctime,refcount,size) "
                     "VALUES (?,?,?,?,?,?,?,?,?)",
                     (inode, stat.S_IFLNK | stat.S_IRUSR | stat.S_IWUSR,
                      os.getuid(), os.getgid(), time.time(), time.time(), time.time(), 1, 0))

        self.db.execute('INSERT INTO contents (name, inode, parent_inode) VALUES(?,?,?)',
                     ('test-entry', inode, ROOT_INODE))

        self.fsck.found_errors = False
        self.fsck.check_inode_unix()
        self.assertFalse(self.fsck.found_errors)

        self.db.execute('UPDATE inodes SET size = 1 WHERE id=?', (inode,))
        self.fsck.check_inode_unix()
        self.assertTrue(self.fsck.found_errors)

    def test_unix_target(self):

        inode = 42
        self.db.execute("INSERT INTO inodes (id, mode,uid,gid,mtime,atime,ctime,refcount) "
                     "VALUES (?,?,?,?,?,?,?,?)",
                     (inode, stat.S_IFCHR | stat.S_IRUSR | stat.S_IWUSR,
                      os.getuid(), os.getgid(), time.time(), time.time(), time.time(), 1))

        self.db.execute('INSERT INTO contents (name, inode, parent_inode) VALUES(?,?,?)',
                     ('test-entry', inode, ROOT_INODE))

        self.fsck.found_errors = False
        self.fsck.check_inode_unix()
        self.assertFalse(self.fsck.found_errors)

        self.db.execute('UPDATE inodes SET target = ? WHERE id=?', ('foo', inode))
        self.fsck.check_inode_unix()
        self.assertTrue(self.fsck.found_errors)

    def test_unix_rdev(self):

        inode = 42
        self.db.execute("INSERT INTO inodes (id, mode,uid,gid,mtime,atime,ctime,refcount) "
                     "VALUES (?,?,?,?,?,?,?,?)",
                     (inode, stat.S_IFIFO | stat.S_IRUSR | stat.S_IWUSR,
                      os.getuid(), os.getgid(), time.time(), time.time(), time.time(), 1))
        self.db.execute('INSERT INTO contents (name, inode, parent_inode) VALUES(?,?,?)',
                     ('test-entry', inode, ROOT_INODE))

        self.fsck.found_errors = False
        self.fsck.check_inode_unix()
        self.assertFalse(self.fsck.found_errors)

        self.db.execute('UPDATE inodes SET rdev=? WHERE id=?', (42, inode))
        self.fsck.check_inode_unix()
        self.assertTrue(self.fsck.found_errors)

    def test_unix_child(self):

        inode = self.db.rowid("INSERT INTO inodes (mode,uid,gid,mtime,atime,ctime,refcount) "
                   "VALUES (?,?,?,?,?,?,?)",
                   (stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR,
                    os.getuid(), os.getgid(), time.time(), time.time(), time.time(), 1))

        self.db.execute('INSERT INTO contents (name, inode, parent_inode) VALUES(?,?,?)',
                     ('test-entry', inode, ROOT_INODE))

        self.fsck.found_errors = False
        self.fsck.check_inode_unix()
        self.assertFalse(self.fsck.found_errors)
        self.db.execute('INSERT INTO contents (name, inode, parent_inode) VALUES(?,?,?)',
                     ('foo', ROOT_INODE, inode))
        self.fsck.check_inode_unix()
        self.assertTrue(self.fsck.found_errors)

    def test_unix_blocks(self):

        obj_id = 87
        inode = self.db.rowid("INSERT INTO inodes (mode,uid,gid,mtime,atime,ctime,refcount) "
                   "VALUES (?,?,?,?,?,?,?)",
                   (stat.S_IFSOCK | stat.S_IRUSR | stat.S_IWUSR,
                    os.getuid(), os.getgid(), time.time(), time.time(), time.time(), 1))

        self.db.execute('INSERT INTO contents (name, inode, parent_inode) VALUES(?,?,?)',
                     ('test-entry', inode, ROOT_INODE))

        self.fsck.found_errors = False
        self.fsck.check_inode_unix()
        self.assertFalse(self.fsck.found_errors)

        self.db.execute('INSERT INTO objects (id, refcount, size) VALUES(?, ?, ?)',
                     (obj_id, 2, 0))
        self.db.execute('INSERT INTO blocks (inode, blockno, obj_id) VALUES(?, ?, ?)',
                     (inode, 1, obj_id))
        self.fsck.check_inode_unix()
        self.assertTrue(self.fsck.found_errors)


# Somehow important according to pyunit documentation
def suite():
    return unittest.makeSuite(fsck_tests)

if __name__ == "__main__":
    unittest.main()
