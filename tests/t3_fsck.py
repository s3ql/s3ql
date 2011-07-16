'''
t3_fsck.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import division, print_function

import unittest2 as unittest
from s3ql.fsck import Fsck
from s3ql.backends import local
from s3ql.database import Connection, NoSuchRowError
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
        inode = self.db.get_val("SELECT inode FROM contents_v WHERE name=? AND parent_inode=?",
                                (b"lost+found", ROOT_INODE))
        self.db.execute('DELETE FROM contents WHERE parent_inode=?', (inode,))
        self.db.execute('UPDATE inodes SET mode=?, size=? WHERE id=?',
                        (stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR, 0, inode))

        self.assert_fsck(self.fsck.check_lof)

    def test_lof2(self):
        # Remove lost+found
        name_id = self.db.get_val('SELECT id FROM names WHERE name=?', (b'lost+found',))
        self.db.execute('DELETE FROM contents WHERE name_id=? and parent_inode=?',
                        (name_id, ROOT_INODE))

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
        self._link('name1', inode)
        self._link('name2', inode)
        
        self.assert_fsck(self.fsck.check_inode_refcount)

    def test_name_refcount(self):

        # Create inode
        inode = self.db.rowid("INSERT INTO inodes (mode,uid,gid,mtime,atime,ctime,refcount,size) "
                              "VALUES (?,?,?,?,?,?,?,?)",
                              (stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR,
                               0, 0, time.time(), time.time(), time.time(), 1, 0))
        self._link('name1', inode)
        self._link('name2', inode)
        
        self.db.execute('UPDATE names SET refcount=refcount+1 WHERE name=?', ('name1',))
        
        self.assert_fsck(self.fsck.check_name_refcount)
        
    def _add_name(self, name):
        '''Get id for *name* and increase refcount
        
        Name is inserted in table if it does not yet exist.
        '''
        
        try:
            name_id = self.db.get_val('SELECT id FROM names WHERE name=?', (name,))
        except NoSuchRowError:
            name_id = self.db.rowid('INSERT INTO names (name, refcount) VALUES(?,?)',
                                    (name, 1))
        else:
            self.db.execute('UPDATE names SET refcount=refcount+1 WHERE id=?', (name_id,))
        return name_id
            
    def _link(self, name, inode):
        '''Link /*name* to *inode*'''

        self.db.execute('INSERT INTO contents (name_id, inode, parent_inode) VALUES(?,?,?)',
                        (self._add_name(name), inode, ROOT_INODE))
                
    def test_inode_sizes(self):

        id_ = self.db.rowid("INSERT INTO inodes (mode,uid,gid,mtime,atime,ctime,refcount,size) "
                            "VALUES (?,?,?,?,?,?,?,?)",
                            (stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR,
                             0, 0, time.time(), time.time(), time.time(), 1, 128))
        self._link('test-entry', id_)

        obj_id = self.db.rowid('INSERT INTO objects (refcount) VALUES(1)')
        block_id = self.db.rowid('INSERT INTO blocks (refcount, obj_id, size) VALUES(?,?,?)',
                                 (1, obj_id, 512))
        
        # Case 1
        self.db.execute('UPDATE inodes SET block_id=?, size=? WHERE id=?',
                        (None, self.blocksize + 120, id_))
        self.db.execute('INSERT INTO inode_blocks (inode, blockno, block_id) VALUES(?, ?, ?)',
                        (id_, 1, block_id))
        self.assert_fsck(self.fsck.check_inode_sizes)

        # Case 2
        self.db.execute('DELETE FROM inode_blocks WHERE inode=?', (id_,))
        self.db.execute('UPDATE inodes SET block_id=?, size=? WHERE id=?',
                        (block_id, 129, id_))
        self.assert_fsck(self.fsck.check_inode_sizes)

        # Case 3
        self.db.execute('INSERT INTO inode_blocks (inode, blockno, block_id) VALUES(?, ?, ?)',
                        (id_, 1, block_id))
        self.db.execute('UPDATE inodes SET block_id=?, size=? WHERE id=?',
                        (block_id, self.blocksize + 120, id_))
        self.assert_fsck(self.fsck.check_inode_sizes)
        

    def test_keylist(self):
        # Create an object that only exists in the bucket
        self.bucket['s3ql_data_4364'] = 'Testdata'
        self.assert_fsck(self.fsck.check_keylist)

        # Create an object that does not exist in the bucket
        self.db.execute('INSERT INTO objects (id, refcount) VALUES(?, ?)', (34, 1))
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
            self.db.execute('INSERT INTO contents (name_id, inode, parent_inode) VALUES(?, ?, ?)',
                            (self._add_name(bytes(inode)), inode, last))
            last = inode

        self.fsck.found_errors = False
        self.fsck.check_inode_refcount()
        self.assertFalse(self.fsck.found_errors)
        self.fsck.check_loops()
        self.assertTrue(self.fsck.found_errors)
        # We can't fix loops yet

    def test_obj_refcounts(self):

        obj_id = self.db.rowid('INSERT INTO objects (refcount) VALUES(1)')
        self.db.execute('INSERT INTO blocks (refcount, obj_id, size) VALUES(?,?,?)',
                        (0, obj_id, 0))
        self.db.execute('INSERT INTO blocks (refcount, obj_id, size) VALUES(?,?,?)',
                        (0, obj_id, 0))

        self.assert_fsck(self.fsck.check_obj_refcounts)

    def test_block_refcounts(self):

        obj_id = self.db.rowid('INSERT INTO objects (refcount) VALUES(1)')
        block_id = self.db.rowid('INSERT INTO blocks (refcount, obj_id, size) VALUES(?,?,?)',
                                 (1, obj_id, 0))
        
        inode = self.db.rowid("INSERT INTO inodes (mode,uid,gid,mtime,atime,ctime,refcount,size,block_id) "
                              "VALUES (?,?,?,?,?,?,?,?,?)",
                              (stat.S_IFIFO | stat.S_IRUSR | stat.S_IWUSR, os.getuid(), os.getgid(),
                               time.time(), time.time(), time.time(), 1, 0, block_id))
            
        self.db.execute('INSERT INTO inode_blocks (inode, blockno, block_id) VALUES(?,?,?)',
                        (inode, 1, block_id))

        self.assert_fsck(self.fsck.check_block_refcount)
        
    def test_unix_size(self):

        inode = 42
        self.db.execute("INSERT INTO inodes (id, mode,uid,gid,mtime,atime,ctime,refcount,size) "
                        "VALUES (?,?,?,?,?,?,?,?,?)",
                        (inode, stat.S_IFIFO | stat.S_IRUSR | stat.S_IWUSR,
                         os.getuid(), os.getgid(), time.time(), time.time(), time.time(), 1, 0))
        self._link('test-entry', inode)

        self.fsck.found_errors = False
        self.fsck.check_inode_unix()
        self.assertFalse(self.fsck.found_errors)

        self.db.execute('UPDATE inodes SET size = 1 WHERE id=?', (inode,))
        self.fsck.check_inode_unix()
        self.assertTrue(self.fsck.found_errors)

    
    def test_unix_size_symlink(self):

        inode = 42
        target = 'some funny random string'
        self.db.execute("INSERT INTO inodes (id, mode,uid,gid,mtime,atime,ctime,refcount,size) "
                        "VALUES (?,?,?,?,?,?,?,?,?)",
                        (inode, stat.S_IFLNK | stat.S_IRUSR | stat.S_IWUSR,
                         os.getuid(), os.getgid(), time.time(), time.time(), time.time(), 1, 
                         len(target)))
        self.db.execute('INSERT INTO symlink_targets (inode, target) VALUES(?,?)', (inode, target))
        self._link('test-entry', inode)

        self.fsck.found_errors = False
        self.fsck.check_inode_unix()
        self.assertFalse(self.fsck.found_errors)

        self.db.execute('UPDATE inodes SET size = 0 WHERE id=?', (inode,))
        self.fsck.check_inode_unix()
        self.assertTrue(self.fsck.found_errors)
        
    def test_unix_target(self):

        inode = 42
        self.db.execute("INSERT INTO inodes (id, mode,uid,gid,mtime,atime,ctime,refcount) "
                     "VALUES (?,?,?,?,?,?,?,?)",
                     (inode, stat.S_IFCHR | stat.S_IRUSR | stat.S_IWUSR,
                      os.getuid(), os.getgid(), time.time(), time.time(), time.time(), 1))
        self._link('test-entry', inode)

        self.fsck.found_errors = False
        self.fsck.check_inode_unix()
        self.assertFalse(self.fsck.found_errors)

        self.db.execute('INSERT INTO symlink_targets (inode, target) VALUES(?,?)', (inode, 'foo'))
        self.fsck.check_inode_unix()
        self.assertTrue(self.fsck.found_errors)

    def test_unix_rdev(self):

        inode = 42
        self.db.execute("INSERT INTO inodes (id, mode,uid,gid,mtime,atime,ctime,refcount) "
                     "VALUES (?,?,?,?,?,?,?,?)",
                     (inode, stat.S_IFIFO | stat.S_IRUSR | stat.S_IWUSR,
                      os.getuid(), os.getgid(), time.time(), time.time(), time.time(), 1))
        self._link('test-entry', inode)

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
        self._link('test-entry', inode)

        self.fsck.found_errors = False
        self.fsck.check_inode_unix()
        self.assertFalse(self.fsck.found_errors)
        self.db.execute('INSERT INTO contents (name_id, inode, parent_inode) VALUES(?,?,?)',
                        (self._add_name('foo'), ROOT_INODE, inode))
        self.fsck.check_inode_unix()
        self.assertTrue(self.fsck.found_errors)

    def test_unix_blocks(self):

        inode = self.db.rowid("INSERT INTO inodes (mode,uid,gid,mtime,atime,ctime,refcount) "
                              "VALUES (?,?,?,?,?,?,?)", 
                              (stat.S_IFSOCK | stat.S_IRUSR | stat.S_IWUSR,
                               os.getuid(), os.getgid(), time.time(), time.time(), time.time(), 1))
        self._link('test-entry', inode)

        self.fsck.found_errors = False
        self.fsck.check_inode_unix()
        self.assertFalse(self.fsck.found_errors)
        
        obj_id = self.db.rowid('INSERT INTO objects (refcount) VALUES(1)')
        block_id = self.db.rowid('INSERT INTO blocks (refcount, obj_id, size) VALUES(?,?,?)',
                                 (1, obj_id, 0))

        self.db.execute('INSERT INTO inode_blocks (inode, blockno, block_id) VALUES(?,?,?)',
                        (inode, 1, block_id))

        self.fsck.check_inode_unix()
        self.assertTrue(self.fsck.found_errors)



# Somehow important according to pyunit documentation
def suite():
    return unittest.makeSuite(fsck_tests)

if __name__ == "__main__":
    unittest.main()
