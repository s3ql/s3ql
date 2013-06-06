'''
t3_fsck.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from s3ql.backends import local
from s3ql.common import ROOT_INODE
from s3ql.mkfs import init_tables
from s3ql.metadata import create_tables
from s3ql.database import Connection, NoSuchRowError
from s3ql.fsck import Fsck
import os
import shutil
import stat
import tempfile
import time
import unittest


class fsck_tests(unittest.TestCase):

    def setUp(self):
        self.backend_dir = tempfile.mkdtemp(prefix='s3ql-backend-')
        self.backend = local.Backend('local://' + self.backend_dir, None, None)
        self.cachedir = tempfile.mkdtemp(prefix='s3ql-cache-')
        self.max_obj_size = 1024

        self.dbfile = tempfile.NamedTemporaryFile()
        self.db = Connection(self.dbfile.name)
        create_tables(self.db)
        init_tables(self.db)

        self.fsck = Fsck(self.cachedir, self.backend,
                  { 'max_obj_size': self.max_obj_size }, self.db)
        self.fsck.expect_errors = True

    def tearDown(self):
        shutil.rmtree(self.cachedir)
        shutil.rmtree(self.backend_dir)
        self.dbfile.close()

    def assert_fsck(self, fn):
        '''Check that fn detects and corrects an error'''

        self.fsck.found_errors = False
        fn()
        self.assertTrue(self.fsck.found_errors)
        self.fsck.found_errors = False
        self.fsck.check()
        self.assertFalse(self.fsck.found_errors)

    def test_cache(self):
        inode = self.db.rowid("INSERT INTO inodes (mode,uid,gid,mtime,atime,ctime,refcount,size) "
                              "VALUES (?,?,?,?,?,?,?,?)",
                              (stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR
                               | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH,
                               os.getuid(), os.getgid(), time.time(), time.time(), time.time(),
                               1, 8))
        self._link(b'test-entry', inode)

        # Create new block
        fh = open(self.cachedir + '/%d-0' % inode, 'wb')
        fh.write(b'somedata')
        fh.close()
        self.assert_fsck(self.fsck.check_cache)
        self.assertEqual(self.backend['s3ql_data_1'], b'somedata')

        # Existing block
        self.db.execute('UPDATE inodes SET size=? WHERE id=?',
                        (self.max_obj_size + 8, inode))
        with open(self.cachedir + '/%d-1' % inode, 'wb') as fh:
            fh.write(b'somedata')
        self.assert_fsck(self.fsck.check_cache)

        # Old block preserved
        with open(self.cachedir + '/%d-0' % inode, 'wb') as fh:
            fh.write(b'somedat2')
        self.assert_fsck(self.fsck.check_cache)

        # Old block removed
        with open(self.cachedir + '/%d-1' % inode, 'wb') as fh:
            fh.write(b'somedat3')
        self.assert_fsck(self.fsck.check_cache)


    def test_lof1(self):

        # Make lost+found a file
        inode = self.db.get_val("SELECT inode FROM contents_v WHERE name=? AND parent_inode=?",
                                (b"lost+found", ROOT_INODE))
        self.db.execute('DELETE FROM contents WHERE parent_inode=?', (inode,))
        self.db.execute('UPDATE inodes SET mode=?, size=? WHERE id=?',
                        (stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR, 0, inode))

        def check():
            self.fsck.check_lof()
            self.fsck.check_inodes_refcount()

        self.assert_fsck(check)

    def test_lof2(self):
        # Remove lost+found
        name_id = self.db.get_val('SELECT id FROM names WHERE name=?', (b'lost+found',))
        inode = self.db.get_val('SELECT inode FROM contents WHERE name_id=? AND '
                                'parent_inode=?', (name_id, ROOT_INODE))
        self.db.execute('DELETE FROM inodes WHERE id=?', (inode,))
        self.db.execute('DELETE FROM contents WHERE name_id=? and parent_inode=?',
                        (name_id, ROOT_INODE))
        self.db.execute('UPDATE names SET refcount = refcount-1 WHERE id=?', (name_id,))

        self.assert_fsck(self.fsck.check_lof)

    def test_wrong_inode_refcount(self):

        inode = self.db.rowid("INSERT INTO inodes (mode,uid,gid,mtime,atime,ctime,refcount,size) "
                              "VALUES (?,?,?,?,?,?,?,?)",
                              (stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR,
                               0, 0, time.time(), time.time(), time.time(), 1, 0))
        self._link(b'name1', inode)
        self._link(b'name2', inode)
        self.assert_fsck(self.fsck.check_inodes_refcount)

    def test_orphaned_inode(self):

        self.db.rowid("INSERT INTO inodes (mode,uid,gid,mtime,atime,ctime,refcount,size) "
                      "VALUES (?,?,?,?,?,?,?,?)",
                      (stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR,
                       0, 0, time.time(), time.time(), time.time(), 1, 0))
        self.assert_fsck(self.fsck.check_inodes_refcount)

    def test_name_refcount(self):

        inode = self.db.rowid("INSERT INTO inodes (mode,uid,gid,mtime,atime,ctime,refcount,size) "
                              "VALUES (?,?,?,?,?,?,?,?)",
                              (stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR,
                               0, 0, time.time(), time.time(), time.time(), 2, 0))
        self._link(b'name1', inode)
        self._link(b'name2', inode)

        self.db.execute('UPDATE names SET refcount=refcount+1 WHERE name=?', (b'name1',))

        self.assert_fsck(self.fsck.check_names_refcount)

    def test_orphaned_name(self):

        self._add_name(b'zupbrazl')
        self.assert_fsck(self.fsck.check_names_refcount)

    def test_contents_inode(self):

        self.db.execute('INSERT INTO contents (name_id, inode, parent_inode) VALUES(?,?,?)',
                        (self._add_name(b'foobar'), 124, ROOT_INODE))

        self.assert_fsck(self.fsck.check_contents_inode)

    def test_contents_inode_p(self):

        inode = self.db.rowid("INSERT INTO inodes (mode,uid,gid,mtime,atime,ctime,refcount,size) "
                              "VALUES (?,?,?,?,?,?,?,?)",
                              (stat.S_IFDIR | stat.S_IRUSR | stat.S_IWUSR,
                               0, 0, time.time(), time.time(), time.time(), 1, 0))
        self.db.execute('INSERT INTO contents (name_id, inode, parent_inode) VALUES(?,?,?)',
                        (self._add_name(b'foobar'), inode, 123))

        self.assert_fsck(self.fsck.check_contents_parent_inode)

    def test_contents_name(self):

        inode = self.db.rowid("INSERT INTO inodes (mode,uid,gid,mtime,atime,ctime,refcount,size) "
                              "VALUES (?,?,?,?,?,?,?,?)",
                              (stat.S_IFDIR | stat.S_IRUSR | stat.S_IWUSR,
                               0, 0, time.time(), time.time(), time.time(), 1, 0))
        self.db.execute('INSERT INTO contents (name_id, inode, parent_inode) VALUES(?,?,?)',
                        (42, inode, ROOT_INODE))

        self.assert_fsck(self.fsck.check_contents_name)

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

    def _link(self, name, inode, parent_inode=ROOT_INODE):
        '''Link /*name* to *inode*'''

        self.db.execute('INSERT INTO contents (name_id, inode, parent_inode) VALUES(?,?,?)',
                        (self._add_name(name), inode, parent_inode))

    def test_inodes_size(self):

        id_ = self.db.rowid("INSERT INTO inodes (mode,uid,gid,mtime,atime,ctime,refcount,size) "
                            "VALUES (?,?,?,?,?,?,?,?)",
                            (stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR,
                             0, 0, time.time(), time.time(), time.time(), 1, 128))
        self._link(b'test-entry', id_)

        obj_id = self.db.rowid('INSERT INTO objects (refcount,size) VALUES(?,?)', (1, 36))
        block_id = self.db.rowid('INSERT INTO blocks (refcount, obj_id, size) VALUES(?,?,?)',
                                 (1, obj_id, 512))
        self.backend['s3ql_data_%d' % obj_id] = b'foo'

        # Case 1
        self.db.execute('UPDATE inodes SET size=? WHERE id=?', (self.max_obj_size + 120, id_))
        self.db.execute('INSERT INTO inode_blocks (inode, blockno, block_id) VALUES(?, ?, ?)',
                        (id_, 1, block_id))
        self.assert_fsck(self.fsck.check_inodes_size)

        # Case 2
        self.db.execute('DELETE FROM inode_blocks WHERE inode=?', (id_,))
        self.db.execute('INSERT INTO inode_blocks (inode, blockno, block_id) VALUES(?, ?, ?)',
                        (id_, 0, block_id))
        self.db.execute('UPDATE inodes SET size=? WHERE id=?', (129, id_))
        self.assert_fsck(self.fsck.check_inodes_size)

        # Case 3
        self.db.execute('INSERT INTO inode_blocks (inode, blockno, block_id) VALUES(?, ?, ?)',
                        (id_, 1, block_id))
        self.db.execute('UPDATE inodes SET size=? WHERE id=?',
                        (self.max_obj_size + 120, id_))
        self.db.execute('UPDATE blocks SET refcount = refcount + 1 WHERE id = ?',
                        (block_id,))
        self.assert_fsck(self.fsck.check_inodes_size)


    def test_objects_id(self):
        # Create an object that only exists in the backend
        self.backend['s3ql_data_4364'] = b'Testdata'
        self.assert_fsck(self.fsck.check_objects_id)

        # Create an object that does not exist in the backend
        self.db.execute('INSERT INTO objects (id, refcount, size) VALUES(?, ?, ?)', (34, 1, 27))
        self.assert_fsck(self.fsck.check_objects_id)

    def test_blocks_obj_id(self):

        block_id = self.db.rowid('INSERT INTO blocks (refcount, obj_id, size) VALUES(?,?,?)',
                                 (1, 48, 128))

        id_ = self.db.rowid("INSERT INTO inodes (mode,uid,gid,mtime,atime,ctime,refcount,size) "
                            "VALUES (?,?,?,?,?,?,?,?)",
                            (stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR,
                             0, 0, time.time(), time.time(), time.time(), 1, 128))
        self.db.execute('INSERT INTO inode_blocks (inode, blockno, block_id) VALUES(?,?,?)',
                        (id_, 0, block_id))

        self._link(b'test-entry', id_)
        self.assert_fsck(self.fsck.check_blocks_obj_id)

    def test_missing_obj(self):

        obj_id = self.db.rowid('INSERT INTO objects (refcount, size) VALUES(1, 32)')
        block_id = self.db.rowid('INSERT INTO blocks (refcount, obj_id, size) VALUES(?,?,?)',
                                 (1, obj_id, 128))

        id_ = self.db.rowid("INSERT INTO inodes (mode,uid,gid,mtime,atime,ctime,refcount,size) "
                            "VALUES (?,?,?,?,?,?,?,?)",
                            (stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR,
                             0, 0, time.time(), time.time(), time.time(), 1, 128))
        self.db.execute('INSERT INTO inode_blocks (inode, blockno, block_id) VALUES(?,?,?)',
                        (id_, 0, block_id))

        self._link(b'test-entry', id_)
        self.assert_fsck(self.fsck.check_objects_id)


    def test_inode_blocks_inode(self):

        obj_id = self.db.rowid('INSERT INTO objects (refcount, size) VALUES(1, 42)')
        self.backend['s3ql_data_%d' % obj_id] = b'foo'

        block_id = self.db.rowid('INSERT INTO blocks (refcount, obj_id, size) VALUES(?,?,?)',
                                 (1, obj_id, 34))

        self.db.execute('INSERT INTO inode_blocks (inode, blockno, block_id) VALUES(?,?,?)',
                        (27, 0, block_id))

        self.assert_fsck(self.fsck.check_inode_blocks_inode)

    def test_inode_blocks_block_id(self):

        id_ = self.db.rowid("INSERT INTO inodes (mode,uid,gid,mtime,atime,ctime,refcount,size) "
                            "VALUES (?,?,?,?,?,?,?,?)",
                            (stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR,
                             0, 0, time.time(), time.time(), time.time(), 1, 128))
        self.db.execute('INSERT INTO inode_blocks (inode, blockno, block_id) VALUES(?,?,?)',
                        (id_, 0, 35))

        self._link(b'test-entry', id_)
        self.assert_fsck(self.fsck.check_inode_blocks_block_id)

    def test_symlinks_inode(self):

        self.db.execute('INSERT INTO symlink_targets (inode, target) VALUES(?,?)',
                        (42, b'somewhere else'))

        self.assert_fsck(self.fsck.check_symlinks_inode)

    def test_ext_attrs_inode(self):

        self.db.execute('INSERT INTO ext_attributes (name_id, inode, value) VALUES(?,?,?)',
                        (self._add_name(b'some name'), 34, b'some value'))

        self.assert_fsck(self.fsck.check_ext_attributes_inode)

    def test_ext_attrs_name(self):

        id_ = self.db.rowid("INSERT INTO inodes (mode,uid,gid,mtime,atime,ctime,refcount,size) "
                            "VALUES (?,?,?,?,?,?,?,?)",
                            (stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR,
                             0, 0, time.time(), time.time(), time.time(), 1, 128))
        self._link(b'test-entry', id_)

        self.db.execute('INSERT INTO ext_attributes (name_id, inode, value) VALUES(?,?,?)',
                        (34, id_, b'some value'))

        self.assert_fsck(self.fsck.check_ext_attributes_name)

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
                            (self._add_name(str(inode).encode()), inode, last))
            last = inode

        self.assert_fsck(self.fsck.check_loops)

    def test_obj_refcounts(self):

        obj_id = self.db.rowid('INSERT INTO objects (refcount, size) VALUES(1, 42)')
        block_id_1 = self.db.rowid('INSERT INTO blocks (refcount, obj_id, size) VALUES(?,?,?)',
                                 (1, obj_id, 0))
        block_id_2 = self.db.rowid('INSERT INTO blocks (refcount, obj_id, size) VALUES(?,?,?)',
                                   (1, obj_id, 0))
        self.backend['s3ql_data_%d' % obj_id] = b'foo'

        inode = self.db.rowid("INSERT INTO inodes (mode,uid,gid,mtime,atime,ctime,refcount,size) "
                              "VALUES (?,?,?,?,?,?,?,?)",
                              (stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR,
                               os.getuid(), os.getgid(), time.time(), time.time(), time.time(),
                               1, 2048))
        self._link(b'test-entry', inode)
        self.db.execute('INSERT INTO inode_blocks (inode, blockno, block_id) VALUES(?,?,?)',
                        (inode, 1, block_id_1))
        self.db.execute('INSERT INTO inode_blocks (inode, blockno, block_id) VALUES(?,?,?)',
                        (inode, 2, block_id_2))

        self.assert_fsck(self.fsck.check_objects_refcount)

    def test_orphaned_obj(self):

        self.db.rowid('INSERT INTO objects (refcount, size) VALUES(1, 33)')
        self.assert_fsck(self.fsck.check_objects_refcount)

    def test_wrong_block_refcount(self):

        obj_id = self.db.rowid('INSERT INTO objects (refcount, size) VALUES(1, 23)')
        self.backend['s3ql_data_%d' % obj_id] = b'foo'
        block_id = self.db.rowid('INSERT INTO blocks (refcount, obj_id, size) VALUES(?,?,?)',
                                 (1, obj_id, 0))

        inode = self.db.rowid("INSERT INTO inodes (mode,uid,gid,mtime,atime,ctime,refcount,size) "
                              "VALUES (?,?,?,?,?,?,?,?)",
                              (stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR, os.getuid(), os.getgid(),
                               time.time(), time.time(), time.time(), 1, self.max_obj_size))
        self._link(b'test-entry', inode)

        self.db.execute('INSERT INTO inode_blocks (inode, blockno, block_id) VALUES(?,?,?)',
                        (inode, 0, block_id))
        self.db.execute('INSERT INTO inode_blocks (inode, blockno, block_id) VALUES(?,?,?)',
                        (inode, 1, block_id))

        self.assert_fsck(self.fsck.check_blocks_refcount)

    def test_orphaned_block(self):

        obj_id = self.db.rowid('INSERT INTO objects (refcount, size) VALUES(1, 24)')
        self.backend['s3ql_data_%d' % obj_id] = b'foo'
        self.db.rowid('INSERT INTO blocks (refcount, obj_id, size) VALUES(?,?,?)',
                      (1, obj_id, 3))
        self.assert_fsck(self.fsck.check_blocks_refcount)

    def test_unix_size(self):

        inode = 42
        self.db.execute("INSERT INTO inodes (id, mode,uid,gid,mtime,atime,ctime,refcount,size) "
                        "VALUES (?,?,?,?,?,?,?,?,?)",
                        (inode, stat.S_IFIFO | stat.S_IRUSR | stat.S_IWUSR,
                         os.getuid(), os.getgid(), time.time(), time.time(), time.time(), 1, 0))
        self._link(b'test-entry', inode)

        self.fsck.found_errors = False
        self.fsck.check_unix()
        self.assertFalse(self.fsck.found_errors)

        self.db.execute('UPDATE inodes SET size = 1 WHERE id=?', (inode,))
        self.fsck.check_unix()
        self.assertTrue(self.fsck.found_errors)


    def test_unix_size_symlink(self):

        inode = 42
        target = b'some funny random string'
        self.db.execute("INSERT INTO inodes (id, mode,uid,gid,mtime,atime,ctime,refcount,size) "
                        "VALUES (?,?,?,?,?,?,?,?,?)",
                        (inode, stat.S_IFLNK | stat.S_IRUSR | stat.S_IWUSR,
                         os.getuid(), os.getgid(), time.time(), time.time(), time.time(), 1,
                         len(target)))
        self.db.execute('INSERT INTO symlink_targets (inode, target) VALUES(?,?)', (inode, target))
        self._link(b'test-entry', inode)

        self.fsck.found_errors = False
        self.fsck.check_unix()
        self.assertFalse(self.fsck.found_errors)

        self.db.execute('UPDATE inodes SET size = 0 WHERE id=?', (inode,))
        self.fsck.check_unix()
        self.assertTrue(self.fsck.found_errors)

    def test_unix_target(self):

        inode = 42
        self.db.execute("INSERT INTO inodes (id, mode,uid,gid,mtime,atime,ctime,refcount) "
                     "VALUES (?,?,?,?,?,?,?,?)",
                     (inode, stat.S_IFCHR | stat.S_IRUSR | stat.S_IWUSR,
                      os.getuid(), os.getgid(), time.time(), time.time(), time.time(), 1))
        self._link(b'test-entry', inode)

        self.fsck.found_errors = False
        self.fsck.check_unix()
        self.assertFalse(self.fsck.found_errors)

        self.db.execute('INSERT INTO symlink_targets (inode, target) VALUES(?,?)', (inode, 'foo'))
        self.fsck.check_unix()
        self.assertTrue(self.fsck.found_errors)

    def test_unix_nomode_reg(self):

        perms = stat.S_IRUSR | stat.S_IWUSR | stat.S_IROTH | stat.S_IRGRP
        stamp = time.time()
        inode = self.db.rowid("INSERT INTO inodes (mode,uid,gid,mtime,atime,ctime,refcount) "
                              "VALUES (?,?,?,?,?,?,?)",
                              (perms, os.getuid(), os.getgid(), stamp, stamp, stamp, 1))
        self._link(b'test-entry', inode)

        self.assert_fsck(self.fsck.check_unix)
        
        newmode = self.db.get_val('SELECT mode FROM inodes WHERE id=?', (inode,))
        self.assertEqual(stat.S_IMODE(newmode), perms)
        self.assertEqual(stat.S_IFMT(newmode), stat.S_IFREG)

    def test_unix_nomode_dir(self):

        perms = stat.S_IRUSR | stat.S_IWUSR | stat.S_IROTH | stat.S_IRGRP
        stamp = time.time()
        inode = self.db.rowid("INSERT INTO inodes (mode,uid,gid,mtime,atime,ctime,refcount) "
                              "VALUES (?,?,?,?,?,?,?)",
                              (perms, os.getuid(), os.getgid(), stamp, stamp, stamp, 1))
        inode2 = self.db.rowid("INSERT INTO inodes (mode,uid,gid,mtime,atime,ctime,refcount) "
                              "VALUES (?,?,?,?,?,?,?)",
                              (perms | stat.S_IFREG, os.getuid(), os.getgid(), stamp, 
                               stamp, stamp, 1))

        self._link(b'test-entry', inode)
        self._link(b'subentry', inode2, inode)

        self.assert_fsck(self.fsck.check_unix)
        
        newmode = self.db.get_val('SELECT mode FROM inodes WHERE id=?', (inode,))
        self.assertEqual(stat.S_IMODE(newmode), perms)
        self.assertEqual(stat.S_IFMT(newmode), stat.S_IFDIR)
        
        
    def test_unix_symlink_no_target(self):

        inode = self.db.rowid("INSERT INTO inodes (mode,uid,gid,mtime,atime,ctime,refcount) "
                              "VALUES (?,?,?,?,?,?,?)",
                              (stat.S_IFLNK | stat.S_IRUSR | stat.S_IWUSR,
                               os.getuid(), os.getgid(), time.time(), time.time(), time.time(), 1))
        self._link(b'test-entry', inode)
        self.fsck.check_unix()
        self.assertTrue(self.fsck.found_errors)

    def test_unix_rdev(self):

        inode = 42
        self.db.execute("INSERT INTO inodes (id, mode,uid,gid,mtime,atime,ctime,refcount) "
                     "VALUES (?,?,?,?,?,?,?,?)",
                     (inode, stat.S_IFIFO | stat.S_IRUSR | stat.S_IWUSR,
                      os.getuid(), os.getgid(), time.time(), time.time(), time.time(), 1))
        self._link(b'test-entry', inode)

        self.fsck.found_errors = False
        self.fsck.check_unix()
        self.assertFalse(self.fsck.found_errors)

        self.db.execute('UPDATE inodes SET rdev=? WHERE id=?', (42, inode))
        self.fsck.check_unix()
        self.assertTrue(self.fsck.found_errors)

    def test_unix_child(self):

        inode = self.db.rowid("INSERT INTO inodes (mode,uid,gid,mtime,atime,ctime,refcount) "
                   "VALUES (?,?,?,?,?,?,?)",
                   (stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR,
                    os.getuid(), os.getgid(), time.time(), time.time(), time.time(), 1))
        self._link(b'test-entry', inode)

        self.fsck.found_errors = False
        self.fsck.check_unix()
        self.assertFalse(self.fsck.found_errors)
        self.db.execute('INSERT INTO contents (name_id, inode, parent_inode) VALUES(?,?,?)',
                        (self._add_name(b'foo'), ROOT_INODE, inode))
        self.fsck.check_unix()
        self.assertTrue(self.fsck.found_errors)

    def test_unix_blocks(self):

        inode = self.db.rowid("INSERT INTO inodes (mode,uid,gid,mtime,atime,ctime,refcount) "
                              "VALUES (?,?,?,?,?,?,?)",
                              (stat.S_IFSOCK | stat.S_IRUSR | stat.S_IWUSR,
                               os.getuid(), os.getgid(), time.time(), time.time(), time.time(), 1))
        self._link(b'test-entry', inode)

        self.fsck.found_errors = False
        self.fsck.check_unix()
        self.assertFalse(self.fsck.found_errors)

        obj_id = self.db.rowid('INSERT INTO objects (refcount, size) VALUES(1, 32)')
        block_id = self.db.rowid('INSERT INTO blocks (refcount, obj_id, size) VALUES(?,?,?)',
                                 (1, obj_id, 0))

        self.db.execute('INSERT INTO inode_blocks (inode, blockno, block_id) VALUES(?,?,?)',
                        (inode, 1, block_id))

        self.fsck.check_unix()
        self.assertTrue(self.fsck.found_errors)
