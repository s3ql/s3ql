'''
t3_fs_api.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import division, print_function
from _common import TestCase
from llfuse import FUSEError
from random import randint
from s3ql import fs
from s3ql.backends import local
from s3ql.backends.common import BucketPool
from s3ql.block_cache import BlockCache
from s3ql.common import ROOT_INODE, create_tables, init_tables
from s3ql.database import Connection
from s3ql.fsck import Fsck
import errno
import llfuse
import os
import shutil
import stat
import tempfile
import time
import unittest2 as unittest

        
# We need to access to protected members
#pylint: disable=W0212
        
class Ctx(object):
    def __init__(self):
        self.uid = randint(0, 2 ** 32)
        self.gid = randint(0, 2 ** 32)

# Determine system clock granularity
stamp1 = time.time()
stamp2 = stamp1
while stamp1 == stamp2:
    stamp2 = time.time()
CLOCK_GRANULARITY = 2 * (stamp2 - stamp1)
del stamp1
del stamp2

class fs_api_tests(TestCase):

    def setUp(self):
        self.bucket_dir = tempfile.mkdtemp()
        self.bucket_pool = BucketPool(lambda: local.Bucket(self.bucket_dir, None, None))
        self.bucket = self.bucket_pool.pop_conn()
        self.cachedir = tempfile.mkdtemp()
        self.blocksize = 1024

        self.dbfile = tempfile.NamedTemporaryFile()
        self.db = Connection(self.dbfile.name)
        create_tables(self.db)
        init_tables(self.db)

        # Tested methods assume that they are called from
        # file system request handler
        llfuse.lock.acquire()
        
        self.block_cache = BlockCache(self.bucket_pool, self.db, self.cachedir + "/cache",
                                      self.blocksize * 5)
        self.server = fs.Operations(self.block_cache, self.db, self.blocksize)
          
        self.server.init()

        # Keep track of unused filenames
        self.name_cnt = 0

    def tearDown(self):
        self.server.destroy()
        self.block_cache.destroy()     
        shutil.rmtree(self.cachedir)
        shutil.rmtree(self.bucket_dir)
        llfuse.lock.release()

    @staticmethod
    def random_data(len_):
        with open("/dev/urandom", "rb") as fd:
            return fd.read(len_)

    def fsck(self):
        self.block_cache.clear()
        self.server.inodes.flush()
        fsck = Fsck(self.cachedir + '/cache', self.bucket,
                  { 'blocksize': self.blocksize }, self.db)
        fsck.check()
        self.assertFalse(fsck.found_errors)

    def newname(self):
        self.name_cnt += 1
        return "s3ql_%d" % self.name_cnt

    def test_getattr_root(self):
        self.assertTrue(stat.S_ISDIR(self.server.getattr(ROOT_INODE).mode))
        self.fsck()

    def test_create(self):
        ctx = Ctx()
        mode = self.dir_mode()
        name = self.newname()

        inode_p_old = self.server.getattr(ROOT_INODE).copy()
        time.sleep(CLOCK_GRANULARITY)
        self.server._create(ROOT_INODE, name, mode, ctx)

        id_ = self.db.get_val('SELECT inode FROM contents JOIN names ON name_id = names.id '
                              'WHERE name=? AND parent_inode = ?', (name, ROOT_INODE))

        inode = self.server.getattr(id_)

        self.assertEqual(inode.mode, mode)
        self.assertEqual(inode.uid, ctx.uid)
        self.assertEqual(inode.gid, ctx.gid)
        self.assertEqual(inode.refcount, 1)
        self.assertEqual(inode.size, 0)

        inode_p_new = self.server.getattr(ROOT_INODE)

        self.assertGreater(inode_p_new.mtime, inode_p_old.mtime)
        self.assertGreater(inode_p_new.ctime, inode_p_old.ctime)

        self.fsck()

    def test_extstat(self):
        # Test with zero contents
        self.server.extstat()

        # Test with empty file
        (fh, inode) = self.server.create(ROOT_INODE, self.newname(),
                                         self.file_mode(), Ctx())
        self.server.release(fh)
        self.server.extstat()

        # Test with data in file
        fh = self.server.open(inode.id, os.O_RDWR)
        self.server.write(fh, 0, 'foobar')
        self.server.release(fh)

        self.server.extstat()

        self.fsck()

    @staticmethod
    def dir_mode():
        return (randint(0, 07777) & ~stat.S_IFDIR) | stat.S_IFDIR

    @staticmethod
    def file_mode():
        return (randint(0, 07777) & ~stat.S_IFREG) | stat.S_IFREG

    def test_getxattr(self):
        (fh, inode) = self.server.create(ROOT_INODE, self.newname(),
                                         self.file_mode(), Ctx())
        self.server.release(fh)

        self.assertRaises(FUSEError, self.server.getxattr, inode.id, 'nonexistant-attr')

        self.server.setxattr(inode.id, 'my-attr', 'strabumm!')
        self.assertEqual(self.server.getxattr(inode.id, 'my-attr'), 'strabumm!')

        self.fsck()

    def test_link(self):
        name = self.newname()

        inode_p_new = self.server.mkdir(ROOT_INODE, self.newname(),
                                        self.dir_mode(), Ctx())
        inode_p_new_before = self.server.getattr(inode_p_new.id).copy()

        (fh, inode) = self.server.create(ROOT_INODE, self.newname(),
                                         self.file_mode(), Ctx())
        self.server.release(fh)
        time.sleep(CLOCK_GRANULARITY)

        inode_before = self.server.getattr(inode.id).copy()
        self.server.link(inode.id, inode_p_new.id, name)

        inode_after = self.server.lookup(inode_p_new.id, name)
        inode_p_new_after = self.server.getattr(inode_p_new.id)

        id_ = self.db.get_val('SELECT inode FROM contents JOIN names ON names.id = name_id '
                              'WHERE name=? AND parent_inode = ?', (name, inode_p_new.id))

        self.assertEqual(inode_before.id, id_)
        self.assertEqual(inode_after.refcount, 2)
        self.assertGreater(inode_after.ctime, inode_before.ctime)
        self.assertLess(inode_p_new_before.mtime, inode_p_new_after.mtime)
        self.assertLess(inode_p_new_before.ctime, inode_p_new_after.ctime)

        self.fsck()

    def test_listxattr(self):
        (fh, inode) = self.server.create(ROOT_INODE, self.newname(),
                                         self.file_mode(), Ctx())
        self.server.release(fh)

        self.assertListEqual([], self.server.listxattr(inode.id))

        self.server.setxattr(inode.id, 'key1', 'blub')
        self.assertListEqual(['key1'], self.server.listxattr(inode.id))

        self.server.setxattr(inode.id, 'key2', 'blub')
        self.assertListEqual(sorted(['key1', 'key2']),
                             sorted(self.server.listxattr(inode.id)))

        self.fsck()

    def test_read(self):

        len_ = self.blocksize
        data = self.random_data(len_)
        off = self.blocksize // 2
        (fh, inode) = self.server.create(ROOT_INODE, self.newname(),
                                     self.file_mode(), Ctx())

        self.server.write(fh, off, data)
        inode_before = self.server.getattr(inode.id).copy()
        time.sleep(CLOCK_GRANULARITY)
        self.assertTrue(self.server.read(fh, off, len_) == data)
        inode_after = self.server.getattr(inode.id)
        self.assertGreater(inode_after.atime, inode_before.atime)
        self.assertTrue(self.server.read(fh, 0, len_) == b"\0" * off + data[:off])
        self.assertTrue(self.server.read(fh, self.blocksize, len_) == data[off:])
        self.server.release(fh)

        self.fsck()

    def test_readdir(self):

        # Create a few entries
        names = [ 'entry_%2d' % i for i in range(20) ]
        for name in names:
            (fh, _) = self.server.create(ROOT_INODE, name,
                                         self.file_mode(), Ctx())
            self.server.release(fh)
            
        # Delete some to make sure that we don't have continous rowids
        remove_no = [0, 2, 3, 5, 9]
        for i in remove_no:
            self.server.unlink(ROOT_INODE, names[i])
            del names[i]

        # Read all
        fh = self.server.opendir(ROOT_INODE)
        self.assertListEqual(sorted(names + ['lost+found']) ,
                             sorted(x[0] for x in self.server.readdir(fh, 0)))
        self.server.releasedir(fh)

        # Read in parts
        fh = self.server.opendir(ROOT_INODE)
        entries = list()
        try:
            next_ = 0
            while True:
                gen = self.server.readdir(fh, next_)
                for _ in range(3):
                    (name, _, next_) = next(gen)
                    entries.append(name)
                    
        except StopIteration:
            pass

        self.assertListEqual(sorted(names + ['lost+found']) ,
                             sorted(entries))
        self.server.releasedir(fh)

        self.fsck()

    def test_release(self):
        name = self.newname()

        # Test that entries are deleted when they're no longer referenced
        (fh, inode) = self.server.create(ROOT_INODE, name,
                                         self.file_mode(), Ctx())
        self.server.write(fh, 0, 'foobar')
        self.server.unlink(ROOT_INODE, name)
        self.assertFalse(self.db.has_val('SELECT 1 FROM contents JOIN names ON names.id = name_id '
                                         'WHERE name=? AND parent_inode = ?', (name, ROOT_INODE)))
        self.assertTrue(self.server.getattr(inode.id).id)
        self.server.release(fh)

        self.assertFalse(self.db.has_val('SELECT 1 FROM inodes WHERE id=?', (inode.id,)))

        self.fsck()

    def test_removexattr(self):
        (fh, inode) = self.server.create(ROOT_INODE, self.newname(),
                                         self.file_mode(), Ctx())
        self.server.release(fh)

        self.assertRaises(FUSEError, self.server.removexattr, inode.id, 'some name')
        self.server.setxattr(inode.id, 'key1', 'blub')
        self.server.removexattr(inode.id, 'key1')
        self.assertListEqual([], self.server.listxattr(inode.id))

        self.fsck()

    def test_rename(self):
        oldname = self.newname()
        newname = self.newname()

        inode = self.server.mkdir(ROOT_INODE, oldname, self.dir_mode(), Ctx())

        inode_p_new = self.server.mkdir(ROOT_INODE, self.newname(), self.dir_mode(), Ctx())
        inode_p_new_before = self.server.getattr(inode_p_new.id).copy()
        inode_p_old_before = self.server.getattr(ROOT_INODE).copy()
        time.sleep(CLOCK_GRANULARITY)

        self.server.rename(ROOT_INODE, oldname, inode_p_new.id, newname)

        inode_p_old_after = self.server.getattr(ROOT_INODE)
        inode_p_new_after = self.server.getattr(inode_p_new.id)

        self.assertFalse(self.db.has_val('SELECT inode FROM contents JOIN names ON names.id = name_id '
                                         'WHERE name=? AND parent_inode = ?', (oldname, ROOT_INODE)))
        id_ = self.db.get_val('SELECT inode FROM contents JOIN names ON names.id == name_id '
                              'WHERE name=? AND parent_inode = ?', (newname, inode_p_new.id))
        self.assertEqual(inode.id, id_)

        self.assertLess(inode_p_new_before.mtime, inode_p_new_after.mtime)
        self.assertLess(inode_p_new_before.ctime, inode_p_new_after.ctime)
        self.assertLess(inode_p_old_before.mtime, inode_p_old_after.mtime)
        self.assertLess(inode_p_old_before.ctime, inode_p_old_after.ctime)


        self.fsck()

    def test_replace_file(self):
        oldname = self.newname()
        newname = self.newname()

        (fh, inode) = self.server.create(ROOT_INODE, oldname, self.file_mode(), Ctx())
        self.server.write(fh, 0, 'some data to deal with')
        self.server.release(fh)
        self.server.setxattr(inode.id, 'test_xattr', '42*8')

        inode_p_new = self.server.mkdir(ROOT_INODE, self.newname(), self.dir_mode(), Ctx())
        inode_p_new_before = self.server.getattr(inode_p_new.id).copy()
        inode_p_old_before = self.server.getattr(ROOT_INODE).copy()

        (fh, inode2) = self.server.create(inode_p_new.id, newname, self.file_mode(), Ctx())
        self.server.write(fh, 0, 'even more data to deal with')
        self.server.release(fh)
        self.server.setxattr(inode2.id, 'test_xattr', '42*8')

        time.sleep(CLOCK_GRANULARITY)
        self.server.rename(ROOT_INODE, oldname, inode_p_new.id, newname)

        inode_p_old_after = self.server.getattr(ROOT_INODE)
        inode_p_new_after = self.server.getattr(inode_p_new.id)

        self.assertFalse(self.db.has_val('SELECT inode FROM contents JOIN names ON names.id = name_id '
                                         'WHERE name=? AND parent_inode = ?', (oldname, ROOT_INODE)))
        id_ = self.db.get_val('SELECT inode FROM contents JOIN names ON names.id = name_id '
                              'WHERE name=? AND parent_inode = ?', (newname, inode_p_new.id))
        self.assertEqual(inode.id, id_)

        self.assertLess(inode_p_new_before.mtime, inode_p_new_after.mtime)
        self.assertLess(inode_p_new_before.ctime, inode_p_new_after.ctime)
        self.assertLess(inode_p_old_before.mtime, inode_p_old_after.mtime)
        self.assertLess(inode_p_old_before.ctime, inode_p_old_after.ctime)

        self.assertFalse(self.db.has_val('SELECT id FROM inodes WHERE id=?', (inode2.id,)))

        self.fsck()

    def test_replace_dir(self):
        oldname = self.newname()
        newname = self.newname()

        inode = self.server.mkdir(ROOT_INODE, oldname, self.dir_mode(), Ctx())

        inode_p_new = self.server.mkdir(ROOT_INODE, self.newname(), self.dir_mode(), Ctx())
        inode_p_new_before = self.server.getattr(inode_p_new.id).copy()
        inode_p_old_before = self.server.getattr(ROOT_INODE).copy()

        inode2 = self.server.mkdir(inode_p_new.id, newname, self.dir_mode(), Ctx())

        time.sleep(CLOCK_GRANULARITY)
        self.server.rename(ROOT_INODE, oldname, inode_p_new.id, newname)

        inode_p_old_after = self.server.getattr(ROOT_INODE)
        inode_p_new_after = self.server.getattr(inode_p_new.id)

        self.assertFalse(self.db.has_val('SELECT inode FROM contents JOIN names ON names.id = name_id '
                                         'WHERE name=? AND parent_inode = ?', (oldname, ROOT_INODE)))
        id_ = self.db.get_val('SELECT inode FROM contents JOIN names ON names.id = name_id '
                              'WHERE name=? AND parent_inode = ?', (newname, inode_p_new.id))
        self.assertEqual(inode.id, id_)

        self.assertLess(inode_p_new_before.mtime, inode_p_new_after.mtime)
        self.assertLess(inode_p_new_before.ctime, inode_p_new_after.ctime)
        self.assertLess(inode_p_old_before.mtime, inode_p_old_after.mtime)
        self.assertLess(inode_p_old_before.ctime, inode_p_old_after.ctime)

        self.assertFalse(self.db.has_val('SELECT id FROM inodes WHERE id=?', (inode2.id,)))

        self.fsck()

    def test_setattr(self):
        (fh, inode) = self.server.create(ROOT_INODE, self.newname(), 0641, Ctx())
        self.server.release(fh)
        inode_old = self.server.getattr(inode.id).copy()

        attr = llfuse.EntryAttributes()
        attr.st_mode = self.file_mode()
        attr.st_uid = randint(0, 2 ** 32)
        attr.st_gid = randint(0, 2 ** 32)
        attr.st_rdev = randint(0, 2 ** 32)
        attr.st_atime = time.timezone + randint(0, 2 ** 32) / 10 ** 6
        attr.st_mtime = time.timezone + randint(0, 2 ** 32) / 10 ** 6

        time.sleep(CLOCK_GRANULARITY)
        self.server.setattr(inode.id, attr)
        inode_new = self.server.getattr(inode.id)
        self.assertGreater(inode_new.ctime, inode_old.ctime)

        for key in attr.__slots__:
            if getattr(attr, key) is not None:
                self.assertEquals(getattr(attr, key), 
                                  getattr(inode_new, key))


    def test_truncate(self):
        len_ = int(2.7 * self.blocksize)
        data = self.random_data(len_)
        attr = llfuse.EntryAttributes()

        (fh, inode) = self.server.create(ROOT_INODE, self.newname(), self.file_mode(), Ctx())
        self.server.write(fh, 0, data)
        
        attr.st_size = len_ // 2
        self.server.setattr(inode.id, attr)
        self.assertTrue(self.server.read(fh, 0, len_) == data[:len_ // 2])
        attr.st_size = len_ 
        self.server.setattr(inode.id, attr)
        self.assertTrue(self.server.read(fh, 0, len_)
                        == data[:len_ // 2] + b'\0' * (len_ // 2))
        self.server.release(fh)

        self.fsck()

    def test_truncate_0(self):
        len1 = 158
        len2 = 133
        attr = llfuse.EntryAttributes()
        
        (fh, inode) = self.server.create(ROOT_INODE, self.newname(),
                                         self.file_mode(), Ctx())
        self.server.write(fh, 0, self.random_data(len1))
        self.server.release(fh)
        self.server.inodes.flush()
        
        fh = self.server.open(inode.id, os.O_RDWR)     
        attr.st_size = 0
        self.server.setattr(inode.id, attr)
        self.server.write(fh, 0, self.random_data(len2))
        self.server.release(fh)

        self.fsck()
        
    def test_setxattr(self):
        (fh, inode) = self.server.create(ROOT_INODE, self.newname(),
                                         self.file_mode(), Ctx())
        self.server.release(fh)

        self.server.setxattr(inode.id, 'my-attr', 'strabumm!')
        self.assertEqual(self.server.getxattr(inode.id, 'my-attr'), 'strabumm!')

        self.fsck()

    def test_statfs(self):
        # Test with zero contents
        self.server.statfs()

        # Test with empty file
        (fh, inode) = self.server.create(ROOT_INODE, self.newname(),
                                         self.file_mode(), Ctx())
        self.server.release(fh)
        self.server.statfs()

        # Test with data in file
        fh = self.server.open(inode.id, None)
        self.server.write(fh, 0, 'foobar')
        self.server.release(fh)

        self.server.statfs()

    def test_symlink(self):
        target = self.newname()
        name = self.newname()

        inode_p_before = self.server.getattr(ROOT_INODE).copy()
        time.sleep(CLOCK_GRANULARITY)
        inode = self.server.symlink(ROOT_INODE, name, target, Ctx())
        inode_p_after = self.server.getattr(ROOT_INODE)

        self.assertEqual(target, self.server.readlink(inode.id))

        id_ = self.db.get_val('SELECT inode FROM contents JOIN names ON names.id = name_id '
                              'WHERE name=? AND parent_inode = ?', (name, ROOT_INODE))

        self.assertEqual(inode.id, id_)
        self.assertLess(inode_p_before.mtime, inode_p_after.mtime)
        self.assertLess(inode_p_before.ctime, inode_p_after.ctime)


    def test_unlink(self):
        name = self.newname()

        (fh, inode) = self.server.create(ROOT_INODE, name, self.file_mode(), Ctx())
        self.server.write(fh, 0, 'some data to deal with')
        self.server.release(fh)

        # Add extended attributes
        self.server.setxattr(inode.id, 'test_xattr', '42*8')

        inode_p_before = self.server.getattr(ROOT_INODE).copy()
        time.sleep(CLOCK_GRANULARITY)
        self.server.unlink(ROOT_INODE, name)
        inode_p_after = self.server.getattr(ROOT_INODE)

        self.assertLess(inode_p_before.mtime, inode_p_after.mtime)
        self.assertLess(inode_p_before.ctime, inode_p_after.ctime)

        self.assertFalse(self.db.has_val('SELECT inode FROM contents JOIN names ON names.id = name_id '
                                         'WHERE name=? AND parent_inode = ?', (name, ROOT_INODE)))
        self.assertFalse(self.db.has_val('SELECT id FROM inodes WHERE id=?', (inode.id,)))

        self.fsck()

    def test_rmdir(self):
        name = self.newname()
        inode = self.server.mkdir(ROOT_INODE, name, self.dir_mode(), Ctx())
        inode_p_before = self.server.getattr(ROOT_INODE).copy()
        time.sleep(CLOCK_GRANULARITY)
        self.server.rmdir(ROOT_INODE, name)
        inode_p_after = self.server.getattr(ROOT_INODE)

        self.assertLess(inode_p_before.mtime, inode_p_after.mtime)
        self.assertLess(inode_p_before.ctime, inode_p_after.ctime)
        self.assertFalse(self.db.has_val('SELECT inode FROM contents JOIN names ON names.id = name_id '
                                         'WHERE name=? AND parent_inode = ?', (name, ROOT_INODE)))
        self.assertFalse(self.db.has_val('SELECT id FROM inodes WHERE id=?', (inode.id,)))

        self.fsck()

    def test_relink(self):
        name = self.newname()
        name2 = self.newname()
        data = 'some data to deal with'

        (fh, inode) = self.server.create(ROOT_INODE, name, self.file_mode(), Ctx())
        self.server.write(fh, 0, data)
        self.server.unlink(ROOT_INODE, name)
        self.assertFalse(self.db.has_val('SELECT inode FROM contents JOIN names ON names.id = name_id '
                                         'WHERE name=? AND parent_inode = ?', (name, ROOT_INODE)))
        self.assertTrue(self.db.has_val('SELECT id FROM inodes WHERE id=?', (inode.id,)))

        self.server.link(inode.id, ROOT_INODE, name2)
        self.server.release(fh)

        fh = self.server.open(inode.id, os.O_RDONLY)
        self.assertTrue(self.server.read(fh, 0, len(data)) == data)
        self.server.release(fh)
        self.fsck()

    def test_write(self):
        len_ = self.blocksize
        data = self.random_data(len_)
        off = self.blocksize // 2
        (fh, inode) = self.server.create(ROOT_INODE, self.newname(),
                                     self.file_mode(), Ctx())
        inode_before = self.server.getattr(inode.id).copy()
        time.sleep(CLOCK_GRANULARITY)
        self.server.write(fh, off, data)
        inode_after = self.server.getattr(inode.id)

        self.assertGreater(inode_after.mtime, inode_before.mtime)
        self.assertGreater(inode_after.ctime, inode_before.ctime)
        self.assertEqual(inode_after.size, off + len_)

        self.server.write(fh, 0, data)
        inode_after = self.server.getattr(inode.id)
        self.assertEqual(inode_after.size, off + len_)

        self.server.release(fh)

        self.fsck()

    def test_edit(self):
        len_ = self.blocksize
        data = self.random_data(len_)
        (fh, inode) = self.server.create(ROOT_INODE, self.newname(),
                                     self.file_mode(), Ctx())
        self.server.write(fh, 0, data)
        self.server.release(fh)
        
        self.block_cache.clear()
        
        fh = self.server.open(inode.id, os.O_RDWR)
        attr = llfuse.EntryAttributes()
        attr.st_size = 0
        self.server.setattr(inode.id, attr)
        self.server.write(fh, 0, data[50:])
        self.server.release(fh)
        
        self.fsck()
        
    def test_copy_tree(self):

        src_inode = self.server.mkdir(ROOT_INODE, 'source', self.dir_mode(), Ctx())
        dst_inode = self.server.mkdir(ROOT_INODE, 'dest', self.dir_mode(), Ctx())

        # Create file
        (fh, f1_inode) = self.server.create(src_inode.id, 'file1',
                                            self.file_mode(), Ctx())
        self.server.write(fh, 0, 'file1 contents')
        self.server.release(fh)

        # Create hardlink
        (fh, f2_inode) = self.server.create(src_inode.id, 'file2',
                                            self.file_mode(), Ctx())
        self.server.write(fh, 0, 'file2 contents')
        self.server.release(fh)
        f2_inode = self.server.link(f2_inode.id, src_inode.id, 'file2_hardlink')

        # Create subdirectory
        d1_inode = self.server.mkdir(src_inode.id, 'dir1', self.dir_mode(), Ctx())
        d2_inode = self.server.mkdir(d1_inode.id, 'dir2', self.dir_mode(), Ctx())

        # ..with a 3rd hardlink
        f2_inode = self.server.link(f2_inode.id, d1_inode.id, 'file2_hardlink')

        # Replicate
        self.server.copy_tree(src_inode.id, dst_inode.id)

        # Change files
        fh = self.server.open(f1_inode.id, os.O_RDWR)
        self.server.write(fh, 0, 'new file1 contents')
        self.server.release(fh)

        fh = self.server.open(f2_inode.id, os.O_RDWR)
        self.server.write(fh, 0, 'new file2 contents')
        self.server.release(fh)

        # Get copy properties
        f1_inode_c = self.server.lookup(dst_inode.id, 'file1')
        f2_inode_c = self.server.lookup(dst_inode.id, 'file2')
        f2h_inode_c = self.server.lookup(dst_inode.id, 'file2_hardlink')
        d1_inode_c = self.server.lookup(dst_inode.id, 'dir1')
        d2_inode_c = self.server.lookup(d1_inode_c.id, 'dir2')
        f2_h_inode_c = self.server.lookup(d1_inode_c.id, 'file2_hardlink')
        
        # Check file1
        fh = self.server.open(f1_inode_c.id, os.O_RDWR)
        self.assertEqual(self.server.read(fh, 0, 42), 'file1 contents')
        self.server.release(fh)
        self.assertNotEqual(f1_inode.id, f1_inode_c.id)

        # Check file2
        fh = self.server.open(f2_inode_c.id, os.O_RDWR)
        self.assertTrue(self.server.read(fh, 0, 42) == 'file2 contents')
        self.server.release(fh)
        self.assertEqual(f2_inode_c.id, f2h_inode_c.id)
        self.assertEqual(f2_inode_c.refcount, 3)
        self.assertNotEqual(f2_inode.id, f2_inode_c.id)
        self.assertEqual(f2_h_inode_c.id, f2_inode_c.id)
        
        # Check subdir1
        self.assertNotEqual(d1_inode.id, d1_inode_c.id)
        self.assertNotEqual(d2_inode.id, d2_inode_c.id)     
        
        self.fsck()

    def test_lock_tree(self):

        inode1 = self.server.mkdir(ROOT_INODE, 'source', self.dir_mode(), Ctx())

        # Create file
        (fh, inode1a) = self.server.create(inode1.id, 'file1',
                                            self.file_mode(), Ctx())
        self.server.write(fh, 0, 'file1 contents')
        self.server.release(fh)

        # Create subdirectory
        inode2 = self.server.mkdir(inode1.id, 'dir1', self.dir_mode(), Ctx())
        (fh, inode2a) = self.server.create(inode2.id, 'file2',
                                           self.file_mode(), Ctx())
        self.server.write(fh, 0, 'file2 contents')
        self.server.release(fh)   

        # Another file
        (fh, inode3) = self.server.create(ROOT_INODE, 'file1',
                                          self.file_mode(), Ctx())
        self.server.release(fh)  
        
        # Lock
        self.server.lock_tree(inode1.id)
        
        for i in (inode1.id, inode1a.id, inode2.id, inode2a.id):
            self.assertTrue(self.server.inodes[i].locked)
        
        # Remove
        with self.assertRaises(FUSEError) as cm:
            self.server._remove(inode1.id, 'file1', inode1a.id)
        self.assertEqual(cm.exception.errno, errno.EPERM)
        
        # Rename / Replace
        with self.assertRaises(FUSEError) as cm:
            self.server.rename(ROOT_INODE, 'file1', inode1.id, 'file2')
        self.assertEqual(cm.exception.errno, errno.EPERM)
        with self.assertRaises(FUSEError) as cm:
            self.server.rename(inode1.id, 'file1', ROOT_INODE, 'file2')
        self.assertEqual(cm.exception.errno, errno.EPERM)
                
        # Open
        with self.assertRaises(FUSEError) as cm:
            self.server.open(inode2a.id, os.O_RDWR)
        self.assertEqual(cm.exception.errno, errno.EPERM)
        with self.assertRaises(FUSEError) as cm:
            self.server.open(inode2a.id, os.O_WRONLY)
        self.assertEqual(cm.exception.errno, errno.EPERM)
        self.server.release(self.server.open(inode3.id, os.O_WRONLY))
                
        # Write
        fh = self.server.open(inode2a.id, os.O_RDONLY)
        with self.assertRaises(FUSEError) as cm:
            self.server.write(fh, 0, 'foo')
        self.assertEqual(cm.exception.errno, errno.EPERM)
        self.server.release(fh)
        
        # Create
        with self.assertRaises(FUSEError) as cm:
            self.server._create(inode2.id, 'dir1', self.dir_mode(), Ctx())
        self.assertEqual(cm.exception.errno, errno.EPERM)
                 
        # Setattr
        with self.assertRaises(FUSEError) as cm:
            self.server.setattr(inode2a.id, dict())
        self.assertEqual(cm.exception.errno, errno.EPERM)
        
        # xattr
        with self.assertRaises(FUSEError) as cm:
            self.server.setxattr(inode2.id, 'name', 'value')
        self.assertEqual(cm.exception.errno, errno.EPERM)        
        with self.assertRaises(FUSEError) as cm:
            self.server.removexattr(inode2.id, 'name')
        self.assertEqual(cm.exception.errno, errno.EPERM)        

        self.fsck()
        
    def test_remove_tree(self):

        inode1 = self.server.mkdir(ROOT_INODE, 'source', self.dir_mode(), Ctx())

        # Create file
        (fh, inode1a) = self.server.create(inode1.id, 'file1',
                                            self.file_mode(), Ctx())
        self.server.write(fh, 0, 'file1 contents')
        self.server.release(fh)

        # Create subdirectory
        inode2 = self.server.mkdir(inode1.id, 'dir1', self.dir_mode(), Ctx())
        (fh, inode2a) = self.server.create(inode2.id, 'file2',
                                           self.file_mode(), Ctx())
        self.server.write(fh, 0, 'file2 contents')
        self.server.release(fh)   

        # Remove
        self.server.remove_tree(ROOT_INODE, 'source')

        for (id_p, name) in ((ROOT_INODE, 'source'),
                             (inode1.id, 'file1'),
                             (inode1.id, 'dir1'),
                             (inode2.id, 'file2')):
            self.assertFalse(self.db.has_val('SELECT inode FROM contents JOIN names ON names.id = name_id '
                                             'WHERE name=? AND parent_inode = ?', (name, id_p)))
            
        for id_ in (inode1.id, inode1a.id, inode2.id, inode2a.id):
            self.assertFalse(self.db.has_val('SELECT id FROM inodes WHERE id=?', (id_,)))
        
        self.fsck()
        

def suite():
    return unittest.makeSuite(fs_api_tests)

if __name__ == "__main__":
    unittest.main()
