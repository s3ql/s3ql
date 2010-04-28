'''
t3_fs_api.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function

from random import randint
from s3ql import mkfs, fs, fsck
from s3ql.backends import local
from s3ql.common import ROOT_INODE
from llfuse import FUSEError
from s3ql.block_cache import BlockCache
from s3ql.database import ConnectionManager
from _common import TestCase
import os
import stat
import time
import unittest2 as unittest
import shutil
import threading
import tempfile

class Ctx(object):
    def __init__(self):
        self.uid = randint(0, 2 ** 32)
        self.gid = randint(0, 2 ** 32)


class fs_api_tests(TestCase):

    def setUp(self):
        self.bucket_dir = tempfile.mkdtemp()
        self.bucket = local.Connection().get_bucket(self.bucket_dir)
        self.cachedir = tempfile.mkdtemp() + "/"
        self.blocksize = 1024

        self.dbfile = tempfile.NamedTemporaryFile()
        self.dbcm = ConnectionManager(self.dbfile.name)
        with self.dbcm() as conn:
            mkfs.setup_tables(conn)
            mkfs.init_tables(conn)

        self.cache = BlockCache(self.bucket, self.cachedir, self.blocksize * 5, self.dbcm)
        self.lock = threading.Lock()
        self.server = fs.Operations(self.dbcm, self.cache, self.lock, self.blocksize)
        self.server.init()

        # Keep track of unused filenames
        self.name_cnt = 0

        self.lock.acquire()

    def tearDown(self):
        self.server.destroy()
        self.cache.clear()
        shutil.rmtree(self.cachedir)
        shutil.rmtree(self.bucket_dir)

    @staticmethod
    def random_data(len_):
        with open("/dev/urandom", "rb") as fd:
            return fd.read(len_)

    def fsck(self):
        self.cache.clear()
        self.server.inodes.flush()
        fsck.fsck(self.dbcm, self.cachedir, self.bucket, 
                  { 'blocksize': self.blocksize })
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
        self.server._create(ROOT_INODE, name, mode, ctx, nlink_off=1)

        id_ = self.dbcm.get_val('SELECT inode FROM contents WHERE name=? AND '
                                'parent_inode = ?', (name, ROOT_INODE))

        inode = self.server.getattr(id_)

        self.assertEqual(inode.mode, mode)
        self.assertEqual(inode.uid, ctx.uid)
        self.assertEqual(inode.gid, ctx.gid)
        self.assertEqual(inode.refcount, 1)
        self.assertEqual(inode.nlink_off, 1)
        self.assertEqual(inode.size, 0)

        inode_p_new = self.server.getattr(ROOT_INODE)

        self.assertEqual(inode_p_new.nlink_off, inode_p_old.nlink_off + 1)
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
        inode_p_old_before = self.server.getattr(ROOT_INODE).copy()

        (fh, inode) = self.server.create(ROOT_INODE, self.newname(),
                                         self.file_mode(), Ctx())
        self.server.release(fh)

        inode_before = self.server.getattr(inode.id).copy()
        self.server.link(inode.id, inode_p_new.id, name)

        inode_after = self.server.lookup(inode_p_new.id, name)
        inode_p_old_after = self.server.getattr(ROOT_INODE)
        inode_p_new_after = self.server.getattr(inode_p_new.id)

        id_ = self.dbcm.get_val('SELECT inode FROM contents WHERE name=? AND '
                                'parent_inode = ?', (name, inode_p_new.id))

        self.assertEqual(inode_before.id, id_)
        self.assertEqual(inode_after.refcount, 2)
        self.assertGreater(inode_after.ctime, inode_before.ctime)
        self.assertLess(inode_p_new_before.mtime, inode_p_new_after.mtime)
        self.assertLess(inode_p_new_before.ctime, inode_p_new_after.ctime)
        self.assertEqual(inode_p_old_before.nlink_off, inode_p_old_after.nlink_off)
        self.assertEqual(inode_p_new_before.nlink_off, inode_p_new_after.nlink_off)

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
        self.assertTrue(self.server.read(fh, off, len_) == data)
        inode_after = self.server.getattr(inode.id)
        self.assertGreater(inode_after.atime, inode_before.atime)
        self.assertGreater(inode_after.ctime, inode_before.ctime)
        self.assertTrue(self.server.read(fh, 0, len_) == b"\0" * off + data[:off])
        self.assertTrue(self.server.read(fh, self.blocksize, len_) == data[off:])
        self.server.release(fh)

        self.fsck()

    def test_readdir(self):

        # Create a few entries
        names = [ 'entry_%2d' % i for i in range(10) ]
        for name in names:
            (fh, _) = self.server.create(ROOT_INODE, name,
                                         self.file_mode(), Ctx())
            self.server.release(fh)

        # Read all
        fh = self.server.opendir(ROOT_INODE)
        self.assertListEqual(sorted(names + ['.', '..', 'lost+found']) ,
                             sorted(x[0] for x in self.server.readdir(fh, 0)))
        self.server.releasedir(fh)

        # Read in parts
        fh = self.server.opendir(ROOT_INODE)
        entries = list()
        off = 0
        try:
            while True:
                gen = self.server.readdir(fh, off)
                entries.append(gen.next())
                off += 1
                entries.append(gen.next())
                off += 1
        except StopIteration:
            pass

        self.assertListEqual(sorted(names + ['.', '..', 'lost+found']) ,
                             sorted(x[0] for x in entries))
        self.server.releasedir(fh)

        self.fsck()

    def test_release(self):
        name = self.newname()

        # Test that entries are deleted when they're no longer referenced
        (fh, inode) = self.server.create(ROOT_INODE, name,
                                         self.file_mode(), Ctx())
        self.server.write(fh, 0, 'foobar')
        self.server.unlink(ROOT_INODE, name)
        self.assertFalse(self.dbcm.has_val('SELECT 1 FROM contents WHERE name=? AND '
                                           'parent_inode = ?', (name, ROOT_INODE)))
        self.assertTrue(self.server.getattr(inode.id).id)
        self.server.release(fh)

        self.assertFalse(self.dbcm.has_val('SELECT 1 FROM inodes WHERE id=?', (inode.id,)))

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

        self.server.rename(ROOT_INODE, oldname, inode_p_new.id, newname)

        inode_p_old_after = self.server.getattr(ROOT_INODE)
        inode_p_new_after = self.server.getattr(inode_p_new.id)

        self.assertFalse(self.dbcm.has_val('SELECT inode FROM contents WHERE name=? AND '
                                           'parent_inode = ?', (oldname, ROOT_INODE)))
        id_ = self.dbcm.get_val('SELECT inode FROM contents WHERE name=? AND '
                                'parent_inode = ?', (newname, inode_p_new.id))
        self.assertEqual(inode.id, id_)

        self.assertLess(inode_p_new_before.mtime, inode_p_new_after.mtime)
        self.assertLess(inode_p_new_before.ctime, inode_p_new_after.ctime)
        self.assertLess(inode_p_old_before.mtime, inode_p_old_after.mtime)
        self.assertLess(inode_p_old_before.ctime, inode_p_old_after.ctime)
        self.assertEqual(inode_p_old_before.nlink_off, inode_p_old_after.nlink_off + 1)
        self.assertEqual(inode_p_new_before.nlink_off, inode_p_new_after.nlink_off - 1)

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

        self.server.rename(ROOT_INODE, oldname, inode_p_new.id, newname)

        inode_p_old_after = self.server.getattr(ROOT_INODE)
        inode_p_new_after = self.server.getattr(inode_p_new.id)

        self.assertFalse(self.dbcm.has_val('SELECT inode FROM contents WHERE name=? AND '
                                           'parent_inode = ?', (oldname, ROOT_INODE)))
        id_ = self.dbcm.get_val('SELECT inode FROM contents WHERE name=? AND '
                                'parent_inode = ?', (newname, inode_p_new.id))
        self.assertEqual(inode.id, id_)

        self.assertLess(inode_p_new_before.mtime, inode_p_new_after.mtime)
        self.assertLess(inode_p_new_before.ctime, inode_p_new_after.ctime)
        self.assertLess(inode_p_old_before.mtime, inode_p_old_after.mtime)
        self.assertLess(inode_p_old_before.ctime, inode_p_old_after.ctime)
        self.assertEqual(inode_p_old_before.nlink_off, inode_p_old_after.nlink_off)
        self.assertEqual(inode_p_new_before.nlink_off, inode_p_new_after.nlink_off)

        self.assertFalse(self.dbcm.has_val('SELECT id FROM inodes WHERE id=?', (inode2.id,)))

        self.fsck()

    def test_replace_dir(self):
        oldname = self.newname()
        newname = self.newname()

        inode = self.server.mkdir(ROOT_INODE, oldname, self.dir_mode(), Ctx())

        inode_p_new = self.server.mkdir(ROOT_INODE, self.newname(), self.dir_mode(), Ctx())
        inode_p_new_before = self.server.getattr(inode_p_new.id).copy()
        inode_p_old_before = self.server.getattr(ROOT_INODE).copy()

        inode2 = self.server.mkdir(inode_p_new.id, newname, self.dir_mode(), Ctx())

        self.server.rename(ROOT_INODE, oldname, inode_p_new.id, newname)

        inode_p_old_after = self.server.getattr(ROOT_INODE)
        inode_p_new_after = self.server.getattr(inode_p_new.id)

        self.assertFalse(self.dbcm.has_val('SELECT inode FROM contents WHERE name=? AND '
                                           'parent_inode = ?', (oldname, ROOT_INODE)))
        id_ = self.dbcm.get_val('SELECT inode FROM contents WHERE name=? AND '
                                'parent_inode = ?', (newname, inode_p_new.id))
        self.assertEqual(inode.id, id_)

        self.assertLess(inode_p_new_before.mtime, inode_p_new_after.mtime)
        self.assertLess(inode_p_new_before.ctime, inode_p_new_after.ctime)
        self.assertLess(inode_p_old_before.mtime, inode_p_old_after.mtime)
        self.assertLess(inode_p_old_before.ctime, inode_p_old_after.ctime)
        self.assertEqual(inode_p_old_before.nlink_off, inode_p_old_after.nlink_off + 1)
        self.assertEqual(inode_p_new_before.nlink_off, inode_p_new_after.nlink_off - 1)

        self.assertFalse(self.dbcm.has_val('SELECT id FROM inodes WHERE id=?', (inode2.id,)))

        self.fsck()

    def test_setattr(self):
        (fh, inode) = self.server.create(ROOT_INODE, self.newname(), 0641, Ctx())
        self.server.release(fh)
        inode_old = self.server.getattr(inode.id).copy()

        attr = {
                'st_mode': self.file_mode(),
                'st_uid': randint(0, 2 ** 32),
                'st_gid': randint(0, 2 ** 32),
                'st_rdev': randint(0, 2 ** 32),
                'st_atime': time.timezone + randint(0, 2 ** 32) / 10 ** 6,
                'st_mtime': time.timezone + randint(0, 2 ** 32) / 10 ** 6
                }

        self.server.setattr(inode.id, attr)
        inode_new = self.server.getattr(inode.id)
        self.assertGreater(inode_new.ctime, inode_old.ctime)

        for key in attr:
            self.assertEquals(attr[key], getattr(inode_new, key))


    def test_truncate(self):
        len_ = 2 * self.blocksize
        data = self.random_data(len_)

        (fh, inode) = self.server.create(ROOT_INODE, self.newname(), self.file_mode(), Ctx())
        self.server.write(fh, 0, data)
        self.server.setattr(inode.id, { 'st_size': len_ // 2 })
        self.assertTrue(self.server.read(fh, 0, len_) == data[:len_ // 2])
        self.server.setattr(inode.id, { 'st_size': len_ })
        self.assertTrue(self.server.read(fh, 0, len_)
                        == data[:len_ // 2] + b'\0' * (len_ // 2))
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
        inode = self.server.symlink(ROOT_INODE, name, target, Ctx())
        inode_p_after = self.server.getattr(ROOT_INODE)

        self.assertEqual(target, self.server.readlink(inode.id))

        id_ = self.dbcm.get_val('SELECT inode FROM contents WHERE name=? AND '
                                'parent_inode = ?', (name, ROOT_INODE))

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
        self.server.unlink(ROOT_INODE, name)
        inode_p_after = self.server.getattr(ROOT_INODE)

        self.assertLess(inode_p_before.mtime, inode_p_after.mtime)
        self.assertLess(inode_p_before.ctime, inode_p_after.ctime)

        self.assertFalse(self.dbcm.has_val('SELECT inode FROM contents WHERE name=? AND '
                                           'parent_inode = ?', (name, ROOT_INODE)))
        self.assertFalse(self.dbcm.has_val('SELECT id FROM inodes WHERE id=?', (inode.id,)))

        self.fsck()

    def test_rmdir(self):
        name = self.newname()
        inode = self.server.mkdir(ROOT_INODE, name, self.dir_mode(), Ctx())
        inode_p_before = self.server.getattr(ROOT_INODE).copy()
        self.server.rmdir(ROOT_INODE, name)
        inode_p_after = self.server.getattr(ROOT_INODE)

        self.assertLess(inode_p_before.mtime, inode_p_after.mtime)
        self.assertLess(inode_p_before.ctime, inode_p_after.ctime)
        self.assertEqual(inode_p_before.nlink_off, inode_p_after.nlink_off + 1)
        self.assertFalse(self.dbcm.has_val('SELECT inode FROM contents WHERE name=? AND '
                                           'parent_inode = ?', (name, ROOT_INODE)))
        self.assertFalse(self.dbcm.has_val('SELECT id FROM inodes WHERE id=?', (inode.id,)))

        self.fsck()

    def test_relink(self):
        name = self.newname()
        name2 = self.newname()
        data = 'some data to deal with'

        (fh, inode) = self.server.create(ROOT_INODE, name, self.file_mode(), Ctx())
        self.server.write(fh, 0, data)
        self.server.unlink(ROOT_INODE, name)
        self.assertFalse(self.dbcm.has_val('SELECT inode FROM contents WHERE name=? AND '
                                           'parent_inode = ?', (name, ROOT_INODE)))
        self.assertTrue(self.dbcm.has_val('SELECT id FROM inodes WHERE id=?', (inode.id,)))

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
        self.server.cache.flush_all()
        queue = list()
        id_cache = dict()
        self.assertEqual(self.server._copy_tree(src_inode.id, dst_inode.id, queue, id_cache), 4)

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
        dst_inode_c = self.server.getattr(dst_inode.id)

        # Check destination
        self.assertEqual(dst_inode_c.nlink_off, 2)

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
        self.assertEqual(f2_inode_c.refcount, 2)
        self.assertNotEqual(f2_inode.id, f2_inode_c.id)

        # Check subdir1
        self.assertNotEqual(d1_inode.id, d1_inode_c.id)
        self.assertEqual(d1_inode_c.nlink_off, 1)

        # Copy again
        (tmp1, tmp2) = queue.pop()
        self.assertEqual(self.server._copy_tree(tmp1, tmp2, queue, id_cache), 2)

        # Update attributes
        d2_inode_c = self.server.lookup(d1_inode_c.id, 'dir2')
        f2_h_inode_c = self.server.lookup(d1_inode_c.id, 'file2_hardlink')

        # Check subdir1
        self.assertEqual(self.server.getattr(d1_inode_c.id).nlink_off, 2)
        self.assertNotEqual(d2_inode.id, d2_inode_c.id)

        # Check file2
        self.assertEqual(f2_h_inode_c.id, f2_inode_c.id)
        self.assertEqual(self.server.getattr(f2_inode_c.id).refcount, 3)



def suite():
    return unittest.makeSuite(fs_api_tests)

if __name__ == "__main__":
    unittest.main()
