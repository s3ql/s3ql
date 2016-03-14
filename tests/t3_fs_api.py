#!/usr/bin/env python3
'''
t3_fs_api.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

if __name__ == '__main__':
    import pytest
    import sys
    sys.exit(pytest.main([__file__] + sys.argv[1:]))

from llfuse import FUSEError
from random import randint
from s3ql import fs
from s3ql.backends import local
from s3ql.backends.pool import BackendPool
from s3ql.backends.comprenc import ComprencBackend
from s3ql.block_cache import BlockCache
from s3ql import ROOT_INODE
from s3ql.mkfs import init_tables
from s3ql.metadata import create_tables
from s3ql.database import Connection
from s3ql.fsck import Fsck
from s3ql.inode_cache import InodeCache
from t2_block_cache import DummyQueue
from common import assert_logs, CLOCK_GRANULARITY, safe_sleep
import errno
import llfuse
import os
import shutil
import stat
import logging
import tempfile
import unittest

# We need to access to protected members
#pylint: disable=W0212

# The classes provided by llfuse have read-only attributes,
# so we duck-type our own.
class Ctx:
    def __init__(self):
        self.uid = randint(0, 2 ** 32)
        self.gid = randint(0, 2 ** 32)
        self.pid = randint(0, 2 ** 32)
        self.umask = 0

class SetattrFields:
    def __init__(self, **kw):
        self.update_atime = False
        self.update_mtime = False
        self.update_mode = False
        self.update_uid = False
        self.update_gid = False
        self.update_size = False
        self.__dict__.update(kw)

some_ctx = Ctx()

class fs_api_tests(unittest.TestCase):

    def setUp(self):
        self.backend_dir = tempfile.mkdtemp(prefix='s3ql-backend-')
        plain_backend = local.Backend('local://' + self.backend_dir, None, None)
        self.backend_pool = BackendPool(lambda: ComprencBackend(b'schwubl', ('zlib', 6),
                                                              plain_backend))
        self.backend = self.backend_pool.pop_conn()
        self.cachedir = tempfile.mkdtemp(prefix='s3ql-cache-')
        self.max_obj_size = 1024

        # Destructors are not guaranteed to run, and we can't unlink
        # the file immediately because apsw refers to it by name.
        # Therefore, we unlink the file manually in tearDown()
        self.dbfile = tempfile.NamedTemporaryFile(delete=False)

        self.db = Connection(self.dbfile.name)
        create_tables(self.db)
        init_tables(self.db)

        # Tested methods assume that they are called from
        # file system request handler
        llfuse.lock.acquire()

        cache = BlockCache(self.backend_pool, self.db, self.cachedir + "/cache",
                           self.max_obj_size * 5)
        self.block_cache = cache
        self.server = fs.Operations(cache, self.db, self.max_obj_size,
                                    InodeCache(self.db, 0))
        self.server.init()

        # Monkeypatch around the need for removal and upload threads
        cache.to_remove = DummyQueue(cache)

        class DummyDistributor:
            def put(self, arg, timeout=None):
                cache._do_upload(*arg)
                return True
        cache.to_upload = DummyDistributor()

        # Keep track of unused filenames
        self.name_cnt = 0

    def tearDown(self):
        self.server.inodes.destroy()
        llfuse.lock.release()
        self.block_cache.destroy()
        shutil.rmtree(self.cachedir)
        shutil.rmtree(self.backend_dir)
        os.unlink(self.dbfile.name)
        self.dbfile.close()

    @staticmethod
    def random_data(len_):
        with open("/dev/urandom", "rb") as fd:
            return fd.read(len_)

    def fsck(self):
        self.block_cache.clear()
        self.server.inodes.flush()
        fsck = Fsck(self.cachedir + '/cache', self.backend,
                  { 'max_obj_size': self.max_obj_size }, self.db)
        fsck.check()
        self.assertFalse(fsck.found_errors)

    def newname(self):
        self.name_cnt += 1
        return ("s3ql_%d" % self.name_cnt).encode()

    def test_getattr_root(self):
        self.assertTrue(stat.S_ISDIR(self.server.getattr(ROOT_INODE, some_ctx).st_mode))
        self.fsck()

    def test_create(self):
        ctx = Ctx()
        mode = self.dir_mode()
        name = self.newname()

        inode_p_old = self.server.getattr(ROOT_INODE, some_ctx)
        safe_sleep(CLOCK_GRANULARITY)
        self.server._create(ROOT_INODE, name, mode, ctx)

        id_ = self.db.get_val('SELECT inode FROM contents JOIN names ON name_id = names.id '
                              'WHERE name=? AND parent_inode = ?', (name, ROOT_INODE))

        inode = self.server.getattr(id_, some_ctx)

        self.assertEqual(inode.st_mode, mode)
        self.assertEqual(inode.st_uid, ctx.uid)
        self.assertEqual(inode.st_gid, ctx.gid)
        self.assertEqual(inode.st_nlink, 1)
        self.assertEqual(inode.st_size, 0)

        inode_p_new = self.server.getattr(ROOT_INODE, some_ctx)

        self.assertGreater(inode_p_new.st_mtime_ns, inode_p_old.st_mtime_ns)
        self.assertGreater(inode_p_new.st_ctime_ns, inode_p_old.st_ctime_ns)

        self.server.forget([(id_, 1)])
        self.fsck()

    def test_extstat(self):
        # Test with zero contents
        self.server.extstat()

        # Test with empty file
        (fh, inode) = self.server.create(ROOT_INODE, self.newname(),
                                         self.file_mode(), os.O_RDWR, some_ctx)
        self.server.release(fh)
        self.server.extstat()

        # Test with data in file
        fh = self.server.open(inode.st_ino, os.O_RDWR, some_ctx)
        self.server.write(fh, 0, b'foobar')
        self.server.release(fh)

        self.server.extstat()
        self.server.forget([(inode.st_ino, 1)])
        self.fsck()

    @staticmethod
    def dir_mode():
        return (randint(0, 0o7777) & ~stat.S_IFDIR) | stat.S_IFDIR

    @staticmethod
    def file_mode():
        return (randint(0, 0o7777) & ~stat.S_IFREG) | stat.S_IFREG

    def test_getxattr(self):
        (fh, inode) = self.server.create(ROOT_INODE, self.newname(),
                                         self.file_mode(), os.O_RDWR, some_ctx)
        self.server.release(fh)

        self.assertRaises(FUSEError, self.server.getxattr, inode.st_ino,
                          b'nonexistant-attr', some_ctx)

        self.server.setxattr(inode.st_ino, b'my-attr', b'strabumm!', some_ctx)
        self.assertEqual(self.server.getxattr(inode.st_ino, b'my-attr', some_ctx),
                         b'strabumm!')

        self.server.forget([(inode.st_ino, 1)])
        self.fsck()

    def test_link(self):
        name = self.newname()

        inode_p_new = self.server.mkdir(ROOT_INODE, self.newname(),
                                        self.dir_mode(), some_ctx)
        inode_p_new_before = self.server.getattr(inode_p_new.st_ino, some_ctx)

        (fh, inode) = self.server.create(ROOT_INODE, self.newname(),
                                         self.file_mode(), os.O_RDWR, some_ctx)
        self.server.release(fh)
        safe_sleep(CLOCK_GRANULARITY)

        inode_before = self.server.getattr(inode.st_ino, some_ctx)
        self.server.link(inode.st_ino, inode_p_new.st_ino, name, some_ctx)

        inode_after = self.server.lookup(inode_p_new.st_ino, name, some_ctx)
        inode_p_new_after = self.server.getattr(inode_p_new.st_ino, some_ctx)

        id_ = self.db.get_val('SELECT inode FROM contents JOIN names ON names.id = name_id '
                              'WHERE name=? AND parent_inode = ?', (name, inode_p_new.st_ino))

        self.assertEqual(inode_before.st_ino, id_)
        self.assertEqual(inode_after.st_nlink, 2)
        self.assertGreater(inode_after.st_ctime_ns, inode_before.st_ctime_ns)
        self.assertLess(inode_p_new_before.st_mtime_ns, inode_p_new_after.st_mtime_ns)
        self.assertLess(inode_p_new_before.st_ctime_ns, inode_p_new_after.st_ctime_ns)
        self.server.forget([(inode.st_ino, 1), (inode_p_new.st_ino, 1), (inode_after.st_ino, 1)])
        self.fsck()

    def test_listxattr(self):
        (fh, inode) = self.server.create(ROOT_INODE, self.newname(),
                                         self.file_mode(), os.O_RDWR, some_ctx)
        self.server.release(fh)

        self.assertListEqual([], self.server.listxattr(inode.st_ino, some_ctx))

        self.server.setxattr(inode.st_ino, b'key1', b'blub', some_ctx)
        self.assertListEqual([b'key1'], self.server.listxattr(inode.st_ino, some_ctx))

        self.server.setxattr(inode.st_ino, b'key2', b'blub', some_ctx)
        self.assertListEqual(sorted([b'key1', b'key2']),
                             sorted(self.server.listxattr(inode.st_ino, some_ctx)))
        self.server.forget([(inode.st_ino, 1)])
        self.fsck()

    def test_read(self):

        len_ = self.max_obj_size
        data = self.random_data(len_)
        off = self.max_obj_size // 2
        (fh, inode) = self.server.create(ROOT_INODE, self.newname(),
                                     self.file_mode(), os.O_RDWR, some_ctx)

        self.server.write(fh, off, data)
        inode_before = self.server.getattr(inode.st_ino, some_ctx)
        safe_sleep(CLOCK_GRANULARITY)
        self.assertTrue(self.server.read(fh, off, len_) == data)
        inode_after = self.server.getattr(inode.st_ino, some_ctx)
        self.assertGreater(inode_after.st_atime_ns, inode_before.st_atime_ns)
        self.assertTrue(self.server.read(fh, 0, len_) == b"\0" * off + data[:off])
        self.assertTrue(self.server.read(fh, self.max_obj_size, len_) == data[off:])
        self.server.release(fh)
        self.server.forget([(inode.st_ino, 1)])
        self.fsck()

    def test_readdir(self):

        # Create a few entries
        names = [ ('entry_%2d' % i).encode() for i in range(20) ]
        for name in names:
            (fh, inode) = self.server.create(ROOT_INODE, name,
                                         self.file_mode(), os.O_RDWR, some_ctx)
            self.server.release(fh)
            self.server.forget([(inode.st_ino, 1)])

        # Delete some to make sure that we don't have continous rowids
        remove_no = [0, 2, 3, 5, 9]
        for i in remove_no:
            self.server.unlink(ROOT_INODE, names[i], some_ctx)
            del names[i]

        # Read all
        fh = self.server.opendir(ROOT_INODE, some_ctx)
        self.assertListEqual(sorted(names + [b'lost+found']) ,
                             sorted(x[0] for x in self.server.readdir(fh, 0)))
        self.server.releasedir(fh)

        # Read in parts
        fh = self.server.opendir(ROOT_INODE, some_ctx)
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

        self.assertListEqual(sorted(names + [b'lost+found']) ,
                             sorted(entries))
        self.server.releasedir(fh)

        self.fsck()

    def test_forget(self):
        name = self.newname()

        # Test that entries are deleted when they're no longer referenced
        (fh, inode) = self.server.create(ROOT_INODE, name,
                                         self.file_mode(), os.O_RDWR, some_ctx)
        self.server.write(fh, 0, b'foobar')
        self.server.unlink(ROOT_INODE, name, some_ctx)
        self.assertFalse(self.db.has_val('SELECT 1 FROM contents JOIN names ON names.id = name_id '
                                         'WHERE name=? AND parent_inode = ?', (name, ROOT_INODE)))
        self.assertTrue(self.server.getattr(inode.st_ino, some_ctx).st_ino)
        self.server.release(fh)
        self.server.forget([(inode.st_ino, 1)])

        self.assertFalse(self.db.has_val('SELECT 1 FROM inodes WHERE id=?', (inode.st_ino,)))

        self.fsck()

    def test_removexattr(self):
        (fh, inode) = self.server.create(ROOT_INODE, self.newname(),
                                         self.file_mode(), os.O_RDWR, some_ctx)
        self.server.release(fh)

        self.assertRaises(FUSEError, self.server.removexattr, inode.st_ino, b'some name',
                          some_ctx)
        self.server.setxattr(inode.st_ino, b'key1', b'blub', some_ctx)
        self.server.removexattr(inode.st_ino, b'key1', some_ctx)
        self.assertListEqual([], self.server.listxattr(inode.st_ino, some_ctx))
        self.server.forget([(inode.st_ino, 1)])
        self.fsck()

    def test_rename(self):
        oldname = self.newname()
        newname = self.newname()

        inode = self.server.mkdir(ROOT_INODE, oldname, self.dir_mode(), some_ctx)

        inode_p_new = self.server.mkdir(ROOT_INODE, self.newname(), self.dir_mode(), some_ctx)
        inode_p_new_before = self.server.getattr(inode_p_new.st_ino, some_ctx)
        inode_p_old_before = self.server.getattr(ROOT_INODE, some_ctx)
        safe_sleep(CLOCK_GRANULARITY)

        self.server.rename(ROOT_INODE, oldname, inode_p_new.st_ino, newname, some_ctx)

        inode_p_old_after = self.server.getattr(ROOT_INODE, some_ctx)
        inode_p_new_after = self.server.getattr(inode_p_new.st_ino, some_ctx)

        self.assertFalse(self.db.has_val('SELECT inode FROM contents JOIN names ON names.id = name_id '
                                         'WHERE name=? AND parent_inode = ?', (oldname, ROOT_INODE)))
        id_ = self.db.get_val('SELECT inode FROM contents JOIN names ON names.id == name_id '
                              'WHERE name=? AND parent_inode = ?', (newname, inode_p_new.st_ino))
        self.assertEqual(inode.st_ino, id_)

        assert inode_p_new_before.st_mtime_ns < inode_p_new_after.st_mtime_ns
        assert inode_p_new_before.st_ctime_ns < inode_p_new_after.st_ctime_ns
        assert inode_p_old_before.st_mtime_ns < inode_p_old_after.st_mtime_ns
        assert inode_p_old_before.st_ctime_ns < inode_p_old_after.st_ctime_ns

        self.server.forget([(inode.st_ino, 1), (inode_p_new.st_ino, 1)])
        self.fsck()

    def test_replace_file(self):
        oldname = self.newname()
        newname = self.newname()

        (fh, inode) = self.server.create(ROOT_INODE, oldname, self.file_mode(),
                                         os.O_RDWR, some_ctx)
        self.server.write(fh, 0, b'some data to deal with')
        self.server.release(fh)
        self.server.setxattr(inode.st_ino, b'test_xattr', b'42*8', some_ctx)

        inode_p_new = self.server.mkdir(ROOT_INODE, self.newname(), self.dir_mode(), some_ctx)
        inode_p_new_before = self.server.getattr(inode_p_new.st_ino, some_ctx)
        inode_p_old_before = self.server.getattr(ROOT_INODE, some_ctx)

        (fh, inode2) = self.server.create(inode_p_new.st_ino, newname, self.file_mode(),
                                          os.O_RDWR, some_ctx)
        self.server.write(fh, 0, b'even more data to deal with')
        self.server.release(fh)
        self.server.setxattr(inode2.st_ino, b'test_xattr', b'42*8', some_ctx)
        self.server.forget([(inode2.st_ino, 1)])

        safe_sleep(CLOCK_GRANULARITY)
        self.server.rename(ROOT_INODE, oldname, inode_p_new.st_ino, newname, some_ctx)

        inode_p_old_after = self.server.getattr(ROOT_INODE, some_ctx)
        inode_p_new_after = self.server.getattr(inode_p_new.st_ino, some_ctx)

        self.assertFalse(self.db.has_val('SELECT inode FROM contents JOIN names ON names.id = name_id '
                                         'WHERE name=? AND parent_inode = ?', (oldname, ROOT_INODE)))
        id_ = self.db.get_val('SELECT inode FROM contents JOIN names ON names.id = name_id '
                              'WHERE name=? AND parent_inode = ?', (newname, inode_p_new.st_ino))
        self.assertEqual(inode.st_ino, id_)

        self.assertLess(inode_p_new_before.st_mtime_ns, inode_p_new_after.st_mtime_ns)
        self.assertLess(inode_p_new_before.st_ctime_ns, inode_p_new_after.st_ctime_ns)
        self.assertLess(inode_p_old_before.st_mtime_ns, inode_p_old_after.st_mtime_ns)
        self.assertLess(inode_p_old_before.st_ctime_ns, inode_p_old_after.st_ctime_ns)

        self.assertFalse(self.db.has_val('SELECT id FROM inodes WHERE id=?', (inode2.st_ino,)))
        self.server.forget([(inode.st_ino, 1), (inode_p_new.st_ino, 1)])
        self.fsck()

    def test_replace_dir(self):
        oldname = self.newname()
        newname = self.newname()

        inode = self.server.mkdir(ROOT_INODE, oldname, self.dir_mode(), some_ctx)

        inode_p_new = self.server.mkdir(ROOT_INODE, self.newname(), self.dir_mode(), some_ctx)
        inode_p_new_before = self.server.getattr(inode_p_new.st_ino, some_ctx)
        inode_p_old_before = self.server.getattr(ROOT_INODE, some_ctx)

        inode2 = self.server.mkdir(inode_p_new.st_ino, newname, self.dir_mode(), some_ctx)
        self.server.forget([(inode2.st_ino, 1)])

        safe_sleep(CLOCK_GRANULARITY)
        self.server.rename(ROOT_INODE, oldname, inode_p_new.st_ino, newname, some_ctx)

        inode_p_old_after = self.server.getattr(ROOT_INODE, some_ctx)
        inode_p_new_after = self.server.getattr(inode_p_new.st_ino, some_ctx)

        self.assertFalse(self.db.has_val('SELECT inode FROM contents JOIN names ON names.id = name_id '
                                         'WHERE name=? AND parent_inode = ?', (oldname, ROOT_INODE)))
        id_ = self.db.get_val('SELECT inode FROM contents JOIN names ON names.id = name_id '
                              'WHERE name=? AND parent_inode = ?', (newname, inode_p_new.st_ino))
        self.assertEqual(inode.st_ino, id_)

        self.assertLess(inode_p_new_before.st_mtime_ns, inode_p_new_after.st_mtime_ns)
        self.assertLess(inode_p_new_before.st_ctime_ns, inode_p_new_after.st_ctime_ns)
        self.assertLess(inode_p_old_before.st_mtime_ns, inode_p_old_after.st_mtime_ns)
        self.assertLess(inode_p_old_before.st_ctime_ns, inode_p_old_after.st_ctime_ns)

        self.server.forget([(inode.st_ino, 1), (inode_p_new.st_ino, 1)])
        self.assertFalse(self.db.has_val('SELECT id FROM inodes WHERE id=?', (inode2.st_ino,)))
        self.fsck()

    def test_setattr_one(self):
        (fh, inode) = self.server.create(ROOT_INODE, self.newname(), self.file_mode(),
                                         os.O_RDWR, some_ctx)
        self.server.release(fh)
        inode_old = self.server.getattr(inode.st_ino, some_ctx)

        attr = self.server.getattr(inode.st_ino, some_ctx)  # this is a fresh instance
        attr.st_mode = self.file_mode()
        attr.st_uid = randint(0, 2 ** 32)
        attr.st_gid = randint(0, 2 ** 32) # should be ignored
        attr.st_atime_ns = randint(0, 2 ** 50)
        attr.st_mtime_ns = randint(0, 2 ** 50)

        safe_sleep(CLOCK_GRANULARITY)
        sf = SetattrFields(update_mode=True, update_uid=True,
                           update_atime=True, update_mtime=True)
        self.server.setattr(inode.st_ino, attr, sf, None, some_ctx)
        inode_new = self.server.getattr(inode.st_ino, some_ctx)

        for name in ('st_mode', 'st_uid', 'st_atime_ns', 'st_mtime_ns'):
            assert getattr(attr, name) == getattr(inode_new, name)
        for name in ('st_gid', 'st_size', 'st_nlink', 'st_rdev',
                     'st_blocks', 'st_blksize'):
            assert getattr(inode_old, name) == getattr(inode_new, name)
        assert inode_old.st_ctime_ns < inode_new.st_ctime_ns

        self.server.forget([(inode.st_ino, 1)])
        self.fsck()

    def test_setattr_two(self):
        (fh, inode_old) = self.server.create(ROOT_INODE, self.newname(), self.file_mode(),
                                             os.O_RDWR, some_ctx)

        attr = self.server.getattr(inode_old.st_ino, some_ctx)
        attr.st_mode = self.file_mode()
        attr.st_uid = randint(0, 2 ** 32)
        attr.st_gid = randint(0, 2 ** 32)
        attr.st_mtime_ns = randint(0, 2 ** 50)
        attr.st_ctime_ns = 5e9

        safe_sleep(CLOCK_GRANULARITY)
        sf = SetattrFields(update_gid=True, update_mtime=True)

        self.server.setattr(inode_old.st_ino, attr, sf, None, some_ctx)
        inode_new = self.server.getattr(inode_old.st_ino, some_ctx)

        for name in ('st_gid', 'st_mtime_ns'):
            assert getattr(attr, name) == getattr(inode_new, name)
        for name in ('st_uid', 'st_size', 'st_nlink', 'st_rdev',
                     'st_blocks', 'st_blksize', 'st_mode', 'st_atime_ns'):
            assert getattr(inode_old, name) == getattr(inode_new, name)
        assert inode_old.st_ctime_ns < inode_new.st_ctime_ns

        self.server.release(fh)
        self.server.forget([(inode_old.st_ino, 1)])
        self.fsck()

    def test_truncate(self):
        len_ = int(2.7 * self.max_obj_size)
        data = self.random_data(len_)

        (fh, inode) = self.server.create(ROOT_INODE, self.newname(), self.file_mode(),
                                         os.O_RDWR, some_ctx)
        self.server.write(fh, 0, data)

        attr = self.server.getattr(inode.st_ino, some_ctx)
        attr.st_size = len_ // 2
        sf = SetattrFields(update_size=True)
        self.server.setattr(inode.st_ino, attr, sf, None, some_ctx)
        self.assertTrue(self.server.read(fh, 0, len_) == data[:len_ // 2])
        attr.st_size = len_
        self.server.setattr(inode.st_ino, attr, sf, None, some_ctx)
        self.assertTrue(self.server.read(fh, 0, len_)
                        == data[:len_ // 2] + b'\0' * (len_ // 2))
        self.server.release(fh)
        self.server.forget([(inode.st_ino, 1)])
        self.fsck()

    def test_truncate_0(self):
        len1 = 158
        len2 = 133

        (fh, inode) = self.server.create(ROOT_INODE, self.newname(),
                                         self.file_mode(), os.O_RDWR, some_ctx)
        self.server.write(fh, 0, self.random_data(len1))
        self.server.release(fh)
        self.server.inodes.flush()

        attr = self.server.getattr(inode.st_ino, some_ctx)
        fh = self.server.open(inode.st_ino, os.O_RDWR, some_ctx)
        attr.st_size = 0
        self.server.setattr(inode.st_ino, attr, SetattrFields(update_size=True),
                            fh, some_ctx)
        self.server.write(fh, 0, self.random_data(len2))
        self.server.release(fh)
        self.server.forget([(inode.st_ino, 1)])
        self.fsck()

    def test_setxattr(self):
        (fh, inode) = self.server.create(ROOT_INODE, self.newname(),
                                         self.file_mode(), os.O_RDWR, some_ctx)
        self.server.release(fh)

        self.server.setxattr(inode.st_ino, b'my-attr', b'strabumm!', some_ctx)
        self.assertEqual(self.server.getxattr(inode.st_ino, b'my-attr', some_ctx),
                         b'strabumm!')
        self.server.forget([(inode.st_ino, 1)])
        self.fsck()

    def test_names(self):
        name1 = self.newname()
        name2 = self.newname()

        (fh, inode) = self.server.create(ROOT_INODE, name1, self.file_mode(),
                                     os.O_RDWR, some_ctx)
        self.server.release(fh)
        self.server.forget([(inode.st_ino, 1)])

        (fh, inode) = self.server.create(ROOT_INODE, name2, self.file_mode(),
                                         os.O_RDWR, some_ctx)
        self.server.release(fh)

        self.server.setxattr(inode.st_ino, name1, b'strabumm!', some_ctx)
        self.fsck()

        self.server.removexattr(inode.st_ino, name1, some_ctx)
        self.fsck()

        self.server.setxattr(inode.st_ino, name1, b'strabumm karacho!!',
                             some_ctx)
        self.server.unlink(ROOT_INODE, name1, some_ctx)
        self.server.forget([(inode.st_ino, 1)])
        self.fsck()


    def test_statfs(self):
        # Test with zero contents
        self.server.statfs(some_ctx)

        # Test with empty file
        (fh, inode) = self.server.create(ROOT_INODE, self.newname(),
                                         self.file_mode(), os.O_RDWR, some_ctx)
        self.server.release(fh)
        self.server.statfs(some_ctx)

        # Test with data in file
        fh = self.server.open(inode.st_ino, os.O_RDWR, some_ctx)
        self.server.write(fh, 0, b'foobar')
        self.server.release(fh)
        self.server.forget([(inode.st_ino, 1)])
        self.server.statfs(some_ctx)

    def test_symlink(self):
        target = self.newname()
        name = self.newname()

        inode_p_before = self.server.getattr(ROOT_INODE, some_ctx)
        safe_sleep(CLOCK_GRANULARITY)
        inode = self.server.symlink(ROOT_INODE, name, target, some_ctx)
        inode_p_after = self.server.getattr(ROOT_INODE, some_ctx)

        self.assertEqual(target, self.server.readlink(inode.st_ino, some_ctx))

        id_ = self.db.get_val('SELECT inode FROM contents JOIN names ON names.id = name_id '
                              'WHERE name=? AND parent_inode = ?', (name, ROOT_INODE))

        self.assertEqual(inode.st_ino, id_)
        self.assertLess(inode_p_before.st_mtime_ns, inode_p_after.st_mtime_ns)
        self.assertLess(inode_p_before.st_ctime_ns, inode_p_after.st_ctime_ns)

        self.server.forget([(inode.st_ino, 1)])
        self.fsck()

    def test_unlink(self):
        name = self.newname()

        (fh, inode) = self.server.create(ROOT_INODE, name, self.file_mode(), os.O_RDWR, some_ctx)
        self.server.write(fh, 0, b'some data to deal with')
        self.server.release(fh)

        # Add extended attributes
        self.server.setxattr(inode.st_ino, b'test_xattr', b'42*8', some_ctx)
        self.server.forget([(inode.st_ino, 1)])

        inode_p_before = self.server.getattr(ROOT_INODE, some_ctx)
        safe_sleep(CLOCK_GRANULARITY)
        self.server.unlink(ROOT_INODE, name, some_ctx)
        inode_p_after = self.server.getattr(ROOT_INODE, some_ctx)

        self.assertLess(inode_p_before.st_mtime_ns, inode_p_after.st_mtime_ns)
        self.assertLess(inode_p_before.st_ctime_ns, inode_p_after.st_ctime_ns)

        self.assertFalse(self.db.has_val('SELECT inode FROM contents JOIN names ON names.id = name_id '
                                         'WHERE name=? AND parent_inode = ?', (name, ROOT_INODE)))
        self.assertFalse(self.db.has_val('SELECT id FROM inodes WHERE id=?', (inode.st_ino,)))

        self.fsck()

    def test_rmdir(self):
        name = self.newname()
        inode = self.server.mkdir(ROOT_INODE, name, self.dir_mode(), some_ctx)
        self.server.forget([(inode.st_ino, 1)])
        inode_p_before = self.server.getattr(ROOT_INODE, some_ctx)
        safe_sleep(CLOCK_GRANULARITY)
        self.server.rmdir(ROOT_INODE, name, some_ctx)
        inode_p_after = self.server.getattr(ROOT_INODE, some_ctx)

        self.assertLess(inode_p_before.st_mtime_ns, inode_p_after.st_mtime_ns)
        self.assertLess(inode_p_before.st_ctime_ns, inode_p_after.st_ctime_ns)
        self.assertFalse(self.db.has_val('SELECT inode FROM contents JOIN names ON names.id = name_id '
                                         'WHERE name=? AND parent_inode = ?', (name, ROOT_INODE)))
        self.assertFalse(self.db.has_val('SELECT id FROM inodes WHERE id=?', (inode.st_ino,)))

        self.fsck()

    def test_relink(self):
        name = self.newname()
        name2 = self.newname()
        data = b'some data to deal with'

        (fh, inode) = self.server.create(ROOT_INODE, name, self.file_mode(), os.O_RDWR, some_ctx)
        self.server.write(fh, 0, data)
        self.server.release(fh)
        self.server.unlink(ROOT_INODE, name, some_ctx)
        self.server.inodes.flush()
        self.assertFalse(self.db.has_val('SELECT inode FROM contents JOIN names ON names.id = name_id '
                                         'WHERE name=? AND parent_inode = ?', (name, ROOT_INODE)))
        self.assertTrue(self.db.has_val('SELECT id FROM inodes WHERE id=?', (inode.st_ino,)))
        self.server.link(inode.st_ino, ROOT_INODE, name2, some_ctx)
        self.server.forget([(inode.st_ino, 2)])

        inode = self.server.lookup(ROOT_INODE, name2, some_ctx)
        fh = self.server.open(inode.st_ino, os.O_RDONLY, some_ctx)
        self.assertTrue(self.server.read(fh, 0, len(data)) == data)
        self.server.release(fh)
        self.server.forget([(inode.st_ino, 1)])
        self.fsck()

    def test_write(self):
        len_ = self.max_obj_size
        data = self.random_data(len_)
        off = self.max_obj_size // 2
        (fh, inode) = self.server.create(ROOT_INODE, self.newname(),
                                     self.file_mode(), os.O_RDWR, some_ctx)
        inode_before = self.server.getattr(inode.st_ino, some_ctx)
        safe_sleep(CLOCK_GRANULARITY)
        self.server.write(fh, off, data)
        inode_after = self.server.getattr(inode.st_ino, some_ctx)

        self.assertGreater(inode_after.st_mtime_ns, inode_before.st_mtime_ns)
        self.assertGreater(inode_after.st_ctime_ns, inode_before.st_ctime_ns)
        self.assertEqual(inode_after.st_size, off + len_)

        self.server.write(fh, 0, data)
        inode_after = self.server.getattr(inode.st_ino, some_ctx)
        self.assertEqual(inode_after.st_size, off + len_)

        self.server.release(fh)
        self.server.forget([(inode.st_ino, 1)])
        self.fsck()

    def test_failsafe(self):
        len_ = self.max_obj_size
        data = self.random_data(len_)
        (fh, inode) = self.server.create(ROOT_INODE, self.newname(),
                                     self.file_mode(), os.O_RDWR, some_ctx)
        self.server.write(fh, 0, data)
        self.server.cache.clear()
        self.assertTrue(self.server.failsafe is False)

        datafile = os.path.join(self.backend_dir, 's3ql_data_', 's3ql_data_1')
        shutil.copy(datafile, datafile + '.bak')

        # Modify contents
        with open(datafile, 'rb+') as rfh:
            rfh.seek(560)
            rfh.write(b'blrub!')
        with self.assertRaises(FUSEError) as cm:
            with assert_logs('^Backend returned malformed data for',
                              count=1, level=logging.ERROR):
                self.server.read(fh, 0, len_)
        self.assertEqual(cm.exception.errno, errno.EIO)
        self.assertTrue(self.server.failsafe)

        # Restore contents, but should be marked as damaged now
        os.rename(datafile + '.bak', datafile)
        with self.assertRaises(FUSEError) as cm:
            self.server.read(fh, 0, len_)
        self.assertEqual(cm.exception.errno, errno.EIO)

        # Release and re-open, now we should be able to access again
        self.server.release(fh)
        self.server.forget([(inode.st_ino, 1)])

        # ..but not write access since we are in failsafe mode
        with self.assertRaises(FUSEError) as cm:
            self.server.open(inode.st_ino, os.O_RDWR, some_ctx)
        self.assertEqual(cm.exception.errno, errno.EPERM)

        # ..ready only is fine.
        fh = self.server.open(inode.st_ino, os.O_RDONLY, some_ctx)
        self.server.read(fh, 0, len_)

        # Remove completely, should give error after cache flush
        os.unlink(datafile)
        self.server.read(fh, 3, len_//2)
        self.server.cache.clear()
        with self.assertRaises(FUSEError) as cm:
            with assert_logs('^Backend lost block',
                              count=1, level=logging.ERROR):
                self.server.read(fh, 5, len_//2)
        self.assertEqual(cm.exception.errno, errno.EIO)


        # Don't call fsck, we're missing a block

    def test_create_open(self):
        name = self.newname()
        # Create a new file
        (fh, inode) = self.server.create(ROOT_INODE, name, self.file_mode(),
                                         os.O_RDWR, some_ctx)
        self.server.release(fh)
        self.server.forget([(inode, 1)])

        # Open it atomically
        (fh, inode) = self.server.create(ROOT_INODE, name, self.file_mode(),
                                         os.O_RDWR, some_ctx)
        self.server.release(fh)
        self.server.forget([(inode, 1)])

        self.fsck()

    def test_edit(self):
        len_ = self.max_obj_size
        data = self.random_data(len_)
        (fh, inode) = self.server.create(ROOT_INODE, self.newname(),
                                         self.file_mode(), os.O_RDWR, some_ctx)
        self.server.write(fh, 0, data)
        self.server.release(fh)

        self.block_cache.clear()

        fh = self.server.open(inode.st_ino, os.O_RDWR, some_ctx)
        attr = self.server.getattr(inode.st_ino, some_ctx)
        attr.st_size = 0
        self.server.setattr(inode.st_ino, attr, SetattrFields(update_size=True),
                            fh, some_ctx)
        self.server.write(fh, 0, data[50:])
        self.server.release(fh)
        self.server.forget([(inode.st_ino, 1)])
        self.fsck()

    def test_copy_tree(self):
        ext_attr_name = b'system.foo.brazl'
        ext_attr_val = b'schulla dku woumm bramp'

        src_inode = self.server.mkdir(ROOT_INODE, b'source', self.dir_mode(), some_ctx)
        dst_inode = self.server.mkdir(ROOT_INODE, b'dest', self.dir_mode(), some_ctx)

        # Create file
        (fh, f1_inode) = self.server.create(src_inode.st_ino, b'file1',
                                            self.file_mode(), os.O_RDWR, some_ctx)
        self.server.write(fh, 0, b'file1 contents')
        self.server.release(fh)
        self.server.setxattr(f1_inode.st_ino, ext_attr_name, ext_attr_val, some_ctx)

        # Create hardlink
        (fh, f2_inode) = self.server.create(src_inode.st_ino, b'file2',
                                            self.file_mode(), os.O_RDWR, some_ctx)
        self.server.write(fh, 0, b'file2 contents')
        self.server.release(fh)
        f2_inode = self.server.link(f2_inode.st_ino, src_inode.st_ino, b'file2_hardlink',
                                    some_ctx)

        # Create subdirectory
        d1_inode = self.server.mkdir(src_inode.st_ino, b'dir1', self.dir_mode(), some_ctx)
        d2_inode = self.server.mkdir(d1_inode.st_ino, b'dir2', self.dir_mode(), some_ctx)

        # ..with a 3rd hardlink
        f2_inode = self.server.link(f2_inode.st_ino, d1_inode.st_ino, b'file2_hardlink',
                                    some_ctx)

        # Replicate
        self.server.copy_tree(src_inode.st_ino, dst_inode.st_ino)

        # Change files
        fh = self.server.open(f1_inode.st_ino, os.O_RDWR, some_ctx)
        self.server.write(fh, 0, b'new file1 contents')
        self.server.release(fh)

        fh = self.server.open(f2_inode.st_ino, os.O_RDWR, some_ctx)
        self.server.write(fh, 0, b'new file2 contents')
        self.server.release(fh)

        # Get copy properties
        f1_inode_c = self.server.lookup(dst_inode.st_ino, b'file1', some_ctx)
        f2_inode_c = self.server.lookup(dst_inode.st_ino, b'file2', some_ctx)
        f2h_inode_c = self.server.lookup(dst_inode.st_ino, b'file2_hardlink', some_ctx)
        d1_inode_c = self.server.lookup(dst_inode.st_ino, b'dir1', some_ctx)
        d2_inode_c = self.server.lookup(d1_inode_c.st_ino, b'dir2', some_ctx)
        f2_h_inode_c = self.server.lookup(d1_inode_c.st_ino, b'file2_hardlink', some_ctx)

        # Check file1
        fh = self.server.open(f1_inode_c.st_ino, os.O_RDWR, some_ctx)
        self.assertEqual(self.server.read(fh, 0, 42), b'file1 contents')
        self.server.release(fh)
        self.assertNotEqual(f1_inode.st_ino, f1_inode_c.st_ino)
        self.assertEqual(self.server.getxattr(f1_inode_c.st_ino, ext_attr_name,
                                              some_ctx), ext_attr_val)

        # Check file2
        fh = self.server.open(f2_inode_c.st_ino, os.O_RDWR, some_ctx)
        self.assertTrue(self.server.read(fh, 0, 42) == b'file2 contents')
        self.server.release(fh)
        self.assertEqual(f2_inode_c.st_ino, f2h_inode_c.st_ino)
        self.assertEqual(f2_inode_c.st_nlink, 3)
        self.assertNotEqual(f2_inode.st_ino, f2_inode_c.st_ino)
        self.assertEqual(f2_h_inode_c.st_ino, f2_inode_c.st_ino)

        # Check subdir1
        self.assertNotEqual(d1_inode.st_ino, d1_inode_c.st_ino)
        self.assertNotEqual(d2_inode.st_ino, d2_inode_c.st_ino)
        self.server.forget(list(self.server.open_inodes.items()))
        self.fsck()

    def test_copy_tree_2(self):
        src_inode = self.server.mkdir(ROOT_INODE, b'source', self.dir_mode(), some_ctx)
        dst_inode = self.server.mkdir(ROOT_INODE, b'dest', self.dir_mode(), some_ctx)

        # Create file
        (fh, inode) = self.server.create(src_inode.st_ino, b'file1',
                                     self.file_mode(), os.O_RDWR, some_ctx)
        self.server.write(fh, 0, b'block 1 contents')
        self.server.write(fh, self.max_obj_size, b'block 1 contents')
        self.server.release(fh)
        self.server.forget([(inode.st_ino, 1)])

        self.server.copy_tree(src_inode.st_ino, dst_inode.st_ino)

        self.server.forget([(src_inode.st_ino, 1), (dst_inode.st_ino, 1)])
        self.fsck()

    def test_lock_tree(self):

        inode1 = self.server.mkdir(ROOT_INODE, b'source', self.dir_mode(), some_ctx)

        # Create file
        (fh, inode1a) = self.server.create(inode1.st_ino, b'file1',
                                            self.file_mode(), os.O_RDWR, some_ctx)
        self.server.write(fh, 0, b'file1 contents')
        self.server.release(fh)

        # Create subdirectory
        inode2 = self.server.mkdir(inode1.st_ino, b'dir1', self.dir_mode(), some_ctx)
        (fh, inode2a) = self.server.create(inode2.st_ino, b'file2',
                                           self.file_mode(), os.O_RDWR, some_ctx)
        self.server.write(fh, 0, b'file2 contents')
        self.server.release(fh)

        # Another file
        (fh, inode3) = self.server.create(ROOT_INODE, b'file1',
                                          self.file_mode(), os.O_RDWR, some_ctx)
        self.server.release(fh)

        # Lock
        self.server.lock_tree(inode1.st_ino)

        for i in (inode1.st_ino, inode1a.st_ino, inode2.st_ino, inode2a.st_ino):
            self.assertTrue(self.server.inodes[i].locked)

        # Remove
        with self.assertRaises(FUSEError) as cm:
            self.server._remove(inode1.st_ino, b'file1', inode1a.st_ino)
        self.assertEqual(cm.exception.errno, errno.EPERM)

        # Rename / Replace
        with self.assertRaises(FUSEError) as cm:
            self.server.rename(ROOT_INODE, b'file1', inode1.st_ino, b'file2', some_ctx)
        self.assertEqual(cm.exception.errno, errno.EPERM)
        with self.assertRaises(FUSEError) as cm:
            self.server.rename(inode1.st_ino, b'file1', ROOT_INODE, b'file2', some_ctx)
        self.assertEqual(cm.exception.errno, errno.EPERM)

        # Open
        with self.assertRaises(FUSEError) as cm:
            self.server.open(inode2a.st_ino, os.O_RDWR, some_ctx)
        self.assertEqual(cm.exception.errno, errno.EPERM)
        with self.assertRaises(FUSEError) as cm:
            self.server.open(inode2a.st_ino, os.O_WRONLY, some_ctx)
        self.assertEqual(cm.exception.errno, errno.EPERM)
        self.server.release(self.server.open(inode3.st_ino, os.O_WRONLY, some_ctx))

        # Write
        fh = self.server.open(inode2a.st_ino, os.O_RDONLY, some_ctx)
        with self.assertRaises(FUSEError) as cm:
            self.server.write(fh, 0, b'foo')
        self.assertEqual(cm.exception.errno, errno.EPERM)
        self.server.release(fh)

        # Create
        with self.assertRaises(FUSEError) as cm:
            self.server._create(inode2.st_ino, b'dir1', self.dir_mode(), os.O_RDWR, some_ctx)
        self.assertEqual(cm.exception.errno, errno.EPERM)

        # Setattr
        attr = self.server.getattr(inode2a.st_ino, some_ctx)
        with self.assertRaises(FUSEError) as cm:
            self.server.setattr(inode2a.st_ino, attr, SetattrFields(update_mtime=True),
                                None, some_ctx)
        self.assertEqual(cm.exception.errno, errno.EPERM)

        # xattr
        with self.assertRaises(FUSEError) as cm:
            self.server.setxattr(inode2.st_ino, b'name', b'value', some_ctx)
        self.assertEqual(cm.exception.errno, errno.EPERM)
        with self.assertRaises(FUSEError) as cm:
            self.server.removexattr(inode2.st_ino, b'name', some_ctx)
        self.assertEqual(cm.exception.errno, errno.EPERM)
        self.server.forget(list(self.server.open_inodes.items()))
        self.fsck()

    def test_remove_tree(self):

        inode1 = self.server.mkdir(ROOT_INODE, b'source', self.dir_mode(), some_ctx)

        # Create file
        (fh, inode1a) = self.server.create(inode1.st_ino, b'file1',
                                            self.file_mode(), os.O_RDWR, some_ctx)
        self.server.write(fh, 0, b'file1 contents')
        self.server.release(fh)

        # Create subdirectory
        inode2 = self.server.mkdir(inode1.st_ino, b'dir1', self.dir_mode(), some_ctx)
        (fh, inode2a) = self.server.create(inode2.st_ino, b'file2',
                                           self.file_mode(), os.O_RDWR, some_ctx)
        self.server.write(fh, 0, b'file2 contents')
        self.server.release(fh)

        # Remove
        self.server.forget(list(self.server.open_inodes.items()))
        self.server.remove_tree(ROOT_INODE, b'source')

        for (id_p, name) in ((ROOT_INODE, b'source'),
                             (inode1.st_ino, b'file1'),
                             (inode1.st_ino, b'dir1'),
                             (inode2.st_ino, b'file2')):
            self.assertFalse(self.db.has_val('SELECT inode FROM contents JOIN names ON names.id = name_id '
                                             'WHERE name=? AND parent_inode = ?', (name, id_p)))

        for id_ in (inode1.st_ino, inode1a.st_ino, inode2.st_ino, inode2a.st_ino):
            self.assertFalse(self.db.has_val('SELECT id FROM inodes WHERE id=?', (id_,)))

        self.fsck()
