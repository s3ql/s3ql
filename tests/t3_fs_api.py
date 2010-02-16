'''
$Id$

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function

from random import randint
from s3ql import mkfs, fs, fsck
from s3ql.backends import local
import time
from time import sleep
from s3ql.common import ROOT_INODE, ExceptionStoringThread
from llfuse import FUSEError
from s3ql.s3cache import S3Cache
from s3ql.database import ConnectionManager
from _common import TestCase
import os
import stat
import tempfile
import unittest
import shutil

class Ctx(object):
    def __init__(self):
        self.uid = randint(0, 2 ** 32)
        self.gid = randint(0, 2 ** 32)


# TODO: Rewrite this test case

# Each test should correspond to exactly one function in the tested
# module, and testing should be done under the assumption that any
# other functions that are called by the tested function work perfectly.
class fs_api_tests(TestCase):

    def setUp(self):
        self.bucket_dir = tempfile.mkdtemp()
        self.passphrase = 'sdfds'
        self.bucket = local.Connection().get_bucket(self.bucket_dir, self.passphrase)
        self.dbfile = tempfile.NamedTemporaryFile()
        self.cachedir = tempfile.mkdtemp() + "/"
        self.blocksize = 1024

        self.dbcm = ConnectionManager(self.dbfile.name)
        with self.dbcm() as conn:
            mkfs.setup_db(conn, self.blocksize)

        self.cache = S3Cache(self.bucket, self.cachedir, self.blocksize * 5, self.dbcm)
        self.cache.timeout = 1
        self.server = fs.Operations(self.cache, self.dbcm)

        self.root_inode = ROOT_INODE

        # Keep track of unused filenames
        self.name_cnt = 0

    def tearDown(self):
        self.cache.clear()
        self.dbfile.close()
        shutil.rmtree(self.cachedir)
        sleep(local.LOCAL_PROP_DELAY * 1.1)
        shutil.rmtree(self.bucket_dir)


    def fsck(self):
        self.cache.clear()
        sleep(local.LOCAL_PROP_DELAY * 1.1)
        fsck.fsck(self.dbcm, self.cachedir, self.bucket)
        self.assertFalse(fsck.found_errors)

    def newname(self):
        self.name_cnt += 1
        return "s3ql_%d" % self.name_cnt

    @staticmethod
    def random_data(len_):
        with open("/dev/urandom", "rb") as fd:
            return fd.read(len_)

    def assert_entry_doesnt_exist(self, inode_p, name):
        self.assertRaises(FUSEError, self.server.lookup, inode_p, name)
        fh = self.server.opendir(inode_p)
        entries = [ x[0] for x in self.server.readdir(fh, 0) ]
        self.server.releasedir(fh)
        self.assertTrue(name not in entries)

    def assert_entry_exists(self, inode_p, name):
        self.assertTrue('st_ino' in self.server.lookup(inode_p, name))
        fh = self.server.opendir(inode_p)
        entries = [ x[0] for x in self.server.readdir(fh, 0) ]
        self.server.releasedir(fh)
        self.assertTrue(name in entries)

    def test_01_getattr_root(self):
        fstat = self.server.getattr(self.root_inode)
        self.assertTrue(stat.S_ISDIR(fstat["st_mode"]))
        self.fsck()

    def mkdir(self, inode_p, name):
        '''Make directory and verify creation
        
        Return inode of new directory.'''

        ctx = Ctx()

        mode = randint(0, 07777) | stat.S_IFDIR

        self.assert_entry_doesnt_exist(inode_p, name)
        attr_p_old = self.server.getattr(inode_p)
        self.server.mkdir(inode_p, name, mode, ctx)
        self.assert_entry_exists(inode_p, name)
        attr_p_new = self.server.getattr(inode_p)
        attr = self.server.lookup(inode_p, name)
        self.assertTrue(attr_p_new["st_mtime"] > attr_p_old['st_mtime'])
        self.assertEquals(attr_p_old["st_nlink"] + 1, attr_p_new['st_nlink'])

        self.assertEquals(attr['st_mode'], mode)
        self.assertEquals(attr["st_nlink"], 2)
        self.assertEquals(attr["st_uid"], ctx.uid)
        self.assertEquals(attr["st_gid"], ctx.gid)

        return attr['st_ino']

    def rmdir(self, inode_p, name):
        '''Remove directory and verify removal'''

        self.assert_entry_exists(inode_p, name)
        attr_p_old = self.server.getattr(inode_p)
        self.server.rmdir(inode_p, name)
        self.assert_entry_doesnt_exist(inode_p, name)
        attr_p_new = self.server.getattr(inode_p)

        self.assertTrue(attr_p_new["st_mtime"] > attr_p_old['st_mtime'])
        self.assertEquals(attr_p_old["st_nlink"] - 1, attr_p_new['st_nlink'])

    def unlink(self, inode_p, name):
        '''Remove file and verify removal'''

        self.assert_entry_exists(inode_p, name)
        attr_p_old = self.server.getattr(inode_p)
        attr_old = self.server.lookup(inode_p, name)
        self.server.unlink(inode_p, name)
        self.assert_entry_doesnt_exist(inode_p, name)
        attr_p_new = self.server.getattr(inode_p)

        self.assertTrue(attr_p_new["st_mtime"] > attr_p_old['st_mtime'])
        self.assertEquals(attr_p_old["st_nlink"], attr_p_new['st_nlink'])

        if attr_old['st_nlink'] == 1:
            self.assertRaises(KeyError, self.server.getattr, attr_old['st_ino'])
        else:
            self.assertEquals(attr_old['st_nlink'] - 1,
                              self.server.getattr(attr_old['st_ino'])['st_nlink'])


    def test_03_mkdir_rmdir(self):

        name1 = self.newname()
        inode_p1 = self.root_inode

        inode_p2 = self.mkdir(inode_p1, name1)
        name2 = self.newname()
        self.mkdir(inode_p2, name2)
        self.assertRaises(FUSEError, self.server.rmdir, inode_p1, name1)
        self.rmdir(inode_p2, name2)
        self.rmdir(inode_p1, name1)

        self.fsck()

    def symlink(self, inode_p, name, target):
        '''Make symlink and verify.
        
        Return inode of new symlink
        '''

        ctx = Ctx()

        self.assert_entry_doesnt_exist(inode_p, name)
        attr_p_old = self.server.getattr(inode_p)
        self.server.symlink(inode_p, name, target, ctx)
        self.assert_entry_exists(inode_p, name)
        attr_p_new = self.server.getattr(inode_p)
        attr = self.server.lookup(inode_p, name)
        self.assertTrue(attr_p_new["st_mtime"] > attr_p_old['st_mtime'])
        self.assertEquals(attr_p_old["st_nlink"], attr_p_new['st_nlink'])

        self.assertEquals(attr["st_nlink"], 1)
        self.assertEquals(attr["st_uid"], ctx.uid)
        self.assertEquals(attr["st_gid"], ctx.gid)
        self.assertEquals(self.server.readlink(attr['st_ino']), target)

        return attr['st_ino']


    def test_04_symlink(self):
        name = self.newname()
        target = os.path.join(*[ self.newname() for dummy in range(5) ])

        self.symlink(self.root_inode, name, target)
        self.unlink(self.root_inode, name)

        self.fsck()

    def create(self, inode_p, name):
        '''Create file and verify operation.
        
        Return inode of new file.
        '''

        ctx = Ctx()
        mode = randint(0, 07777) | stat.S_IFREG

        self.assert_entry_doesnt_exist(inode_p, name)
        attr_p_old = self.server.getattr(inode_p)
        (fh, dummy) = self.server.create(inode_p, name, mode, ctx)
        self.assert_entry_exists(inode_p, name)
        attr_p_new = self.server.getattr(inode_p)
        attr = self.server.lookup(inode_p, name)
        self.assertTrue(attr_p_new["st_mtime"] > attr_p_old['st_mtime'])
        self.assertEquals(attr_p_old["st_nlink"], attr_p_new['st_nlink'])

        self.assertEquals(attr['st_mode'], mode)
        self.assertEquals(attr["st_nlink"], 1)
        self.assertEquals(attr["st_uid"], ctx.uid)
        self.assertEquals(attr["st_gid"], ctx.gid)

        self.server.release(fh)

        return attr['st_ino']

    def test_05_create_unlink(self):
        name = self.newname()
        inode_p = self.root_inode

        self.create(inode_p, name)
        self.unlink(inode_p, name)

        self.fsck()

    def test_06_setattr(self):

        name = self.newname()
        inode_p = self.root_inode

        inode = self.create(inode_p, name)
        self.setattr(inode)
        self.setattr(inode)
        self.unlink(inode_p, name)

        inode = self.mkdir(inode_p, name)
        self.setattr(inode)
        self.setattr(inode)
        self.rmdir(inode_p, name)

    def setattr(self, inode):
        '''Change attributes and verify operation'''

        mode_old = self.server.getattr(inode)['st_mode']
        ctime_old = self.server.getattr(inode)["st_ctime"]

        attr = {
                'st_mode': stat.S_IFMT(mode_old) | (randint(0, 07777) & ~stat.S_IFMT(mode_old)),
                'st_uid': randint(0, 2 ** 32),
                'st_gid': randint(0, 2 ** 32),
                'st_atime': randint(0, 2 ** 32) / 10 ** 6 + time.timezone,
                'st_mtime': randint(0, 2 ** 32) / 10 ** 6 + time.timezone
                }

        self.server.setattr(inode, attr)
        attr_ = self.server.getattr(inode)
        self.assertTrue(attr_['st_ctime'] > ctime_old)

        for key in attr:
            self.assertEquals(attr[key], attr_[key])

    def test_07_open_write_read(self):

        name = self.newname()
        inode_p = self.root_inode
        inode = self.create(inode_p, name)

        data = self.random_data(10 * self.blocksize)
        fh = self.server.open(inode, os.O_RDWR)
        self.assertEquals(self.server.write(fh, 0, data),
                          len(data))
        self.assertTrue(self.server.read(fh, 0, len(data)) == data)
        self.server.release(fh)

        self.cache.clear()
        fh = self.server.open(inode, os.O_RDWR)
        self.assertTrue(self.server.read(fh, 0, len(data)) == data)
        self.server.release(fh)
        self.fsck()

    def test_07_unlink(self):

        # We check what happens if we try to delete an object
        # that has not yet propagated. 
        bak = local.LOCAL_PROP_DELAY
        local.LOCAL_PROP_DELAY = 2
        self.cache.timeout = 3

        name1 = self.newname()
        inode_p = self.root_inode
        data = self.random_data(5 * self.blocksize)

        (fh, dummy) = self.server.create(inode_p, name1, 0644, Ctx())
        self.server.write(fh, 0, data)
        self.server.fsync(fh, True)
        self.server.release(fh)

        self.server.unlink(inode_p, name1)

        self.fsck()

        local.LOCAL_PROP_DELAY = bak

    def test_08_link(self):
        name = self.newname()
        dirname = self.newname()
        inode_p = self.root_inode
        inode = self.create(inode_p, name)

        inode_p2 = self.mkdir(inode_p, dirname)
        name2 = self.newname()
        self.assert_entry_doesnt_exist(inode_p2, name2)
        mtime_old = self.server.getattr(inode_p2)['st_mtime']
        self.server.link(inode, inode_p2, name2)
        self.assert_entry_exists(inode_p2, name2)
        self.assertTrue(self.server.getattr(inode_p2)["st_mtime"] > mtime_old)
        self.assertEquals(self.server.lookup(inode_p2, name2)['st_ino'], inode)
        fstat = self.server.getattr(inode)

        self.assertEquals(fstat["st_nlink"], 2)

        self.unlink(inode_p, name)
        self.assertEquals(self.server.getattr(inode)["st_nlink"], 1)

        self.unlink(inode_p2, name2)
        self.assertRaises(KeyError, self.server.getattr, inode)

        self.fsck()

    def test_09_write_read_cmplx(self):

        inode_p = self.root_inode
        name = self.newname()
        inode = self.create(inode_p, name)

        off = int(5.9 * self.blocksize)
        datalen = int(0.2 * self.blocksize)
        data = self.random_data(datalen)
        fh = self.server.open(inode, os.O_RDWR)
        self.server.write(fh, off, data)
        filelen = datalen + off
        self.assertEquals(self.server.getattr(inode)["st_size"], filelen)

        off2 = int(0.5 * self.blocksize)
        self.assertTrue(self.server.read(fh, off, len(data) + off2) == data)
        self.assertEquals(self.server.read(fh, off - off2, len(data) + off2),
                          b"\0" * off2 + data)
        self.assertEquals(self.server.read(fh, off + len(data), 182), "")

        # Write at another position
        off = int(1.9 * self.blocksize)
        self.server.write(fh, off, data)
        self.assertEquals(self.server.getattr(inode)["st_size"], filelen)
        self.assertEquals(self.server.read(fh, off, len(data) + off2), data + b"\0" * off2)

        self.server.flush(fh)
        self.server.release(fh)

        self.fsck()

    def test_11_truncate_within(self):

        inode_p = self.root_inode
        name = self.newname()
        inode = self.create(inode_p, name)

        off = int(5.5 * self.blocksize)
        datalen = int(0.3 * self.blocksize)
        data = self.random_data(datalen)

        fh = self.server.open(inode, os.O_RDWR)
        self.server.write(fh, off, data)
        filelen = datalen + off
        self.assertEquals(self.server.getattr(inode)["st_size"], filelen)

        # Extend within same block
        ext = int(0.15 * self.blocksize)
        self.server.setattr(fh, { 'st_size': filelen + ext })
        self.assertEquals(self.server.getattr(inode)["st_size"], filelen + ext)
        self.assertTrue(self.server.read(fh, off, len(data) + 2 * ext) ==
                          data + b"\0" * ext)
        self.assertTrue(self.server.read(fh, off + len(data), 2 * ext) ==
                          b"\0" * ext)

        # Truncate it
        self.server.setattr(fh, { 'st_size': filelen - ext })
        self.assertEquals(self.server.getattr(inode)["st_size"], filelen - ext)
        self.assertEquals(self.server.read(fh, off, len(data) + 2 * ext),
                          data[0:-ext])

        # And back to original size, data should have been lost
        self.server.setattr(fh, { 'st_size': filelen })
        self.assertEquals(self.server.getattr(inode)["st_size"], filelen)
        self.assertEquals(self.server.read(fh, off, len(data) + 2 * ext),
                          data[0:-ext] + b"\0" * ext)

        self.server.flush(fh)
        self.server.release(fh)

        self.fsck()

    def test_12_truncate_across(self):

        inode_p = self.root_inode
        name = self.newname()
        inode = self.create(inode_p, name)
        off = int(5.5 * self.blocksize)
        datalen = int(0.3 * self.blocksize)
        data = self.random_data(datalen)

        fh = self.server.open(inode, os.O_RDWR)
        self.server.write(fh, off, data)
        filelen = datalen + off
        self.assertEquals(self.server.getattr(inode)["st_size"], filelen)

        # Extend within same block
        ext = int(0.5 * self.blocksize)
        self.server.setattr(fh, { 'st_size': filelen + ext })
        self.assertEquals(self.server.getattr(inode)["st_size"], filelen + ext)
        self.assertEquals(self.server.read(fh, off, len(data) + 2 * ext),
                          data + b"\0" * ext)
        self.assertEquals(self.server.read(fh, off + len(data), 2 * ext),
                          b"\0" * ext)

        # Truncate it
        ext = int(0.1 * self.blocksize)
        self.server.setattr(fh, { 'st_size': filelen - ext })
        self.assertEquals(self.server.getattr(inode)["st_size"], filelen - ext)
        self.assertEquals(self.server.read(fh, off, len(data) + 2 * ext),
                          data[0:-ext])

        # And back to original size, data should have been lost
        self.server.setattr(fh, { 'st_size': filelen })
        self.assertEquals(self.server. getattr(inode)["st_size"], filelen)
        self.assertTrue(self.server.read(fh, off, len(data) + 2 * ext) ==
                          data[0:-ext] + b"\0" * ext)

        self.server.flush(fh)
        self.server.release(fh)
        self.fsck()


    def test_11_truncate_expire(self):
        '''Check that setattr() releases db lock before calling s3cache.get'''

        inode_p = self.root_inode
        name = self.newname()
        inode = self.create(inode_p, name)
        fh = self.server.open(inode, os.O_RDWR)

        # We need at least two blocks
        self.server.write(fh, 0, self.random_data(42))
        self.server.write(fh, self.blocksize, self.random_data(42))
        self.server.release(fh)

        # Force cache expiration
        self.cache.maxsize = 0

        # Need to set some other attribute to get lock
        self.server.setattr(inode, { 'st_size': 42,
                                     'st_uid': 1 })

        self.fsck()

    def test_10_rename(self):
        dirname_old = self.newname()
        dirname_new = self.newname()
        filename_old = self.newname()
        filename_new = self.newname()

        inode_p = self.root_inode

        # Create directory with file
        inode_dir = self.mkdir(inode_p, dirname_old)
        inode_file = self.create(inode_p, filename_old)

        # Rename file
        mtime_old1 = self.server.getattr(inode_p)["st_mtime"]
        mtime_old2 = self.server.getattr(inode_dir)["st_mtime"]
        self.server.rename(inode_p, filename_old, inode_dir, filename_new)
        self.assert_entry_doesnt_exist(inode_p, filename_old)
        self.assert_entry_exists(inode_dir, filename_new)
        self.assertEquals(inode_file, self.server.lookup(inode_dir, filename_new)['st_ino'])
        self.assertTrue(self.server.getattr(inode_p)["st_mtime"] > mtime_old1)
        self.assertTrue(self.server.getattr(inode_dir)["st_mtime"] > mtime_old2)

        # Rename directory
        mtime_old1 = self.server.getattr(inode_p)["st_mtime"]
        self.server.rename(inode_p, dirname_old, inode_p, dirname_new)
        self.assert_entry_doesnt_exist(inode_p, dirname_old)
        self.assert_entry_exists(inode_p, dirname_new)
        self.assertEquals(inode_dir, self.server.lookup(inode_p, dirname_new)['st_ino'])
        self.assertTrue(self.server.getattr(inode_p)["st_mtime"] > mtime_old1)

        self.assert_entry_doesnt_exist(inode_p, dirname_old)
        self.assert_entry_exists(inode_p, dirname_new)

        self.fsck()

    def test_xattr(self):
        name = self.newname()
        key1 = 'xattr_1_key'
        key2 = 'xattr_2_key'
        value = 'blablabla!'
        inode = self.create(self.root_inode, name)

        self.assertEqual(self.server.listxattr(inode), [])
        self.assertRaises(FUSEError, self.server.getxattr, inode, key1)
        self.assertRaises(FUSEError, self.server.removexattr, inode, key1)

        self.server.setxattr(inode, key1, value)
        self.assertEqual(self.server.listxattr(inode), [ key1 ])
        self.assertEqual(self.server.getxattr(inode, key1), value)

        self.server.setxattr(inode, key2, value)
        self.assertEqual(sorted(self.server.listxattr(inode)), sorted([ key1, key2 ]))
        self.assertEqual(self.server.getxattr(inode, key2), value)

        self.server.removexattr(inode, key1)
        self.assertRaises(FUSEError, self.server.getxattr, inode, key1)
        self.assertRaises(FUSEError, self.server.removexattr, inode, key1)
        self.assertEqual(self.server.listxattr(inode), [ key2 ])
        self.assertEqual(self.server.getxattr(inode, key2), value)

        self.server.removexattr(inode, key2)
        self.assertRaises(FUSEError, self.server.getxattr, inode, key2)
        self.assertRaises(FUSEError, self.server.removexattr, inode, key2)
        self.assertEqual(self.server.listxattr(inode), [ ])

        self.fsck()

    def test_move_dir(self):
        dir1 = self.newname()
        dir2 = self.newname()

        n = self.server.getattr(self.root_inode)['st_nlink']
        inode1 = self.mkdir(self.root_inode, dir1)
        inode2 = self.mkdir(self.root_inode, dir2)

        self.assertEqual(self.server.getattr(inode1)['st_nlink'], 2)
        self.assertEqual(self.server.getattr(inode2)['st_nlink'], 2)
        self.assertEqual(self.server.getattr(self.root_inode)['st_nlink'], n + 2)

        self.server.rename(self.root_inode, dir2, inode1, dir2)
        self.assert_entry_doesnt_exist(self.root_inode, dir2)
        self.assert_entry_exists(inode1, dir2)

        self.assertEqual(self.server.getattr(inode1)['st_nlink'], 3)
        self.assertEqual(self.server.getattr(inode2)['st_nlink'], 2)
        self.assertEqual(self.server.getattr(self.root_inode)['st_nlink'], n + 1)

    def fill_file(self, inode):
        '''Put some data into the given file'''

        data = self.random_data(3 * self.blocksize)
        fh = self.server.open(inode, os.O_RDWR)
        self.assertEquals(self.server.write(fh, 0, data),
                          len(data))
        self.server.release(fh)

    def test_10_overwrite_file(self):
        filename1 = self.newname()
        filename2 = self.newname()
        inode_p = self.root_inode

        # Create two files
        inode1 = self.create(inode_p, filename1)
        inode2 = self.create(inode_p, filename2)

        self.fill_file(inode1)
        self.fill_file(inode2)

        # Rename file, overwrite existing one
        mtime_old = self.server.getattr(inode_p)["st_mtime"]
        self.server.rename(inode_p, filename1, inode_p, filename2)
        self.assert_entry_doesnt_exist(inode_p, filename1)
        self.assert_entry_exists(inode_p, filename2)
        self.assertEquals(inode1, self.server.lookup(inode_p, filename2)['st_ino'])
        self.assertTrue(self.server.getattr(inode_p)["st_mtime"] > mtime_old)

        # Make sure original one vanished
        self.assertRaises(KeyError, self.server.getattr, inode2)

        self.fsck()

    def test_issue_65(self):

        from s3ql import multi_lock
        bak = multi_lock.FAKEDELAY
        multi_lock.FAKEDELAY = 0.02

        name = self.newname()
        inode_p = self.root_inode

        # Create file
        (fh, dummy) = self.server.create(inode_p, name, 0664, Ctx())
        self.server.write(fh, 0, self.random_data(8192))
        self.server.flush(fh)

        # Now release and unlink in parallel
        t1 = ExceptionStoringThread(self.server.unlink, args=(inode_p, name))
        t1.start()
        self.server.release(fh)
        t1.join_and_raise()

        self.fsck()
        multi_lock.FAKEDELAY = bak

    def test_10_overwrite_dir(self):
        dirname1 = self.newname()
        dirname2 = self.newname()
        filename = self.newname()
        inode_p = self.root_inode

        inode1 = self.mkdir(inode_p, dirname1)
        inode2 = self.mkdir(inode_p, dirname2)
        self.assertNotEquals(inode1, inode2)

        self.create(inode2, filename)

        # Attempt to overwrite, should fail
        self.assertRaises(FUSEError, self.server.rename, inode_p, dirname1,
                          inode_p, dirname2)

        # Delete file in target
        self.unlink(inode2, filename)

        # Now we should be able to rename 
        mtime_old = self.server.getattr(inode_p)["st_mtime"]
        self.server.rename(inode_p, dirname1, inode_p, dirname2)
        self.assertEquals(inode1, self.server.lookup(inode_p, dirname2)['st_ino'])
        self.assert_entry_doesnt_exist(inode_p, dirname1)
        self.assert_entry_exists(inode_p, dirname2)
        self.assertTrue(self.server.getattr(inode_p)["st_mtime"] > mtime_old)

        # Make sure original is vanished
        self.assertRaises(KeyError, self.server.getattr, inode2)

        self.fsck()

    def mknod(self, inode_p, name, type_):
        '''Create file and verify operation.
        
        Return inode of new file.
        '''

        ctx = Ctx()
        mode = randint(0, 07777) | type_

        if stat.S_ISBLK(type_) or stat.S_ISCHR(type_):
            rdev = randint(0, 2 ** 32)
        else:
            rdev = None

        self.assert_entry_doesnt_exist(inode_p, name)
        attr_p_old = self.server.getattr(inode_p)
        self.server.mknod(inode_p, name, mode, rdev, ctx)
        self.assert_entry_exists(inode_p, name)
        attr_p_new = self.server.getattr(inode_p)
        attr = self.server.lookup(inode_p, name)
        self.assertTrue(attr_p_new["st_mtime"] > attr_p_old['st_mtime'])
        self.assertEquals(attr_p_old["st_nlink"], attr_p_new['st_nlink'])

        self.assertEquals(attr['st_mode'], mode)
        self.assertEquals(attr["st_nlink"], 1)
        self.assertEquals(attr["st_uid"], ctx.uid)
        self.assertEquals(attr["st_gid"], ctx.gid)
        self.assertEquals(attr["st_rdev"], rdev)

        return attr['st_ino']

    def test_05_mknod_unlink(self):
        name = self.newname()
        inode_p = self.root_inode

        for type_ in [stat.S_IFSOCK, stat.S_IFCHR, stat.S_IFBLK, stat.S_IFIFO]:
            self.mknod(inode_p, name, type_)
            self.unlink(inode_p, name)

        self.fsck()

    def test_13_statfs(self):
        self.assertTrue(isinstance(self.server.statfs(), dict))

    def test_15_delayed_unlink(self):

        inode_p = self.root_inode
        name = self.newname()
        inode = self.create(inode_p, name)
        fh = self.server.open(inode, os.O_RDWR)
        self.server.write(fh, 0, self.random_data(self.blocksize))
        self.server.unlink(inode_p, name)
        self.assert_entry_doesnt_exist(inode_p, name)
        self.assertTrue(isinstance(self.server.getattr(inode), dict))
        self.assertEquals(len(self.cache), 1)
        self.cache.clear()
        sleep(local.LOCAL_PROP_DELAY * 1.1)
        self.assertEquals(len(list(self.bucket.keys())), 1)
        self.server.release(fh)
        self.assertEquals(len(self.cache), 0)
        sleep(local.LOCAL_PROP_DELAY * 1.1)
        self.assertEquals(len(list(self.bucket.keys())), 0)
        self.assertRaises(KeyError, self.server.getattr, inode)
        self.fsck()

    def test_15_rescued_unlink(self):

        inode_p = self.root_inode
        name = self.newname()
        newname = self.newname()
        inode = self.create(inode_p, name)
        data = self.random_data(self.blocksize)
        fh = self.server.open(inode, os.O_RDWR)
        self.server.write(fh, 0, data)
        self.server.unlink(inode_p, name)
        self.server.link(inode, inode_p, newname)
        self.server.release(fh)

        fh = self.server.open(inode, os.O_RDWR)
        self.assertTrue(self.server.read(fh, 0, len(data)) == data)
        self.server.release(fh)

        self.fsck()

    def test_14_fsync(self):
        blocks = 3
        name = self.newname()
        inode_p = self.root_inode
        inode = self.create(inode_p, name)
        fh = self.server.open(inode, os.O_RDWR)

        for i in range(blocks):
            self.server.write(fh, i * self.blocksize, self.random_data(32 + i))

        self.assertEqual(len(list(self.bucket.keys())), 0)

        self.server.fsync(fh, True)

        sleep(local.LOCAL_PROP_DELAY * 1.1)
        self.assertEqual(len(list(self.bucket.keys())), blocks)

        self.server.flush(fh)
        self.server.release(fh)

        self.fsck()


def suite():
    return unittest.makeSuite(fs_api_tests)

if __name__ == "__main__":
    unittest.main()
