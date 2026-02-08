#!/usr/bin/env python3
'''
t3_fsck.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

if __name__ == '__main__':
    import sys

    import pytest

    sys.exit(pytest.main([__file__] + sys.argv[1:]))

import _thread
import hashlib
import inspect
import os
import stat
from argparse import Namespace
from collections.abc import AsyncIterator
from dataclasses import dataclass

import pytest

from s3ql import ROOT_INODE
from s3ql.backends import local
from s3ql.backends.comprenc import AsyncComprencBackend
from s3ql.common import time_ns
from s3ql.database import Connection, FsAttributes, NoSuchRowError, create_tables
from s3ql.fsck import Fsck
from s3ql.mkfs import init_tables


def sha256(s):
    return hashlib.sha256(s).digest()


@dataclass
class FsckCtx:
    backend_dir: str
    raw_backend: local.AsyncBackend
    backend: AsyncComprencBackend
    cachedir: str
    max_obj_size: int
    db: Connection
    fsck: Fsck


@pytest.fixture()
async def ctx(tmp_path) -> AsyncIterator[FsckCtx]:
    backend_dir = str(tmp_path / 'backend')
    os.makedirs(backend_dir)
    raw_backend = await local.AsyncBackend.create(Namespace(storage_url='local://' + backend_dir))
    backend = await AsyncComprencBackend.create(None, (None, 0), raw_backend)
    cachedir = str(tmp_path / 'cache')
    os.makedirs(cachedir)
    max_obj_size = 1024

    db = Connection(str(tmp_path / 'test.db'))
    create_tables(db)
    init_tables(db)

    fsck = Fsck(
        cachedir,
        backend,
        FsAttributes(data_block_size=max_obj_size, metadata_block_size=max_obj_size),
        db,
    )

    yield FsckCtx(
        backend_dir=backend_dir,
        raw_backend=raw_backend,
        backend=backend,
        cachedir=cachedir,
        max_obj_size=max_obj_size,
        db=db,
        fsck=fsck,
    )

    db.close()


async def assert_fsck(fsck: Fsck, *fns, can_fix: bool = True):
    '''Check that fns detects and corrects errors'''

    fsck.expect_errors = True
    for fn in fns:
        fsck.found_errors = False
        if inspect.iscoroutinefunction(fn):
            await fn()
        else:
            fn()
        assert fsck.found_errors
    if not can_fix:
        return
    fsck.found_errors = False
    fsck.expect_errors = False
    await fsck.check()
    assert not fsck.found_errors


def add_name(db: Connection, name: bytes) -> int:
    '''Get id for *name* and increase refcount

    Name is inserted in table if it does not yet exist.
    '''

    try:
        name_id: int = db.get_int_val('SELECT id FROM names WHERE name=?', (name,))
    except NoSuchRowError:
        name_id = db.rowid('INSERT INTO names (name, refcount) VALUES(?,?)', (name, 1))
    else:
        db.execute('UPDATE names SET refcount=refcount+1 WHERE id=?', (name_id,))
    return name_id


def link(db: Connection, name: bytes, inode: int, parent_inode: int = ROOT_INODE):
    '''Link /*name* to *inode*'''

    db.execute(
        'INSERT INTO contents (name_id, inode, parent_inode) VALUES(?,?,?)',
        (add_name(db, name), inode, parent_inode),
    )


async def test_cache(ctx: FsckCtx):
    inode = ctx.db.rowid(
        "INSERT INTO inodes (mode,uid,gid,mtime_ns,atime_ns,ctime_ns,refcount,size) "
        "VALUES (?,?,?,?,?,?,?,?)",
        (
            stat.S_IFREG
            | stat.S_IRUSR
            | stat.S_IWUSR
            | stat.S_IXUSR
            | stat.S_IRGRP
            | stat.S_IXGRP
            | stat.S_IROTH
            | stat.S_IXOTH,
            os.getuid(),
            os.getgid(),
            time_ns(),
            time_ns(),
            time_ns(),
            1,
            8,
        ),
    )
    link(ctx.db, b'test-entry', inode)

    # Create new block
    with open(ctx.cachedir + '/%d-0' % inode, 'wb') as fh:
        fh.write(b'somedata')
    size = os.stat(fh.name).st_size
    await assert_fsck(ctx.fsck, ctx.fsck.check_cache)
    assert (await ctx.backend.fetch('s3ql_data_1'))[0] == b'somedata'
    assert ctx.db.get_val('SELECT size FROM inodes WHERE id=?', (inode,)) == size

    # Existing block
    ctx.db.execute('UPDATE inodes SET size=? WHERE id=?', (ctx.max_obj_size + 8, inode))
    with open(ctx.cachedir + '/%d-1' % inode, 'wb') as fh:
        fh.write(b'somedata')
    await assert_fsck(ctx.fsck, ctx.fsck.check_cache)

    # Old block preserved
    with open(ctx.cachedir + '/%d-0' % inode, 'wb') as fh:
        fh.write(b'somedat2')
    await assert_fsck(ctx.fsck, ctx.fsck.check_cache)

    # Old block removed
    with open(ctx.cachedir + '/%d-1' % inode, 'wb') as fh:
        fh.write(b'somedat3')
    await assert_fsck(ctx.fsck, ctx.fsck.check_cache)


async def test_lof1(ctx: FsckCtx):
    # Make lost+found a file
    inode = ctx.db.get_val(
        "SELECT inode FROM contents_v WHERE name=? AND parent_inode=?",
        (b"lost+found", ROOT_INODE),
    )
    ctx.db.execute('DELETE FROM contents WHERE parent_inode=?', (inode,))
    ctx.db.execute(
        'UPDATE inodes SET mode=?, size=? WHERE id=?',
        (stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR, 0, inode),
    )

    fsck = ctx.fsck

    def check():
        fsck.check_lof()
        fsck.check_inodes_refcount()

    await assert_fsck(ctx.fsck, check)


async def test_lof2(ctx: FsckCtx):
    # Remove lost+found
    name_id = ctx.db.get_val('SELECT id FROM names WHERE name=?', (b'lost+found',))
    inode = ctx.db.get_val(
        'SELECT inode FROM contents WHERE name_id=? AND parent_inode=?',
        (name_id, ROOT_INODE),
    )
    ctx.db.execute('DELETE FROM inodes WHERE id=?', (inode,))
    ctx.db.execute('DELETE FROM contents WHERE name_id=? and parent_inode=?', (name_id, ROOT_INODE))
    ctx.db.execute('UPDATE names SET refcount = refcount-1 WHERE id=?', (name_id,))

    await assert_fsck(ctx.fsck, ctx.fsck.check_lof)


async def test_lof_issue_273(ctx: FsckCtx):
    name_id = ctx.db.get_val('SELECT id FROM names WHERE name=?', (b'lost+found',))
    inode = ctx.db.get_val(
        'SELECT inode FROM contents WHERE name_id=? AND parent_inode=?',
        (name_id, ROOT_INODE),
    )
    ctx.db.execute('UPDATE contents SET parent_inode = ? WHERE inode = ?', (inode, inode))

    await assert_fsck(ctx.fsck, ctx.fsck.check_lof)


async def test_wrong_inode_refcount(ctx: FsckCtx):
    inode = ctx.db.rowid(
        "INSERT INTO inodes (mode,uid,gid,mtime_ns,atime_ns,ctime_ns,refcount,size) "
        "VALUES (?,?,?,?,?,?,?,?)",
        (
            stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR,
            0,
            0,
            time_ns(),
            time_ns(),
            time_ns(),
            1,
            0,
        ),
    )
    link(ctx.db, b'name1', inode)
    link(ctx.db, b'name2', inode)
    await assert_fsck(ctx.fsck, ctx.fsck.check_inodes_refcount)


async def test_orphaned_inode(ctx: FsckCtx):
    ctx.db.rowid(
        "INSERT INTO inodes (mode,uid,gid,mtime_ns,atime_ns,ctime_ns,refcount,size) "
        "VALUES (?,?,?,?,?,?,?,?)",
        (
            stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR,
            0,
            0,
            time_ns(),
            time_ns(),
            time_ns(),
            1,
            0,
        ),
    )
    await assert_fsck(ctx.fsck, ctx.fsck.check_inodes_refcount)


async def test_name_refcount(ctx: FsckCtx):
    inode = ctx.db.rowid(
        "INSERT INTO inodes (mode,uid,gid,mtime_ns,atime_ns,ctime_ns,refcount,size) "
        "VALUES (?,?,?,?,?,?,?,?)",
        (
            stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR,
            0,
            0,
            time_ns(),
            time_ns(),
            time_ns(),
            2,
            0,
        ),
    )
    link(ctx.db, b'name1', inode)
    link(ctx.db, b'name2', inode)

    ctx.db.execute('UPDATE names SET refcount=refcount+1 WHERE name=?', (b'name1',))

    await assert_fsck(ctx.fsck, ctx.fsck.check_names_refcount)


async def test_orphaned_name(ctx: FsckCtx):
    add_name(ctx.db, b'zupbrazl')
    await assert_fsck(ctx.fsck, ctx.fsck.check_names_refcount)


async def test_contents_inode(ctx: FsckCtx):
    ctx.db.execute(
        'INSERT INTO contents (name_id, inode, parent_inode) VALUES(?,?,?)',
        (add_name(ctx.db, b'foobar'), 124, ROOT_INODE),
    )

    await assert_fsck(ctx.fsck, ctx.fsck.check_contents_inode)


async def test_contents_inode_p(ctx: FsckCtx):
    inode = ctx.db.rowid(
        "INSERT INTO inodes (mode,uid,gid,mtime_ns,atime_ns,ctime_ns,refcount,size) "
        "VALUES (?,?,?,?,?,?,?,?)",
        (
            stat.S_IFDIR | stat.S_IRUSR | stat.S_IWUSR,
            0,
            0,
            time_ns(),
            time_ns(),
            time_ns(),
            1,
            0,
        ),
    )
    ctx.db.execute(
        'INSERT INTO contents (name_id, inode, parent_inode) VALUES(?,?,?)',
        (add_name(ctx.db, b'foobar'), inode, 123),
    )

    await assert_fsck(ctx.fsck, ctx.fsck.check_contents_parent_inode)


async def test_contents_name(ctx: FsckCtx):
    inode = ctx.db.rowid(
        "INSERT INTO inodes (mode,uid,gid,mtime_ns,atime_ns,ctime_ns,refcount,size) "
        "VALUES (?,?,?,?,?,?,?,?)",
        (
            stat.S_IFDIR | stat.S_IRUSR | stat.S_IWUSR,
            0,
            0,
            time_ns(),
            time_ns(),
            time_ns(),
            1,
            0,
        ),
    )
    ctx.db.execute(
        'INSERT INTO contents (name_id, inode, parent_inode) VALUES(?,?,?)',
        (42, inode, ROOT_INODE),
    )

    await assert_fsck(ctx.fsck, ctx.fsck.check_contents_name)


async def test_inodes_size(ctx: FsckCtx):
    id_ = ctx.db.rowid(
        "INSERT INTO inodes (mode,uid,gid,mtime_ns,atime_ns,ctime_ns,refcount,size) "
        "VALUES (?,?,?,?,?,?,?,?)",
        (
            stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR,
            0,
            0,
            time_ns(),
            time_ns(),
            time_ns(),
            1,
            128,
        ),
    )
    link(ctx.db, b'test-entry', id_)

    block_size = ctx.max_obj_size // 3
    obj_id = ctx.db.rowid(
        'INSERT INTO objects (refcount, phys_size, length, hash) VALUES(?, ?, ?, ?)',
        (1, 36, block_size, sha256(b'foo')),
    )
    await ctx.backend.store('s3ql_data_%d' % obj_id, b'foo')

    # One block, no holes, size plausible
    ctx.db.execute('UPDATE inodes SET size=? WHERE id=?', (block_size, id_))
    ctx.db.execute(
        'INSERT INTO inode_blocks (inode, blockno, obj_id) VALUES(?, ?, ?)', (id_, 0, obj_id)
    )
    ctx.fsck.found_errors = False
    await ctx.fsck.check()
    assert not ctx.fsck.found_errors

    # One block, size not plausible
    ctx.db.execute('UPDATE inodes SET size=? WHERE id=?', (block_size - 1, id_))
    await assert_fsck(ctx.fsck, ctx.fsck.check_inodes_size)

    # Two blocks, hole at the beginning, size plausible
    ctx.db.execute('DELETE FROM inode_blocks WHERE inode=?', (id_,))
    ctx.db.execute('UPDATE inodes SET size=? WHERE id=?', (ctx.max_obj_size + block_size, id_))
    ctx.db.execute(
        'INSERT INTO inode_blocks (inode, blockno, obj_id) VALUES(?, ?, ?)', (id_, 1, obj_id)
    )
    ctx.fsck.found_errors = False
    await ctx.fsck.check()
    assert not ctx.fsck.found_errors

    # Two blocks, no holes, size plausible
    ctx.db.execute('UPDATE objects SET refcount = 2 WHERE id = ?', (obj_id,))
    ctx.db.execute(
        'INSERT INTO inode_blocks (inode, blockno, obj_id) VALUES(?, ?, ?)', (id_, 0, obj_id)
    )
    ctx.fsck.found_errors = False
    await ctx.fsck.check()
    assert not ctx.fsck.found_errors

    # Two blocks, size not plausible
    ctx.db.execute('UPDATE inodes SET size=? WHERE id=?', (ctx.max_obj_size + block_size - 1, id_))
    await assert_fsck(ctx.fsck, ctx.fsck.check_inodes_size)

    # Two blocks, hole at the end, size plausible
    ctx.db.execute('UPDATE inodes SET size=? WHERE id=?', (ctx.max_obj_size + block_size + 1, id_))
    ctx.fsck.found_errors = False
    await ctx.fsck.check()
    assert not ctx.fsck.found_errors

    # Two blocks, size not plausible
    ctx.db.execute('UPDATE inodes SET size=? WHERE id=?', (ctx.max_obj_size, id_))
    await assert_fsck(ctx.fsck, ctx.fsck.check_inodes_size)


async def test_objects_id(ctx: FsckCtx):
    # Create an object that only exists in the backend
    await ctx.backend.store('s3ql_data_4364', b'Testdata')
    await assert_fsck(ctx.fsck, ctx.fsck.check_objects_id)

    # Create an object that does not exist in the backend
    ctx.db.execute(
        'INSERT INTO objects (id, refcount, phys_size, length) VALUES(?, ?, ?, ?)',
        (34, 1, 27, 50),
    )
    await assert_fsck(ctx.fsck, ctx.fsck.check_objects_id)


async def test_objects_hash(ctx: FsckCtx):
    id_ = ctx.db.rowid(
        "INSERT INTO inodes (mode,uid,gid,mtime_ns,atime_ns,ctime_ns,refcount,size) "
        "VALUES (?,?,?,?,?,?,?,?)",
        (
            stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR,
            0,
            0,
            time_ns(),
            time_ns(),
            time_ns(),
            1,
            8,
        ),
    )
    link(ctx.db, b'test-entry', id_)

    # Assume that due to a crash we did not write the hash for the block
    await ctx.backend.store('s3ql_data_4364', b'Testdata')
    ctx.db.execute(
        'INSERT INTO objects (id, refcount, phys_size, length) VALUES(?, ?, ?, ?)',
        (4364, 1, 8, 8),
    )
    ctx.db.execute(
        'INSERT INTO inode_blocks (inode, blockno, obj_id) VALUES(?, ?, ?)', (id_, 0, 4364)
    )

    # Should pick up wrong hash and delete objects
    await assert_fsck(
        ctx.fsck,
        ctx.fsck.check_objects_hash,
        ctx.fsck.check_objects_id,
        ctx.fsck.check_inode_blocks_obj_id,
    )

    assert not ctx.db.has_val('SELECT obj_id FROM inode_blocks WHERE inode=?', (id_,))
    inode_p = ctx.db.get_val('SELECT parent_inode FROM contents_v WHERE inode=?', (id_,))
    lof_id = ctx.db.get_val(
        "SELECT inode FROM contents_v WHERE name=? AND parent_inode=?",
        (b"lost+found", ROOT_INODE),
    )
    assert inode_p == lof_id


async def test_missing_obj(ctx: FsckCtx):
    obj_id = ctx.db.rowid('INSERT INTO objects (refcount, phys_size, length) VALUES(1, 32, 128)')

    id_ = ctx.db.rowid(
        "INSERT INTO inodes (mode,uid,gid,mtime_ns,atime_ns,ctime_ns,refcount,size) "
        "VALUES (?,?,?,?,?,?,?,?)",
        (
            stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR,
            0,
            0,
            time_ns(),
            time_ns(),
            time_ns(),
            1,
            128,
        ),
    )
    ctx.db.execute(
        'INSERT INTO inode_blocks (inode, blockno, obj_id) VALUES(?,?,?)', (id_, 0, obj_id)
    )

    link(ctx.db, b'test-entry', id_)
    await assert_fsck(ctx.fsck, ctx.fsck.check_objects_id, ctx.fsck.check_inode_blocks_obj_id)


async def test_inode_blocks_inode(ctx: FsckCtx):
    obj_id = ctx.db.rowid(
        'INSERT INTO objects (refcount, phys_size, length, hash) VALUES(1, 42, 34, ?)',
        (sha256(b'foo'),),
    )
    await ctx.backend.store('s3ql_data_%d' % obj_id, b'foo')

    ctx.db.execute(
        'INSERT INTO inode_blocks (inode, blockno, obj_id) VALUES(?,?,?)', (27, 0, obj_id)
    )

    await assert_fsck(ctx.fsck, ctx.fsck.check_inode_blocks_inode)


async def test_inode_blocks_obj_id(ctx: FsckCtx):
    id_ = ctx.db.rowid(
        "INSERT INTO inodes (mode,uid,gid,mtime_ns,atime_ns,ctime_ns,refcount,size) "
        "VALUES (?,?,?,?,?,?,?,?)",
        (
            stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR,
            0,
            0,
            time_ns(),
            time_ns(),
            time_ns(),
            1,
            128,
        ),
    )
    ctx.db.execute('INSERT INTO inode_blocks (inode, blockno, obj_id) VALUES(?,?,?)', (id_, 0, 35))

    link(ctx.db, b'test-entry', id_)
    await assert_fsck(ctx.fsck, ctx.fsck.check_inode_blocks_obj_id)


async def test_symlinks_inode(ctx: FsckCtx):
    ctx.db.execute(
        'INSERT INTO symlink_targets (inode, target) VALUES(?,?)', (42, b'somewhere else')
    )

    await assert_fsck(ctx.fsck, ctx.fsck.check_symlinks_inode)


async def test_ext_attrs_inode(ctx: FsckCtx):
    ctx.db.execute(
        'INSERT INTO ext_attributes (name_id, inode, value) VALUES(?,?,?)',
        (add_name(ctx.db, b'some name'), 34, b'some value'),
    )

    await assert_fsck(ctx.fsck, ctx.fsck.check_ext_attributes_inode)


async def test_ext_attrs_name(ctx: FsckCtx):
    id_ = ctx.db.rowid(
        "INSERT INTO inodes (mode,uid,gid,mtime_ns,atime_ns,ctime_ns,refcount,size) "
        "VALUES (?,?,?,?,?,?,?,?)",
        (
            stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR,
            0,
            0,
            time_ns(),
            time_ns(),
            time_ns(),
            1,
            128,
        ),
    )
    link(ctx.db, b'test-entry', id_)

    ctx.db.execute(
        'INSERT INTO ext_attributes (name_id, inode, value) VALUES(?,?,?)',
        (34, id_, b'some value'),
    )

    await assert_fsck(ctx.fsck, ctx.fsck.check_ext_attributes_name)


async def test_loops(ctx: FsckCtx):
    # Create some directory inodes
    inodes = [
        ctx.db.rowid(
            "INSERT INTO inodes (mode,uid,gid,mtime_ns,atime_ns,ctime_ns,refcount) "
            "VALUES (?,?,?,?,?,?,?)",
            (
                stat.S_IFDIR | stat.S_IRUSR | stat.S_IWUSR,
                0,
                0,
                time_ns(),
                time_ns(),
                time_ns(),
                1,
            ),
        )
        for _ in range(3)
    ]

    inodes.append(inodes[0])
    last = inodes[0]
    for inode in inodes[1:]:
        ctx.db.execute(
            'INSERT INTO contents (name_id, inode, parent_inode) VALUES(?, ?, ?)',
            (add_name(ctx.db, str(inode).encode()), inode, last),
        )
        last = inode

    await assert_fsck(ctx.fsck, ctx.fsck.check_loops)


async def test_tmpfile(ctx: FsckCtx):
    # Ensure that path exists
    objname = 's3ql_data_38375'
    await ctx.backend.store(objname, b'bla')
    await ctx.backend.delete(objname)
    path = ctx.raw_backend._key_to_path(objname)
    tmpname = '%s#%d-%d.tmp' % (path, os.getpid(), _thread.get_ident())
    with open(tmpname, 'wb') as fh:
        fh.write(b'Hello, world')

    await assert_fsck(ctx.fsck, ctx.fsck.check_objects_temp)


async def test_obj_refcounts(ctx: FsckCtx):
    obj_id = ctx.db.rowid(
        'INSERT INTO objects (refcount, phys_size, length, hash) VALUES(1, 42, 0, ?)',
        (sha256(b'foo'),),
    )
    await ctx.backend.store('s3ql_data_%d' % obj_id, b'foo')

    inode = ctx.db.rowid(
        "INSERT INTO inodes (mode,uid,gid,mtime_ns,atime_ns,ctime_ns,refcount,size) "
        "VALUES (?,?,?,?,?,?,?,?)",
        (
            stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR,
            os.getuid(),
            os.getgid(),
            time_ns(),
            time_ns(),
            time_ns(),
            1,
            2048,
        ),
    )
    link(ctx.db, b'test-entry', inode)
    ctx.db.execute(
        'INSERT INTO inode_blocks (inode, blockno, obj_id) VALUES(?,?,?)', (inode, 1, obj_id)
    )
    ctx.db.execute(
        'INSERT INTO inode_blocks (inode, blockno, obj_id) VALUES(?,?,?)', (inode, 2, obj_id)
    )

    await assert_fsck(ctx.fsck, ctx.fsck.check_objects_refcount)


async def test_orphaned_obj(ctx: FsckCtx):
    ctx.db.rowid(
        'INSERT INTO objects (refcount, phys_size, length, hash) VALUES(1, 33, 50, ?)',
        (sha256(b'foobar'),),
    )
    await assert_fsck(ctx.fsck, ctx.fsck.check_objects_refcount)


async def test_unix_size(ctx: FsckCtx):
    inode = 42
    ctx.db.execute(
        "INSERT INTO inodes (id, mode,uid,gid,mtime_ns,atime_ns,ctime_ns,refcount,size) "
        "VALUES (?,?,?,?,?,?,?,?,?)",
        (
            inode,
            stat.S_IFIFO | stat.S_IRUSR | stat.S_IWUSR,
            os.getuid(),
            os.getgid(),
            time_ns(),
            time_ns(),
            time_ns(),
            1,
            0,
        ),
    )
    link(ctx.db, b'test-entry', inode)
    ctx.db.execute('UPDATE inodes SET size = 1 WHERE id=?', (inode,))
    await assert_fsck(ctx.fsck, ctx.fsck.check_unix, can_fix=False)


async def test_unix_size_symlink(ctx: FsckCtx):
    inode = 42
    target = b'some funny random string'
    ctx.db.execute(
        "INSERT INTO inodes (id, mode,uid,gid,mtime_ns,atime_ns,ctime_ns,refcount,size) "
        "VALUES (?,?,?,?,?,?,?,?,?)",
        (
            inode,
            stat.S_IFLNK | stat.S_IRUSR | stat.S_IWUSR,
            os.getuid(),
            os.getgid(),
            time_ns(),
            time_ns(),
            time_ns(),
            1,
            len(target),
        ),
    )
    ctx.db.execute('INSERT INTO symlink_targets (inode, target) VALUES(?,?)', (inode, target))
    link(ctx.db, b'test-entry', inode)
    ctx.db.execute('UPDATE inodes SET size = 0 WHERE id=?', (inode,))
    await assert_fsck(ctx.fsck, ctx.fsck.check_unix, can_fix=False)


async def test_unix_target(ctx: FsckCtx):
    inode = 42
    ctx.db.execute(
        "INSERT INTO inodes (id, mode,uid,gid,mtime_ns,atime_ns,ctime_ns,refcount) "
        "VALUES (?,?,?,?,?,?,?,?)",
        (
            inode,
            stat.S_IFCHR | stat.S_IRUSR | stat.S_IWUSR,
            os.getuid(),
            os.getgid(),
            time_ns(),
            time_ns(),
            time_ns(),
            1,
        ),
    )
    link(ctx.db, b'test-entry', inode)
    ctx.db.execute('INSERT INTO symlink_targets (inode, target) VALUES(?,?)', (inode, 'foo'))
    await assert_fsck(ctx.fsck, ctx.fsck.check_unix, can_fix=False)


async def test_unix_nomode_reg(ctx: FsckCtx):
    perms = stat.S_IRUSR | stat.S_IWUSR | stat.S_IROTH | stat.S_IRGRP
    stamp = time_ns()
    inode = ctx.db.rowid(
        "INSERT INTO inodes (mode,uid,gid,mtime_ns,atime_ns,ctime_ns,refcount) "
        "VALUES (?,?,?,?,?,?,?)",
        (perms, os.getuid(), os.getgid(), stamp, stamp, stamp, 1),
    )
    link(ctx.db, b'test-entry', inode)

    await assert_fsck(ctx.fsck, ctx.fsck.check_unix)

    newmode: int = ctx.db.get_val('SELECT mode FROM inodes WHERE id=?', (inode,))  # type: ignore[assignment]
    assert stat.S_IMODE(newmode) == perms
    assert stat.S_IFMT(newmode) == stat.S_IFREG


async def test_unix_nomode_dir(ctx: FsckCtx):
    perms = stat.S_IRUSR | stat.S_IWUSR | stat.S_IROTH | stat.S_IRGRP
    stamp = time_ns()
    inode = ctx.db.rowid(
        "INSERT INTO inodes (mode,uid,gid,mtime_ns,atime_ns,ctime_ns,refcount) "
        "VALUES (?,?,?,?,?,?,?)",
        (perms, os.getuid(), os.getgid(), stamp, stamp, stamp, 1),
    )
    inode2 = ctx.db.rowid(
        "INSERT INTO inodes (mode,uid,gid,mtime_ns,atime_ns,ctime_ns,refcount) "
        "VALUES (?,?,?,?,?,?,?)",
        (perms | stat.S_IFREG, os.getuid(), os.getgid(), stamp, stamp, stamp, 1),
    )

    link(ctx.db, b'test-entry', inode)
    link(ctx.db, b'subentry', inode2, inode)

    await assert_fsck(ctx.fsck, ctx.fsck.check_unix)

    newmode: int = ctx.db.get_val('SELECT mode FROM inodes WHERE id=?', (inode,))  # type: ignore[assignment]
    assert stat.S_IMODE(newmode) == perms
    assert stat.S_IFMT(newmode) == stat.S_IFDIR


async def test_unix_symlink_no_target(ctx: FsckCtx):
    inode = ctx.db.rowid(
        "INSERT INTO inodes (mode,uid,gid,mtime_ns,atime_ns,ctime_ns,refcount) "
        "VALUES (?,?,?,?,?,?,?)",
        (
            stat.S_IFLNK | stat.S_IRUSR | stat.S_IWUSR,
            os.getuid(),
            os.getgid(),
            time_ns(),
            time_ns(),
            time_ns(),
            1,
        ),
    )
    link(ctx.db, b'test-entry', inode)
    await assert_fsck(ctx.fsck, ctx.fsck.check_unix, can_fix=False)


async def test_unix_rdev(ctx: FsckCtx):
    inode = 42
    ctx.db.execute(
        "INSERT INTO inodes (id, mode,uid,gid,mtime_ns,atime_ns,ctime_ns,refcount) "
        "VALUES (?,?,?,?,?,?,?,?)",
        (
            inode,
            stat.S_IFIFO | stat.S_IRUSR | stat.S_IWUSR,
            os.getuid(),
            os.getgid(),
            time_ns(),
            time_ns(),
            time_ns(),
            1,
        ),
    )
    link(ctx.db, b'test-entry', inode)
    ctx.db.execute('UPDATE inodes SET rdev=? WHERE id=?', (42, inode))
    await assert_fsck(ctx.fsck, ctx.fsck.check_unix, can_fix=False)


async def test_unix_child(ctx: FsckCtx):
    inode = ctx.db.rowid(
        "INSERT INTO inodes (mode,uid,gid,mtime_ns,atime_ns,ctime_ns,refcount) "
        "VALUES (?,?,?,?,?,?,?)",
        (
            stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR,
            os.getuid(),
            os.getgid(),
            time_ns(),
            time_ns(),
            time_ns(),
            1,
        ),
    )
    link(ctx.db, b'test-entry', inode)
    ctx.db.execute(
        'INSERT INTO contents (name_id, inode, parent_inode) VALUES(?,?,?)',
        (add_name(ctx.db, b'foo'), ROOT_INODE, inode),
    )
    await assert_fsck(ctx.fsck, ctx.fsck.check_unix, can_fix=False)


async def test_unix_blocks(ctx: FsckCtx):
    # Socket with data blocks
    inode = ctx.db.rowid(
        "INSERT INTO inodes (mode,uid,gid,mtime_ns,atime_ns,ctime_ns,refcount) "
        "VALUES (?,?,?,?,?,?,?)",
        (
            stat.S_IFSOCK | stat.S_IRUSR | stat.S_IWUSR,
            os.getuid(),
            os.getgid(),
            time_ns(),
            time_ns(),
            time_ns(),
            1,
        ),
    )
    link(ctx.db, b'test-entry', inode)
    obj_id = ctx.db.rowid('INSERT INTO objects (refcount, phys_size, length) VALUES(1, 32, 0)')
    ctx.db.execute(
        'INSERT INTO inode_blocks (inode, blockno, obj_id) VALUES(?,?,?)', (inode, 1, obj_id)
    )

    await assert_fsck(ctx.fsck, ctx.fsck.check_unix, can_fix=False)
