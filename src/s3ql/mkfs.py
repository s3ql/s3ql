'''
mkfs.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import annotations

import argparse
import functools
import logging
import os
import shutil
import stat
import sys
import time
from base64 import b64encode
from collections.abc import Sequence
from getpass import getpass

import trio

from . import CTRL_INODE, CURRENT_FS_REV, ROOT_INODE
from .backends import s3
from .backends.comprenc import AsyncComprencBackend, ComprencBackend
from .common import split_by_n, time_ns
from .database import (
    Connection,
    FsAttributes,
    create_tables,
    upload_metadata,
    upload_params,
    write_params,
)
from .logging import QuietError, setup_logging, setup_warnings
from .parse_args import ArgumentParser

log: logging.Logger = logging.getLogger(__name__)


def parse_args(args: Sequence[str]) -> argparse.Namespace:
    parser = ArgumentParser(description="Initializes an S3QL file system")

    parser.add_cachedir()
    parser.add_log()
    parser.add_debug()
    parser.add_quiet()
    parser.add_backend_options()
    parser.add_version()
    parser.add_storage_url()

    parser.add_argument(
        "-L",
        default='',
        help="Filesystem label",
        dest="label",
        metavar='<name>',
    )
    parser.add_argument(
        "--data-block-size",
        type=int,
        default=1024,
        metavar='<size>',
        help="Block size (in KiB) to use for storing file contents. Files "
        "larger than this size will be stored in multiple backend objects. "
        "Backend object size may be smaller than this due to compression. "
        "Default: %(default)d KiB.",
    )
    parser.add_argument(
        "--metadata-block-size",
        type=int,
        default=64,
        metavar='<size>',
        help="Block size (in KiB) to use for storing filesystem metadata. "
        "Backend object size may be smaller than this due to compression. "
        "Default: %(default)d KiB.",
    )
    parser.add_argument(
        "--plain", action="store_true", default=False, help="Create unencrypted file system."
    )

    options = parser.parse_args(args)

    return options


def init_tables(conn: Connection) -> None:
    # Insert root directory
    now_ns = time_ns()
    conn.execute(
        "INSERT INTO inodes (id,mode,uid,gid,mtime_ns,atime_ns,ctime_ns,refcount) "
        "VALUES (?,?,?,?,?,?,?,?)",
        (
            ROOT_INODE,
            stat.S_IFDIR
            | stat.S_IRUSR
            | stat.S_IWUSR
            | stat.S_IXUSR
            | stat.S_IRGRP
            | stat.S_IXGRP
            | stat.S_IROTH
            | stat.S_IXOTH,
            os.getuid(),
            os.getgid(),
            now_ns,
            now_ns,
            now_ns,
            1,
        ),
    )

    # Insert control inode, the actual values don't matter that much
    conn.execute(
        "INSERT INTO inodes (id,mode,uid,gid,mtime_ns,atime_ns,ctime_ns,refcount) "
        "VALUES (?,?,?,?,?,?,?,?)",
        (CTRL_INODE, stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR, 0, 0, now_ns, now_ns, now_ns, 42),
    )

    # Insert lost+found directory
    inode = conn.rowid(
        "INSERT INTO inodes (mode,uid,gid,mtime_ns,atime_ns,ctime_ns,refcount) "
        "VALUES (?,?,?,?,?,?,?)",
        (
            stat.S_IFDIR | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR,
            os.getuid(),
            os.getgid(),
            now_ns,
            now_ns,
            now_ns,
            1,
        ),
    )
    name_id = conn.rowid('INSERT INTO names (name, refcount) VALUES(?,?)', (b'lost+found', 1))
    conn.execute(
        "INSERT INTO contents (name_id, inode, parent_inode) VALUES(?,?,?)",
        (name_id, inode, ROOT_INODE),
    )


def main(args: Sequence[str] | None = None) -> None:
    if args is None:
        args = sys.argv[1:]

    options = parse_args(args)
    setup_logging(options)
    setup_warnings()
    trio.run(main_async, options)


async def main_async(options: argparse.Namespace) -> None:
    plain_backend = await options.backend_class.create(options)
    backend: AsyncComprencBackend | None = None

    log.info(
        "Before using S3QL, make sure to read the user's guide, especially\n"
        "the 'Important Rules to Avoid Losing Data' section."
    )

    if isinstance(plain_backend, s3.AsyncBackend) and '.' in plain_backend.bucket_name:
        log.warning(
            'S3 Buckets with names containing dots cannot be '
            'accessed using SSL!'
            '(cf. https://forums.aws.amazon.com/thread.jspa?threadID=130560)'
        )

    if await plain_backend.contains('s3ql_params') or await plain_backend.contains('s3ql_metadata'):
        raise QuietError(
            "Refusing to overwrite existing file system! (use `s3qladm clear` to delete)"
        )

    data_pw: bytes | None
    if not options.plain:
        if sys.stdin.isatty():
            wrap_pw = getpass("Enter encryption password: ")
            if not wrap_pw == getpass("Confirm encryption password: "):
                raise QuietError("Passwords don't match.")
        else:
            wrap_pw = sys.stdin.readline().rstrip()
        wrap_pw_bytes = wrap_pw.encode('utf-8')

        # Generate data encryption passphrase
        log.info('Generating random encryption key...')
        with open('/dev/urandom', "rb", 0) as fh:  # No buffering
            data_pw = fh.read(32)

        backend = await AsyncComprencBackend.create(wrap_pw_bytes, ('lzma', 2), plain_backend)
        await backend.store('s3ql_passphrase', data_pw)
        await backend.store('s3ql_passphrase_bak1', data_pw)
        await backend.store('s3ql_passphrase_bak2', data_pw)
        await backend.store('s3ql_passphrase_bak3', data_pw)
    else:
        data_pw = None

    backend = await AsyncComprencBackend.create(data_pw, ('lzma', 2), plain_backend)
    cachepath = options.cachepath

    # There can't be a corresponding backend, so we can safely delete
    # these files.
    if os.path.exists(cachepath + '.db'):
        os.unlink(cachepath + '.db')
    if os.path.exists(cachepath + '-cache'):
        shutil.rmtree(cachepath + '-cache')

    param = FsAttributes(
        revision=CURRENT_FS_REV,
        seq_no=int(time.time()),
        label=options.label,
        data_block_size=options.data_block_size * 1024,
        metadata_block_size=options.metadata_block_size * 1024,
        last_fsck=time.time(),
        last_modified=time.time(),
    )

    log.info('Creating metadata tables...')
    db = Connection(cachepath + '.db', param.metadata_block_size)
    create_tables(db)
    init_tables(db)

    log.info('Uploading metadata...')
    db.close()
    sync_backend = ComprencBackend.from_async_backend(backend)
    await trio.to_thread.run_sync(functools.partial(upload_metadata, sync_backend, db, param))
    write_params(cachepath, param)
    await trio.to_thread.run_sync(functools.partial(upload_params, sync_backend, param))
    if os.path.exists(cachepath + '-cache'):
        shutil.rmtree(cachepath + '-cache')

    if data_pw is not None:
        data_pw_groups = split_by_n(b64encode(data_pw).decode(), 4)
        print(
            'Please store the following master key in a safe location. It allows ',
            'decryption of the S3QL file system in case the storage objects holding ',
            'this information get corrupted:',
            '---BEGIN MASTER KEY---',
            ' '.join(data_pw_groups),
            '---END MASTER KEY---',
            sep='\n',
        )


if __name__ == '__main__':
    main(sys.argv[1:])
