'''
mkfs.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import annotations

import logging
import os
import shutil
import stat
import sys
import time
from base64 import b64encode
from collections.abc import Sequence
from contextlib import AsyncExitStack
from getpass import getpass
from typing import Annotated

import typer

from . import CTRL_INODE, CURRENT_FS_REV, ROOT_INODE
from .authinfo import Authinfo
from .backends import open_raw_backend_v2, s3
from .backends.comprenc import AsyncComprencBackend
from .common import split_by_n, time_ns
from .database import (
    Connection,
    FsAttributes,
    create_tables,
    upload_metadata,
    upload_params,
    write_params,
)
from .logging import QuietError, setup_logging
from .mount import determine_threads
from .parse_args import (
    DEFAULT_AUTHFILE,
    AuthFile,
    BackendOptions,
    CacheDir,
    DebugFlag,
    DebugModules,
    MaxConnections,
    MaxThreads,
    QuietFlag,
    StorageUrl,
    init_cachedir,
    make_app,
    pick,
    run_app,
    trio_command,
)

log: logging.Logger = logging.getLogger(__name__)

app = make_app()


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


@app.command()
@trio_command
async def mkfs(
    storage_url: StorageUrl,
    authfile: AuthFile = None,
    cachedir: CacheDir = None,
    backend_options: BackendOptions = None,
    label: Annotated[
        str, typer.Option('-L', '--label', metavar='<name>', help='Filesystem label')
    ] = '',
    data_block_size: Annotated[
        int,
        typer.Option(
            metavar='<size>',
            help='Block size (in KiB) to use for storing file contents. Files larger than this '
            'size will be stored in multiple backend objects. Backend object size may be smaller '
            'than this due to compression.',
        ),
    ] = 1024,
    metadata_block_size: Annotated[
        int,
        typer.Option(
            metavar='<size>',
            help='Block size (in KiB) to use for storing filesystem metadata. Backend object '
            'size may be smaller than this due to compression.',
        ),
    ] = 64,
    plain: Annotated[bool, typer.Option('--plain', help='Create unencrypted file system.')] = False,
    max_connections: MaxConnections = None,
    max_threads: MaxThreads = None,
    debug: DebugFlag = False,
    debug_modules: DebugModules = None,
    quiet: QuietFlag = False,
    *,
    stack: AsyncExitStack,
) -> None:
    '''Initialize an S3QL file system.'''
    setup_logging(quiet=quiet, log=None, debug=debug, debug_modules=debug_modules)

    authinfo = Authinfo.from_file(pick(authfile, DEFAULT_AUTHFILE), storage_url)
    cachepath = init_cachedir(pick(cachedir, authinfo.cachedir), storage_url)

    # mkfs only uploads the initial (small) metadata snapshot, so size the thread pool from the
    # CPU count alone.
    threads = pick(max_threads, authinfo.max_threads) or determine_threads(None)
    AsyncComprencBackend.set_max_threads(threads)

    plain_backend = await stack.enter_async_context(
        await open_raw_backend_v2(
            storage_url,
            authinfo,
            backend_options=backend_options,
            max_connections=pick(max_connections, authinfo.max_connections),
        )
    )

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
    if not plain:
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

        backend = await AsyncComprencBackend.create(
            wrap_pw_bytes, authinfo.compress.to_comprenc(), plain_backend
        )
        await backend.store('s3ql_passphrase', data_pw)
        await backend.store('s3ql_passphrase_bak1', data_pw)
        await backend.store('s3ql_passphrase_bak2', data_pw)
        await backend.store('s3ql_passphrase_bak3', data_pw)
    else:
        data_pw = None

    # There can't be a corresponding backend, so we can safely delete
    # these files.
    if os.path.exists(cachepath + '.db'):
        os.unlink(cachepath + '.db')
    if os.path.exists(cachepath + '-cache'):
        shutil.rmtree(cachepath + '-cache')

    param = FsAttributes(
        revision=CURRENT_FS_REV,
        seq_no=int(time.time()),
        label=label,
        data_block_size=data_block_size * 1024,
        metadata_block_size=metadata_block_size * 1024,
        last_fsck=time.time(),
        last_modified=time.time(),
    )

    log.info('Creating metadata tables...')
    db = Connection(cachepath + '.db', param.metadata_block_size)
    create_tables(db)
    init_tables(db)
    db.close()

    backend = await AsyncComprencBackend.create(
        data_pw, authinfo.compress.to_comprenc(), plain_backend
    )
    log.info('Uploading metadata...')
    await upload_metadata(backend, db, param)
    write_params(cachepath, param)
    await upload_params(backend, param)
    # *backend* wraps *plain_backend*; its close() would also close
    # *plain_backend*, which the exit stack already handles, so leave it alone.

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


def main(args: Sequence[str] | None = None) -> None:
    run_app(app, args, prog_name='mkfs.s3ql')


if __name__ == '__main__':
    main(sys.argv[1:])
