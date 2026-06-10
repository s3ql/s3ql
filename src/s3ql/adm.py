'''
adm.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import annotations

import dataclasses
import logging
import os
import re
import shutil
import sys
import tempfile
import textwrap
import time
from base64 import b64decode
from collections.abc import Sequence
from contextlib import AsyncExitStack
from datetime import datetime
from getpass import getpass
from typing import Annotated, cast

import typer

from s3ql.database import (
    Connection,
    FsAttributes,
    download_metadata,
    expire_objects,
    get_available_seq_nos,
    read_remote_params,
    upload_metadata,
    upload_params,
    write_params,
)
from s3ql.mount import determine_threads, get_metadata

from . import CURRENT_FS_REV, REV_VER_MAP
from .authinfo import Authinfo
from .backends import open_backend, open_raw_backend
from .backends.common import NoSuchObject
from .backends.comprenc import AsyncComprencBackend
from .common import (
    is_mounted,
    pretty_print_size,
    thaw_basic_mapping,
)
from .logging import QuietError, setup_logging
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
from .types import BasicMappingT

log: logging.Logger = logging.getLogger(__name__)

app = make_app(help='Manage S3QL File Systems.')


@dataclasses.dataclass
class SharedArgs:
    '''Backend-related options shared by all `s3qladm` subcommands.

    They are declared on the group callback (`_setup`) because Typer builds the group-level option
    parser from its signature, but they are consumed by the subcommands, which reach them through
    `ctx.obj`.
    '''

    authfile: str | None
    backend_options: str | None
    cachedir: str | None
    max_connections: int | None
    max_threads: int | None


def get_shared_args(ctx: typer.Context) -> SharedArgs:
    '''Return the `SharedArgs` that the group callback stored on *ctx*.'''
    return cast(SharedArgs, ctx.obj)


# Typer builds the group-level option parser from this callback's signature, so every shared option
# must be declared here even though the subcommands are what consume the backend ones. The callback
# stashes them on `ctx.obj` as a `SharedArgs` for the subcommands to read back.
@app.callback()
def _setup(
    ctx: typer.Context,
    authfile: AuthFile = None,
    backend_options: BackendOptions = None,
    cachedir: CacheDir = None,
    max_connections: MaxConnections = None,
    max_threads: MaxThreads = None,
    debug: DebugFlag = False,
    debug_modules: DebugModules = None,
    quiet: QuietFlag = False,
) -> None:
    '''Manage S3QL File Systems.'''
    setup_logging(quiet=quiet, log=None, debug=debug, debug_modules=debug_modules)
    ctx.obj = SharedArgs(
        authfile=authfile,
        backend_options=backend_options,
        cachedir=cachedir,
        max_connections=max_connections,
        max_threads=max_threads,
    )


def _resolve(args: SharedArgs, storage_url: str) -> tuple[Authinfo, str]:
    '''Read authinfo2, guard against a mounted file system, size the thread pool, and return the
    parsed `Authinfo` together with the per-file-system cache path.
    '''
    authinfo = Authinfo.from_file(pick(args.authfile, DEFAULT_AUTHFILE), storage_url)

    # Check if fs is mounted on this computer. This is not foolproof but should prevent
    # common mistakes.
    if is_mounted(storage_url):
        raise QuietError('Can not work on mounted file system.')

    # Admin actions never bulk-upload data, so size the thread pool purely from the CPU count.
    threads = pick(args.max_threads, authinfo.max_threads) or determine_threads(None)
    AsyncComprencBackend.set_max_threads(threads)

    cachepath = init_cachedir(pick(args.cachedir, authinfo.cachedir), storage_url)
    return (authinfo, cachepath)


@app.command()
@trio_command
async def passphrase(ctx: typer.Context, storage_url: StorageUrl, *, stack: AsyncExitStack) -> None:
    '''Change file system passphrase.'''
    args = get_shared_args(ctx)
    authinfo, _cachepath = _resolve(args, storage_url)
    backend = await stack.enter_async_context(
        await open_backend(
            storage_url,
            authinfo,
            backend_options=args.backend_options,
            max_connections=args.max_connections,
            compress=authinfo.compress,
        )
    )

    if not backend.passphrase:
        raise QuietError('File system is not encrypted.')

    data_pw = backend.passphrase
    assert data_pw is not None

    print(
        textwrap.dedent(
            '''\
       NOTE: If your password has been compromised already, then changing
       it WILL NOT PROTECT YOUR DATA, because an attacker may have already
       retrieved the master key.
       '''
        )
    )
    if sys.stdin.isatty():
        wrap_pw = getpass("Enter new encryption password: ")
        if not wrap_pw == getpass("Confirm new encryption password: "):
            raise QuietError("Passwords don't match")
    else:
        wrap_pw = sys.stdin.readline().rstrip()
    wrap_pw_bytes = wrap_pw.encode('utf-8')

    backend.passphrase = wrap_pw_bytes
    await backend.store('s3ql_passphrase', data_pw)
    await backend.store('s3ql_passphrase_bak1', data_pw)
    await backend.store('s3ql_passphrase_bak2', data_pw)
    await backend.store('s3ql_passphrase_bak3', data_pw)
    backend.passphrase = data_pw


@app.command(name='recover-key')
@trio_command
async def recover_key(
    ctx: typer.Context, storage_url: StorageUrl, *, stack: AsyncExitStack
) -> None:
    '''Recover master key from offline copy.'''
    args = get_shared_args(ctx)
    authinfo, _cachepath = _resolve(args, storage_url)
    backend = await stack.enter_async_context(
        await open_raw_backend(
            storage_url,
            authinfo,
            backend_options=args.backend_options,
            max_connections=args.max_connections,
        )
    )

    print("Enter master key (should be 11 blocks of 4 characters each): ")
    data_pw_str = sys.stdin.readline()
    data_pw_str = re.sub(r'\s+', '', data_pw_str)
    try:
        data_pw = b64decode(data_pw_str)
    except ValueError:
        raise QuietError("Malformed master key. Expected valid base64.")

    if len(data_pw) != 32:
        raise QuietError("Malformed master key. Expected length 32, got %d." % len(data_pw))

    if sys.stdin.isatty():
        wrap_pw = getpass("Enter new encryption password: ")
        if not wrap_pw == getpass("Confirm new encryption password: "):
            raise QuietError("Passwords don't match")
    else:
        wrap_pw = sys.stdin.readline().rstrip()
    wrap_pw_bytes = wrap_pw.encode('utf-8')

    comprenc_backend = await AsyncComprencBackend.create(
        wrap_pw_bytes, authinfo.compress.to_comprenc(), backend
    )
    await comprenc_backend.store('s3ql_passphrase', data_pw)
    await comprenc_backend.store('s3ql_passphrase_bak1', data_pw)
    await comprenc_backend.store('s3ql_passphrase_bak2', data_pw)
    await comprenc_backend.store('s3ql_passphrase_bak3', data_pw)


@app.command()
@trio_command
async def clear(ctx: typer.Context, storage_url: StorageUrl, *, stack: AsyncExitStack) -> None:
    '''Delete file system and all data.'''
    args = get_shared_args(ctx)
    authinfo, cachepath = _resolve(args, storage_url)
    backend = await stack.enter_async_context(
        await open_raw_backend(
            storage_url,
            authinfo,
            backend_options=args.backend_options,
            max_connections=args.max_connections,
        )
    )

    print(
        'I am about to DELETE ALL DATA in %s.' % backend,
        'This includes not just S3QL file systems but *all* stored objects.',
        'Depending on the storage service, it may be necessary to run this command',
        'several times to delete all data, and it may take a while until the ',
        'removal becomes effective.',
        'Please enter "yes" to continue.',
        '> ',
        sep='\n',
        end='',
    )
    sys.stdout.flush()

    if sys.stdin.readline().strip().lower() != 'yes':
        raise QuietError()

    log.info('Deleting in batches...')
    for suffix in ('.db', '.params'):
        name = cachepath + suffix
        if os.path.exists(name):
            os.unlink(name)

    name = cachepath + '-cache'
    if os.path.exists(name):
        shutil.rmtree(name)

    batch: list[str] = []
    total = 0
    async for key in backend.list():
        batch.append(key)
        if len(batch) >= 1000:
            count = len(batch)
            await backend.delete_multi(batch, log_progress=True)
            if batch:
                raise RuntimeError(f'Failed to delete {len(batch)} objects')
            total += count
            batch.clear()
    if batch:
        count = len(batch)
        await backend.delete_multi(batch, log_progress=True)
        if batch:
            raise RuntimeError(f'Failed to delete {len(batch)} objects')
        total += count
    log.info('Deleted %d objects total.', total)


def get_old_rev_msg(rev: int, prog: str) -> str:
    return textwrap.dedent(
        '''\
        The last S3QL version that supported this file system revision
        was %(version)s. To run this version's %(prog)s, proceed along
        the following steps:

          $ # retrieve and unpack required release
          $ (cd s3ql-%(version)s; ./setup.py build_ext --inplace)
          $ s3ql-%(version)s/bin/%(prog)s <options>
        '''
        % {'version': REV_VER_MAP[rev], 'prog': prog}
    )


@app.command(name='shrink-db')
@trio_command
async def shrink_db(ctx: typer.Context, storage_url: StorageUrl, *, stack: AsyncExitStack) -> None:
    '''Recover unused space in the metadata database.'''
    args = get_shared_args(ctx)
    authinfo, cachepath = _resolve(args, storage_url)
    backend = await stack.enter_async_context(
        await open_backend(
            storage_url,
            authinfo,
            backend_options=args.backend_options,
            max_connections=args.max_connections,
            compress=authinfo.compress,
        )
    )

    (param, db) = await get_metadata(backend, cachepath)
    param.seq_no += 1
    param.is_mounted = True
    write_params(cachepath, param)
    await upload_params(backend, param)

    old_size = os.path.getsize(db.file)
    db.execute('VACUUM')
    db.close()
    new_size = os.path.getsize(db.file)
    log.info(
        'Database size reduced from %s to %s (%.1f%%)',
        pretty_print_size(old_size),
        pretty_print_size(new_size),
        100 * (old_size - new_size) / old_size,
    )

    param.is_mounted = False
    param.last_modified = time.time()
    await upload_metadata(backend, db, param)
    write_params(cachepath, param)
    await upload_params(backend, param)
    await expire_objects(backend)


@app.command()
@trio_command
async def upgrade(
    ctx: typer.Context,
    storage_url: StorageUrl,
    metadata_block_size: Annotated[
        int,
        typer.Option(
            metavar='<size>',
            help='Block size (in KiB) to use for storing filesystem metadata. Backend object '
            'size may be smaller than this due to compression.',
        ),
    ] = 64,
    *,
    stack: AsyncExitStack,
) -> None:
    '''Upgrade file system to newest revision.'''
    args = get_shared_args(ctx)
    authinfo, cachepath = _resolve(args, storage_url)
    backend = await stack.enter_async_context(
        await open_backend(
            storage_url,
            authinfo,
            backend_options=args.backend_options,
            max_connections=args.max_connections,
            compress=authinfo.compress,
        )
    )

    params_path = cachepath + '.params'
    if not os.path.exists(cachepath + '.db') or not os.path.exists(params_path):
        print(
            'To upgrade the filesystem, first download file system metadata using the previous',
            'version of S3QL (eg. by running fsck.s3ql)',
            sep='\n',
        )
        sys.exit(1)

    with open(params_path, 'rb') as fh:
        local_params: BasicMappingT = thaw_basic_mapping(fh.read())
    try:
        remote_params = await backend.lookup('s3ql_metadata')

    # Filesystem may have been updated already
    except NoSuchObject:
        remote_params_typed = await read_remote_params(backend)
        remote_params = {
            'seq_no': remote_params_typed.seq_no,
            'revision': remote_params_typed.revision,
        }

    local_seq_no = local_params['seq_no']
    remote_seq_no = remote_params['seq_no']
    local_revision = local_params['revision']
    assert isinstance(local_seq_no, int) and isinstance(remote_seq_no, int)
    assert isinstance(local_revision, int)

    if local_seq_no < remote_seq_no:
        print(
            'Local metadata copy is not up-to-date. To upgrade the filesystem, first download',
            'file system metadata using the previous  version of S3QL (eg. by running fsck.s3ql)',
            sep='\n',
        )
        sys.exit(1)

    elif local_seq_no > remote_seq_no or local_params['needs_fsck']:
        print(
            'Filesystem was not cleanly unmounted. Check file system using fsck.s3ql',
            'from the previous version of S3QL.',
            get_old_rev_msg(local_revision, 'fsck.s3ql'),
            sep='\n',
        )
        sys.exit(30)

    if local_revision < CURRENT_FS_REV - 1:
        print(
            textwrap.dedent(
                '''\
            File system revision too old to upgrade!

            You need to use an older S3QL version to upgrade to a more recent revision before you
            can use this version to upgrade to the newest revision.
            '''
            )
        )
        print(get_old_rev_msg(local_revision + 1, 's3qladm'))
        raise QuietError()

    elif local_revision >= CURRENT_FS_REV:
        print('File system already at most-recent revision')
        return

    print(
        textwrap.dedent(
            f'''
        I am about to update the file system to the newest revision. You will not be able to access
        the file system with any older version of S3QL after this operation.

        You should make very sure that this command is not interrupted and that no one else tries to
        mount, fsck or upgrade the file system at the same time. You may interrupt the update with
        Ctrl+C and resume at a later time, but the filesystem will not be in a usable state until
        the upgrade is complete.

        Filesystem metadata will be split into {metadata_block_size} kB blocks for remote storage.
        This value CAN NOT BE CHANGED after upgrade. To use a different value, restart the update
        with a different --metadata-block-size parameter.
        '''
        )
    )

    print('Please enter "yes" to continue.', '> ', sep='\n', end='')
    sys.stdout.flush()

    if sys.stdin.readline().strip().lower() != 'yes':
        raise QuietError()

    log.info('Upgrading from revision %d to %d...', local_revision, CURRENT_FS_REV)

    new_params: BasicMappingT = {k.replace('-', '_'): v for k, v in local_params.items()}
    new_params['data_block_size'] = new_params['max_obj_size']
    new_params['metadata_block_size'] = metadata_block_size * 1024
    new_params['revision'] = CURRENT_FS_REV
    # remove all deprecated attributes
    valid_keys = {f.name for f in dataclasses.fields(FsAttributes)}
    for k in list(new_params.keys()):
        if k not in valid_keys:
            del new_params[k]
    params = FsAttributes(**new_params)  # type: ignore[arg-type]

    db = Connection(cachepath + '.db', metadata_block_size * 1024)

    params.last_modified = time.time()
    params.seq_no += 1

    await upload_metadata(backend, db, params, incremental=False)
    write_params(cachepath, params)
    await upload_params(backend, params)

    # Re-upload the old metadata object to make sure that we don't accidentally
    # re-mount the filesystem with an older S3QL version.
    log.info('Backing up old metadata...')
    local_params['revision'] = CURRENT_FS_REV
    local_params['seq_no'] = local_seq_no + 1
    with tempfile.TemporaryFile() as tmpfh:
        await backend.readinto_fh('s3ql_metadata', tmpfh)
        len_ = tmpfh.tell()
        tmpfh.seek(0)
        await backend.write_fh(
            "s3ql_metadata", tmpfh, len_, metadata=local_params, dont_compress=True
        )

    print('File system upgrade complete.')


@app.command(name='restore-metadata')
@trio_command
async def restore_metadata(
    ctx: typer.Context, storage_url: StorageUrl, *, stack: AsyncExitStack
) -> None:
    '''Interactively restore metadata backups.'''
    args = get_shared_args(ctx)
    authinfo, cachepath = _resolve(args, storage_url)
    backend = await stack.enter_async_context(
        await open_backend(
            storage_url,
            authinfo,
            backend_options=args.backend_options,
            max_connections=args.max_connections,
            compress=authinfo.compress,
        )
    )

    backups = sorted(await get_available_seq_nos(backend))

    if not backups:
        raise QuietError('No metadata backups found.')

    print('The following backups are available:')
    print('Idx    Seq No: Last Modified:')
    for i, backup_seq_no in enumerate(backups):
        # De-serialize directly instead of using read_remote_params() to avoid exceptions on old
        # filesystem revisions.
        d = thaw_basic_mapping((await backend.fetch('s3ql_params_%010x' % backup_seq_no))[0])
        if d['revision'] != CURRENT_FS_REV:
            print(f'{i:3d} {backup_seq_no:010x} (unsupported revision)')
            continue
        params = FsAttributes(**d)  # type: ignore[arg-type]
        assert params.seq_no == backup_seq_no
        date = datetime.fromtimestamp(params.last_modified).strftime('%Y-%m-%d %H:%M:%S')
        print(f'{i:3d} {backup_seq_no:010x} {date}')

    print(
        'Restoring a metadata backup will almost always result in partial data loss and',
        'should only be done if the filesystem metadata has been corruped beyond repair.',
        sep='\n',
    )

    seq_no: int | None = None
    while seq_no is None:
        buf = input('Enter index to revert to: ')
        try:
            seq_no = backups[int(buf.strip())]
        except (ValueError, IndexError):
            print('Invalid selection.')

    params = await read_remote_params(backend, seq_no=seq_no)

    log.info('Downloading metadata...')
    conn = await download_metadata(
        backend,
        cachepath + ".db",
        params,
    )
    conn.close()

    params.is_mounted = False
    params.needs_fsck = True
    new_seq = max(int(time.time()), backups[-1] + 1)
    params.seq_no = new_seq

    write_params(cachepath, params)

    print(
        'Backup restored into local metadata. Run fsck.s3ql to commit changes to',
        'backend and ensure file system consistency',
        sep='\n',
    )


def main(args: Sequence[str] | None = None) -> None:
    '''Change or show S3QL file system parameters'''

    run_app(app, args, prog_name='s3qladm')


if __name__ == '__main__':
    main(sys.argv[1:])
