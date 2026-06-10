'''
verify.py - this file is part of S3QL.

Copyright © 2014 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import annotations

import atexit
import dataclasses
import faulthandler
import io
import logging
import os
import signal
import sys
from collections.abc import AsyncIterable, Iterator, Sequence
from contextlib import AsyncExitStack
from typing import IO, Annotated

import typer

from .authinfo import Authinfo
from .backends import open_backend
from .backends.common import CorruptedObjectError, NoSuchObject
from .backends.comprenc import AsyncComprencBackend
from .common import ParallelPipeline, pretty_print_size, sha256_fh
from .database import Connection
from .logging import delay_eval, setup_logging
from .mount import determine_threads, get_metadata
from .parse_args import (
    DEFAULT_AUTHFILE,
    AuthFile,
    BackendOptions,
    CacheDir,
    DebugFlag,
    DebugModules,
    LogDest,
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


@dataclasses.dataclass
class ObjectToVerify:
    obj_id: int
    exp_hash: bytes
    exp_size: int


def _open_new_file(path: str, encoding: str = 'utf-8') -> IO[str]:
    '''Open *path* for writing, refusing to overwrite an existing non-empty file.'''

    if os.path.exists(path) and os.stat(path).st_size != 0:
        raise typer.BadParameter('File already exists - refusing to overwrite: %s' % path)

    fh = open(path, 'w', encoding=encoding)  # noqa: SIM115
    atexit.register(fh.close)
    return fh


@app.command()
@trio_command
async def verify(
    storage_url: StorageUrl,
    authfile: AuthFile = None,
    cachedir: CacheDir = None,
    backend_options: BackendOptions = None,
    missing_file: Annotated[
        str, typer.Option(metavar='<name>', help='File to store keys of missing objects.')
    ] = 'missing_objects.txt',
    corrupted_file: Annotated[
        str, typer.Option(metavar='<name>', help='File to store keys of corrupted objects.')
    ] = 'corrupted_objects.txt',
    data: Annotated[
        bool,
        typer.Option(
            '--data', help='Read every object completely, instead of checking just the metadata.'
        ),
    ] = False,
    start_with: Annotated[
        int,
        typer.Option(
            metavar='<n>', help='Skip over first <n> objects and start verifying object <n>+1.'
        ),
    ] = 0,
    max_connections: MaxConnections = None,
    max_threads: MaxThreads = None,
    log_target: LogDest = None,
    debug: DebugFlag = False,
    debug_modules: DebugModules = None,
    quiet: QuietFlag = False,
    *,
    stack: AsyncExitStack,
) -> None:
    '''Verify that all data in an S3QL file system can be downloaded from the storage backend.

    In contrast to fsck.s3ql, this program does not trust the object listing returned by the
    backend, but actually attempts to retrieve every object. It therefore takes a lot longer.
    '''
    setup_logging(quiet=quiet, log=log_target, debug=debug, debug_modules=debug_modules)

    authinfo = Authinfo.from_file(pick(authfile, DEFAULT_AUTHFILE), storage_url)
    cachepath = init_cachedir(pick(cachedir, authinfo.cachedir), storage_url)
    max_conns = pick(max_connections, authinfo.max_connections)

    # verify only reads objects, so size the thread pool from the CPU count alone.
    threads = pick(max_threads, authinfo.max_threads) or determine_threads(None)
    AsyncComprencBackend.set_max_threads(threads)

    missing_fh = _open_new_file(missing_file)
    corrupted_fh = _open_new_file(corrupted_file)

    backend = await stack.enter_async_context(
        await open_backend(
            storage_url,
            authinfo,
            backend_options=backend_options,
            max_connections=max_conns,
            compress=authinfo.compress,
        )
    )
    (_, db) = await get_metadata(backend, cachepath)

    await retrieve_objects(
        db,
        backend,
        corrupted_fh,
        missing_fh,
        worker_count=max_conns + threads,
        full=data,
        offset=start_with,
    )

    db.close()

    if corrupted_fh.tell() or missing_fh.tell():
        sys.exit(46)
    else:
        os.unlink(corrupted_fh.name)
        os.unlink(missing_fh.name)


def main(args: Sequence[str] | None = None) -> None:
    faulthandler.enable()
    faulthandler.register(signal.SIGUSR1)
    run_app(app, args, prog_name='s3ql_verify')


@dataclasses.dataclass
class RetrieveObjects(ParallelPipeline[ObjectToVerify]):
    '''Parallel pipeline that fetches every object from the backend.

    Workers share a single *backend* instance whose connection pool caps wire concurrency. If *full*
    is False, only object metadata is looked up; if True, the full object is read and its hash is
    compared to the database. Corrupted object keys are written into *corrupted_fh*, missing ones
    into *missing_fh*.
    '''

    db: Connection
    backend: AsyncComprencBackend
    corrupted_fh: IO[str]
    missing_fh: IO[str]
    full: bool = False
    offset: int = 0

    # Populated in compute_totals().
    total_count: int = dataclasses.field(init=False, default=0)
    total_size: int = dataclasses.field(init=False, default=0)

    def compute_totals(self) -> None:
        log.info('Reading all objects...')
        self.total_size = (
            self.db.get_int_val_or_none('SELECT SUM(phys_size) FROM objects WHERE phys_size > 0')
            or 0
        )
        self.total_count = self.db.get_int_val_or_none('SELECT COUNT(id) FROM objects') or 0

    async def produce(self) -> Iterator[tuple[int, bytes, int]]:
        self.compute_totals()
        return self._iter_objects()

    def _iter_objects(self) -> Iterator[tuple[int, bytes, int]]:
        sql = 'SELECT id, phys_size, hash, length FROM objects ORDER BY id'
        size_acc = 0
        # Can't use query_typed because hash_ may be None.
        for i, (obj_id, obj_size, hash_, block_size) in enumerate(self.db.query(sql), start=1):
            assert isinstance(obj_id, int)
            assert isinstance(obj_size, int)
            assert isinstance(block_size, int)

            if hash_ is None:
                raise RuntimeError(
                    f'Object {obj_id} has NULL hash (likely failed upload), run '
                    'fsck.s3ql to fix this before verifying data integrity.',
                )
            assert isinstance(hash_, bytes)

            extra = {
                'rate_limit': 1,
                'update_console': True,
                'is_last': i == self.total_count,
            }
            if self.full:
                log.info(
                    'Checked %d objects (%.2f%%) / %s (%.2f%%)',
                    i,
                    i / self.total_count * 100 if self.total_count else 0,
                    delay_eval(pretty_print_size, size_acc),
                    size_acc / self.total_size * 100 if self.total_size else 0,
                    extra=extra,
                )
            else:
                log.info(
                    'Checked %d objects (%.2f%%)',
                    i,
                    i / self.total_count * 100 if self.total_count else 0,
                    extra=extra,
                )

            size_acc += obj_size
            if i <= self.offset:
                continue
            yield ObjectToVerify(obj_id=obj_id, exp_hash=hash_, exp_size=block_size)

    async def consume(self, jobs: AsyncIterable[ObjectToVerify]) -> None:
        buf = io.BytesIO()
        async for obj in jobs:
            log.debug('reading object %s', obj.obj_id)
            key = 's3ql_data_%d' % obj.obj_id
            try:
                if self.full:
                    buf.seek(0)
                    await self.backend.readinto_fh(key, buf, size_hint=obj.exp_size)
                    buf.truncate()
                else:
                    await self.backend.lookup(key)
            except NoSuchObject:
                log.warning('Backend seems to have lost object %d', obj.obj_id)
                print(key, file=self.missing_fh)
                continue
            except CorruptedObjectError:
                log.warning('Object %d is corrupted', obj.obj_id)
                print(key, file=self.corrupted_fh)
                continue

            if not self.full:
                continue

            size = buf.tell()
            buf.seek(0)
            hash_ = sha256_fh(buf).digest()

            if obj.exp_size != size:
                log.warning(
                    'Object %d is corrupted (expected size %d, actual size %d)',
                    obj.obj_id,
                    obj.exp_size,
                    size,
                )
                print(key, file=self.corrupted_fh)
                continue

            if obj.exp_hash != hash_:
                log.warning(
                    'Object %d is corrupted (expected hash %s, got %s)',
                    obj.obj_id,
                    obj.exp_hash,
                    hash_,
                )
                print(key, file=self.corrupted_fh)
                continue

    def finalize(self) -> None:
        log.info('Verified all %d storage objects.', self.total_count)


async def retrieve_objects(
    db: Connection,
    backend: AsyncComprencBackend,
    corrupted_fh: IO[str],
    missing_fh: IO[str],
    worker_count: int = 1,
    full: bool = False,
    offset: int = 0,
) -> None:
    """Attempt to retrieve every object"""

    op = RetrieveObjects(
        db=db,
        backend=backend,
        corrupted_fh=corrupted_fh,
        missing_fh=missing_fh,
        full=full,
        offset=offset,
    )
    await op.run(n_workers=worker_count, buffer_size=worker_count)


if __name__ == '__main__':
    main(sys.argv[1:])
