'''
verify.py - this file is part of S3QL.

Copyright © 2014 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import annotations

import argparse
import atexit
import dataclasses
import faulthandler
import io
import logging
import os
import signal
import sys
import textwrap
from collections.abc import AsyncIterable, Iterator, Sequence
from typing import IO

import trio

from s3ql.types import BackendFactory

from .backends.common import CorruptedObjectError, NoSuchObject
from .backends.comprenc import AsyncComprencBackend
from .backends.pool import BackendPool
from .common import ParallelPipeline, get_backend_factory, pretty_print_size, sha256_fh
from .database import Connection
from .logging import delay_eval, setup_logging, setup_warnings
from .mount import determine_threads, get_metadata
from .parse_args import ArgumentParser

log: logging.Logger = logging.getLogger(__name__)


@dataclasses.dataclass
class ObjectToVerify:
    obj_id: int
    exp_hash: bytes
    exp_size: int


def _new_file_type(s: str, encoding: str = 'utf-8') -> IO[str]:
    '''An argparse type for a file that does not yet exist'''

    if os.path.exists(s) and os.stat(s).st_size != 0:
        msg = 'File already exists - refusing to overwrite: %s' % s
        raise argparse.ArgumentTypeError(msg)

    fh = open(s, 'w', encoding=encoding)  # noqa: SIM115
    atexit.register(fh.close)
    return fh


def parse_args(args: Sequence[str]) -> argparse.Namespace:
    '''Parse command line'''

    parser = ArgumentParser(
        description=textwrap.dedent(
            '''\
        Verifies that all data in an S3QL file system can be downloaded
        from the storage backend.

        In contrast to fsck.s3ql, this program does not trust the object
        listing returned by the backend, but actually attempts to retrieve
        every object. It therefore takes a lot longer.
        '''
        )
    )

    parser.add_log()
    parser.add_debug()
    parser.add_quiet()
    parser.add_version()
    parser.add_cachedir()
    parser.add_backend_options()
    parser.add_storage_url()

    parser.add_argument(
        "--missing-file",
        type=_new_file_type,
        metavar='<name>',
        default='missing_objects.txt',
        help="File to store keys of missing objects.",
    )

    parser.add_argument(
        "--corrupted-file",
        type=_new_file_type,
        metavar='<name>',
        default='corrupted_objects.txt',
        help="File to store keys of corrupted objects.",
    )

    parser.add_argument(
        "--data",
        action="store_true",
        default=False,
        help="Read every object completely, instead of checking just the metadata.",
    )

    parser.add_max_threads()
    parser.add_max_connections()

    parser.add_argument(
        "--start-with",
        default=0,
        type=int,
        metavar='<n>',
        help="Skip over first <n> objects and with verifying object <n>+1.",
    )

    options = parser.parse_args(args)

    return options


def main(args: Sequence[str] | None = None) -> None:
    faulthandler.enable()
    faulthandler.register(signal.SIGUSR1)

    if args is None:
        args = sys.argv[1:]

    setup_warnings()
    options = parse_args(args)
    setup_logging(options)

    if options.max_threads is None:
        options.max_threads = determine_threads(None)
    AsyncComprencBackend.set_max_threads(options.max_threads)

    trio.run(main_async, options)


async def main_async(options: argparse.Namespace) -> None:
    backend_factory = await get_backend_factory(options)
    backend_pool = BackendPool(backend_factory, options.max_connections)
    try:
        # Retrieve metadata
        (_, db) = await get_metadata(backend_pool, options.cachepath)

        await retrieve_objects(
            db,
            backend_factory,
            options.corrupted_file,
            options.missing_file,
            worker_count=options.max_connections,
            full=options.data,
            offset=options.start_with,
        )

        db.close()
    finally:
        await backend_pool.flush()

    if options.corrupted_file.tell() or options.missing_file.tell():
        sys.exit(46)
    else:
        os.unlink(options.corrupted_file.name)
        os.unlink(options.missing_file.name)
        sys.exit(0)


@dataclasses.dataclass
class RetrieveObjects(ParallelPipeline[ObjectToVerify]):
    '''Parallel pipeline that fetches every object from the backend.

    Each worker holds a backend connection for its full lifetime via
    *backend_factory* (the verify path does not use a `BackendPool`). If
    *full* is False, only object metadata is looked up; if True, the full
    object is read and its hash is compared to the database. Corrupted
    object keys are written into *corrupted_fh*, missing ones into
    *missing_fh*.
    '''

    db: Connection
    backend_factory: BackendFactory
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
        async with await self.backend_factory() as backend:
            buf = io.BytesIO()
            async for obj in jobs:
                log.debug('reading object %s', obj.obj_id)
                key = 's3ql_data_%d' % obj.obj_id
                try:
                    if self.full:
                        buf.seek(0)
                        await backend.readinto_fh(key, buf, size_hint=obj.exp_size)
                        buf.truncate()
                    else:
                        await backend.lookup(key)
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
    backend_factory: BackendFactory,
    corrupted_fh: IO[str],
    missing_fh: IO[str],
    worker_count: int = 1,
    full: bool = False,
    offset: int = 0,
) -> None:
    """Attempt to retrieve every object"""

    op = RetrieveObjects(
        db=db,
        backend_factory=backend_factory,
        corrupted_fh=corrupted_fh,
        missing_fh=missing_fh,
        full=full,
        offset=offset,
    )
    await op.run(n_workers=worker_count, buffer_size=worker_count)


if __name__ == '__main__':
    main(sys.argv[1:])
