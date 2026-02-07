'''
verify.py - this file is part of S3QL.

Copyright © 2014 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import annotations

import argparse
import atexit
import faulthandler
import io
import logging
import os
import signal
import sys
import textwrap
from collections.abc import Sequence
from typing import IO

import trio

from s3ql.types import BackendFactory

from .backends.common import CorruptedObjectError, NoSuchObject
from .common import get_backend_factory, pretty_print_size, sha256_fh
from .database import Connection
from .logging import delay_eval, setup_logging, setup_warnings
from .mount import get_metadata
from .parse_args import ArgumentParser

log: logging.Logger = logging.getLogger(__name__)


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

    parser.add_argument(
        "--parallel", default=4, type=int, help="Number of connections to use in parallel."
    )

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

    trio.run(main_async, options)


async def main_async(options: argparse.Namespace) -> None:
    backend_factory = await get_backend_factory(options)

    # Retrieve metadata
    (_, db) = await get_metadata(await backend_factory(), options.cachepath)

    await retrieve_objects(
        db,
        backend_factory,
        options.corrupted_file,
        options.missing_file,
        worker_count=options.parallel,
        full=options.data,
        offset=options.start_with,
    )

    if options.corrupted_file.tell() or options.missing_file.tell():
        sys.exit(46)
    else:
        os.unlink(options.corrupted_file.name)
        os.unlink(options.missing_file.name)
        sys.exit(0)


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

    log.info('Reading all objects...')

    total_size_raw = db.get_val('SELECT SUM(phys_size) FROM objects WHERE phys_size > 0')
    total_count_raw = db.get_val('SELECT COUNT(id) FROM objects')
    assert total_size_raw is None or isinstance(total_size_raw, int)
    assert total_count_raw is None or isinstance(total_count_raw, int)
    total_size: int = total_size_raw if total_size_raw is not None else 0
    total_count: int = total_count_raw if total_count_raw is not None else 0
    size_acc = 0

    send_channel, receive_channel = trio.open_memory_channel[tuple[int, bytes | None, int]](
        max_buffer_size=worker_count
    )

    async with trio.open_nursery() as nursery:
        for _ in range(worker_count):
            nursery.start_soon(
                _retrieve_loop,
                receive_channel.clone(),
                backend_factory,
                corrupted_fh,
                missing_fh,
                full,
            )
        # Close original receive end — workers each have their own clone
        await receive_channel.aclose()

        sql = 'SELECT id, phys_size, hash, length FROM objects ORDER BY id'
        i = 0  # Make sure this is set if there are zero objects

        # Can't use query_typed because hash_ may be None
        async with send_channel:
            for i, (obj_id, obj_size, hash_, block_size) in enumerate(db.query(sql)):
                assert isinstance(obj_id, int)
                assert isinstance(obj_size, int)
                assert isinstance(block_size, int)

                # TODO: Check if we're handling None correctly here
                assert hash_ is None or isinstance(hash_, bytes)

                i += 1  # start at 1
                extra = {'rate_limit': 1, 'update_console': True, 'is_last': i == total_count}
                if full:
                    log.info(
                        'Checked %d objects (%.2f%%) / %s (%.2f%%)',
                        i,
                        i / total_count * 100 if total_count else 0,
                        delay_eval(pretty_print_size, size_acc),
                        size_acc / total_size * 100 if total_size else 0,
                        extra=extra,
                    )
                else:
                    log.info(
                        'Checked %d objects (%.2f%%)',
                        i,
                        i / total_count * 100 if total_count else 0,
                        extra=extra,
                    )

                size_acc += obj_size
                if i < offset:
                    continue

                await send_channel.send((obj_id, hash_, block_size))

    log.info('Verified all %d storage objects.', i)


async def _retrieve_loop(
    receive_channel: trio.MemoryReceiveChannel[tuple[int, bytes | None, int]],
    backend_factory: BackendFactory,
    corrupted_fh: IO[str],
    missing_fh: IO[str],
    full: bool = False,
) -> None:
    '''Retrieve objects arriving via *receive_channel* from *backend*

    If *full* is False, lookup and read metadata. If *full* is True,
    read entire object.

    Corrupted objects are written into *corrupted_fh*. Missing objects
    are written into *missing_fh*.
    '''

    async with receive_channel, await backend_factory() as backend:
        buf = io.BytesIO()
        async for obj_id, exp_hash, exp_size in receive_channel:
            log.debug('reading object %s', obj_id)
            key = 's3ql_data_%d' % obj_id
            try:
                if full:
                    buf.seek(0)
                    await backend.readinto_fh(key, buf, size_hint=exp_size)
                    buf.truncate()
                else:
                    await backend.lookup(key)
            except NoSuchObject:
                log.warning('Backend seems to have lost object %d', obj_id)
                print(key, file=missing_fh)
                continue
            except CorruptedObjectError:
                log.warning('Object %d is corrupted', obj_id)
                print(key, file=corrupted_fh)
                continue

            if not full:
                continue

            size = buf.tell()
            buf.seek(0)
            hash_ = sha256_fh(buf).digest()

            if exp_size != size:
                log.warning(
                    'Object %d is corrupted (expected size %d, actual size %d)',
                    obj_id,
                    exp_size,
                    size,
                )
                print(key, file=corrupted_fh)
                continue

            if exp_hash != hash_:
                log.warning(
                    'Object %d is corrupted (expected hash %s, got %s)',
                    obj_id,
                    exp_hash,
                    hash_,
                )
                print(key, file=corrupted_fh)
                continue


if __name__ == '__main__':
    main(sys.argv[1:])
