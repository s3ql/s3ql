#!/usr/bin/env python3
'''
benchmark.py - this file is part of S3QL.

Benchmark compression and upload performance and recommend compression
algorithm that maximizes throughput.

---
Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

import io
import logging
import os
import shutil
import subprocess
import sys
import tempfile
import time
from collections.abc import Sequence
from contextlib import AsyncExitStack, ExitStack
from typing import Annotated, Any, BinaryIO

import typer

from s3ql import BUFSIZE
from s3ql.authinfo import Authinfo
from s3ql.backends import open_raw_backend
from s3ql.backends.comprenc import AsyncComprencBackend
from s3ql.logging import setup_logging
from s3ql.parse_args import (
    DEFAULT_AUTHFILE,
    DEFAULT_CACHEDIR,
    AuthFile,
    BackendOptions,
    CacheDir,
    DebugFlag,
    DebugModules,
    LogDest,
    QuietFlag,
    StorageUrl,
    pick,
    run_app,
    trio_command,
)

ALGS = ('lzma', 'bzip2', 'zlib')

log = logging.getLogger(__name__)

app = typer.Typer(add_completion=False, rich_markup_mode=None, pretty_exceptions_enable=False)


def test_write_speed(size, blocksize, cachedir, rnd_fh):
    with ExitStack() as mgr:
        mnt_dir = tempfile.mkdtemp(prefix='s3ql-mnt')
        mgr.callback(shutil.rmtree, mnt_dir)

        backend_dir = tempfile.mkdtemp(prefix='s3ql-benchmark-')
        mgr.callback(shutil.rmtree, backend_dir)

        subprocess.check_call(
            [
                'mkfs.s3ql',
                '--plain',
                'local://%s' % backend_dir,
                '--quiet',
                '--cachedir',
                cachedir,
            ]
        )
        subprocess.check_call(
            [
                'mount.s3ql',
                '--threads',
                '1',
                '--quiet',
                '--cachesize',
                '%d' % (2 * size / 1024),
                '--log',
                '%s/mount.log' % backend_dir,
                '--cachedir',
                cachedir,
                'local://%s' % backend_dir,
                mnt_dir,
            ]
        )
        try:
            write_time = 0
            while write_time < 3:
                log.debug('Write took %.3g seconds, retrying', write_time)
                size *= 2
                with open('%s/bigfile' % mnt_dir, 'wb', 0) as dst:
                    rnd_fh.seek(0)
                    write_time = time.time()
                    copied = 0
                    while copied < size:
                        buf = rnd_fh.read(blocksize)
                        if not buf:
                            rnd_fh.seek(0)
                            continue
                        dst.write(buf)
                        copied += len(buf)
                write_time = time.time() - write_time
                os.unlink('%s/bigfile' % mnt_dir)
        finally:
            subprocess.check_call(['umount.s3ql', mnt_dir])

    return copied / write_time


class AsyncMockBackend:
    """Minimal async backend stub for compression benchmarking."""

    has_delete_multi = False

    async def write_fh(
        self,
        key: str,
        fh: BinaryIO,
        len_: int,
        metadata: dict[str, Any] | None = None,
    ) -> int:
        cur_off = fh.tell()
        fh.seek(cur_off + len_)
        return len_

    def is_temp_failure(self, exc: BaseException) -> bool:
        return False


@app.command()
@trio_command
async def benchmark(
    storage_url: StorageUrl,
    file: Annotated[str, typer.Argument(metavar='<file>', help='File to transfer')],
    authfile: AuthFile = None,
    cachedir: CacheDir = DEFAULT_CACHEDIR,
    backend_options: BackendOptions = None,
    threads: Annotated[
        int | None,
        typer.Option(metavar='<n>', help='Also include statistics for <n> threads in results.'),
    ] = None,
    log_target: LogDest = None,
    debug: DebugFlag = False,
    debug_modules: DebugModules = None,
    quiet: QuietFlag = False,
    *,
    stack: AsyncExitStack,
) -> None:
    '''Measure S3QL write performance, uplink bandwidth and compression speed.'''
    setup_logging(quiet=quiet, log=log_target, debug=debug, debug_modules=debug_modules)

    authinfo = Authinfo.from_file(pick(authfile, DEFAULT_AUTHFILE), storage_url)
    backend = await stack.enter_async_context(
        await open_raw_backend(
            storage_url, authinfo, backend_options=backend_options, max_connections=1
        )
    )

    # /dev/urandom may be slow, so we cache the data first
    log.info('Preparing test data...')
    rnd_fh = tempfile.TemporaryFile()  # noqa: SIM115
    with open('/dev/urandom', 'rb', 0) as src:
        copied = 0
        while copied < 50 * 1024 * 1024:
            buf = src.read(BUFSIZE)
            rnd_fh.write(buf)
            copied += len(buf)
    rnd_fh_size = copied

    log.info('Measuring throughput to cache...')
    block_sizes = [2**b for b in range(8, 18)]
    for blocksize in block_sizes:
        size = 50 * 1024 * 1024
        fuse_speed = test_write_speed(size, blocksize, cachedir, rnd_fh)
        log.info(
            'Cache throughput with %4.1f kB blocks: %d KiB/sec', blocksize / 1024, fuse_speed / 1024
        )

    # Upload random data to prevent effects of compression
    # on the network layer
    log.info('Measuring raw backend throughput..')
    upload_time: float = 0
    size = 512 * 1024
    while upload_time < 10 and size <= (rnd_fh_size / 2):
        size *= 2
        rnd_fh.seek(0)
        upload_fh = io.BytesIO(rnd_fh.read(size))
        stamp = time.time()
        upload_size = await backend.write_fh('s3ql_testdata', upload_fh)
        upload_time = time.time() - stamp
        assert upload_time > 0, 'Upload took 0 seconds'
    backend_speed = upload_size / upload_time
    log.info('Backend throughput: %d KiB/sec', backend_speed / 1024)
    await backend.delete('s3ql_testdata')

    src = stack.enter_context(open(file, 'rb'))  # noqa: SIM115
    size = os.fstat(src.fileno()).st_size
    log.info('Test file size: %.2f MiB', (size / 1024**2))

    in_speed = dict()
    out_speed = dict()
    for alg in ALGS:
        log.info('compressing with %s-6...', alg)
        compr_backend = await AsyncComprencBackend.create(
            b'pass',
            (alg, 6),
            AsyncMockBackend(),  # type: ignore[arg-type]
        )

        src.seek(0)
        stamp = time.time()
        obj_size = await compr_backend.write_fh('s3ql_testdata', src, size)
        dt = time.time() - stamp
        in_speed[alg] = size / dt
        out_speed[alg] = obj_size / dt
        log.info(
            '%s test file compression ratio: %3.1f %% of original', alg, (obj_size / size) * 100
        )
        log.info('%s compression speed: %d KiB/sec per thread (in)', alg, in_speed[alg] / 1024)
        log.info('%s compression speed: %d KiB/sec per thread (out)', alg, out_speed[alg] / 1024)

    print(
        '',
        'With %d KiB blocks, maximum performance for different compression'
        % (block_sizes[-1] / 1024),
        'algorithms and thread counts is:',
        '',
        sep='\n',
    )

    thread_counts = {1, 2, 4, 8}
    cores = os.sysconf('SC_NPROCESSORS_ONLN')
    if cores != -1:
        thread_counts.add(cores)
    if threads is not None:
        thread_counts.add(threads)

    print('%-26s' % 'Threads:', ('%16d' * len(thread_counts)) % tuple(sorted(thread_counts)))

    for alg in ALGS:
        speeds = []
        limits = []
        for t in sorted(thread_counts):
            if fuse_speed > t * in_speed[alg]:
                limit = 'CPU'
                speed = t * in_speed[alg]
            else:
                limit = 'S3QL/FUSE'
                speed = fuse_speed

            if speed / in_speed[alg] * out_speed[alg] > backend_speed:
                limit = 'uplink'
                speed = backend_speed * in_speed[alg] / out_speed[alg]

            limits.append(limit)
            speeds.append(speed / 1024)

        print(
            '%-26s' % ('Max FS throughput (%s):' % alg),
            ('%10d KiB/s' * len(thread_counts)) % tuple(speeds),
        )
        print('%-26s' % '..limited by:', ('%16s' * len(thread_counts)) % tuple(limits))

    print('')
    print(
        'All numbers assume that the test file is representative and that',
        'there are enough processor cores to run all active threads in parallel.',
        'To compensate for network latency, you should use about twice as',
        'many upload threads as indicated by the above table.\n',
        sep='\n',
    )


def main(args: Sequence[str] | None = None) -> None:
    run_app(app, args, prog_name='benchmark.py')


if __name__ == '__main__':
    main(sys.argv[1:])
