'''
statfs.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import annotations

import logging
import struct
import sys
from collections.abc import Sequence
from typing import Annotated

import pyfuse3
import typer

from .common import assert_fs_owner, pretty_print_size
from .logging import setup_logging
from .parse_args import (
    DebugFlag,
    DebugModules,
    LogDest,
    QuietFlag,
    make_app,
    run_app,
    trio_command,
)

log: logging.Logger = logging.getLogger(__name__)

app = make_app()


@app.command()
@trio_command
async def statfs(
    mountpoint: Annotated[
        str,
        typer.Argument(metavar='<mountpoint>', help='Mount point of the file system to examine'),
    ],
    raw: Annotated[bool, typer.Option('--raw', help='Do not pretty-print numbers')] = False,
    log_target: LogDest = None,
    debug: DebugFlag = False,
    debug_modules: DebugModules = None,
    quiet: QuietFlag = False,
) -> None:
    '''Print file system statistics.'''
    setup_logging(quiet=quiet, log=log_target, debug=debug, debug_modules=debug_modules)

    mountpoint = mountpoint.rstrip('/')

    if raw:

        def pprint(bytes_: int) -> str:
            return '%d bytes' % bytes_
    else:
        pprint = pretty_print_size

    ctrlfile = assert_fs_owner(mountpoint, mountpoint=True)

    # Use a decent sized buffer, otherwise the statistics have to be
    # calculated three(!) times because we need to invoke getxattr
    # three times.
    buf = pyfuse3.getxattr(ctrlfile, 's3qlstat', size_guess=256)

    (
        entries,
        objects,
        inodes,
        fs_size,
        dedup_size,
        compr_size,
        db_size,
        cache_cnt,
        cache_size,
        dirty_cnt,
        dirty_size,
        removal_cnt,
    ) = struct.unpack('QQQQQQQQQQQQ', buf)
    p_dedup = dedup_size * 100 / fs_size if fs_size else 0
    p_compr_1 = compr_size * 100 / fs_size if fs_size else 0
    p_compr_2 = compr_size * 100 / dedup_size if dedup_size else 0
    print(
        'Directory entries:    %d' % entries,
        'Inodes:               %d' % inodes,
        'Data objects:         %d' % objects,
        'Total data size:      %s' % pprint(fs_size),
        'After de-duplication: %s (%.2f%% of total)' % (pprint(dedup_size), p_dedup),
        'After compression:    %s (%.2f%% of total, %.2f%% of de-duplicated)'
        % (pprint(compr_size), p_compr_1, p_compr_2),
        'Database size:        %s (uncompressed)' % pprint(db_size),
        'Cache size:           %s, %d entries' % (pprint(cache_size), cache_cnt),
        'Cache size (dirty):   %s, %d entries' % (pprint(dirty_size), dirty_cnt),
        'Queued object removals: %d' % (removal_cnt,),
        sep='\n',
    )


def main(args: Sequence[str] | None = None) -> None:
    '''Print file system statistics to sys.stdout'''

    run_app(app, args, prog_name='s3qlstat')


if __name__ == '__main__':
    main(sys.argv[1:])
