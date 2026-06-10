#!/usr/bin/env python3
'''
pcp.py - this file is part of S3QL.

Parallel, recursive copy of directory trees.

---
Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

import logging
import subprocess
import sys
from collections.abc import Sequence
from typing import Annotated

import typer

from s3ql.logging import setup_logging
from s3ql.parse_args import (
    DebugFlag,
    DebugModules,
    LogDest,
    QuietFlag,
    run_app,
    trio_command,
)

log = logging.getLogger(__name__)

pool = ('abcdefghijklmnopqrstuvwxyz', 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', '0123456789')

app = typer.Typer(add_completion=False, rich_markup_mode=None, pretty_exceptions_enable=False)


@app.command()
@trio_command
async def pcp(
    paths: Annotated[
        list[str],
        typer.Argument(metavar='<source>... <dest>', help='Directories to copy, then the target'),
    ],
    archive: Annotated[bool, typer.Option('-a', help='Pass -aHAX option to rsync.')] = False,
    processes: Annotated[
        int, typer.Option(metavar='<no>', help='Number of rsync processes to use.')
    ] = 10,
    log_target: LogDest = None,
    debug: DebugFlag = False,
    debug_modules: DebugModules = None,
    quiet: QuietFlag = False,
) -> None:
    '''Recursively copy source(s) to destination using multiple parallel rsync processes.'''
    setup_logging(quiet=quiet, log=log_target, debug=debug, debug_modules=debug_modules)

    if len(paths) < 2:
        raise typer.BadParameter('Need at least one source and a destination.')
    if processes < 2 or processes > len(pool[0]) + 1:
        raise typer.BadParameter(
            '%d needs to be between 2 and %d (inclusive)' % (processes, len(pool[0]) + 1)
        )

    steps = [len(x) / (processes - 1) for x in pool]
    prefixes = list()
    for i in range(processes - 1):
        parts = [x[int(i * y) : int((i + 1) * y)] for (x, y) in zip(pool, steps, strict=True)]
        prefixes.append(''.join(parts))

    filters = ['-! [%s]*' % x for x in prefixes]

    # Catch all
    filters.append('- [%s]*' % ''.join(prefixes))

    rsync_args = ['rsync', '-f', '+ */']
    if not quiet:
        rsync_args.append('--out-format')
        rsync_args.append('%n%L')
    if archive:
        rsync_args.append('-aHAX')

    processes_ = list()
    for filter_ in filters:
        cmd = rsync_args + ['-f', filter_] + paths
        log.debug('Calling %s', cmd)
        processes_.append(subprocess.Popen(cmd))

    if all([c.wait() == 0 for c in processes_]):
        sys.exit(0)
    else:
        sys.exit(1)


def main(args: Sequence[str] | None = None) -> None:
    run_app(app, args, prog_name='pcp.py')


if __name__ == '__main__':
    main(sys.argv[1:])
