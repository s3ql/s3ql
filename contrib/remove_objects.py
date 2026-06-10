#!/usr/bin/env python3
'''
remove_objects.py - this file is part of S3QL.

Copyright © 2014 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

import logging
import sys
from collections.abc import Sequence
from contextlib import AsyncExitStack
from typing import Annotated

import typer

from s3ql.authinfo import Authinfo
from s3ql.backends import open_raw_backend
from s3ql.logging import setup_logging
from s3ql.parse_args import (
    DEFAULT_AUTHFILE,
    AuthFile,
    BackendOptions,
    DebugFlag,
    DebugModules,
    LogDest,
    QuietFlag,
    StorageUrl,
    pick,
    run_app,
    trio_command,
)

log = logging.getLogger(__name__)

app = typer.Typer(add_completion=False, rich_markup_mode=None, pretty_exceptions_enable=False)


@app.command()
@trio_command
async def remove_objects(
    storage_url: StorageUrl,
    file: Annotated[
        str,
        typer.Argument(metavar='<file>', help='File with newline separated object keys to delete'),
    ],
    authfile: AuthFile = None,
    backend_options: BackendOptions = None,
    log_target: LogDest = None,
    debug: DebugFlag = False,
    debug_modules: DebugModules = None,
    quiet: QuietFlag = False,
    *,
    stack: AsyncExitStack,
) -> None:
    '''Batch remove objects from an S3QL backend.'''
    setup_logging(quiet=quiet, log=log_target, debug=debug, debug_modules=debug_modules)

    authinfo = Authinfo.from_file(pick(authfile, DEFAULT_AUTHFILE), storage_url)
    backend = await stack.enter_async_context(
        await open_raw_backend(
            storage_url, authinfo, backend_options=backend_options, max_connections=1
        )
    )
    fh = stack.enter_context(open(file, encoding='utf-8'))  # noqa: SIM115
    for line in fh:
        key = line.rstrip()
        log.info('Deleting %s', key)
        await backend.delete(key)


def main(args: Sequence[str] | None = None) -> None:
    run_app(app, args, prog_name='remove_objects.py')


if __name__ == '__main__':
    main(sys.argv[1:])
