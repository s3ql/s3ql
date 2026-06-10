'''
parse_args.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.

This module provides the shared building blocks for S3QL's Typer-based command-line entry points.
'''

from __future__ import annotations

import functools
import inspect
import logging
import os
import re
import stat
import sys
from collections.abc import Awaitable, Callable, Sequence
from contextlib import AsyncExitStack
from typing import Annotated, Any, Literal, ParamSpec

import trio
import typer

# Re-exported so entry points can keep importing them from `parse_args`; `DEFAULT_AUTHFILE`,
# `DEFAULT_CACHEDIR`, and `DEFAULT_MAX_CONNECTIONS` are also used below in option help text.
from .authinfo import DEFAULT_AUTHFILE, DEFAULT_CACHEDIR, DEFAULT_MAX_CONNECTIONS
from .authinfo import pick as pick
from .common import escape
from .logging import QuietError, setup_warnings

log: logging.Logger = logging.getLogger(__name__)

P = ParamSpec('P')


def trio_command(fn: Callable[P, Awaitable[None]]) -> Callable[P, None]:
    '''Adapt an `async def` Typer command so it runs under `trio.run`.

    Typer inspects a command's signature via `inspect.signature`, which follows the
    `__wrapped__` attribute set by `functools.wraps`. The async function's full
    `Annotated[...]` signature is therefore visible to Typer even though the wrapper
    itself is synchronous, so command parameters need to be declared only once.

    A command that manages resources may declare a keyword-only `stack: AsyncExitStack`
    parameter. The decorator then keeps an `AsyncExitStack` open for the duration of the
    call and passes it in, so the command can register cleanups with `enter_async_context`
    instead of nesting its body in a `try`/`finally`. This parameter is stripped from the
    signature Typer inspects, so it does not turn into a command-line option.
    '''
    sig = inspect.signature(fn, eval_str=True)
    wants_stack = 'stack' in sig.parameters

    @functools.wraps(fn)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> None:
        async def _entry() -> None:
            if wants_stack:
                async with AsyncExitStack() as stack:
                    # The `stack` parameter is part of *fn*'s signature but hidden from the
                    # ParamSpec-typed wrapper, so this injected argument cannot be expressed.
                    await fn(*args, stack=stack, **kwargs)  # ty: ignore[invalid-argument-type]
            else:
                await fn(*args, **kwargs)

        trio.run(_entry)

    if wants_stack:
        visible = [p for name, p in sig.parameters.items() if name != 'stack']
        # Override the signature and annotations Typer sees so the injected `stack` never
        # surfaces as a CLI option, while the remaining parameters keep their resolved
        # `Annotated[...]` types.
        wrapper.__signature__ = sig.replace(parameters=visible)  # ty: ignore[unresolved-attribute]
        wrapper.__annotations__ = {
            name: p.annotation
            for name, p in sig.parameters.items()
            if name != 'stack' and p.annotation is not inspect.Parameter.empty
        }

    return wrapper


def run_app(app: typer.Typer, args: Sequence[str] | None, prog_name: str) -> None:
    '''Run a Typer *app* as an entry point.

    Unlike calling *app* directly, this returns normally when the command succeeds instead
    of raising `SystemExit(0)`. Non-zero exits (argument errors, explicit failures)
    still propagate so the process exits with the right status.
    '''
    setup_warnings()
    try:
        app(args if args is not None else sys.argv[1:], prog_name=prog_name)
    except SystemExit as exc:
        if exc.code:
            raise


def make_app(**kwargs: Any) -> typer.Typer:
    '''Construct a Typer app with S3QL's standard settings.

    Rich `--help` formatting (colored, boxed) is used interactively but suppressed while the
    documentation is built (when `S3QL_BUILDING_DOCS` is set in the environment), so that
    captured help output stays plain text for inclusion in the Sphinx sources. Additional
    keyword arguments are forwarded to `typer.Typer`.
    '''
    rich_markup_mode: Literal['rich'] | None = (
        None if os.environ.get('S3QL_BUILDING_DOCS') else 'rich'
    )
    kwargs.setdefault('pretty_exceptions_enable', False)
    return typer.Typer(rich_markup_mode=rich_markup_mode, **kwargs)


# Shared parameter type aliases. Defining the help text once here keeps it consistent
# across all entry points that accept the same option.
LogDest = Annotated[
    str | None,
    typer.Option(
        '--log',
        metavar='<target>',
        help="Destination for log messages. Specify 'none' for standard output or 'syslog' "
        "for the system logging daemon. Anything else is interpreted as a file name. Log files "
        "are rotated when they reach 1 MiB, keeping at most 5 old files.",
    ),
]
DebugModules = Annotated[
    list[str] | None,
    typer.Option(
        metavar='<module>',
        help="Activate debugging output from the named module. May be given multiple times; "
        "use 'all' for everything. Debug messages are written to the destination given by --log.",
    ),
]
DebugFlag = Annotated[
    bool,
    typer.Option(
        '--debug',
        help="Activate debugging output from all S3QL modules. Debug messages are written to "
        "the destination given by --log.",
    ),
]
QuietFlag = Annotated[bool, typer.Option('--quiet', help="be really quiet")]


def _canonicalize_storage_url(value: str) -> str:
    '''Typer callback that validates and canonicalizes a storage URL.'''
    if not re.match(r'^([a-zA-Z0-9]+)://(.+)$', value):
        raise typer.BadParameter('%s is not a valid storage url.' % value)
    if value.startswith('local://'):
        # Append trailing slash so we can match patterns with a trailing slash in authinfo2.
        return 'local://%s/' % os.path.abspath(value[len('local://') :])
    # Normalise prefix-less URLs so that e.g. s3://foo and s3://foo/ share a cache directory.
    if re.match(r'^(s3|gs)://[^/]+$', value) or re.match(
        r'^(s3c|s3c4|swift(ks)?|rackspace)://[^/]+/[^/]+$', value
    ):
        value += '/'
    return value


# Shared parameter type aliases for entry points that operate on a storage backend. The backend
# options carry `None` sentinels so that "unset" is distinguishable from an explicit value (see
# `pick`); their real defaults are spelled out in the help text rather than shown by Typer.
StorageUrl = Annotated[
    str,
    typer.Argument(
        metavar='<storage-url>',
        help='Storage URL of the backend that contains the file system',
        callback=_canonicalize_storage_url,
    ),
]
AuthFile = Annotated[
    str | None,
    typer.Option(
        metavar='<path>',
        show_default=False,
        help='Read authentication credentials from this file (default: %s).' % DEFAULT_AUTHFILE,
    ),
]
BackendOptions = Annotated[
    str | None,
    typer.Option(
        metavar='<options>',
        show_default=False,
        help='Backend specific options (separate by commas). See backend documentation for '
        'available options.',
    ),
]
CacheDir = Annotated[
    str | None,
    typer.Option(
        metavar='<path>',
        show_default=False,
        help='Store cached data in this directory (default: %s).' % DEFAULT_CACHEDIR,
    ),
]
MaxConnections = Annotated[
    int | None,
    typer.Option(
        metavar='<no>',
        show_default=False,
        help='Maximum number of concurrent backend I/O operations (default: %d). This is '
        'independent of --max-threads, which governs compression.' % DEFAULT_MAX_CONNECTIONS,
    ),
]
MaxThreads = Annotated[
    int | None,
    typer.Option(
        metavar='<no>',
        show_default=False,
        help='Number of parallel compression/encryption threads to use (default: auto). This '
        'bounds CPU parallelism only and is independent of --max-connections.',
    ),
]
Compress = Annotated[
    str | None,
    typer.Option(
        '--compress',
        metavar='<algorithm-lvl>',
        show_default=False,
        help='Compression algorithm and level to use when storing new data. *algorithm* may be '
        'any of `lzma`, `bzip2`, `zlib`, or none. *lvl* may be any integer from 0 (fastest) to '
        '9 (slowest). Default: lzma-6.',
    ),
]


def init_cachedir(cachedir: str, storage_url: str) -> str:
    '''Ensure *cachedir* exists and return the cache path for *storage_url*.

    Creates *cachedir* if it is missing, points `SQLITE_TMPDIR` at it, and returns the
    per-file-system cache path. Raises `QuietError` if the directory cannot be created or
    accessed.
    '''
    if not os.path.exists(cachedir):
        try:
            os.mkdir(cachedir, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
        except PermissionError:
            raise QuietError('No permission to create cache directory ' + cachedir, exitcode=45)

    if not os.access(cachedir, os.R_OK | os.W_OK | os.X_OK):
        raise QuietError('No permission to access cache directory ' + cachedir, exitcode=45)

    cachedir = os.path.abspath(cachedir)
    os.environ['SQLITE_TMPDIR'] = cachedir
    return os.path.join(cachedir, escape(storage_url))
