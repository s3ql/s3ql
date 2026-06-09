'''
authinfo.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.

Configuration-file and compression primitives shared by the command-line layer (`parse_args`)
and the storage backends (`backends`).
'''

from __future__ import annotations

import configparser
import dataclasses
import logging
import os
import re
import stat
import sys
from enum import Enum
from getpass import getpass
from typing import TypeVar

from pydantic import ConfigDict, Field, TypeAdapter, ValidationError, field_validator
from pydantic.dataclasses import dataclass as pydantic_dataclass

from .logging import QuietError

log: logging.Logger = logging.getLogger(__name__)

T = TypeVar('T')

# Default locations, computed once at import time. Defined here (rather than in `parse_args`) to
# avoid circular imports (authinfo.py needs to set the default values, parse_args.py needs to
# interpolate them into --help output).
DEFAULT_AUTHFILE = os.path.expanduser('~/.s3ql/authinfo2')
DEFAULT_CACHEDIR = os.path.expanduser('~/.s3ql')

# Default number of concurrent backend I/O operations. Defined here for the same reason as the
# paths above: `parse_args` interpolates it into --help output, while the value itself is applied
# as the `max_connections` default in `AuthInfo` and `BackendConfig`.
DEFAULT_MAX_CONNECTIONS = 32


class CompressAlgorithm(Enum):
    '''A compression algorithm understood by the CLI and configuration layer.

    The member value is the lowercase token used on the command line and in authinfo2; the
    uppercase object-metadata form stays inside `comprenc`.
    '''

    LZMA = 'lzma'
    BZIP2 = 'bzip2'
    ZLIB = 'zlib'
    NONE = 'none'


@dataclasses.dataclass(frozen=True)
class CompressSpec:
    '''A compression *algorithm* together with its *level*.'''

    algorithm: CompressAlgorithm
    level: int

    def to_comprenc(self) -> tuple[str | None, int]:
        '''Return the `(algorithm, level)` tuple accepted by `AsyncComprencBackend.create`.

        `CompressAlgorithm.NONE` maps to a `None` algorithm, matching comprenc's convention that
        the absence of compression is represented by `None` rather than a string.
        '''
        if self.algorithm is CompressAlgorithm.NONE:
            return (None, self.level)
        return (self.algorithm.value, self.level)

    @classmethod
    def parse(cls, value: str) -> CompressSpec:
        '''Parse a compression string such as `lzma-6` into a `CompressSpec`.

        A bare algorithm defaults to level 6. A malformed specification or an unknown algorithm
        raises `QuietError` with exit code 2 (a usage-level error).
        '''
        hit = re.match(r'^([a-z0-9]+)(?:-([0-9]))?$', value)
        if not hit:
            raise QuietError('%s is not a valid compression specification' % value, exitcode=2)
        try:
            algorithm = CompressAlgorithm(hit.group(1))
        except ValueError:
            raise QuietError('Invalid compression algorithm: %s' % hit.group(1), exitcode=2)
        level = 6 if hit.group(2) is None else int(hit.group(2))
        return cls(algorithm=algorithm, level=level)


@pydantic_dataclass(config=ConfigDict(extra='forbid', populate_by_name=True))
class Authinfo:
    '''Parsed authinfo2 settings for a single storage URL, with defaults applied.

    The credentials and every command option that authinfo2 may override are represented as typed
    fields. The authinfo2 keys use hyphens (`backend-login`, `max-connections`, ...); they are
    mapped to the underscore field names via field aliases. Unrecognized keys are rejected.
    '''

    backend_login: str | None = Field(default=None, alias='backend-login')
    backend_password: str | None = Field(default=None, alias='backend-password')
    fs_passphrase: str | None = Field(default=None, alias='fs-passphrase')
    backend_options: str = Field(default='', alias='backend-options')
    cachedir: str = DEFAULT_CACHEDIR
    max_connections: int = Field(default=DEFAULT_MAX_CONNECTIONS, alias='max-connections')
    max_threads: int | None = Field(default=None, alias='max-threads')
    compress: CompressSpec = Field(default_factory=lambda: CompressSpec.parse('lzma-6'))

    @field_validator('compress', mode='before')
    @classmethod
    def _parse_compress(cls, value: object) -> object:
        '''Parse a compression string from authinfo2 into a `CompressSpec`.

        A malformed value raises `QuietError` (exit 2) directly from `CompressSpec.parse`.
        '''
        if isinstance(value, str):
            return CompressSpec.parse(value)
        return value

    @classmethod
    def from_file(cls, authfile: str, storage_url: str) -> Authinfo:
        '''Return the parsed authinfo2 settings that apply to *storage_url*.

        The file at *authfile* consists of sections, each carrying a `storage-url` prefix; every
        section whose prefix matches *storage_url* contributes its entries, with later sections
        taking precedence. Insecure file permissions raise `QuietError` (exit 12); an unrecognized
        key or a malformed value (including a bad `compress` specification) raises `QuietError`
        (exit 2).
        '''
        ini_config = configparser.ConfigParser()
        if os.path.isfile(authfile):
            mode = os.stat(authfile).st_mode
            if mode & (stat.S_IRGRP | stat.S_IROTH):
                raise QuietError('%s has insecure permissions, aborting.' % authfile, exitcode=12)
            ini_config.read(authfile)

        merged: dict[str, str] = {}
        for section in ini_config.sections():
            pattern = ini_config[section].get('storage-url', None)
            if not pattern or not storage_url.startswith(pattern):
                continue
            for key, val in ini_config[section].items():
                if key != 'storage-url':
                    merged[key] = val

        log.debug('Read %d authinfo2 setting(s) for %s', len(merged), storage_url)
        try:
            return _authinfo_adapter.validate_python(merged)
        except ValidationError as exc:
            # Pydantic dataclasses report extra keys as `unexpected_keyword_argument`; plain models
            # would use `extra_forbidden`. Accept both so the friendly message keeps working if the
            # underlying model type changes.
            unknown = sorted(
                str(err['loc'][0])
                for err in exc.errors()
                if err['type'] in ('unexpected_keyword_argument', 'extra_forbidden')
            )
            if unknown:
                raise QuietError(
                    'Unknown key(s) in authentication file: ' + ', '.join(unknown), exitcode=2
                )
            raise QuietError('Invalid authentication file %s: %s' % (authfile, exc), exitcode=2)


_authinfo_adapter = TypeAdapter(Authinfo)


def _prompt_credential(prompt: str) -> str:
    '''Read a credential from the terminal, or from stdin when it is not a TTY.'''
    if sys.stdin.isatty():
        return getpass(prompt)
    return sys.stdin.readline().rstrip()


def pick(cli: T | None, cfg: T) -> T:
    '''Return the command-line value *cli* unless it is unset, in which case return *cfg*.

    The test is `cli is None`, not a truthiness test, so a valid falsy value (for example
    `--max-connections 0`) is honoured rather than mistaken for an unset option.
    '''
    return cfg if cli is None else cli
