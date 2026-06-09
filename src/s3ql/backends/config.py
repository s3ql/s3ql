'''
backends/config.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import annotations

from dataclasses import dataclass

from ..authinfo import DEFAULT_MAX_CONNECTIONS
from .common import AsyncBackend


@dataclass
class BackendConfig:
    '''Fully resolved parameters for opening a storage backend.

    Combines the backend class and storage URL with parsed backend options, credentials, the
    connection limit, and the optional file system passphrase. It is internal to the `backends`
    layer and handed to `backend_class.create()`; it conforms to
    `s3ql.types.BackendOptionsProtocol`.
    '''

    backend_class: type[AsyncBackend]
    storage_url: str
    backend_options: dict[str, str | bool]
    backend_login: str = ''
    backend_password: str = ''
    max_connections: int = DEFAULT_MAX_CONNECTIONS
    fs_passphrase: str | None = None


def parse_suboptions(value: str) -> dict[str, str | bool]:
    '''Parse a comma-separated backend suboption string into a mapping.

    Each element is either `key=value` or a bare flag `key` (mapped to `True`), for example
    `ssl,timeout=42,sse`.
    '''
    opts: dict[str, str | bool] = {}
    for raw_opt in value.split(','):
        opt = raw_opt.strip()
        if not opt:
            continue
        if '=' in opt:
            (key, val) = opt.split('=', 1)
            opts[key.strip()] = val.strip()
        else:
            opts[opt] = True

    return opts
