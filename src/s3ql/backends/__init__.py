'''
backends/__init__.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import annotations

import re

from ..authinfo import Authinfo, CompressSpec, _prompt_credential, pick
from ..http import HostnameNotResolvable
from ..logging import QuietError
from . import b2ng, gs, local, rackspace, s3, s3c, s3c4, swift, swiftks
from .common import (
    AsyncBackend,
    AuthenticationError,
    AuthorizationError,
    CorruptedObjectError,
    DanglingStorageURLError,
    NoSuchObject,
)
from .comprenc import AsyncComprencBackend
from .config import parse_suboptions

async_prefix_map: dict[str, type[AsyncBackend]] = {
    's3': s3.AsyncBackend,
    'local': local.AsyncBackend,
    'gs': gs.AsyncBackend,
    's3c': s3c.AsyncBackend,
    's3c4': s3c4.AsyncBackend,
    'swift': swift.AsyncBackend,
    'swiftks': swiftks.AsyncBackend,
    'rackspace': rackspace.AsyncBackend,
    'b2': b2ng.AsyncBackend,
}


async def open_raw_backend(
    storage_url: str,
    authinfo: Authinfo,
    *,
    backend_options: str | None = None,
    max_connections: int | None = None,
) -> AsyncBackend:
    '''Return a bare backend for *storage_url*, resolving its options against *authinfo*.

    A command-line override (*backend_options*, *max_connections*) wins over the corresponding
    authinfo2 value; an unset (`None`) override falls back to *authinfo*. Credentials are taken
    from *authinfo*, prompting interactively when the backend needs a login and authinfo2 does not
    supply one. The storage URL is neither probed for an existing S3QL file system nor wrapped for
    compression or encryption.
    '''
    options_str = pick(backend_options, authinfo.backend_options)
    connections = pick(max_connections, authinfo.max_connections)

    hit = re.match(r'^([a-zA-Z0-9]+)://', storage_url)
    if not hit:
        raise QuietError('Unable to parse storage url ' + storage_url, exitcode=2)
    try:
        backend_class = async_prefix_map[hit.group(1)]
    except KeyError:
        raise QuietError('No such backend: ' + hit.group(1), exitcode=11)

    parsed_options = parse_suboptions(options_str) if options_str else {}
    for opt in parsed_options:
        if opt not in backend_class.known_options:
            raise QuietError('Unknown backend option: ' + opt, exitcode=3)

    backend_login = authinfo.backend_login
    backend_password = authinfo.backend_password
    if backend_class.needs_login:
        if backend_login is None:
            backend_login = _prompt_credential('Enter backend login: ')
        if backend_password is None:
            backend_password = _prompt_credential('Enter backend password: ')

    try:
        backend = await backend_class.create(
            storage_url=storage_url,
            backend_options=parsed_options,
            backend_login=backend_login or '',
            backend_password=backend_password or '',
            max_connections=connections,
        )
    except AuthenticationError:
        raise QuietError('Invalid credentials (or skewed system clock?).', exitcode=14)
    except AuthorizationError:
        raise QuietError('No permission to access backend.', exitcode=15)
    except HostnameNotResolvable:
        raise QuietError("Can't connect to backend: unable to resolve hostname", exitcode=19)
    except DanglingStorageURLError as exc:
        raise QuietError(str(exc), exitcode=16)

    return backend


async def open_backend(
    storage_url: str,
    authinfo: Authinfo,
    *,
    backend_options: str | None = None,
    max_connections: int | None = None,
    compress: CompressSpec,
) -> AsyncComprencBackend:
    '''Return a configured backend ready to access the file system.

    Probes *storage_url* for an existing S3QL file system, obtains the file system passphrase from
    *authinfo* (prompting when the file system is encrypted and none is configured), and wraps the
    underlying backend in `AsyncComprencBackend`. *compress* determines the algorithm and level
    used for newly stored data; it is supplied by the caller (already parsed) because mount also
    needs it for thread sizing.
    '''
    backend: AsyncBackend | None = None
    try:
        try:
            backend = await open_raw_backend(
                storage_url,
                authinfo,
                backend_options=backend_options,
                max_connections=max_connections,
            )

            # Do not use backend.lookup(), this would use a HEAD request and not provide any useful
            # error messages if something goes wrong (e.g. wrong credentials)
            await backend.fetch('s3ql_passphrase')

        except CorruptedObjectError:
            raise QuietError(
                'File system revision needs upgrade (or backend data is corrupted)', exitcode=32
            )

        except NoSuchObject:
            encrypted = False
        else:
            encrypted = True
    except:
        if backend is not None:
            await backend.close()
        raise

    fs_passphrase: str | None = authinfo.fs_passphrase
    if not encrypted:
        fs_passphrase = None
    elif fs_passphrase is None:
        fs_passphrase = _prompt_credential('Enter file system encryption passphrase: ')

    if fs_passphrase is not None:
        fs_passphrase_bytes: bytes | None = fs_passphrase.encode('utf-8')
    else:
        fs_passphrase_bytes = None

    try:
        assert backend is not None
        comprenc_backend = await AsyncComprencBackend.create(fs_passphrase_bytes, compress, backend)
        if encrypted:
            try:
                data_pw = (await comprenc_backend.fetch('s3ql_passphrase'))[0]
            except CorruptedObjectError:
                raise QuietError(
                    'Wrong file system passphrase (or file system '
                    'revision needs upgrade, or backend data is corrupted).',
                    exitcode=17,
                )
            comprenc_backend.passphrase = data_pw
        else:
            # Allow for old versions, since this function is also used during upgrades
            if not (
                await backend.contains('s3ql_params') or await backend.contains('s3ql_metadata')
            ):
                raise QuietError('No S3QL file system found at given storage URL.', exitcode=18)
    except:
        if backend is not None:
            await backend.close()
        raise

    return comprenc_backend
