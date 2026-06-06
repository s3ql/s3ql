'''
backends/__init__.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import annotations

import sys
from getpass import getpass

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


async def open_raw_backend(options) -> AsyncBackend:
    '''Return a bare backend for the storage URL given in *options*.

    The backend uses *options.max_connections* (default 1) HTTP connections. The storage URL is not
    probed for an existing S3QL file system and the result is not wrapped for compression or
    encryption.
    '''

    max_connections = getattr(options, 'max_connections', 1)
    try:
        backend = await options.backend_class.create(options, max_connections=max_connections)
    except AuthenticationError:
        raise QuietError('Invalid credentials (or skewed system clock?).', exitcode=14)

    except AuthorizationError:
        raise QuietError('No permission to access backend.', exitcode=15)

    except HostnameNotResolvable:
        raise QuietError("Can't connect to backend: unable to resolve hostname", exitcode=19)

    except DanglingStorageURLError as exc:
        raise QuietError(str(exc), exitcode=16)

    return backend


async def open_backend(options) -> AsyncComprencBackend:
    '''Return a configured backend ready to access the file system.

    Probes the storage URL for an existing S3QL file system, prompts for the passphrase if the file
    system is encrypted, and wraps the underlying backend in `AsyncComprencBackend`. The returned
    backend uses *options.max_connections* (default 1) HTTP connections.
    '''

    backend: AsyncBackend | None = None
    try:
        try:
            backend = await open_raw_backend(options)

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

    if encrypted and not hasattr(options, 'fs_passphrase'):
        if sys.stdin.isatty():
            fs_passphrase: str | None = getpass("Enter file system encryption passphrase: ")
        else:
            fs_passphrase = sys.stdin.readline().rstrip()
    elif not encrypted:
        fs_passphrase = None
    else:
        fs_passphrase = options.fs_passphrase

    fs_passphrase_bytes: bytes | None
    if fs_passphrase is not None:
        fs_passphrase_bytes = fs_passphrase.encode('utf-8')
    else:
        fs_passphrase_bytes = None

    compress = getattr(options, 'compress', ('lzma', 2))

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
