'''
local.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import annotations

import _thread
import builtins
import logging
import os
import struct
from collections.abc import AsyncIterator
from contextlib import ExitStack, suppress

import trio

from s3ql.types import BackendOptionsProtocol, BasicMappingT, BinaryInput, BinaryOutput

from ..common import ThawError, copyfh, freeze_basic_mapping, thaw_basic_mapping
from .common import (
    _FACTORY_SENTINEL,
    CorruptedObjectError,
    DanglingStorageURLError,
    NoSuchObject,
    log_delete_progress,
)
from .common import (
    AsyncBackend as AsyncBackendBase,
)

log = logging.getLogger(__name__)


class AsyncBackend(AsyncBackendBase):
    '''
    A backend that stores data on the local hard disk
    '''

    needs_login = False
    known_options: set[str] = set()
    prefix: str

    # Test-only knob that emulates network latency. When set to a positive
    # value, every read/write request awaits `trio.sleep(request_delay)`
    # before doing its filesystem I/O. Tests can deduce that requests
    # overlapped by comparing the wall-clock duration of an operation
    # against the worst-case serial duration.
    request_delay: float = 0.0

    def __init__(self, *, options: BackendOptionsProtocol, max_connections: int = 1) -> None:
        '''Initialize local backend - use AsyncBackend.create() instead.'''

        super().__init__(_factory_sentinel=_FACTORY_SENTINEL)
        self.prefix = options.storage_url[len('local://') :].rstrip('/')
        self._max_connections = max_connections
        # Bound concurrent filesystem operations, mirroring the connection limit
        # that networked backends enforce at the HTTP layer.
        self._io_limiter = trio.CapacityLimiter(max_connections)

        if not os.path.exists(self.prefix):
            raise DanglingStorageURLError(self.prefix)

    @classmethod
    async def create(
        cls: type[AsyncBackend],
        options: BackendOptionsProtocol,
        max_connections: int = 1,
    ) -> AsyncBackend:
        '''Create a new local backend instance.

        *max_connections* bounds how many filesystem operations may be in flight
        at once. The local backend opens no network connections, but honouring
        this limit lets callers throttle concurrent disk I/O - for example to a
        single operation at a time on a spinning disk.
        '''
        return cls(options=options, max_connections=max_connections)

    @property
    def max_connections(self) -> int:
        return self._max_connections

    def __str__(self) -> str:
        return 'local directory %s' % self.prefix

    def is_temp_failure(self, exc: BaseException) -> bool:
        return False

    async def lookup(self, key: str) -> BasicMappingT:
        path = self._key_to_path(key)
        try:
            with open(path, 'rb') as src:
                return _read_meta(src)
        except FileNotFoundError:
            raise NoSuchObject(key)

    async def get_size(self, key: str) -> int:
        return os.path.getsize(self._key_to_path(key))

    async def readinto_fh(self, key: str, ofh: BinaryOutput) -> BasicMappingT:
        '''Transfer data stored under *key* into *fh*, return metadata.

        The data will be inserted at the current offset.
        '''

        async with self._io_limiter:
            if self.request_delay > 0:
                await trio.sleep(self.request_delay)

            path = self._key_to_path(key)
            with ExitStack() as es:
                try:
                    ifh = es.enter_context(open(path, 'rb', buffering=0))
                except FileNotFoundError:
                    raise NoSuchObject(key)

                try:
                    metadata = _read_meta(ifh)
                except ThawError:
                    raise CorruptedObjectError('Invalid metadata')

                await trio.to_thread.run_sync(copyfh, ifh, ofh)
            return metadata

    async def write_fh(
        self,
        key: str,
        fh: BinaryInput,
        metadata: BasicMappingT | None = None,
    ) -> int:
        '''Upload the full contents of *fh* under *key*.

        If a temporary error (as defined by `is_temp_failure`) occurs, the operation is
        retried. Returns the size of the resulting storage object.
        '''

        if metadata is None:
            metadata = dict()

        path = self._key_to_path(key)
        buf = freeze_basic_mapping(metadata)
        if len(buf).bit_length() > 16:
            raise ValueError('Metadata too large')

        async with self._io_limiter:
            if self.request_delay > 0:
                await trio.sleep(self.request_delay)

            # By renaming, we make sure that there are no
            # conflicts between parallel reads, the last one wins
            tmpname = '%s#%d-%d.tmp' % (path, os.getpid(), _thread.get_ident())

            fh.seek(0)
            with ExitStack() as es:
                try:
                    dest = es.enter_context(open(tmpname, 'wb', buffering=0))
                except FileNotFoundError:
                    try:  # noqa: SIM105 # auto-added, needs manual check!
                        os.makedirs(os.path.dirname(path))
                    except FileExistsError:
                        # Another thread may have created the directory already
                        pass
                    dest = es.enter_context(open(tmpname, 'wb', buffering=0))

                dest.write(b's3ql_1\n')
                dest.write(struct.pack('<H', len(buf)))
                dest.write(buf)
                await trio.to_thread.run_sync(copyfh, fh, dest)
                size = dest.tell()
            os.rename(tmpname, path)

        return size

    async def contains(self, key: str) -> bool:
        path = self._key_to_path(key)
        try:
            os.lstat(path)
        except FileNotFoundError:
            return False
        return True

    async def delete_multi(self, keys: builtins.list[str], *, log_progress: bool = False) -> None:
        total = len(keys)
        for i, key in enumerate(keys):
            try:
                await self.delete(key)
            except:
                del keys[:i]
                raise
            if log_progress:
                log_delete_progress(i + 1, total)

        del keys[:]

    async def delete(self, key: str) -> None:
        path = self._key_to_path(key)
        async with self._io_limiter:
            with suppress(FileNotFoundError):
                os.unlink(path)

    async def list(self, prefix: str = '') -> AsyncIterator[str]:
        if prefix:
            base = os.path.dirname(self._key_to_path(prefix))
        else:
            base = self.prefix

        for path, dirnames, filenames in os.walk(base, topdown=True):
            # Do not look in wrong directories
            if prefix:
                rpath = path[len(self.prefix) :]  # path relative to base
                prefix_l = ''.join(rpath.split('/'))

                dirs_to_walk = list()
                for name in dirnames:
                    prefix_ll = unescape(prefix_l + name)
                    if prefix_ll.startswith(prefix[: len(prefix_ll)]):
                        dirs_to_walk.append(name)
                dirnames[:] = dirs_to_walk

            for name in filenames:
                # Skip temporary files
                if '#' in name:
                    continue

                key = unescape(name)

                if not prefix or key.startswith(prefix):
                    yield key

    def _key_to_path(self, key: str) -> str:
        '''Return path for given key'''

        # NOTE: We must not split the path in the middle of an
        # escape sequence, or list() will fail to work.

        key = escape(key)

        if not key.startswith('s3ql_data_'):
            return os.path.join(self.prefix, key)

        no = key[10:]
        path = [self.prefix, 's3ql_data_']
        for i in range(3, len(no), 3):
            path.append(no[:i])
        path.append(key)

        return os.path.join(*path)


def _read_meta(fh: BinaryInput) -> BasicMappingT:
    buf = fh.read(9)
    if not buf.startswith(b's3ql_1\n'):
        raise CorruptedObjectError('Invalid object header: %r' % buf)

    len_ = struct.unpack('<H', buf[-2:])[0]
    try:
        return thaw_basic_mapping(fh.read(len_))
    except ThawError:
        raise CorruptedObjectError('Invalid metadata')


def escape(s: str) -> str:
    '''Escape '/', '=' and '.' in s'''

    s = s.replace('=', '=3D')
    s = s.replace('/', '=2F')
    s = s.replace('#', '=23')

    return s


def unescape(s: str) -> str:
    '''Un-Escape '/', '=' and '.' in s'''

    s = s.replace('=2F', '/')
    s = s.replace('=23', '#')
    s = s.replace('=3D', '=')

    return s
