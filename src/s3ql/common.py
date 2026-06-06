'''
common.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import annotations

import binascii
import errno
import hashlib
import logging
import os
import posixpath
import subprocess
import sys
import time
from ast import literal_eval
from base64 import b64decode, b64encode
from collections.abc import AsyncIterable, Callable, Iterable, Iterator, Sequence, Sized
from typing import TYPE_CHECKING, Generic, NewType, TypeVar, overload

import pyfuse3
import trio
from pyfuse3 import InodeT

from s3ql.types import BasicMappingT, BinaryInput, BinaryOutput, HashFunction

from . import BUFSIZE, CTRL_NAME, ROOT_INODE
from .logging import QuietError

if TYPE_CHECKING:
    from .database import Connection

log = logging.getLogger(__name__)

T = TypeVar('T')
T1 = TypeVar('T1')
T2 = TypeVar('T2')

# S3QL client id and client secret for Google APIs.
# Don't get your hopes up, this isn't truly secret.
OAUTH_CLIENT_ID = '381875429714-6pch5vnnmqab454c68pkt8ugm86ef95v.apps.googleusercontent.com'
OAUTH_CLIENT_SECRET = 'HGl8fJeVML-gZ-1HSZRNZPz_'

file_system_encoding = sys.getfilesystemencoding()


def path2bytes(s: str) -> bytes:
    return s.encode(file_system_encoding, 'surrogateescape')


def bytes2path(s: bytes) -> str:
    return s.decode(file_system_encoding, 'surrogateescape')


def is_mounted(storage_url: str) -> bool:
    '''Try to determine if *storage_url* is mounted

    Note that the result may be wrong.. this is really just
    a best-effort guess.
    '''

    match = storage_url + ' '
    if os.path.exists('/proc/mounts'):
        with open('/proc/mounts') as fh:
            return any(line.startswith(match) for line in fh)

    try:
        for line in subprocess.check_output(
            ['mount'], stderr=subprocess.STDOUT, universal_newlines=True
        ).splitlines():
            if line.startswith(match):
                return True
    except subprocess.CalledProcessError:
        log.warning(
            'Warning! Unable to check if file system is mounted '
            '(/proc/mounts missing and mount call failed)'
        )

    return False


def inode_for_path(path: bytes, conn: Connection) -> InodeT:
    """Return inode of directory entry at `path`

    Raises `KeyError` if the path does not exist.
    """
    from .database import NoSuchRowError

    if not isinstance(path, bytes):
        raise TypeError('path must be of type bytes')

    # Remove leading and trailing /
    path = path.lstrip(b"/").rstrip(b"/")

    # Traverse
    inode = ROOT_INODE
    for el in path.split(b'/'):
        try:
            inode = conn.get_inode_val(
                "SELECT inode FROM contents_v WHERE name=? AND parent_inode=?", (el, inode)
            )
        except NoSuchRowError:
            raise KeyError('Path %r does not exist' % path)

    return inode


def get_path(id_: InodeT, conn: Connection, name: bytes | None = None) -> bytes:
    """Return a full path for inode `id_`.

    If `name` is specified, it is appended at the very end of the
    path (useful if looking up the path for file name with parent
    inode).
    """

    if name is None:
        path: list[bytes] = list()
    else:
        if not isinstance(name, bytes):
            raise TypeError('name must be of type bytes')
        path = [name]

    maxdepth = 255
    while id_ != ROOT_INODE:
        # This can be ambiguous if directories are hardlinked
        (name2, id_) = conn.get_row_typed(
            (bytes, InodeT),
            "SELECT name, parent_inode FROM contents_v WHERE inode=? LIMIT 1",
            (id_,),
        )
        path.append(name2)
        maxdepth -= 1
        if maxdepth == 0:
            raise RuntimeError(f'Failed to resolve name "{name!r}" at inode {id_} to path')

    path.append(b'')
    path.reverse()

    return b'/'.join(path)


def escape(s: str) -> str:
    '''Escape '/', '=' and '\0' in s'''

    s = s.replace('=', '=3D')
    s = s.replace('/', '=2F')
    s = s.replace('\0', '=00')

    return s


def sha256_fh(fh: BinaryInput) -> HashFunction:
    sha = hashlib.sha256()
    fh.seek(0)
    while True:
        buf = fh.read(BUFSIZE)
        if not buf:
            break
        sha.update(buf)
    return sha


def assert_s3ql_fs(path: str) -> str:
    '''Raise `QuietError` if *path* is not on an S3QL file system

    Returns name of the S3QL control file.
    '''
    ctrl_name = CTRL_NAME.decode('us-ascii')
    try:
        os.stat(path)
    except FileNotFoundError:
        raise QuietError('%s does not exist' % path)
    except OSError as exc:
        if exc.errno == errno.ENOTCONN:
            raise QuietError('File system appears to have crashed.')
        raise

    ctrlfile = os.path.join(path, ctrl_name)
    if not (ctrl_name not in pyfuse3.listdir(path) and os.path.exists(ctrlfile)):
        raise QuietError('%s is not on an S3QL file system' % path)

    return ctrlfile


def assert_fs_owner(path: str, mountpoint: bool = False) -> str:
    '''Raise `QuietError` if user is not owner of S3QL fs at *path*

    Implicitly calls `assert_s3ql_fs` first. Returns name of the
    S3QL control file.

    If *mountpoint* is True, also call `assert_s3ql_mountpoint`, i.e.
    fail if *path* is not the mount point of the file system.
    '''

    if mountpoint:
        ctrlfile = assert_s3ql_mountpoint(path)
    else:
        ctrlfile = assert_s3ql_fs(path)

    if os.stat(ctrlfile).st_uid != os.geteuid() and os.geteuid() != 0:
        raise QuietError(
            'Permission denied. %s is was not mounted by you and you are not root.' % path
        )

    return ctrlfile


def assert_s3ql_mountpoint(mountpoint: str) -> str:
    '''Raise QuietError if *mountpoint* is not an S3QL mountpoint

    Implicitly calls `assert_s3ql_fs` first. Returns name of the
    S3QL control file.
    '''

    ctrlfile = assert_s3ql_fs(mountpoint)
    if not posixpath.ismount(mountpoint):
        raise QuietError('%s is not a mount point' % mountpoint)

    return ctrlfile


def pretty_print_size(bytes_: int) -> str:
    '''Return *i* as string with appropriate suffix (MiB, GiB, etc)'''

    i = float(bytes_)
    if i < 1024:
        return '%d bytes' % i

    if i < 1024**2:
        unit = 'KiB'
        i /= 1024

    elif i < 1024**3:
        unit = 'MiB'
        i /= 1024**2

    elif i < 1024**4:
        unit = 'GiB'
        i /= 1024**3

    else:
        unit = 'TiB'
        i /= 1024**4

    if i < 10:
        form = '%.2f %s'
    elif i < 100:
        form = '%.1f %s'
    else:
        form = '%d %s'

    return form % (i, unit)


@overload
def split_by_n(seq: str, n: int) -> Iterator[str]: ...
@overload
def split_by_n(seq: Sequence[T], n: int) -> Iterator[Sequence[T]]: ...


def split_by_n(seq: Sequence[T], n: int) -> Iterator[Sequence[T]]:
    '''Yield elements in iterable *seq* in groups of *n*'''

    while seq:
        yield seq[:n]
        seq = seq[n:]


def _get_actual_type(type_spec: type | NewType) -> type:
    '''Get the actual type, handling NewType objects.'''
    if isinstance(type_spec, type):
        return type_spec
    else:
        t = type_spec.__supertype__
        # Don't support nested NewType objects
        assert isinstance(t, type)
        return t


def parse_literal(buf: bytes, type_spec: type[T] | NewType[T]) -> T:
    '''Try to parse *buf* as *type_spec*

    Raise `ValueError` if *buf* does not contain a valid
    Python literal, or if the literal does not correspond
    to *type_spec*.
    '''
    try:
        obj = literal_eval(buf.decode())
    except UnicodeDecodeError:
        raise ValueError('unable to decode as utf-8')
    except (ValueError, SyntaxError):
        raise ValueError('unable to parse as python literal')

    if type(obj) is _get_actual_type(type_spec):
        return obj

    raise ValueError('literal has wrong type')


def parse_literal_tuple(
    buf: bytes, type_spec: tuple[type[T1] | NewType[T1], type[T2] | NewType[T2]]
) -> tuple[T1, T2]:
    '''Try to parse *buf* as a tuple matching *type_spec*

    Raise `ValueError` if *buf* does not contain a valid
    Python literal, or if the literal is not a tuple matching
    the types in *type_spec*.
    '''
    try:
        obj = literal_eval(buf.decode())
    except UnicodeDecodeError:
        raise ValueError('unable to decode as utf-8')
    except (ValueError, SyntaxError):
        raise ValueError('unable to parse as python literal')

    actual_types = [_get_actual_type(t) for t in type_spec]
    if type(obj) is tuple and all(type(x) is t for x, t in zip(obj, actual_types, strict=True)):
        return obj

    raise ValueError('literal has wrong type')


class ThawError(Exception):
    def __str__(self) -> str:
        return 'Malformed serialization data'


def thaw_basic_mapping(buf: bytes | bytearray) -> BasicMappingT:
    '''Reconstruct dict from serialized representation

    *buf* must be a bytes-like object as created by
    `freeze_basic_mapping`. Raises `ThawError` if *buf* is not a valid
    representation.

    This procedure is safe even if *buf* comes from an untrusted source.
    '''

    try:
        d = literal_eval(buf.decode('utf-8'))
    except (UnicodeDecodeError, SyntaxError, ValueError):
        raise ThawError()

    # Decode bytes values
    for k, v in d.items():
        if not isinstance(v, bytes):
            continue
        try:
            d[k] = b64decode(v)
        except binascii.Error:
            raise ThawError()

    return d


def freeze_basic_mapping(d: BasicMappingT) -> bytes:
    '''Serialize mapping of elementary types

    Keys of *d* must be strings. Values of *d* must be of elementary type (i.e.,
    `str`, `bytes`, `int`, `float`, `complex`, `bool` or None).

    The output is a bytestream that can be used to reconstruct the mapping. The
    bytestream is not guaranteed to be deterministic. Look at
    `checksum_basic_mapping` if you need a deterministic bytestream.
    '''

    els = []
    for k, v in d.items():
        if not isinstance(k, str):
            raise ValueError('key %s must be str, not %s' % (k, type(k)))

        if not isinstance(v, (str, bytes, bytearray, int, float, complex, bool)) and v is not None:
            raise ValueError('value for key %s (%s) is not elementary' % (k, v))

        # To avoid wasting space, we b64encode non-ascii byte values.
        if isinstance(v, (bytes, bytearray)):
            v = b64encode(v)

        # This should be a pretty safe assumption for elementary types, but we
        # add an assert just to be safe (Python docs just say that repr makes
        # "best effort" to produce something parseable)
        (k_repr, v_repr) = (repr(k), repr(v))
        assert (literal_eval(k_repr), literal_eval(v_repr)) == (k, v)

        els.append('%s: %s' % (k_repr, v_repr))

    buf = '{ %s }' % ', '.join(els)
    return buf.encode('utf-8')


def time_ns() -> int:
    return time.time_ns()


def copyfh(
    ifh: BinaryInput,
    ofh: BinaryOutput,
    len_: int | None = None,
    update: Callable[[bytes], None] | None = None,
) -> None:
    '''Copy up to *len* bytes from ifh to ofh.

    If *update* is specified, call it with each block after the block
    has been written to the output handle.

    The reads and writes block, so when invoked from a Trio context the
    call must be dispatched through `trio.to_thread.run_sync` to avoid
    stalling the event loop.
    '''

    while len_ is None or len_ > 0:
        bufsize = BUFSIZE if len_ is None else min(BUFSIZE, len_)
        buf = ifh.read(bufsize)
        if not buf:
            return
        ofh.write(buf)
        if len_:
            len_ -= len(buf)
        if update is not None:
            update(buf)


class ParallelPipeline(Generic[T]):
    '''Fan-out producer/consumer pipeline over a Trio nursery.

    Subclasses override:
      - `produce`: an async method that performs orchestrator-task setup and returns
        an iterable of work units (sync or async). Called once before any worker is
        spawned, so any side effects (creating files, populating instance attributes)
        are visible to workers when they start. Return an `AsyncIterable` to stream
        work units lazily from an async source (e.g. a backend listing).
      - `consume`: receives an async iterable of work units. Invoked once per
        worker.
      - `finalize` (optional): post-pipeline hook, called after all workers complete.

    The only public entry point is `run(n_workers, buffer_size=0)`.
    '''

    async def produce(self) -> Iterable[T] | AsyncIterable[T]:
        raise NotImplementedError

    async def consume(self, jobs: AsyncIterable[T]) -> None:
        raise NotImplementedError

    def finalize(self) -> None:
        pass

    async def run(self, n_workers: int, buffer_size: int = 0) -> None:
        # Run setup synchronously on the orchestrator task: any side effects
        # (e.g. creating a destination file) are observable to workers when
        # they start.
        log.debug('ParallelPipeline.run: producing job list')
        jobs = await self.produce()

        # When we know the job count up front, don't spawn workers we cannot use.
        if isinstance(jobs, Sized):
            n_workers = min(n_workers, len(jobs))

        send_chan, recv_chan = trio.open_memory_channel[T](buffer_size)
        log.debug('ParallelPipeline.run: spawning %d workers', n_workers)
        async with trio.open_nursery() as nursery:
            async with recv_chan:
                for _ in range(n_workers):
                    nursery.start_soon(self._drive_consumer, recv_chan.clone())
            # Drive the producer inline on the orchestrator task. The
            # `await send_chan.send(item)` is a checkpoint, so consumers run
            # concurrently. Closing the send channel on exit lets consumers
            # drain their clones and exit cleanly.
            async with send_chan:
                if isinstance(jobs, AsyncIterable):
                    async for item in jobs:
                        await send_chan.send(item)
                else:
                    for item in jobs:
                        await send_chan.send(item)
        log.debug('ParallelPipeline.run: workers complete, finalizing')
        self.finalize()

    async def _drive_consumer(self, recv_chan: trio.MemoryReceiveChannel[T]) -> None:
        async with recv_chan:
            await self.consume(recv_chan)
