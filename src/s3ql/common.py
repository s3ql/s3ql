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
from collections.abc import Callable, Iterator, Sequence
from getpass import getpass
from typing import TYPE_CHECKING, Any, Literal, NewType, TypeVar, overload

import pyfuse3
from pyfuse3 import InodeT

from s3ql.http import HostnameNotResolvable
from s3ql.types import BackendFactory, BasicMappingT, BinaryInput, BinaryOutput, HashFunction

from . import BUFSIZE, CTRL_NAME, ROOT_INODE
from .logging import QuietError

if TYPE_CHECKING:
    from .backends.common import AsyncBackend
    from .backends.comprenc import AsyncComprencBackend
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


@overload
async def async_get_backend(options, raw: Literal[True]) -> AsyncBackend: ...


@overload
async def async_get_backend(options, raw: Literal[False] = ...) -> AsyncComprencBackend: ...


async def async_get_backend(options, raw: bool = False) -> AsyncComprencBackend | AsyncBackend:
    '''Return async backend for given storage-url

    If *raw* is true, don't attempt to unlock and don't wrap into
    AsyncComprencBackend.
    '''

    if raw:
        return await options.backend_class.create(options)

    factory = await get_backend_factory(options)
    return await factory()


async def get_backend_factory(options) -> BackendFactory:
    '''Return factory producing backend objects'''

    from .backends.common import (
        AuthenticationError,
        AuthorizationError,
        CorruptedObjectError,
        DanglingStorageURLError,
        NoSuchObject,
    )
    from .backends.comprenc import AsyncComprencBackend as AsyncComprencBackendCls

    backend: AsyncBackend | None = None
    try:
        try:
            backend = await options.backend_class.create(options)

            # Do not use backend.lookup(), this would use a HEAD request and
            # not provide any useful error messages if something goes wrong
            # (e.g. wrong credentials)
            await backend.fetch('s3ql_passphrase')

        except AuthenticationError:
            raise QuietError('Invalid credentials (or skewed system clock?).', exitcode=14)

        except AuthorizationError:
            raise QuietError('No permission to access backend.', exitcode=15)

        except HostnameNotResolvable:
            raise QuietError("Can't connect to backend: unable to resolve hostname", exitcode=19)

        except DanglingStorageURLError as exc:
            raise QuietError(str(exc), exitcode=16)

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
    options.fs_passphrase = fs_passphrase_bytes

    compress = getattr(options, 'compress', ('lzma', 2))

    assert backend is not None
    async with await AsyncComprencBackendCls.create(
        fs_passphrase_bytes, compress, backend
    ) as tmp_backend:
        if encrypted:
            try:
                data_pw = (await tmp_backend.fetch('s3ql_passphrase'))[0]
            except CorruptedObjectError:
                raise QuietError(
                    'Wrong file system passphrase (or file system '
                    'revision needs upgrade, or backend data is corrupted).',
                    exitcode=17,
                )
        else:
            data_pw = None
            # Allow for old versions, since this function is also used during upgrades
            if not (
                await backend.contains('s3ql_params') or await backend.contains('s3ql_metadata')
            ):
                raise QuietError('No S3QL file system found at given storage URL.', exitcode=18)

    async def factory() -> AsyncComprencBackend:
        async_plain = await options.backend_class.create(options)
        return await AsyncComprencBackendCls.create(data_pw, compress, async_plain)

    factory.has_delete_multi = backend.has_delete_multi  # type: ignore[attr-defined]

    return factory  # type: ignore[return-value]


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


@overload
def parse_literal(buf: bytes, type_spec: type[InodeT]) -> InodeT: ...


@overload
def parse_literal(buf: bytes, type_spec: type[T]) -> T: ...


def parse_literal(buf: bytes, type_spec: type[T]) -> T:
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
        return obj  # type: ignore[return-value]

    raise ValueError('literal has wrong type')


@overload
def parse_literal_tuple(
    buf: bytes, type_spec: tuple[type[InodeT], type[InodeT]]
) -> tuple[InodeT, InodeT]: ...


@overload
def parse_literal_tuple(
    buf: bytes, type_spec: tuple[type[InodeT], type[T2]]
) -> tuple[InodeT, T2]: ...


@overload
def parse_literal_tuple(buf: bytes, type_spec: tuple[type[T1], type[T2]]) -> tuple[T1, T2]: ...


@overload
def parse_literal_tuple(buf: bytes, type_spec: tuple[type, ...]) -> tuple[Any, ...]: ...


def parse_literal_tuple(buf: bytes, type_spec: tuple[type, ...]) -> tuple[Any, ...]:
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
    return int(time.time() * 1e9)


def copyfh(
    ifh: BinaryInput,
    ofh: BinaryOutput,
    len_: int | None = None,
    update: Callable[[bytes], None] | None = None,
) -> None:
    '''Copy up to *len* bytes from ifh to ofh.

    If *update* is specified, call it with each block after the block
    has been written to the output handle.
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
