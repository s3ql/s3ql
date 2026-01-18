'''
common.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import annotations

import hashlib
import hmac
import inspect
import logging
import os
import random
import re
import ssl
import struct
import textwrap
import threading
import time
from abc import ABCMeta, abstractmethod
from collections.abc import AsyncIterator, Callable, Iterator
from functools import wraps
from io import BytesIO
from typing import TYPE_CHECKING, Literal, Optional, TypeVar

import trio

from s3ql import BUFSIZE
from s3ql.async_bridge import run_async
from s3ql.http import HTTPConnection
from s3ql.logging import LOG_ONCE, QuietError
from s3ql.types import BackendOptionsProtocol, BasicMappingT, BinaryInput, BinaryOutput

if TYPE_CHECKING:
    from hmac import HMAC

F = TypeVar('F', bound=Callable[..., object])
T = TypeVar('T', bound='AsyncBackend')
T2 = TypeVar('T2', bound='AbstractBackend')


class _FactorySentinel:
    '''Sentinel to ensure __init__ is only called via create() factory.'''

    __slots__ = ()


_FACTORY_SENTINEL = _FactorySentinel()


log = logging.getLogger(__name__)


class RateTracker:
    '''
    Maintain an average occurrence rate for events over a configurable time
    window. The rate is computed with one second resolution.
    '''

    buckets: list[int]
    window_length: int
    last_update: int
    lock: threading.Lock

    def __init__(self, window_length: int) -> None:
        if not isinstance(window_length, int):
            raise ValueError('only integer window lengths are supported')

        self.buckets = [0] * window_length
        self.window_length = window_length
        self.last_update = int(time.monotonic())
        self.lock = threading.Lock()

    def register(self, _not_really: bool = False) -> None:
        '''Register occurrence of an event.

        The keyword argument is for class-internal use only.
        '''

        buckets = self.buckets
        bucket_count = len(self.buckets)
        now = int(time.monotonic())

        elapsed = min(now - self.last_update, bucket_count)
        for i in range(elapsed):
            buckets[(now - i) % bucket_count] = 0

        if _not_really:
            return

        with self.lock:
            buckets[now % bucket_count] += 1
            self.last_update = now

    def get_rate(self) -> float:
        '''Return average rate of event occurrence'''

        self.register(_not_really=True)
        return sum(self.buckets) / len(self.buckets)

    def get_count(self) -> int:
        '''Return total number of events in window'''

        self.register(_not_really=True)
        return sum(self.buckets)


# We maintain a (global) running average of temporary errors, so
# that we can log a warning if this number becomes large. We
# use a relatively large window to prevent bogus spikes if
# multiple threads all have to retry after a long period of
# inactivity.
RETRY_TIMEOUT: int = 60 * 60 * 24


def retry(method: F, _tracker: RateTracker = RateTracker(60)) -> F:  # noqa: B008
    '''Wrap async *method* for retrying on some exceptions

    If *method* raises an exception for which the instance's `is_temp_failure(exc)` method is true,
    the *method* is called again at increasing intervals. If this persists for more than
    `RETRY_TIMEOUT` seconds, the most-recently caught exception is re-raised within a TimeoutError.
    If the method defines a keyword parameter *is_retry*, then this parameter will be set to True
    whenever the function is retried.
    '''

    if not inspect.iscoroutinefunction(method):
        raise TypeError('async_retry can only wrap coroutine functions')

    sig = inspect.signature(method)
    has_is_retry = 'is_retry' in sig.parameters

    @wraps(method)
    async def wrapped(*a, **kw):
        self = a[0]
        interval = 1 / 50
        waited = 0
        retries = 0
        while True:
            if has_is_retry:
                kw['is_retry'] = retries > 0
            try:
                return await method(*a, **kw)
            except Exception as exc:
                if not self.is_temp_failure(exc):
                    raise

                _tracker.register()
                rate = _tracker.get_rate()
                if rate > 5:
                    log.warning(
                        'Had to retry %d times over the last %d seconds, '
                        'server or network problem?',
                        rate * _tracker.window_length,
                        _tracker.window_length,
                    )
                else:
                    log.debug('Average retry rate: %.2f Hz', rate)

                if waited > RETRY_TIMEOUT:
                    log.error(
                        '%s.%s(*): Timeout exceeded, re-raising %r exception',
                        self.__class__.__name__,
                        method.__name__,
                        exc,
                    )
                    raise TimeoutError() from exc

                retries += 1
                if retries <= 2:
                    log_fn = log.debug
                elif retries <= 4:
                    log_fn = log.info
                else:
                    log_fn = log.warning

                log_fn(
                    'Encountered %s (%s), retrying %s.%s (attempt %d)...',
                    type(exc).__name__,
                    exc,
                    self.__class__.__name__,
                    method.__name__,
                    retries,
                )

                if hasattr(exc, 'retry_after') and exc.retry_after:
                    log.debug('retry_after is %.2f seconds', exc.retry_after)
                    interval = exc.retry_after

            # Add some random variation to prevent flooding the
            # server with too many concurrent requests.
            await trio.sleep(interval * random.uniform(1, 1.5))
            waited += interval
            interval = min(5 * 60, 2 * interval)

    extend_docstring(
        wrapped,
        'This method has been wrapped and will automatically re-execute in '
        'increasing intervals for up to `s3ql.backends.common.RETRY_TIMEOUT` '
        'seconds if it raises an exception for which the instance\'s '
        '`is_temp_failure` method returns True.',
    )

    return wrapped  # type: ignore[return-value]


def extend_docstring(fun: Callable[..., object], s: str) -> None:
    '''Append *s* to *fun*'s docstring with proper wrapping and indentation'''

    if fun.__doc__ is None:
        fun.__doc__ = ''

    # Figure out proper indentation
    indent = 60
    for line in fun.__doc__.splitlines()[1:]:
        stripped = line.lstrip()
        if stripped:
            indent = min(indent, len(line) - len(stripped))

    indent_s = '\n' + ' ' * indent
    fun.__doc__ += ''.join(indent_s + line for line in textwrap.wrap(s, width=80 - indent))
    fun.__doc__ += '\n'


class AsyncBackend(metaclass=ABCMeta):
    '''Functionality shared between all backends'''

    needs_login = True
    known_options: set[str] = set()

    def __init__(self, *, _factory_sentinel: _FactorySentinel) -> None:
        if _factory_sentinel is not _FACTORY_SENTINEL:
            raise TypeError(
                f'{self.__class__.__name__} cannot be instantiated directly. '
                f'Use {self.__class__.__name__}.create() instead.'
            )

    @classmethod
    async def create(cls: type[T], options: BackendOptionsProtocol) -> T:
        '''Async factory method to create backend instances.

        Subclasses must override this method to perform initialization.
        '''
        raise NotImplementedError(f'{cls.__name__}.create() must be implemented by subclass')

    async def __aenter__(self: T) -> T:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        tb: object,
    ) -> Literal[False]:
        await self.close()
        return False

    @property
    def has_delete_multi(self) -> bool:
        '''True if the backend supports `delete_multi`.'''

        return False

    async def reset(self) -> None:  # noqa: B027 # auto-added, needs manual check!
        '''Reset backend

        This resets the backend and ensures that it is ready to process requests. In most cases,
        this method does nothing. However, if e.g. a previous request was not send completely or
        response not read completely because of an exception, the `reset` method will make sure that
        any underlying connection is properly closed.
        '''

        pass

    @abstractmethod
    async def readinto_fh(self, key: str, fh: BinaryOutput) -> BasicMappingT:
        '''Transfer data stored under *key* into *fh*, return metadata.

        The data will be inserted at the current offset. If a temporary error (as defined by
        `is_temp_failure`) occurs, the operation is retried.
        '''
        pass

    @abstractmethod
    async def write_fh(
        self,
        key: str,
        fh: BinaryInput,
        metadata: BasicMappingT | None = None,
        len_: int | None = None,
    ) -> int:
        '''Upload *len_* bytes from *fh* under *key*.

        The data will be read at the current offset. If *len_* is None, reads until the
        end of the file.

        If a temporary error (as defined by `is_temp_failure`) occurs, the operation is
        retried.  Returns the size of the resulting storage object (which may be less due
        to compression)
        '''
        pass

    async def fetch(self, key: str) -> tuple[bytes, BasicMappingT]:
        """Return data and metadata stored under `key`.

        Returns a tuple with the data and metadata.
        """

        fh = BytesIO()
        metadata = await self.readinto_fh(key, fh)
        return (fh.getvalue(), metadata)

    async def read(self, key: str) -> bytes:
        """Return data stored under `key`."""

        fh = BytesIO()
        await self.readinto_fh(key, fh)
        return fh.getvalue()

    async def store(self, key: str, val: bytes, metadata: BasicMappingT | None = None) -> int:
        """Store data under `key`.

        `metadata` can be mapping with additional attributes to store with the
        object. Keys have to be of type `str`, values have to be of elementary
        type (`str`, `bytes`, `int`, `float` or `bool`).

        If no metadata is required, one can simply assign to the subscripted
        backend instead of using this function: ``backend[key] = val`` is
        equivalent to ``backend.store(key, val)``.
        """

        return await self.write_fh(key, BytesIO(val), metadata)

    @abstractmethod
    def is_temp_failure(self, exc: BaseException) -> bool:
        '''Return true if exc indicates a temporary error

        Return true if the given exception indicates a temporary problem. Most
        instance methods automatically retry the request in this case, so the
        caller does not need to worry about temporary failures.

        However, in same cases (e.g. when reading or writing an object), the
        request cannot automatically be retried. In these case this method can
        be used to check for temporary problems and so that the request can be
        manually restarted if applicable.
        '''

        pass

    @abstractmethod
    async def lookup(self, key: str) -> BasicMappingT:
        """Return metadata for given key.

        If the key does not exist, `NoSuchObject` is raised.
        """

        pass

    @abstractmethod
    async def get_size(self, key: str) -> int:
        '''Return size of object stored under *key*'''
        pass

    async def contains(self, key: str) -> bool:
        '''Check if `key` is in backend'''

        try:
            await self.lookup(key)
        except NoSuchObject:
            return False
        else:
            return True

    @abstractmethod
    async def delete(self, key: str) -> None:
        """Delete object stored under `key`

        Attempts to delete non-existing objects will silently succeed.
        """
        pass

    async def delete_multi(self, keys: list[str]) -> None:
        """Delete objects stored under `keys`

        Deleted objects are removed from the *keys* list, so that the caller can
        determine which objects have not yet been processed if an exception is
        occurs.

        Attempts to delete non-existing objects will silently succeed.
        """

        while keys:
            await self.delete(keys[-1])
            keys.pop()

    @abstractmethod
    def list(self, prefix: str = '') -> AsyncIterator[str]:
        '''List keys in backend

        Returns an iterator over all keys in the backend.
        '''
        pass

    async def close(self) -> None:  # noqa: B027
        '''Close any opened resources

        This method closes any resources allocated by the backend (e.g. network
        sockets). This method should be called explicitly before a backend
        object is garbage collected. The backend object may be re-used after
        `close` has been called, in this case the necessary resources are
        transparently allocated again.
        '''

        pass


class NoSuchObject(Exception):
    '''Raised if the requested object does not exist in the backend'''

    key: str

    def __init__(self, key: str) -> None:
        super().__init__()
        self.key = key

    def __str__(self) -> str:
        return 'Backend does not have anything stored under key %r' % self.key


class DanglingStorageURLError(Exception):
    '''Raised if the backend can't store data at the given location'''

    loc: str
    msg: str | None

    def __init__(self, loc: str, msg: str | None = None) -> None:
        super().__init__()
        self.loc = loc
        self.msg = msg

    def __str__(self) -> str:
        if self.msg is None:
            return '%r does not exist' % self.loc
        else:
            return self.msg


class AuthorizationError(Exception):
    '''Raised if the credentials don't give access to the requested backend'''

    msg: str

    def __init__(self, msg: str) -> None:
        super().__init__()
        self.msg = msg

    def __str__(self) -> str:
        return 'Access denied. Server said: %s' % self.msg


class AuthenticationError(Exception):
    '''Raised if the credentials are invalid'''

    msg: str

    def __init__(self, msg: str) -> None:
        super().__init__()
        self.msg = msg

    def __str__(self) -> str:
        return 'Access denied. Server said: %s' % self.msg


class CorruptedObjectError(Exception):
    """
    Raised if a storage object is corrupted.

    Note that this is different from BadDigest error, which is raised
    if a transmission error has been detected.
    """

    def __init__(self, str_: str) -> None:
        super().__init__()
        self.str: str = str_

    def __str__(self) -> str:
        return self.str


def get_ssl_context(path: str | None) -> ssl.SSLContext:
    '''Construct SSLContext object'''

    # Best practice according to http://docs.python.org/3/library/ssl.html#protocol-versions
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.verify_mode = ssl.CERT_REQUIRED

    if path is None:
        log.debug('Reading default CA certificates.')
        context.set_default_verify_paths()
    elif os.path.isfile(path):
        log.debug('Reading CA certificates from file %s', path)
        context.load_verify_locations(cafile=path)
    else:
        log.debug('Reading CA certificates from directory %s', path)
        context.load_verify_locations(capath=path)

    return context


def get_proxy(ssl: bool) -> tuple[str, int] | None:
    '''Read system proxy settings

    Returns either `None`, or a tuple ``(host, port)``.

    This function may raise `QuietError`.
    '''

    if ssl:
        proxy_env = 'https_proxy'
    else:
        proxy_env = 'http_proxy'

    if proxy_env in os.environ:
        proxy_str = os.environ[proxy_env]
        hit = re.match(r'^(https?://)?([a-zA-Z0-9.-]+)(:[0-9]+)?/?$', proxy_str)
        if not hit:
            raise QuietError(
                'Unable to parse proxy setting %s=%r' % (proxy_env, proxy_str), exitcode=13
            )

        if hit.group(1) == 'https://':
            log.warning(
                'HTTPS connection to proxy is probably pointless and not supported, '
                'will use standard HTTP',
                extra=LOG_ONCE,
            )

        if hit.group(3):
            proxy_port = int(hit.group(3)[1:])
        else:
            proxy_port = 80

        proxy_host = hit.group(2)
        log.info('Using proxy %s:%d', proxy_host, proxy_port, extra=LOG_ONCE)
        return (proxy_host, proxy_port)
    else:
        return None


def checksum_basic_mapping(metadata: BasicMappingT, key: bytes | None = None) -> bytes:
    '''Compute checksum for mapping of elementary types

    Keys of *d* must be strings. Values of *d* must be of elementary
    type (i.e., `str`, `bytes`, `int`, `float`, `complex`, `bool` or
    None). If there is a key named ``signature``, then it is excluded
    from the checksum computation.

    If *key* is None, compute MD5. Otherwise compute HMAC using *key*.
    '''

    # In order to compute a safe checksum, we need to convert each object to a
    # unique representation (i.e, we can't use repr(), as it's output may change
    # in the future). Furthermore, we have to make sure that the representation
    # is theoretically reversible, or there is a potential for collision
    # attacks.

    chk: hashlib._Hash | HMAC
    if key is None:
        chk = hashlib.md5()
    else:
        chk = hmac.new(key, digestmod=hashlib.sha256)

    for mkey in sorted(metadata.keys()):
        assert isinstance(mkey, str)
        if mkey == 'signature':
            continue
        chk.update(mkey.encode('utf-8'))
        val = metadata[mkey]
        if isinstance(val, str):
            val = b'\0s' + val.encode('utf-8') + b'\0'
        elif val is None:
            val = b'\0n'
        elif isinstance(val, int):
            val = b'\0i' + ('%d' % val).encode() + b'\0'
        elif isinstance(val, bool):
            val = b'\0t' if val else b'\0f'
        elif isinstance(val, float):
            val = b'\0d' + struct.pack('<d', val)
        elif isinstance(val, (bytes, bytearray)):
            assert len(val).bit_length() <= 32
            chk.update(b'\0b' + struct.pack('<I', len(val)))
        else:
            raise ValueError("Don't know how to checksum %s instances" % type(val))
        chk.update(val)

    return chk.digest()


async def copy_to_http(
    ifh: BinaryInput,
    ofh: HTTPConnection,
    len_: Optional[int],
    update: Callable[[bytes], None] | None = None,
) -> None:
    '''Copy *len_* bytes from sync *ifh* to async HTTPConnection *ofh*.

    Reads from *ifh* (synchronously if BytesIO, otherwise in a separatet thread) and writes
    asynchronously to *ofh* (an HTTPConnection).

    If *update* is specified, call it with each block after the block has been written to the output
    handle.
    '''

    use_async = not isinstance(ofh, BytesIO)

    while len_ is None or len_ > 0:
        bufsize = min(BUFSIZE, len_) if len_ is not None else BUFSIZE
        if use_async:
            # Wrapping the file object with `trio.wrap_file()` would be cleaner, but adds extra
            # overhead and undefined behavior (will the sync object be closed when the async wrapper
            # is garbage collected?), and just calls trio.to_thread.run_sync() internally anyway.
            buf = await trio.to_thread.run_sync(ifh.read, bufsize)
        else:
            buf = ifh.read(bufsize)
        if not buf:
            return
        await ofh.co_write(buf)
        if len_ is not None:
            len_ -= len(buf)
        if update is not None:
            update(buf)


async def copy_from_http(
    ifh: HTTPConnection,
    ofh: BinaryOutput,
    update: Callable[[bytes], None] | None = None,
) -> None:
    '''Copy all available bytes from async HTTPConnection *ifh* to sync *ofh*.

    Reads asynchronously from *ifh* (an HTTPConnection) and writes
    to *ofh* (synchronously if BytesIO, otherwise in a separate thread).

    If *update* is specified, call it with each block after the block
    has been written to the output handle.
    '''
    use_async = not isinstance(ofh, BytesIO)

    while True:
        buf = await ifh.co_read(BUFSIZE)
        if not buf:
            return
        if use_async:
            # Wrapping the file object with `trio.wrap_file()` would be cleaner, but adds extra
            # overhead and undefined behavior (will the sync object be closed when the async wrapper
            # is garbage collected?), and just calls trio.to_thread.run_sync() internally anyway.
            await trio.to_thread.run_sync(ofh.write, buf)
        else:
            ofh.write(buf)
        if update is not None:
            update(buf)


# TODO: Remove this after async transition is complete
class AbstractBackend:
    '''Synchronous wrapper for AsyncBackend.

    The name of this class is misleading, and the class only exists transitionally until
    all code has been converted to async.
    '''

    needs_login: bool
    known_options: set[str]

    def __init__(self, async_backend: AsyncBackend) -> None:
        self.async_backend = async_backend

    def __enter__(self: T2) -> T2:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        tb: object,
    ) -> Literal[False]:
        self.close()
        return False

    @property
    def has_delete_multi(self) -> bool:
        return self.async_backend.has_delete_multi

    def reset(self) -> None:
        run_async(self.async_backend.reset)

    def fetch(self, key: str) -> tuple[bytes, BasicMappingT]:
        return run_async(self.async_backend.fetch, key)

    def store(self, key: str, val: bytes, metadata: BasicMappingT | None = None) -> int:
        return run_async(self.async_backend.store, key, val, metadata)

    def lookup(self, key: str) -> BasicMappingT:
        return run_async(self.async_backend.lookup, key)

    def get_size(self, key: str) -> int:
        return run_async(self.async_backend.get_size, key)

    def contains(self, key: str) -> bool:
        return run_async(self.async_backend.contains, key)

    def delete(self, key: str) -> None:
        run_async(self.async_backend.delete, key)

    def delete_multi(self, keys: list[str]) -> None:
        run_async(self.async_backend.delete_multi, keys)

    def list(self, prefix: str = '') -> Iterator[str]:
        '''Collect all items from AsyncIterator, return sync Iterator.'''

        async def collect() -> list[str]:
            result = []
            async for key in self.async_backend.list(prefix):
                result.append(key)
            return result

        return iter(run_async(collect))

    def close(self) -> None:
        run_async(self.async_backend.close)

    def readinto_fh(self, key: str, fh: BinaryOutput) -> BasicMappingT:
        return run_async(self.async_backend.readinto_fh, key, fh)

    def write_fh(
        self,
        key: str,
        fh: BinaryInput,
        metadata: BasicMappingT | None = None,
        len_: int | None = None,
    ) -> int:
        return run_async(self.async_backend.write_fh, key, fh, metadata, len_)

    @property
    def is_temp_failure(self) -> Callable[[BaseException], bool]:
        return self.async_backend.is_temp_failure

    # Set by unit tests
    @is_temp_failure.setter
    def is_temp_failure(self, value: Callable[[BaseException], bool]) -> None:
        self.async_backend.is_temp_failure = value  # type: ignore[method-assign,assignment]

    @property
    def prefix(self) -> str:
        return self.async_backend.prefix  # type: ignore[attr-defined]

    @prefix.setter
    def prefix(self, value: str) -> None:
        self.async_backend.prefix = value  # type: ignore[attr-defined]

    @property
    def options(self) -> BackendOptionsProtocol:
        return self.async_backend.options  # type: ignore[attr-defined]

    @property
    def unittest_info(self) -> object:
        return self.async_backend.unittest_info  # type: ignore[attr-defined]

    # Set by unit tests
    @unittest_info.setter
    def unittest_info(self, value: object) -> None:
        self.async_backend.unittest_info = value  # type: ignore[attr-defined]

    @property
    def conn(self) -> object:
        return self.async_backend.conn  # type: ignore[attr-defined]
