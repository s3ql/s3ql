'''
common.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

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
from functools import wraps
from io import BytesIO
from typing import Any, BinaryIO, Dict, Optional

from ..logging import LOG_ONCE, QuietError

log = logging.getLogger(__name__)


class RateTracker:
    '''
    Maintain an average occurrence rate for events over a configurable time
    window. The rate is computed with one second resolution.
    '''

    def __init__(self, window_length):
        if not isinstance(window_length, int):
            raise ValueError('only integer window lengths are supported')

        self.buckets = [0] * window_length
        self.window_length = window_length
        self.last_update = int(time.monotonic())
        self.lock = threading.Lock()

    def register(self, _not_really=False):
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

    def get_rate(self):
        '''Return average rate of event occurrence'''

        self.register(_not_really=True)
        return sum(self.buckets) / len(self.buckets)

    def get_count(self):
        '''Return total number of events in window'''

        self.register(_not_really=True)
        return sum(self.buckets)


# We maintain a (global) running average of temporary errors, so
# that we can log a warning if this number becomes large. We
# use a relatively large window to prevent bogus spikes if
# multiple threads all have to retry after a long period of
# inactivity.
RETRY_TIMEOUT = 60 * 60 * 24


def retry(method, _tracker=RateTracker(60)):  # noqa: B008 # auto-added, needs manual check!
    '''Wrap *method* for retrying on some exceptions

    If *method* raises an exception for which the instance's `is_temp_failure(exc)` method is true,
    the *method* is called again at increasing intervals. If this persists for more than
    `RETRY_TIMEOUT` seconds, the most-recently caught exception is re-raised within a TimeoutError.
    If the method defines a keyword parameter *is_retry*, then this parameter will be set to True
    whenever the function is retried.
    '''

    if inspect.isgeneratorfunction(method):
        raise TypeError('Wrapping a generator function is pointless')

    sig = inspect.signature(method)
    has_is_retry = 'is_retry' in sig.parameters

    @wraps(method)
    def wrapped(*a, **kw):
        self = a[0]
        interval = 1 / 50
        waited = 0
        retries = 0
        while True:
            if has_is_retry:
                kw['is_retry'] = retries > 0
            try:
                return method(*a, **kw)
            except Exception as exc:
                # Access to protected member ok
                # pylint: disable=W0212
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
            time.sleep(interval * random.uniform(1, 1.5))
            waited += interval
            interval = min(5 * 60, 2 * interval)

    extend_docstring(
        wrapped,
        'This method has been wrapped and will automatically re-execute in '
        'increasing intervals for up to `s3ql.backends.common.RETRY_TIMEOUT` '
        'seconds if it raises an exception for which the instance\'s '
        '`is_temp_failure` method returns True.',
    )

    return wrapped


def extend_docstring(fun, s):
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


class AbstractBackend(metaclass=ABCMeta):
    '''Functionality shared between all backends.

    Instances behave similarly to dicts. They can be iterated over and
    indexed into, but raise a separate set of exceptions.
    '''

    needs_login = True

    def __init__(self):
        super().__init__()

    def __getitem__(self, key):
        return self.fetch(key)[0]

    def __setitem__(self, key, value):
        self.store(key, value)

    def __delitem__(self, key):
        self.delete(key)

    def __iter__(self):
        return self.list()

    def __contains__(self, key):
        return self.contains(key)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, tb):
        self.close()
        return False

    def iteritems(self):
        for key in self.list():
            yield (key, self[key])

    @property
    def has_delete_multi(self):
        '''True if the backend supports `delete_multi`.'''

        return False

    def reset(self):  # noqa: B027 # auto-added, needs manual check!
        '''Reset backend

        This resets the backend and ensures that it is ready to process requests. In most cases,
        this method does nothing. However, if e.g. a previous request was not send completely or
        response not read completely because of an exception, the `reset` method will make sure that
        any underlying connection is properly closed.
        '''

        pass

    @abstractmethod
    def readinto_fh(self, key: str, fh: BinaryIO):
        '''Transfer data stored under *key* into *fh*, return metadata.

        The data will be inserted at the current offset. If a temporary error (as defined by
        `is_temp_failure`) occurs, the operation is retried.
        '''
        pass

    @abstractmethod
    def write_fh(
        self,
        key: str,
        fh: BinaryIO,
        metadata: Optional[Dict[str, Any]] = None,
        len_: Optional[int] = None,
    ):
        '''Upload *len_* bytes from *fh* under *key*.

        The data will be read at the current offset. If *len_* is None, reads until the
        end of the file.

        If a temporary error (as defined by `is_temp_failure`) occurs, the operation is
        retried.  Returns the size of the resulting storage object (which may be less due
        to compression)
        '''
        pass

    def fetch(self, key):
        """Return data stored under `key`.

        Returns a tuple with the data and metadata. If only the data itself is
        required, ``backend[key]`` is a more concise notation for
        ``backend.fetch(key)[0]``.
        """

        fh = BytesIO()
        metadata = self.readinto_fh(key, fh)
        return (fh.getvalue(), metadata)

    def store(self, key, val, metadata=None):
        """Store data under `key`.

        `metadata` can be mapping with additional attributes to store with the
        object. Keys have to be of type `str`, values have to be of elementary
        type (`str`, `bytes`, `int`, `float` or `bool`).

        If no metadata is required, one can simply assign to the subscripted
        backend instead of using this function: ``backend[key] = val`` is
        equivalent to ``backend.store(key, val)``.
        """

        return self.write_fh(key, BytesIO(val), metadata)

    @abstractmethod
    def is_temp_failure(self, exc):
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
    def lookup(self, key):
        """Return metadata for given key.

        If the key does not exist, `NoSuchObject` is raised.
        """

        pass

    @abstractmethod
    def get_size(self, key):
        '''Return size of object stored under *key*'''
        pass

    def contains(self, key):
        '''Check if `key` is in backend'''

        try:
            self.lookup(key)
        except NoSuchObject:
            return False
        else:
            return True

    @abstractmethod
    def delete(self, key):
        """Delete object stored under `key`

        Attempts to delete non-existing objects will silently succeed.
        """
        pass

    def delete_multi(self, keys):
        """Delete objects stored under `keys`

        Deleted objects are removed from the *keys* list, so that the caller can
        determine which objects have not yet been processed if an exception is
        occurs.

        Attempts to delete non-existing objects will silently succeed.
        """

        while keys:
            self.delete(keys[-1])
            keys.pop()

    @abstractmethod
    def list(self, prefix=''):
        '''List keys in backend

        Returns an iterator over all keys in the backend.
        '''
        pass

    def close(self):  # noqa: B027 # auto-added, needs manual check!
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

    def __init__(self, key):
        super().__init__()
        self.key = key

    def __str__(self):
        return 'Backend does not have anything stored under key %r' % self.key


class DanglingStorageURLError(Exception):
    '''Raised if the backend can't store data at the given location'''

    def __init__(self, loc, msg=None):
        super().__init__()
        self.loc = loc
        self.msg = msg

    def __str__(self):
        if self.msg is None:
            return '%r does not exist' % self.loc
        else:
            return self.msg


class AuthorizationError(Exception):
    '''Raised if the credentials don't give access to the requested backend'''

    def __init__(self, msg):
        super().__init__()
        self.msg = msg

    def __str__(self):
        return 'Access denied. Server said: %s' % self.msg


class AuthenticationError(Exception):
    '''Raised if the credentials are invalid'''

    def __init__(self, msg):
        super().__init__()
        self.msg = msg

    def __str__(self):
        return 'Access denied. Server said: %s' % self.msg


class CorruptedObjectError(Exception):
    """
    Raised if a storage object is corrupted.

    Note that this is different from BadDigest error, which is raised
    if a transmission error has been detected.
    """

    def __init__(self, str_):
        super().__init__()
        self.str = str_

    def __str__(self):
        return self.str


def get_ssl_context(path):
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


def get_proxy(ssl):
    '''Read system proxy settings

    Returns either `None`, or a tuple ``(host, port)``.

    This function may raise `QuietError`.
    '''

    if ssl:
        proxy_env = 'https_proxy'
    else:
        proxy_env = 'http_proxy'

    if proxy_env in os.environ:
        proxy = os.environ[proxy_env]
        hit = re.match(r'^(https?://)?([a-zA-Z0-9.-]+)(:[0-9]+)?/?$', proxy)
        if not hit:
            raise QuietError(
                'Unable to parse proxy setting %s=%r' % (proxy_env, proxy), exitcode=13
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
        proxy = (proxy_host, proxy_port)
    else:
        proxy = None

    return proxy


def checksum_basic_mapping(metadata, key=None):
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
