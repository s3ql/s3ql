'''
common.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from ..logging import logging, LOG_ONCE # Ensure use of custom logger class
from ..common import QuietError, BUFSIZE, PICKLE_PROTOCOL, ChecksumError, sha256
from ..inherit_docstrings import (copy_ancestor_docstring, prepend_ancestor_docstring,
                                  ABCDocstMeta)
from Crypto.Cipher import AES
from Crypto.Util import Counter
from abc import abstractmethod, ABCMeta
from base64 import b64decode, b64encode
from contextlib import contextmanager
from functools import wraps
from getpass import getpass
from io import BytesIO
import bz2
import ssl
import configparser
import hashlib
import hmac
import http.client
import lzma
import os
import pickle
import io
import re
import socket
import stat
import struct
import sys
import threading
import time
import zlib
import textwrap
import inspect

log = logging.getLogger(__name__)

HMAC_SIZE = 32

RETRY_TIMEOUT = 60 * 60 * 24
def retry(method):
    '''Wrap *method* for retrying on some exceptions
    
    If *method* raises an exception for which the instance's
    `is_temp_failure(exc)` method is true, the *method* is called again
    at increasing intervals. If this persists for more than `RETRY_TIMEOUT`
    seconds, the most-recently caught exception is re-raised.
    '''

    if inspect.isgeneratorfunction(method):
        raise TypeError('Wrapping a generator function is pointless')
    
    @wraps(method)
    def wrapped(*a, **kw):
        self = a[0]
        interval = 1 / 50
        waited = 0
        retries = 0
        while True:
            try:
                return method(*a, **kw)
            except Exception as exc:
                # Access to protected member ok
                #pylint: disable=W0212
                if not self.is_temp_failure(exc):
                    raise
                if waited > RETRY_TIMEOUT:
                    log.error('%s.%s(*): Timeout exceeded, re-raising %r exception',
                            self.__class__.__name__, method.__name__, exc)
                    raise

                retries += 1
                if retries <= 2:
                    log_fn = log.debug
                elif retries <= 4:
                    log_fn = log.info
                else:
                    log_fn = log.warning
                    
                log_fn('Encountered %s exception (%s), retrying call to %s.%s for the %d-th time...',
                       type(exc).__name__, exc, self.__class__.__name__, method.__name__, retries)

                if hasattr(exc, 'retry_after') and exc.retry_after:
                    log.debug('retry_after is %.2f seconds', exc.retry_after)
                    interval = exc.retry_after
                
            time.sleep(interval)
            waited += interval
            interval = min(5*60, 2*interval)

    extend_docstring(wrapped,
                     'This method has been wrapped and will automatically re-execute in '
                     'increasing intervals for up to `s3ql.backends.common.RETRY_TIMEOUT` '
                     'seconds if it raises an exception for which the instance\'s '
                     '`is_temp_failure` method returns True.')

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
    fun.__doc__ += ''.join(indent_s + line
                               for line in textwrap.wrap(s, width=80 - indent))
    fun.__doc__ += '\n'


class RetryIterator:
    '''
    A RetryIterator instance iterates over the elements produced by any
    generator function passed to its constructor, i.e. it wraps the iterator
    obtained by calling the generator function.  When retrieving elements from the
    wrapped iterator, exceptions may occur. Most such exceptions are
    propagated. However, exceptions for which the *is_temp_failure_fn* function
    returns True are caught. If that happens, the wrapped iterator is replaced
    by a new one obtained by calling the generator function again with the
    *start_after* parameter set to the last element that was retrieved before
    the exception occured.

    If attempts to retrieve the next element fail repeatedly, the iterator is
    replaced only after sleeping for increasing intervals. If no new element can
    be obtained after `RETRY_TIMEOUT` seconds, the last exception is no longer
    caught but propagated to the caller. This behavior is implemented by
    wrapping the __next__ method with the `retry` decorator.
    '''

    def __init__(self, generator, is_temp_failure_fn, args=(), kwargs=None):
        if not inspect.isgeneratorfunction(generator):
            raise TypeError('*generator* must be generator function')
        
        self.generator = generator
        self.iterator = None
        self.is_temp_failure = is_temp_failure_fn
        if kwargs is None:
            kwargs = {}
        self.kwargs = kwargs
        self.args = args
        
    def __iter__(self):
        return self

    @retry
    def __next__(self):
        if self.iterator is None:
            self.iterator = self.generator(*self.args, **self.kwargs)

        try:
            el = next(self.iterator)
        except Exception as exc:
            if self.is_temp_failure(exc):
                self.iterator = None
            raise
            
        self.kwargs['start_after'] = el
        return el


def retry_generator(method):
    '''Wrap *method* in a `RetryIterator`

    *method* must return a generator, and accept a keyword argument
    *start_with*. The RetryIterator's `is_temp_failure` attribute
    will be set to the `is_temp_failure` method of the instance
    to which *method* is bound.
    '''

    @wraps(method)
    def wrapped(*a, **kw):
        return RetryIterator(method, a[0].is_temp_failure, args=a, kwargs=kw)

    extend_docstring(wrapped,
                     'This generator method has been wrapped and will return a '
                     '`RetryIterator` instance.')

    return wrapped


def is_temp_network_error(exc):
    '''Return true if *exc* represents a potentially temporary network problem'''

    if isinstance(exc, (http.client.IncompleteRead, socket.timeout,
                        ssl.SSLZeroReturnError, ConnectionError, TimeoutError,
                        InterruptedError, ssl.SSLEOFError, ssl.SSLSyscallError)):
        return True
     
    # Server closed connection
    elif (isinstance(exc, http.client.BadStatusLine)
          and (not exc.line or exc.line == "''")):
        return True

    # Formally this is a permanent error. However, it may also indicate
    # that there is currently no network connection to the DNS server
    elif (isinstance(exc, socket.gaierror) 
          and exc.errno in (socket.EAI_AGAIN, socket.EAI_NONAME)):
        return True 
              
    return False
    
    
def http_connection(hostname, port=None, ssl_context=None):
    '''Return http connection to *hostname*:*port*
    
    This method honors the http_proxy and https_proxy environment
    variables. However, it uses CONNECT-style proxying for both http and https
    connections, so proxied http connections may not work with all proxy
    servers.

    If *port* is None, it defaults to 80 or 443, depending on *ssl_context*.
    '''
    
    log.debug('Connecting to %s...', hostname)

    if port is None:
        if ssl_context is None:
            port = 80
        else:
            port = 443
            
    if ssl_context is None:
        proxy_env = 'http_proxy'
    else:
        proxy_env = 'https_proxy'
            
    if proxy_env in os.environ:
        proxy = os.environ[proxy_env]
        hit = re.match(r'^(https?://)?([a-zA-Z0-9.-]+)(:[0-9]+)?/?$', proxy)
        if not hit:
            raise QuietError('Unable to parse proxy setting %s=%r' %
                             (proxy_env, proxy))
        
        if hit.group(1) == 'https://':
            log.warning('HTTPS connection to proxy is probably pointless and not supported, '
                        'will use standard HTTP', extra=LOG_ONCE)
        
        if hit.group(3):
            proxy_port = int(hit.group(3)[1:])
        else:
            proxy_port = 80
            
        proxy_host = hit.group(2)
        log.info('Using CONNECT proxy %s:%d', proxy_host, proxy_port,
                 extra=LOG_ONCE)
        
        if ssl_context:
            conn = http.client.HTTPSConnection(proxy_host, proxy_port,
                                               context=ssl_context)
        else:
            conn = http.client.HTTPConnection(proxy_host, proxy_port)
        conn.set_tunnel(hostname, port)
        return conn
    
    elif ssl_context:
        return http.client.HTTPSConnection(hostname, port, context=ssl_context)
    else:
        return http.client.HTTPConnection(hostname, port)


def fix_python_bug_7776():    
    '''Install workaround for http://bugs.python.org/issue7776'''

    if sys.version_info > (3,4):
        return

    from http.client import HTTPConnection, HTTPSConnection
    
    init_old = HTTPConnection.__init__ 
    def __init__(self, *a, **kw):
        init_old(self, *a, **kw)
        self.host_real = self.host
        self.port_real = self.port
    HTTPConnection.__init__ = __init__

    connect_old = HTTPConnection.connect
    def connect(self, *a, **kw):
        self.host = self.host_real
        self.port = self.port_real
        return connect_old(self, *a, **kw)
    HTTPConnection.connect = connect

    connect_old_https = HTTPSConnection.connect
    def connect(self, *a, **kw):
        self.host = self.host_real
        self.port = self.port_real
        return connect_old_https(self, *a, **kw)
    HTTPSConnection.connect = connect
    
    set_tunnel_old = HTTPConnection.set_tunnel
    def set_tunnel(self, *a, **kw):
        set_tunnel_old(self, *a, **kw)
        self._set_hostport(self._tunnel_host, self._tunnel_port)
    HTTPConnection.set_tunnel = set_tunnel


def get_ssl_context(options):
    '''Construct SSLContext object from *options*

    If SSL is disabled, return None.
    '''

    if options.no_ssl:
        return None
    
    # Best practice according to http://docs.python.org/3/library/ssl.html#protocol-versions
    context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
    context.options |= ssl.OP_NO_SSLv2
    context.verify_mode = ssl.CERT_REQUIRED

    path = options.ssl_ca_path
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


class BackendPool(object):
    '''A pool of backends

    This class is threadsafe. All methods (except for internal methods
    starting with underscore) may be called concurrently by different
    threads.    
    '''

    def __init__(self, factory):
        '''Init pool
        
        *factory* should be a callable that provides new
        connections.
        '''

        self.factory = factory
        self.pool = []
        self.lock = threading.Lock()

    def pop_conn(self):
        '''Pop connection from pool'''

        with self.lock:
            if self.pool:
                return self.pool.pop()
            else:
                return self.factory()

    def push_conn(self, conn):
        '''Push connection back into pool'''

        conn.reset()
        with self.lock:
            self.pool.append(conn)

    def flush(self):
        '''Close all backends in pool

        This method calls the `close` method on all backends
        currently in the pool.
        '''
        with self.lock:
            while self.pool:
                self.pool.pop().close()
        
    @contextmanager
    def __call__(self, close=False):
        '''Provide connection from pool (context manager)

        If *close* is True, the backend's close method is automatically called
        (which frees any allocated resources, but may slow down reuse of the
        backend object).
        '''

        conn = self.pop_conn()
        try:
            yield conn
        finally:
            if close:
                conn.close()
            self.push_conn(conn)


class AbstractBackend(object, metaclass=ABCMeta):
    '''Functionality shared between all backends.
    
    Instances behave similarly to dicts. They can be iterated over and
    indexed into, but raise a separate set of exceptions.
    
    The backend guarantees get after create consistency, i.e. a newly created
    object will be immediately retrievable. Additional consistency guarantees
    may or may not be available and can be queried for with instance methods.
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

    def  __contains__(self, key):
        return self.contains(key)

    def iteritems(self):
        for key in self.list():
            yield (key, self[key])

    def reset(self):
        '''Reset backend

        This resets the backend and ensures that it is ready to process
        requests. In most cases, this method does nothing. However, if e.g. a
        file handle returned by a previous call to `open_read` was not properly
        closed (e.g. because an exception happened during reading), the `reset`
        method will make sure that any underlying connection is properly closed.

        Obviously, this method must not be called while any file handles
        returned by the backend are still in use.
        '''

        pass

    @retry
    def perform_read(self, fn, key):
        '''Read object data using *fn*, retry on temporary failure
        
        Open object for reading, call `fn(fh)` and close object. If a temporary
        error (as defined by `is_temp_failure`) occurs during opening, closing
        or execution of *fn*, the operation is retried.
        '''
        with self.open_read(key) as fh:
            return fn(fh)

    @retry
    def perform_write(self, fn, key, metadata=None, is_compressed=False):
        '''Read object data using *fn*, retry on temporary failure
        
        Open object for writing, call `fn(fh)` and close object. If a temporary
        error (as defined by `is_temp_failure`) occurs during opening, closing
        or execution of *fn*, the operation is retried.
        '''

        with self.open_write(key, metadata, is_compressed) as fh:
            return fn(fh)

    def fetch(self, key):
        """Return data stored under `key`.

        Returns a tuple with the data and metadata. If only the data itself is
        required, ``backend[key]`` is a more concise notation for
        ``backend.fetch(key)[0]``.
        """

        def do_read(fh):
            data = fh.read()
            return (data, fh.metadata)

        return self.perform_read(do_read, key)

    def store(self, key, val, metadata=None):
        """Store data under `key`.

        `metadata` can be a dict of additional attributes to store with the
        object.

        If no metadata is required, one can simply assign to the subscripted
        backend instead of using this function: ``backend[key] = val`` is
        equivalent to ``backend.store(key, val)``.
        """

        self.perform_write(lambda fh: fh.write(val), key, metadata)

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

    @abstractmethod
    def open_read(self, key):
        """Open object for reading

        Return a file-like object. Data can be read using the `read`
        method. metadata is stored in its *metadata* attribute and can be
        modified by the caller at will. The object must be closed explicitly.
        """

        pass

    @abstractmethod
    def open_write(self, key, metadata=None, is_compressed=False):
        """Open object for writing

        `metadata` can an additional (pickle-able) python object to store with
        the data. Returns a file- like object. The object must be closed closed
        explicitly. After closing, the *get_obj_size* may be used to retrieve
        the size of the stored object (which may differ from the size of the
        written data).

        The *is_compressed* parameter indicates that the caller is going to
        write compressed data, and may be used to avoid recompression by the
        backend.
        """

        pass

    @abstractmethod
    def clear(self):
        """Delete all objects in backend"""
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
    def delete(self, key, force=False):
        """Delete object stored under `key`

        ``backend.delete(key)`` can also be written as ``del backend[key]``.  If
        `force` is true, do not return an error if the key does not exist. Note,
        however, that even if *force* is False, it is not guaranteed that an
        attempt to delete a non-existing object will raise an error.
        """
        pass

    def delete_multi(self, keys, force=False):
        """Delete objects stored under `keys`

        Deleted objects are removed from the *keys* list, so that the caller can
        determine which objects have not yet been processed if an exception is
        occurs.
        
        If *force* is True, attempts to delete non-existing objects will
        succeed. Note, however, that even if *force* is False, it is not
        guaranteed that an attempt to delete a non-existing object will raise an
        error.
        """

        if not isinstance(keys, list):
            raise TypeError('*keys* parameter must be a list')

        for (i, key) in enumerate(keys):
            try:
                self.delete(key, force=force)
            except:
                del keys[:i]
                raise

        del keys[:]
    
    @abstractmethod
    def list(self, prefix=''):
        '''List keys in backend

        Returns an iterator over all keys in the backend.
        '''
        pass

    @abstractmethod
    def copy(self, src, dest):
        """Copy data stored under key `src` to key `dest`
        
        If `dest` already exists, it will be overwritten. The copying
        is done on the remote side. 
        """

        pass

    def rename(self, src, dest):
        """Rename key `src` to `dest`
        
        If `dest` already exists, it will be overwritten. The rename
        is done on the remote side.
        """

        self.copy(src, dest)
        self.delete(src)

    def close(self):
        '''Close any opened resources

        This method closes any resources allocated by the backend (e.g. network
        sockets). This method should be called explicitly before a backend
        object is garbage collected. The backend object may be re-used after
        `close` has been called, in this case the necessary resources are
        transparently allocated again.
        '''
        
        pass
    
class BetterBackend(AbstractBackend, metaclass=ABCDocstMeta):
    '''
    This class adds encryption, compression and integrity protection to a plain
    backend.
    '''

    def __init__(self, passphrase, compression, backend):
        super().__init__()

        assert passphrase is None or isinstance(passphrase, (bytes, bytearray, memoryview))
        
        self.passphrase = passphrase
        self.compression = compression
        self.backend = backend

        if (compression[0] not in ('bzip2', 'lzma', 'zlib', None)
            or compression[1] not in range(10)):
            raise ValueError('Unsupported compression: %s' % compression)

    @copy_ancestor_docstring
    def reset(self):
        self.backend.reset()

    @copy_ancestor_docstring
    def lookup(self, key):
        metadata = self.backend.lookup(key)
        convert_legacy_metadata(metadata)
        return self._unwrap_meta(metadata)

    @prepend_ancestor_docstring
    def get_size(self, key):
        '''
        This method returns the compressed size, i.e. the storage space
        that's actually occupied by the object.
        '''

        return self.backend.get_size(key)

    @copy_ancestor_docstring
    def is_temp_failure(self, exc):
        return self.backend.is_temp_failure(exc)

    def _unwrap_meta(self, metadata):
        '''Unwrap metadata
        
        If the backend has a password set but the object is not encrypted,
        `ObjectNotEncrypted` is raised.
        '''

        if (not isinstance(metadata, dict)
            or 'encryption' not in metadata
            or 'compression' not in metadata):
            raise MalformedObjectError()

        encr_alg = metadata['encryption']
        encrypted = (encr_alg != 'None')

        if encrypted and self.passphrase is None:
            raise ChecksumError('Encrypted object and no passphrase supplied')

        elif not encrypted and self.passphrase is not None:
            raise ObjectNotEncrypted()

        # Pre 2.x buckets
        if any(k.startswith('meta') for k in metadata):
            parts = [ metadata[k] for k in sorted(metadata.keys())
                      if k.startswith('meta') ]
            buf = b64decode(''.join(parts))
        else:
            try:
                buf = metadata['data']
            except KeyError:
                raise MalformedObjectError()
            buf = b64decode(buf)

        if encrypted:
            buf = decrypt(buf, self.passphrase)

        try:
            metadata = pickle.loads(buf)
        except pickle.UnpicklingError as exc:
            if (isinstance(exc.args[0], str)
                and exc.args[0].startswith('invalid load key')):
                raise ChecksumError('Invalid metadata')
            raise

        return metadata

    @prepend_ancestor_docstring
    def open_read(self, key):
        """
        If the backend has a password set but the object is not encrypted,
        `ObjectNotEncrypted` is raised.
        """

        fh = self.backend.open_read(key)
        try:
            convert_legacy_metadata(fh.metadata)

            # Also checks if this is a BetterBucket storage object
            metadata = self._unwrap_meta(fh.metadata)

            compr_alg = fh.metadata['compression']
            encr_alg = fh.metadata['encryption']
    
            if compr_alg == 'BZIP2':
                decompressor = bz2.BZ2Decompressor()
            elif compr_alg == 'LZMA':
                decompressor = lzma.LZMADecompressor()
            elif compr_alg == 'ZLIB':
                decompressor = zlib.decompressobj()
            elif compr_alg == 'None':
                decompressor = None
            else:
                raise RuntimeError('Unsupported compression: %s' % compr_alg)
    
            if encr_alg == 'AES':
                fh = LegacyDecryptDecompressFilter(fh, self.passphrase, decompressor)
            else:
                if encr_alg == 'AES_v2':
                    fh = DecryptFilter(fh, self.passphrase)
                elif encr_alg != 'None':
                    raise RuntimeError('Unsupported encryption: %s' % encr_alg)
    
                if decompressor:
                    fh = DecompressFilter(fh, decompressor)
    
            fh.metadata = metadata
        except:
            fh.close()
            raise
        
        return fh

    @copy_ancestor_docstring
    def open_write(self, key, metadata=None, is_compressed=False):

        # We always store metadata (even if it's just None), so that we can
        # verify that the object has been created by us when we call lookup().
        meta_buf = pickle.dumps(metadata, PICKLE_PROTOCOL)

        meta_raw = dict()

        if self.passphrase is not None:
            meta_raw['encryption'] = 'AES_v2'
            nonce = struct.pack('<f', time.time()) + key.encode('utf-8')
            meta_raw['data'] = b64encode(encrypt(meta_buf, self.passphrase, nonce))
        else:
            meta_raw['encryption'] = 'None'
            meta_raw['data'] = b64encode(meta_buf)
            nonce = None

        if is_compressed or self.compression[0] is None:
            compr = None
            meta_raw['compression'] = 'None'
        elif self.compression[0] == 'zlib':
            compr = zlib.compressobj(self.compression[1])
            meta_raw['compression'] = 'ZLIB'
        elif self.compression[0] == 'bzip2':
            compr = bz2.BZ2Compressor(self.compression[1])
            meta_raw['compression'] = 'BZIP2'
        elif self.compression[0] == 'lzma':
            compr = lzma.LZMACompressor(preset=self.compression[1])
            meta_raw['compression'] = 'LZMA'

        fh = self.backend.open_write(key, meta_raw)

        if nonce:
            fh = EncryptFilter(fh, self.passphrase, nonce)
        if compr:
            fh = CompressFilter(fh, compr)

        return fh


    @copy_ancestor_docstring
    def clear(self):
        return self.backend.clear()

    @copy_ancestor_docstring
    def contains(self, key):
        return self.backend.contains(key)

    @copy_ancestor_docstring
    def delete(self, key, force=False):
        return self.backend.delete(key, force)

    @copy_ancestor_docstring
    def delete_multi(self, keys, force=False):
        return self.backend.delete_multi(keys, force=force)
    
    @copy_ancestor_docstring
    def list(self, prefix=''):
        return self.backend.list(prefix)

    @copy_ancestor_docstring
    def copy(self, src, dest):
        return self.backend.copy(src, dest)

    @copy_ancestor_docstring
    def rename(self, src, dest):
        return self.backend.rename(src, dest)

    @copy_ancestor_docstring
    def close(self):
        self.backend.close()


class CompressFilter(object):
    '''Compress data while writing'''

    def __init__(self, fh, compr):
        '''Initialize
        
        *fh* should be a file-like object. *decomp* should be a fresh compressor
        instance with a *compress* method.
        '''
        super().__init__()

        self.fh = fh
        self.compr = compr
        self.obj_size = 0
        self.closed = False

    def write(self, data):
        '''Write *data*'''

        buf = self.compr.compress(data)
        if buf:
            self.fh.write(buf)
            self.obj_size += len(buf)

    def close(self):
        assert not self.closed
        buf = self.compr.flush()
        if buf:
            self.fh.write(buf)
            self.obj_size += len(buf)
        self.fh.close()
        self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()
        return False

    def get_obj_size(self):
        if not self.closed:
            raise RuntimeError('Object must be closed first.')
        return self.obj_size

class InputFilter(io.RawIOBase):

    def readable(self):
        return True

    def readinto(self, buf):
        var = self.read(len(buf))
        buf[:len(var)] = var
        return var

    def read(self, size=-1):
        if size == -1:
            return self.readall()
        elif size == 0:
            return b''

        b = bytearray(size)
        len_ = self.readinto(b)
        return b[:len_]
    
class DecompressFilter(InputFilter):
    '''Decompress data while reading'''

    def __init__(self, fh, decomp, metadata=None):
        '''Initialize
        
        *fh* should be a file-like object and may be unbuffered. *decomp* should
        be a fresh decompressor instance with a *decompress* method.
        '''
        super().__init__()

        self.fh = fh
        self.decomp = decomp
        self.metadata = metadata
            
    def read(self, size=-1):
        '''Read up to *size* bytes

        This method is currently buggy and may also return *more*
        than *size* bytes. Callers should be prepared to handle
        that. This is because some of the used (de)compression modules
        don't support output limiting.
        '''

        if size == -1:
            return self.readall()
        elif size == 0:
            return b''
        
        buf = b''
        while not buf:
            buf = self.fh.read(size)
            if not buf:
                if not self.decomp.eof:
                    raise ChecksumError('Premature end of stream.')
                if self.decomp.unused_data:
                    raise ChecksumError('Data after end of compressed stream')
                return b''

            try:
                buf = decompress(self.decomp, buf)
            except ChecksumError:
                # Still read the stream completely (so that in case of
                # an encrypted stream we check the HMAC).
                while True:
                    buf = self.fh.read(BUFSIZE)
                    if not buf:
                        break
                raise
            
        return buf

    def close(self):
        self.fh.close()

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()
        return False


class EncryptFilter(object):
    '''Encrypt data while writing'''

    def __init__(self, fh, passphrase, nonce):
        '''Initialize
        
        *fh* should be a file-like object.
        '''
        super().__init__()

        self.fh = fh
        self.obj_size = 0
        self.closed = False

        self.key = sha256(passphrase + nonce)
        self.cipher = aes_cipher(self.key)
        self.hmac = hmac.new(self.key, digestmod=hashlib.sha256)

        self.fh.write(struct.pack(b'<B', len(nonce)))
        self.fh.write(nonce)

        self.obj_size += len(nonce) + 1

    def write(self, data):
        '''Write *data*
        
        len(data) must be < 2**32.
    
        Every invocation of `write` generates a packet that contains both the
        length of the data and the data, so the passed data should have
        reasonable size (if the data is written in e.g. 4 byte chunks, it is
        blown up by 100%)
        '''

        if len(data) == 0:
            return

        buf = struct.pack(b'<I', len(data)) + data
        self.hmac.update(buf)
        buf2 = self.cipher.encrypt(buf)
        assert len(buf2) == len(buf)
        self.fh.write(buf2)
        self.obj_size += len(buf2)

    def close(self):
        assert not self.closed
        
        # Packet length of 0 indicates end of stream, only HMAC follows
        buf = struct.pack(b'<I', 0)
        self.hmac.update(buf)
        buf += self.hmac.digest()
        buf2 = self.cipher.encrypt(buf)
        assert len(buf) == len(buf2)
        self.fh.write(buf2)
        self.obj_size += len(buf2)
        self.fh.close()
        self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()
        return False

    def get_obj_size(self):
        if not self.closed:
            raise RuntimeError('Object must be closed first.')
        return self.obj_size


class DecryptFilter(InputFilter):
    '''Decrypt data while reading
    
    Reader has to read the entire stream in order for HMAC
    checking to work.
    '''

    def __init__(self, fh, passphrase, metadata=None):
        '''Initialize
        
        *fh* should be a file-like object that may be unbuffered.
        '''
        super().__init__()

        self.fh = fh
        self.off_size = struct.calcsize(b'<I')
        self.remaining = 0 # Remaining length of current packet
        self.metadata = metadata
        self.hmac_checked = False
            
        # Read nonce
        len_ = struct.unpack(b'<B', fh.read(struct.calcsize(b'<B')))[0]
        nonce = fh.read(len_)

        key = sha256(passphrase + nonce)
        self.cipher = aes_cipher(key)
        self.hmac = hmac.new(key, digestmod=hashlib.sha256)

    def _read_and_decrypt(self, size):
        '''Read and decrypt up to *size* bytes'''

        if size <= 0:
            raise ValueError("Exact *size* required (got %d)" % size)
        
        buf = self.fh.read(size)
        if not buf:
            if self.hmac_checked:
                return b''
            else:
                raise ChecksumError('Premature end of stream.')

        # Work around https://bugs.launchpad.net/pycrypto/+bug/1256172
        # cipher.decrypt refuses to work with anything but bytes
        if not isinstance(buf, bytes):
            buf = bytes(buf)
            
        len_ = len(buf)
        buf = self.cipher.decrypt(buf)
        assert len(buf) == len_
        
        return buf
    
    def read(self, size=-1):
        '''Read up to *size* bytes'''

        if size == -1:
            return self.readall()
        elif size == 0:
            return b''
        
        outbuf = b''
        inbuf = b''
        while True:
            
            # If all remaining data is part of the same packet, return it.
            if inbuf and len(inbuf) <= self.remaining:
                self.remaining -= len(inbuf)
                self.hmac.update(inbuf)
                outbuf += inbuf
                break
            
            # Otherwise keep reading until we have something to return
            # but make sure not to stop in packet header (so that we don't
            # cache the partially read header from one invocation to the next).
            to_next = self.remaining + self.off_size
            if (not inbuf or len(inbuf) < to_next):
                if not inbuf:
                    buf = self._read_and_decrypt(size - len(outbuf))
                    if not buf:
                        break
                else:
                    buf = self._read_and_decrypt(to_next - len(inbuf))
                    assert buf
                inbuf += buf
                continue
            
            # Copy rest of current packet to output and start reading
            # from next packet
            outbuf += inbuf[:self.remaining]
            self.hmac.update(inbuf[:to_next])
            paket_size = struct.unpack(b'<I', inbuf[self.remaining:to_next])[0]
            inbuf = inbuf[to_next:]
            self.remaining = paket_size

            # End of file, read and check HMAC
            if paket_size == 0:
                while len(inbuf) < HMAC_SIZE:
                    # Don't read exactly the missing amount, we wan't to detect
                    # if there's extraneous data
                    buf = self._read_and_decrypt(HMAC_SIZE)
                    assert buf
                    inbuf += buf
                
                if len(inbuf) > HMAC_SIZE or self.fh.read(1):
                    raise ChecksumError('Extraneous data at end of object')
                
                if not hmac.compare_digest(inbuf, self.hmac.digest()):
                    raise ChecksumError('HMAC mismatch')
                self.hmac_checked = True
                break

        return outbuf

    def close(self):
        self.fh.close()

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()
        return False

class LegacyDecryptDecompressFilter(io.RawIOBase):
    '''Decrypt and Decompress data while reading
    
    Reader has to read the entire stream in order for HMAC
    checking to work.
    '''

    def __init__(self, fh, passphrase, decomp, metadata=None):
        '''Initialize
        
        *fh* should be a file-like object and may be unbuffered.
        '''
        super().__init__()

        self.fh = fh
        self.metadata = metadata
        self.decomp = decomp
        self.hmac_checked = False

        # Read nonce
        len_ = struct.unpack(b'<B', fh.read(struct.calcsize(b'<B')))[0]
        nonce = fh.read(len_)
        self.hash = fh.read(HMAC_SIZE)

        key = sha256(passphrase + nonce)
        self.cipher = aes_cipher(key)
        self.hmac = hmac.new(key, digestmod=hashlib.sha256)

    def _decrypt(self, buf):
        # Work around https://bugs.launchpad.net/pycrypto/+bug/1256172
        # cipher.decrypt refuses to work with anything but bytes
        if not isinstance(buf, bytes):
            buf = bytes(buf)
            
        len_ = len(buf)
        buf = self.cipher.decrypt(buf)
        assert len(buf) == len_
        return buf
    
    def read(self, size=-1):
        '''Read up to *size* bytes
        
        This method is currently buggy and may also return *more*
        than *size* bytes. Callers should be prepared to handle
        that. This is because some of the used (de)compression modules
        don't support output limiting.
        '''

        if size == -1:
            return self.readall()
        elif size == 0:
            return b''

        buf = None
        while not buf:
            buf = self.fh.read(size)
            if not buf and not self.hmac_checked:
                if not hmac.compare_digest(self._decrypt(self.hash), 
                                           self.hmac.digest()):
                    raise ChecksumError('HMAC mismatch')
                elif self.decomp and self.decomp.unused_data:
                    raise ChecksumError('Data after end of compressed stream')
                else:
                    self.hmac_checked = True
                    return b''
            elif not buf:
                return b''

            buf = self._decrypt(buf)
            if not self.decomp:
                break

            buf = decompress(self.decomp, buf)

        self.hmac.update(buf)
        return buf

    def close(self):
        self.fh.close()

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()
        return False


def decompress(decomp, buf):
    '''Decompress *buf* using *decomp*

    This method encapsulates exception handling for different
    decompressors.
    '''

    try:
        return decomp.decompress(buf)
    except IOError as exc:
        if exc.args[0].lower().startswith('invalid data stream'):
            raise ChecksumError('Invalid compressed stream')
        raise
    except lzma.LZMAError as exc:
        if exc.args[0].lower().startswith('corrupt input data'):
            raise ChecksumError('Invalid compressed stream')
        raise
    except zlib.error as exc:
        if exc.args[0].lower().startswith('error -3 while decompressing'):
            raise ChecksumError('Invalid compressed stream')
        raise


def encrypt(buf, passphrase, nonce):
    '''Encrypt *buf*'''

    key = sha256(passphrase + nonce)
    cipher = aes_cipher(key) 
    hmac_ = hmac.new(key, digestmod=hashlib.sha256)

    hmac_.update(buf)
    buf = cipher.encrypt(buf)
    hash_ = cipher.encrypt(hmac_.digest())

    return b''.join(
                    (struct.pack(b'<B', len(nonce)),
                    nonce, hash_, buf))

def decrypt(buf, passphrase):
    '''Decrypt *buf'''

    fh = BytesIO(buf)

    len_ = struct.unpack(b'<B', fh.read(struct.calcsize(b'<B')))[0]
    nonce = fh.read(len_)

    key = sha256(passphrase + nonce)
    cipher = aes_cipher(key) 
    hmac_ = hmac.new(key, digestmod=hashlib.sha256)

    # Read (encrypted) hmac
    hash_ = fh.read(HMAC_SIZE)

    buf = fh.read()
    buf = cipher.decrypt(buf)
    hmac_.update(buf)

    hash_ = cipher.decrypt(hash_)

    if not hmac.compare_digest(hash_, hmac_.digest()):
        raise ChecksumError('HMAC mismatch')

    return buf

class ObjectNotEncrypted(Exception):
    '''
    Raised by the backend if an object was requested from an encrypted
    backend, but the object was stored without encryption.
    
    We do not want to simply return the uncrypted object, because the
    caller may rely on the objects integrity being cryptographically
    verified.
    '''

    pass


class MalformedObjectError(Exception):
    '''
    Raised by BetterBackend when trying to access an object that
    wasn't stored by BetterBackend, i.e. has no information about
    encryption or compression.
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

    def __init__(self, loc):
        super().__init__()
        self.loc = loc

    def __str__(self):
        return '%r does not exist' % self.loc


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
   
def aes_cipher(key):
    '''Return AES cipher in CTR mode for *key*'''
    
    return AES.new(key, AES.MODE_CTR, 
                   counter=Counter.new(128, initial_value=0)) 
        
def convert_legacy_metadata(meta):

    # For legacy format, meta is always a dict
    if not isinstance(meta, dict):
        return
    
    if ('encryption' in meta and
        'compression' in meta):
        return

    if 'encrypted' not in meta:
        meta['encryption'] = 'None'
        meta['compression'] = 'None'
        return

    s = meta.pop('encrypted')

    if s == 'True':
        meta['encryption'] = 'AES'
        meta['compression'] = 'BZIP2'

    elif s == 'False':
        meta['encryption'] = 'None'
        meta['compression'] = 'None'

    elif s.startswith('AES/'):
        meta['encryption'] = 'AES'
        meta['compression'] = s[4:]

    elif s.startswith('PLAIN/'):
        meta['encryption'] = 'None'
        meta['compression'] = s[6:]
    else:
        raise RuntimeError('Unsupported encryption')

    if meta['compression'] == 'BZ2':
        meta['compression'] = 'BZIP2'

    if meta['compression'] == 'NONE':
        meta['compression'] = 'None'


def get_backend(options, plain=False):
    '''Return backend for given storage-url
    
    If *plain* is true, don't attempt to unlock and don't wrap into
    BetterBackend.
    '''

    return get_backend_factory(options, plain)()

def get_backend_factory(options, plain=False):
    '''Return factory producing backend objects for given storage-url
    
    If *plain* is true, don't attempt to unlock and don't wrap into
    BetterBackend.    
    '''

    hit = re.match(r'^([a-zA-Z0-9]+)://', options.storage_url)
    if not hit:
        raise QuietError('Unknown storage url: %s' % options.storage_url)

    backend_name = 's3ql.backends.%s' % hit.group(1)
    try:
        __import__(backend_name)
    except ImportError:
        raise QuietError('No such backend: %s' % hit.group(1))

    backend_class = getattr(sys.modules[backend_name], 'Backend')

    # Read authfile
    config = configparser.ConfigParser()
    if os.path.isfile(options.authfile):
        mode = os.stat(options.authfile).st_mode
        if mode & (stat.S_IRGRP | stat.S_IROTH):
            raise QuietError("%s has insecure permissions, aborting." % options.authfile)
        config.read(options.authfile)

    backend_login = None
    backend_passphrase = None
    fs_passphrase = None
    for section in config.sections():
        def getopt(name):
            try:
                return config.get(section, name)
            except configparser.NoOptionError:
                return None

        pattern = getopt('storage-url')

        if not pattern or not options.storage_url.startswith(pattern):
            continue

        backend_login = getopt('backend-login') or backend_login
        backend_passphrase = getopt('backend-password') or backend_passphrase
        fs_passphrase = getopt('fs-passphrase') or fs_passphrase
        if getopt('fs-passphrase') is None and getopt('bucket-passphrase') is not None:
            fs_passphrase = getopt('bucket-passphrase')
            log.warning("Warning: the 'bucket-passphrase' configuration option has been "
                        "renamed to 'fs-passphrase'! Please update your authinfo file.")

    if not backend_login and backend_class.needs_login:
        if sys.stdin.isatty():
            backend_login = getpass("Enter backend login: ")
        else:
            backend_login = sys.stdin.readline().rstrip()

    if not backend_passphrase and backend_class.needs_login:
        if sys.stdin.isatty():
            backend_passphrase = getpass("Enter backend passphrase: ")
        else:
            backend_passphrase = sys.stdin.readline().rstrip()

    ssl_context = get_ssl_context(options)
    backend = backend_class(options.storage_url, backend_login, backend_passphrase,
                            ssl_context)
    try:
        
        # Do not use backend.lookup(), this would use a HEAD request and
        # not provide any useful error messages if something goes wrong
        # (e.g. wrong credentials)
        backend.fetch('s3ql_passphrase')
        
    except DanglingStorageURLError as exc:
        raise QuietError(str(exc))
    
    except AuthorizationError:
        raise QuietError('No permission to access backend.')

    except AuthenticationError:
        raise QuietError('Invalid credentials (or skewed system clock?).')
        
    except NoSuchObject:
        encrypted = False
        
    else:
        encrypted = True

    finally:
        backend.close()
        
    if plain:
        return lambda: backend_class(options.storage_url, backend_login, backend_passphrase,
                                     ssl_context)
            
    if encrypted and not fs_passphrase:
        if sys.stdin.isatty():
            fs_passphrase = getpass("Enter file system encryption passphrase: ")
        else:
            fs_passphrase = sys.stdin.readline().rstrip()
    elif not encrypted:
        fs_passphrase = None
        
    if fs_passphrase is not None:
        fs_passphrase = fs_passphrase.encode('utf-8')
        
    if hasattr(options, 'compress'):
        compress = options.compress
    else:
        compress = ('lzma', 2)

    if not encrypted:
        return lambda: BetterBackend(None, compress,
                                    backend_class(options.storage_url, backend_login,
                                                  backend_passphrase, ssl_context))

    tmp_backend = BetterBackend(fs_passphrase, compress, backend)

    try:
        data_pw = tmp_backend['s3ql_passphrase']
    except ChecksumError:
        raise QuietError('Wrong file system passphrase')
    finally:
        tmp_backend.close()
        
    return lambda: BetterBackend(data_pw, compress,
                                 backend_class(options.storage_url, backend_login,
                                               backend_passphrase, ssl_context))

fix_python_bug_7776()
