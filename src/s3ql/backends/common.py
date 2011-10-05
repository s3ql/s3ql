'''
common.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import division, print_function, absolute_import

from ..common import QuietError
from abc import ABCMeta, abstractmethod
from base64 import b64decode, b64encode
from cStringIO import StringIO
from contextlib import contextmanager
from getpass import getpass
from pycryptopp.cipher import aes
import ConfigParser
import bz2
import cPickle as pickle
import hashlib
import hmac
import logging
import lzma
import os
import re
import stat
import struct
import sys
import threading
import time
import zlib
from functools import wraps

# Not available in every pycryptopp version
if hasattr(aes, 'start_up_self_test'):
    aes.start_up_self_test()

log = logging.getLogger("backend")

HMAC_SIZE = 32

RETRY_TIMEOUT=60*60*24
def retry(fn):
    '''Decorator for retrying a method on some exceptions
    
    If the decorated method raises an exception for which the instance's
    `is_temp_failure(exc)` method is true, the decorated method is called again
    at increasing intervals. If this persists for more than *timeout* seconds,
    the most-recently caught exception is re-raised.
    '''
    
    @wraps(fn)
    def wrapped(self, *a, **kw):    
        interval = 1/50
        waited = 0
        while True:
            try:
                return fn(self, *a, **kw)
            except Exception as exc:
                # Access to protected member ok
                #pylint: disable=W0212
                if not self.is_temp_failure(exc):
                    raise
                if waited > RETRY_TIMEOUT:
                    log.error('%s.%s(*): Timeout exceeded, re-raising %r exception', 
                            self.__class__.__name__, fn.__name__, exc)
                    raise
                
                log.info('%s.%s(*): trying again after %r exception:', 
                          self.__class__.__name__, fn.__name__, exc)
                
            time.sleep(interval)
            waited += interval
            if interval < 20*60:
                interval *= 2   
                
    # False positive
    #pylint: disable=E1101
    wrapped.__doc__ += '''
This method has been decorated and will automatically recall itself in
increasing intervals for up to s3ql.backends.common.RETRY_TIMEOUT seconds if it
raises an exception for which the instance's `is_temp_failure` method returns
True.
'''  
    return wrapped 

def sha256(s):
    return hashlib.sha256(s).digest()

class BucketPool(object):
    '''A pool of buckets

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
        
        with self.lock:
            self.pool.append(conn)
        
    @contextmanager
    def __call__(self):
        '''Provide connection from pool (context manager)'''
        
        conn = self.pop_conn()
        try:
            yield conn
        finally:
            self.push_conn(conn)
            

class AbstractBucket(object):
    '''Functionality shared between all backends.
    
    Instances behave similarly to dicts. They can be iterated over and
    indexed into, but raise a separate set of exceptions.
    
    The bucket guarantees get after create consistency, i.e. a newly created
    object will be immediately retrievable. Additional consistency guarantees
    may or may not be available and can be queried for with instance methods.
    '''
    __metaclass__ = ABCMeta

    needs_login = True
    
    def __init__(self):
        super(AbstractBucket, self).__init__()

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
    
    @retry
    def perform_read(self, fn, key):
        '''Read bucket data using *fn*, retry on temporary failure
        
        Open bucket for reading, call `fn(fh)` and close bucket. If a temporary
        error (as defined by `is_temp_failure`) occurs during opening, closing
        or execution of *fn*, the operation is retried.
        '''
        with self.open_read(key) as fh:
            return fn(fh)
             
    @retry
    def perform_write(self, fn, key, metadata=None, is_compressed=False):
        '''Read bucket data using *fn*, retry on temporary failure
        
        Open bucket for writing, call `fn(fh)` and close bucket. If a temporary
        error (as defined by `is_temp_failure`) occurs during opening, closing
        or execution of *fn*, the operation is retried.
        '''
        
        with self.open_write(key, metadata, is_compressed) as fh:
            return fn(fh)
                
    def fetch(self, key):
        """Return data stored under `key`.

        Returns a tuple with the data and metadata. If only the data itself is
        required, ``bucket[key]`` is a more concise notation for
        ``bucket.fetch(key)[0]``.
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
        bucket instead of using this function: ``bucket[key] = val`` is
        equivalent to ``bucket.store(key, val)``.
        """
        
        self.perform_write(lambda fh: fh.write(val), key, metadata)

    @abstractmethod
    def is_temp_failure(self, exc):
        '''Return true if exc indicates a temporary error
    
        Return true if the given exception is used by this bucket's backend
        to indicate a temporary problem. Most instance methods automatically
        retry the request in this case, so the caller does not need to
        worry about temporary failures.
        
        However, in same cases (e.g. when reading or writing an object), the
        request cannot automatically be retried. In these case this method can
        be used to check for temporary problems and so that the request can
        be manually restarted if applicable.
        '''   
               
        pass
    
    @abstractmethod
    def lookup(self, key):
        """Return metadata for given key.

        If the key does not exist, `NoSuchObject` is raised.
        """

        pass
    
    @abstractmethod
    def open_read(self, key):
        """Open object for reading

        Return a tuple of a file-like object. Bucket contents can be read from
        the file-like object, metadata is stored in its *metadata* attribute and
        can be modified by the caller at will. The object must be closed
        explicitly. 
        """
        
        pass
    
    @abstractmethod
    def open_write(self, key, metadata=None, is_compressed=False):
        """Open object for writing

        `metadata` can be a dict of additional attributes to store with the
        object. Returns a file-like object. The object must be closed
        explicitly. After closing, the *get_obj_size* may be used to retrieve
        the size of the stored object (which may differ from the size of the
        written data).
        
        The *is_compressed* parameter indicates that the caller is going
        to write compressed data, and may be used to avoid recompression
        by the bucket.
        """
        
        pass

    @abstractmethod
    def is_get_consistent(self):
        '''If True, objects retrievals are guaranteed to be up-to-date
        
        If this method returns True, then creating, deleting, or overwriting an
        object is guaranteed to be immediately reflected in subsequent object
        retrieval attempts.
        '''
        pass
                    
    @abstractmethod
    def is_list_create_consistent(self):
        '''If True, new objects are guaranteed to show up in object listings
        
        If this method returns True, creation of objects will immediately be
        reflected when retrieving the list of available objects.
        '''
        pass
    
    @abstractmethod
    def clear(self):
        """Delete all objects in bucket"""
        pass

    def contains(self, key):
        '''Check if `key` is in bucket'''
        
        try:
            self.lookup(key)
        except NoSuchObject:
            return False
        else:
            return True

    @abstractmethod
    def delete(self, key, force=False):
        """Delete object stored under `key`

        ``bucket.delete(key)`` can also be written as ``del bucket[key]``.
        If `force` is true, do not return an error if the key does not exist.
        """
        pass

    @abstractmethod
    def list(self, prefix=''):
        '''List keys in bucket

        Returns an iterator over all keys in the bucket.
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
    
class BetterBucket(AbstractBucket):
    '''
    This class adds encryption, compression and integrity protection to a plain
    bucket.
    '''

    def __init__(self, passphrase, compression, bucket):
        super(BetterBucket, self).__init__()
        
        self.passphrase = passphrase
        self.compression = compression
        self.bucket = bucket

        if compression not in ('bzip2', 'lzma', 'zlib', None):
            raise ValueError('Unsupported compression: %s' % compression)
        
    def lookup(self, key):
        """Return metadata for given key.

        If the key does not exist, `NoSuchObject` is raised.
        """

        metadata = self.bucket.lookup(key)
        convert_legacy_metadata(metadata)
        return self._unwrap_meta(metadata)
  
    def is_temp_failure(self, exc):
        '''Return true if exc indicates a temporary error
    
        Return true if the given exception is used by this bucket's backend
        to indicate a temporary problem. Most instance methods automatically
        retry the request in this case, so the caller does not need to
        worry about temporary failures.
        
        However, in same cases (e.g. when reading or writing an object), the
        request cannot automatically be retried. In these case this method can
        be used to check for temporary problems and so that the request can
        be manually restarted if applicable.
        '''          
        
        return self.bucket.is_temp_failure(exc)
    
    def _unwrap_meta(self, metadata):
        '''Unwrap metadata
        
        If the bucket has a password set but the object is not encrypted,
        `ObjectNotEncrypted` is raised.
        '''

        encr_alg = metadata['encryption']
        encrypted = (encr_alg != 'None')

        if encrypted and not self.passphrase:
            raise ChecksumError('Encrypted object and no passphrase supplied')

        elif not encrypted and self.passphrase:
            raise ObjectNotEncrypted()

        buf = b64decode(metadata['meta'])
        if encrypted:
            buf = decrypt(buf, self.passphrase)
            
        try:
            metadata = pickle.loads(buf)
        except pickle.UnpicklingError as exc:
            if (isinstance(exc.args[0], str)
                and exc.args[0].startswith('invalid load key')):
                raise ChecksumError('Invalid metadata')
            raise
            
        if metadata is None:
            return dict()
        else:
            return metadata
        
    def open_read(self, key):
        """Open object for reading

        Return a tuple of a file-like object. Bucket contents can be read from
        the file-like object, metadata is stored in its *metadata* attribute and
        can be modified by the caller at will. The object must be closed explicitly.
        
        If the bucket has a password set but the object is not encrypted,
        `ObjectNotEncrypted` is raised.
        """

        fh = self.bucket.open_read(key)
        convert_legacy_metadata(fh.metadata)
        
        compr_alg = fh.metadata['compression']
        encr_alg = fh.metadata['encryption']
           
        metadata = self._unwrap_meta(fh.metadata)

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
        
        return fh

    def open_write(self, key, metadata=None, is_compressed=False):
        """Open object for writing

        `metadata` can be a dict of additional attributes to store with the
        object. Returns a file-like object. The object must be closed
        explicitly. After closing, the *get_obj_size* may be used to retrieve
        the size of the stored object (which may differ from the size of the
        written data).
        
        The *is_compressed* parameter indicates that the caller is going
        to write compressed data, and may be used to avoid recompression
        by the bucket.        
        """
   
        # We always store metadata (even if it's just None), so that we can
        # verify that the object has been created by us when we call lookup().
        meta_buf = pickle.dumps(metadata, 2)

        meta_raw = dict()

        if self.passphrase:
            meta_raw['encryption'] = 'AES_v2'
            nonce = struct.pack(b'<f', time.time() - time.timezone) + bytes(key)
            meta_raw['meta'] = b64encode(encrypt(meta_buf, self.passphrase, nonce))
        else:
            meta_raw['encryption'] = 'None'
            meta_raw['meta'] = b64encode(meta_buf)
            nonce = None

        if is_compressed or not self.compression:
            compr = None
            meta_raw['compression'] = 'None'            
        elif self.compression == 'zlib':
            compr = zlib.compressobj(9)
            meta_raw['compression'] = 'ZLIB'
        elif self.compression == 'bzip2':
            compr = bz2.BZ2Compressor(9)
            meta_raw['compression'] = 'BZIP2'
        elif self.compression == 'lzma':
            compr = lzma.LZMACompressor(options={ 'level': 7 })
            meta_raw['compression'] = 'LZMA'

        fh = self.bucket.open_write(key, meta_raw)

        if nonce:
            fh = EncryptFilter(fh, self.passphrase, nonce)
        if compr:
            fh = CompressFilter(fh, compr)
            
        return fh
            
    def is_get_consistent(self):
        '''If True, objects retrievals are guaranteed to be up-to-date
        
        If this method returns True, then creating, deleting, or overwriting an
        object is guaranteed to be immediately reflected in subsequent object
        retrieval attempts.
        '''
        return self.bucket.is_get_consistent()
                    
    def is_list_create_consistent(self):
        '''If True, new objects are guaranteed to show up in object listings
        
        If this method returns True, creation of objects will immediately be
        reflected when retrieving the list of available objects.
        '''
        return self.bucket.is_get_consistent()
    
    def clear(self):
        """Delete all objects in bucket"""
        return self.bucket.clear()

    def contains(self, key):
        '''Check if `key` is in bucket'''
        return self.bucket.contains(key)

    def delete(self, key, force=False):
        """Delete object stored under `key`

        ``bucket.delete(key)`` can also be written as ``del bucket[key]``.
        If `force` is true, do not return an error if the key does not exist.
        """
        return self.bucket.delete(key, force)

    def list(self, prefix=''):
        '''List keys in bucket

        Returns an iterator over all keys in the bucket.
        '''
        return self.bucket.list(prefix)

    def copy(self, src, dest):
        """Copy data stored under key `src` to key `dest`
        
        If `dest` already exists, it will be overwritten. The copying
        is done on the remote side. 
        """
        return self.bucket.copy(src, dest)

    def rename(self, src, dest):
        """Rename key `src` to `dest`
        
        If `dest` already exists, it will be overwritten. The rename
        is done on the remote side.
        """
        return self.bucket.rename(src, dest)


class AbstractInputFilter(object):
    '''Process data while reading'''
    
    __metaclass__ = ABCMeta
    
    def __init__(self):
        super(AbstractInputFilter, self).__init__()
        self.bs = 256 * 1024
        self.buffer = ''
        
    def read(self, size=None):
        '''Try to read *size* bytes
        
        If *None*, read until EOF.
        '''
        
        if size is None:
            remaining = 1<<31
        else:
            remaining = size - len(self.buffer)

        while remaining > 0:
            buf = self._read(self.bs)
            if not buf:
                break
            remaining -= len(buf)
            self.buffer += buf
                
        if size is None:
            buf = self.buffer
            self.buffer = ''
        else:
            buf = self.buffer[:size]
            self.buffer = self.buffer[size:]
            
        return buf
             
    @abstractmethod
    def _read(self, size):
        '''Read roughly *size* bytes'''    
        pass
        
class CompressFilter(object):
    '''Compress data while writing'''
    
    def __init__(self, fh, compr):
        '''Initialize
        
        *fh* should be a file-like object. *decomp* should be a fresh compressor
        instance with a *compress* method.
        '''
        super(CompressFilter, self).__init__()
        
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
        buf = self.compr.flush()
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
                
                
class DecompressFilter(AbstractInputFilter):
    '''Decompress data while reading'''
    
    def __init__(self, fh, decomp, metadata=None):
        '''Initialize
        
        *fh* should be a file-like object. *decomp* should be a
        fresh decompressor instance with a *decompress* method.
        '''
        super(DecompressFilter, self).__init__()
        
        self.fh = fh
        self.decomp = decomp
        self.metadata = metadata
        
    def _read(self, size):
        '''Read roughly *size* bytes'''
        
        buf = ''
        while not buf:
            buf = self.fh.read(size)
            if not buf:
                if self.decomp.unused_data:
                    raise ChecksumError('Data after end of compressed stream')
                return ''
                
            try:
                buf = self.decomp.decompress(buf)
            except IOError as exc:
                if exc.args == ('invalid data stream',):
                    raise ChecksumError('Invalid compressed stream')
                raise
            except lzma.error as exc:
                if exc.args == ('unknown file format',):
                    raise ChecksumError('Invalid compressed stream')
                raise
            except zlib.error as exc:
                if exc.args[0].startswith('Error -3 while decompressing:'):
                    log.warn('LegacyDecryptDecompressFilter._read(): %s',
                             exc.args[0])
                    raise ChecksumError('Invalid compressed stream')
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
        super(EncryptFilter, self).__init__()
        
        self.fh = fh
        self.obj_size = 0
        self.closed = False
        
        if isinstance(nonce, unicode):
            nonce = nonce.encode('utf-8')
    
        self.key = sha256(passphrase + nonce)
        self.cipher = aes.AES(self.key) #IGNORE:E1102
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
        buf = self.cipher.process(buf)
        if buf:
            self.fh.write(buf)
            self.obj_size += len(buf)
            
    def close(self):
        # Packet length of 0 indicates end of stream, only HMAC follows
        buf = struct.pack(b'<I', 0)
        self.hmac.update(buf)
        buf = self.cipher.process(buf + self.hmac.digest())
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
        
        
class DecryptFilter(AbstractInputFilter):
    '''Decrypt data while reading
    
    Reader has to read the entire stream in order for HMAC
    checking to work.
    '''
    
    def __init__(self, fh, passphrase, metadata=None):
        '''Initialize
        
        *fh* should be a file-like object.
        '''
        super(DecryptFilter, self).__init__()
        
        self.fh = fh
        self.off_size = struct.calcsize(b'<I')
        self.remaining = 0 # Remaining length of current packet
        self.metadata = metadata
        self.hmac_checked = False
        
        # Read nonce
        len_ = struct.unpack(b'<B', fh.read(struct.calcsize(b'<B')))[0]
        nonce = fh.read(len_)
    
        key = sha256(passphrase + nonce)
        self.cipher = aes.AES(key) #IGNORE:E1102
        self.hmac = hmac.new(key, digestmod=hashlib.sha256)
    
    def _read(self, size):
        '''Read roughly *size* bytes'''
         
        buf = self.fh.read(size)
        if not buf:
            if not self.hmac_checked:
                raise ChecksumError('HMAC mismatch')
            return ''
        
        inbuf = self.cipher.process(buf)
        outbuf = ''
        while True:  
        
            if len(inbuf) <= self.remaining:
                self.remaining -= len(inbuf)
                self.hmac.update(inbuf)
                outbuf += inbuf
                break
            
            outbuf += inbuf[:self.remaining]
            self.hmac.update(inbuf[:self.remaining+self.off_size])
            paket_size = struct.unpack(b'<I', inbuf[self.remaining
                                                    :self.remaining+self.off_size])[0]
            inbuf = inbuf[self.remaining + self.off_size:]
            self.remaining = paket_size
            
            # End of file, read and check HMAC
            if paket_size == 0:
                if len(inbuf) != HMAC_SIZE:
                    inbuf += self.cipher.process(self.fh.read(HMAC_SIZE - len(inbuf)))
                if inbuf != self.hmac.digest():
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
    
class LegacyDecryptDecompressFilter(AbstractInputFilter):
    '''Decrypt and Decompress data while reading
    
    Reader has to read the entire stream in order for HMAC
    checking to work.
    '''
    
    def __init__(self, fh, passphrase, decomp, metadata=None):
        '''Initialize
        
        *fh* should be a file-like object.
        '''
        super(LegacyDecryptDecompressFilter, self).__init__()
        
        self.fh = fh
        self.metadata = metadata
        self.decomp = decomp
        self.hmac_checked = False
        
        # Read nonce
        len_ = struct.unpack(b'<B', fh.read(struct.calcsize(b'<B')))[0]
        nonce = fh.read(len_)
        self.hash = fh.read(HMAC_SIZE)
    
        key = sha256(passphrase + nonce)
        self.cipher = aes.AES(key) #IGNORE:E1102
        self.hmac = hmac.new(key, digestmod=hashlib.sha256)
        
    def _read(self, size):
        '''Read roughly *size* bytes'''
         
        buf = None
        while not buf:
            buf = self.fh.read(size)
            if not buf and not self.hmac_checked:
                if self.cipher.process(self.hash) != self.hmac.digest():
                    raise ChecksumError('HMAC mismatch')
                elif self.decomp and self.decomp.unused_data:
                    raise ChecksumError('Data after end of compressed stream')
                else:
                    self.hmac_checked = True
                    return ''
            elif not buf:
                return ''
            
            buf = self.cipher.process(buf)
            if not self.decomp:
                break
            
            try:
                buf = self.decomp.decompress(buf)
            except IOError as exc:
                if exc.args == ('invalid data stream',):
                    raise ChecksumError('Invalid compressed stream')
                raise
            except lzma.error as exc:
                if exc.args == ('unknown file format',):
                    raise ChecksumError('Invalid compressed stream')
                raise            
            except zlib.error as exc:
                if exc.args[0].startswith('Error -3 while decompressing:'):
                    log.warn('LegacyDecryptDecompressFilter._read(): %s',
                             exc.args[0])
                    raise ChecksumError('Invalid compressed stream')
                raise
            
        self.hmac.update(buf)
        return buf                              

    def close(self):
        self.fh.close()

    def __enter__(self):
        return self
    
    def __exit__(self, *a):
        self.close()
        return False       
    
    # pickle requires a readline() method for unpickling, but isn't actually
    # using it if we're using the binary protocols.
    def readline(self):
        raise RuntimeError('not implemented')
    
def encrypt(buf, passphrase, nonce):
    '''Encrypt *buf*'''

    if isinstance(nonce, unicode):
        nonce = nonce.encode('utf-8')

    key = sha256(passphrase + nonce)
    cipher = aes.AES(key) #IGNORE:E1102
    hmac_ = hmac.new(key, digestmod=hashlib.sha256)

    hmac_.update(buf)
    buf = cipher.process(buf)
    hash_ = cipher.process(hmac_.digest())

    return b''.join(
                    (struct.pack(b'<B', len(nonce)),
                    nonce, hash_, buf))
            
def decrypt(buf, passphrase):
    '''Decrypt *buf'''

    fh = StringIO(buf)

    len_ = struct.unpack(b'<B', fh.read(struct.calcsize(b'<B')))[0]
    nonce = fh.read(len_)

    key = sha256(passphrase + nonce)
    cipher = aes.AES(key) #IGNORE:E1102
    hmac_ = hmac.new(key, digestmod=hashlib.sha256)

    # Read (encrypted) hmac
    hash_ = fh.read(HMAC_SIZE)

    buf = fh.read()
    buf = cipher.process(buf)
    hmac_.update(buf)

    hash_ = cipher.process(hash_)

    if hash_ != hmac_.digest():
        raise ChecksumError('HMAC mismatch')

    return buf


class ChecksumError(Exception):
    """
    Raised if there is a checksum error in the data that we received.
    """
    
    def __init__(self, str_):
        super(ChecksumError, self).__init__()
        self.str = str_
    
    def __str__(self):
        return self.str
    

class ObjectNotEncrypted(Exception):
    '''
    Raised by the backend if an object was requested from an encrypted
    bucket, but the object was stored without encryption.
    
    We do not want to simply return the uncrypted object, because the
    caller may rely on the objects integrity being cryptographically
    verified.
    ''' 

    pass

class NoSuchObject(Exception):
    '''Raised if the requested object does not exist in the bucket'''
    
    def __init__(self, key):
        super(NoSuchObject, self).__init__()
        self.key = key
        
    def __str__(self):
        return 'Bucket does not have anything stored under key %r' % self.key

class NoSuchBucket(Exception):
    '''Raised if the requested bucket does not exist'''
    
    def __init__(self, name):
        super(NoSuchBucket, self).__init__()
        self.name = name
        
    def __str__(self):
        return 'Bucket %r does not exist' % self.name
        

def convert_legacy_metadata(meta):
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


def get_bucket(options, plain=False):
    '''Return bucket for given storage-url
    
    If *plain* is true, don't attempt to unlock and don't wrap into
    BetterBucket.
    '''

    return get_bucket_factory(options, plain)()

def get_bucket_factory(options, plain=False):
    '''Return factory producing bucket objects for given storage-url
    
    If *plain* is true, don't attempt to unlock and don't wrap into
    BetterBucket.    
    '''
                                     
    hit = re.match(r'^([a-zA-Z0-9]+)://(.+)$', options.storage_url)
    if not hit:
        raise QuietError('Unknown storage url: %s' % options.storage_url)
    
    backend_name = 's3ql.backends.%s' % hit.group(1)
    bucket_name = hit.group(2)
    try:
        __import__(backend_name)
    except ImportError:
        raise QuietError('No such backend: %s' % hit.group(1))
    
    bucket_class = getattr(sys.modules[backend_name], 'Bucket')
    
    # Read authfile
    config = ConfigParser.SafeConfigParser()
    if os.path.isfile(options.authfile):
        mode = os.stat(options.authfile).st_mode
        if mode & (stat.S_IRGRP | stat.S_IROTH):
            raise QuietError("%s has insecure permissions, aborting." % options.authfile)    
        config.read(options.authfile)
    
    backend_login = None
    backend_pw = None
    bucket_passphrase = None
    for section in config.sections():
        def getopt(name):
            try:
                return config.get(section, name)
            except ConfigParser.NoOptionError:
                return None

        pattern = getopt('storage-url')
        
        if not pattern or not options.storage_url.startswith(pattern):
            continue
        
        backend_login = backend_login or getopt('backend-login')
        backend_pw = backend_pw or getopt('backend-password')
        bucket_passphrase = bucket_passphrase or getopt('bucket-passphrase')
      
    if not backend_login and bucket_class.needs_login:
        if sys.stdin.isatty():
            backend_login = getpass("Enter backend login: ")
        else:
            backend_login = sys.stdin.readline().rstrip()

    if not backend_pw and bucket_class.needs_login:
        if sys.stdin.isatty():
            backend_pw = getpass("Enter backend password: ") 
        else:
            backend_pw = sys.stdin.readline().rstrip()

    if plain:
        return lambda: bucket_class(bucket_name, backend_login, backend_pw)
    
    bucket = bucket_class(bucket_name, backend_login, backend_pw)
     
    try:
        encrypted = 's3ql_passphrase' in bucket
    except NoSuchBucket:
        raise QuietError('Bucket %d does not exist' % bucket_name)
        
    if encrypted and not bucket_passphrase:
        if sys.stdin.isatty():
            bucket_passphrase = getpass("Enter bucket encryption passphrase: ") 
        else:
            bucket_passphrase = sys.stdin.readline().rstrip()
    elif not encrypted:
        bucket_passphrase = None
        
    if hasattr(options, 'compress'):
        compress = options.compress
    else:
        compress = None
            
    if not encrypted:
        return lambda: BetterBucket(None, compress, 
                                    bucket_class(bucket_name, backend_login, backend_pw))
    
    tmp_bucket = BetterBucket(bucket_passphrase, compress, bucket)
    
    try:
        data_pw = tmp_bucket['s3ql_passphrase']
    except ChecksumError:
        raise QuietError('Wrong bucket passphrase')

    return lambda: BetterBucket(data_pw, compress, 
                                bucket_class(bucket_name, backend_login, backend_pw))
    