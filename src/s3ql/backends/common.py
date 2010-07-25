'''
common.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function, absolute_import

from cStringIO import StringIO
from ..common import sha256
import tempfile
import hmac
import logging
import pycryptopp
import cPickle as pickle
import time
import hashlib
import zlib
import bz2
import lzma
from base64 import b64decode, b64encode
import struct
from abc import ABCMeta, abstractmethod

log = logging.getLogger("backend")

__all__ = [ 'AbstractConnection', 'AbstractBucket', 'ChecksumError', 'UnsupportedError',
            'NoSuchObject', 'NoSuchBucket' ]

class AbstractConnection(object):
    '''This class contains functionality shared between all backends.
    
    All derived classes are expected to be completely threadsafe
    (except for internal methods starting with underscore)
    '''
    __metaclass__ = ABCMeta

    def bucket_exists(self, name):
        """Check if the bucket `name` exists"""

        try:
            self.get_bucket(name)
        except NoSuchBucket:
            return False
        else:
            return True

    def __contains__(self, name):
        return self.bucket_exists(name)

    def close(self):
        '''Close connection.
        
        If this method is not called, the interpreter may be kept alive by
        background threads initiated by the connection.
        '''
        pass

    def prepare_fork(self):
        '''Prepare connection for forking
        
        This method must be called before the process is forked, so that
        the connection can properly terminate any threads that it uses.
        
        The connection (or any of its bucket objects) can not be used
        between the calls to `prepare_fork()` and `finish_fork()`.
        '''
        pass

    def finish_fork(self):
        '''Re-initalize connection after forking
        
        This method must be called after the process has forked, so that
        the connection can properly restart any threads that it may
        have stopped for the fork.
        
        The connection (or any of its bucket objects) can not be used
        between the calls to `prepare_fork()` and `finish_fork()`.
        '''
        pass

    @abstractmethod
    def create_bucket(self, name, passphrase=None, compression=None):
        """Create bucket and return `Bucket` instance"""
        pass

    @abstractmethod
    def get_bucket(self, name, passphrase=None, compression=None):
        """Get `Bucket` instance for bucket `name`"""
        pass

    @abstractmethod
    def delete_bucket(self, name, recursive=False):
        """Delete bucket
        
        If `recursive` is False and the bucket still contains objects, the call
        will fail.
        """
        pass


class AbstractBucket(object):
    '''This class contains functionality shared between all backends.
    
    Instances behave similarly to dicts. They can be iterated over and
    indexed into, but raise a separate set of exceptions.
    
    All derived classes are expected to be completely threadsafe
    (except for internal methods starting with underscore)
    '''
    __metaclass__ = ABCMeta

    def __init__(self, passphrase, compression):
        self.passphrase = passphrase
        self.compression = compression
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

    def lookup(self, key):
        """Return metadata for given key.

        If the key does not exist, `NoSuchObject` is raised.
        """

        if not isinstance(key, str):
            raise TypeError('key must be of type str')

        meta_raw = self.raw_lookup(key)
        return self._get_meta(meta_raw)[0]


    def fetch(self, key):
        """Return data stored under `key`.

        Returns a tuple with the data and metadata. If only the data itself is
        required, ``bucket[key]`` is a more concise notation for
        ``bucket.fetch(key)[0]``.
        """

        if not isinstance(key, str):
            raise TypeError('key must be of type str')

        fh = StringIO()
        meta = self.fetch_fh(key, fh)

        return (fh.getvalue(), meta)

    def store(self, key, val, metadata=None):
        """Store data under `key`.

        `metadata` can be a dict of additional attributes to store with the
        object.

        If no metadata is required, one can simply assign to the subscripted
        bucket instead of using this function: ``bucket[key] = val`` is
        equivalent to ``bucket.store(key, val)``.
        """
        if isinstance(val, unicode):
            val = val.encode('us-ascii')

        if not isinstance(key, str):
            raise TypeError('key must be of type str')

        fh = StringIO(val)
        self.store_fh(key, fh, metadata)

    def _get_meta(self, meta_raw, plain=False):
        '''Get metadata & decompressor factory
        
        If the bucket has a password set
        but the object is not encrypted, `ObjectNotEncrypted` is raised
        unless `plain` is true. 
        '''

        convert_legacy_metadata(meta_raw)

        compr_alg = meta_raw['compression']
        encr_alg = meta_raw['encryption']
        encrypted = (encr_alg != 'None')

        if encrypted:
            if not self.passphrase:
                raise ChecksumError('Encrypted object and no passphrase supplied')

            if encr_alg != 'AES':
                raise RuntimeError('Unsupported encryption')
        elif self.passphrase and not plain:
            raise ObjectNotEncrypted()

        if compr_alg == 'BZIP2':
            decomp = bz2.BZ2Decompressor
        elif compr_alg == 'LZMA':
            decomp = lzma.LZMADecompressor
        elif compr_alg == 'ZLIB':
            decomp = zlib.decompressobj
        elif compr_alg == 'None':
            decomp = DummyDecompressor
        else:
            raise RuntimeError('Unsupported compression: %s' % compr_alg)

        if 'meta' in meta_raw:
            buf = b64decode(meta_raw['meta'])
            if encrypted:
                buf = decrypt(buf, self.passphrase)
            metadata = pickle.loads(buf)
        else:
            metadata = dict()

        return (metadata, decomp)

    def fetch_fh(self, key, fh, plain=False):
        """Fetch data for `key` and write to `fh`

        Return a dictionary with the metadata. If the bucket has a password set
        but the object is not encrypted, `ObjectNotEncrypted` is raised
        unless `plain` is true. 
        """

        if not isinstance(key, str):
            raise TypeError('key must be of type str')

        tmp = tempfile.TemporaryFile()
        (fh, tmp) = (tmp, fh)

        meta_raw = self.raw_fetch(key, fh)
        (metadata, decomp) = self._get_meta(meta_raw, plain)

        (fh, tmp) = (tmp, fh)
        tmp.seek(0)
        fh.seek(0)
        if self.passphrase:
            decrypt_uncompress_fh(tmp, fh, self.passphrase, decomp())
        else:
            uncompress_fh(tmp, fh, decomp())
        tmp.close()

        return metadata

    def store_fh(self, key, fh, metadata=None):
        """Store data in `fh` under `key`
        
        `metadata` can be a dict of additional attributes to store with the
        object.
        """
        return self.prep_store_fh(key, fh, metadata)()

    def prep_store_fh(self, key, fh, metadata=None):
        """Prepare to store data in `fh` under `key`
        
        `metadata` can be a dict of additional attributes to store with the
        object. The method compresses and encrypts the data and 
        returns a function that does the actual network transaction.
        """

        if not isinstance(key, str):
            raise TypeError('key must be of type str')

        # We always store metadata (even if it's just None), so that we can
        # verify that the object has been created by us when we call lookup().
        meta_buf = pickle.dumps(metadata, 2)

        meta_raw = dict()

        if self.passphrase:
            meta_raw['encryption'] = 'AES'
            nonce = struct.pack(b'<f', time.time() - time.timezone) + bytes(key)
            meta_raw['meta'] = b64encode(encrypt(meta_buf, self.passphrase, nonce))
        else:
            meta_raw['encryption'] = 'None'
            meta_raw['meta'] = b64encode(meta_buf)

        if self.compression == 'zlib':
            compr = zlib.compressobj(9)
            meta_raw['compression'] = 'ZLIB'
        elif self.compression == 'bzip2':
            compr = bz2.BZ2Compressor(9)
            meta_raw['compression'] = 'BZIP2'
        elif self.compression == 'lzma':
            compr = lzma.LZMACompressor(options={ 'level': 7 })
            meta_raw['compression'] = 'LZMA'
        elif not self.compression:
            compr = DummyCompressor()
            meta_raw['compression'] = 'None'
        else:
            raise ValueError('Invalid compression algorithm')

        # We need to generate a temporary copy to determine the size of the
        # object (which needs to transmitted as Content-Length)
        tmp = tempfile.TemporaryFile()
        fh.seek(0)
        if self.passphrase:
            compress_encrypt_fh(fh, tmp, self.passphrase, nonce, compr)
        else:
            compress_fh(fh, tmp, compr)
        tmp.seek(0)
        return lambda: self.raw_store(key, tmp, meta_raw)

    @abstractmethod
    def read_after_create_consistent(self):
        '''Does this backend provide read-after-create consistency?'''
        pass
    
    @abstractmethod
    def read_after_write_consistent(self):
        '''Does this backend provide read-after-write consistency?'''
        pass
        
    @abstractmethod
    def __str__(self):
        pass

    @abstractmethod
    def clear(self):
        """Delete all objects in bucket"""
        pass

    @abstractmethod
    def contains(self, key):
        '''Check if `key` is in bucket'''
        pass

    @abstractmethod
    def raw_lookup(self, key):
        '''Return meta data for `key`'''
        pass

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
    def get_size(self):
        """Get total size of bucket"""
        pass

    @abstractmethod
    def raw_fetch(self, key, fh):
        '''Fetch contents stored under `key` and write them into `fh`'''
        pass

    @abstractmethod
    def raw_store(self, key, fh, metadata):
        '''Store contents of `fh` in `key` with metadata
        
        `metadata` has to be a dict with lower-case keys.
        '''
        pass


    def copy(self, src, dest):
        """Copy data stored under key `src` to key `dest`
        
        If `dest` already exists, it will be overwritten. The copying
        is done on the remote side. If the backend does not support
        this operation, raises `UnsupportedError`.
        """
        raise UnsupportedError('Backend does not support remote copy')

    def rename(self, src, dest):
        """Rename key `src` to `dest`
        
        If `dest` already exists, it will be overwritten. The rename
        is done on the remote side. If the backend does not support
        this operation, raises `UnsupportedError`.
        """
        raise UnsupportedError('Backend does not support remote rename')


class UnsupportedError(Exception):
    '''Raised if a backend does not support a particular operation'''

    pass


def decrypt_uncompress_fh(ifh, ofh, passphrase, decomp):
    '''Read `ofh` and write decrypted, uncompressed data to `ofh`'''

    bs = 256 * 1024

    # Read nonce
    len_ = struct.unpack(b'<B', ifh.read(struct.calcsize(b'<B')))[0]
    nonce = ifh.read(len_)

    key = sha256(passphrase + nonce)
    cipher = pycryptopp.cipher.aes.AES(key)
    hmac_ = hmac.new(key, digestmod=hashlib.sha256)

    # Read (encrypted) hmac
    hash_ = ifh.read(32) # Length of hash

    while True:
        buf = ifh.read(bs)
        if not buf:
            break

        buf = cipher.process(buf)
        try:
            buf = decomp.decompress(buf)
        except IOError:
            raise ChecksumError('Invalid compressed stream')

        if buf:
            hmac_.update(buf)
            ofh.write(buf)

    if decomp.unused_data:
        raise ChecksumError('Data after end of compressed stream')

    # Decompress hmac
    hash_ = cipher.process(hash_)

    if hash_ != hmac_.digest():
        raise ChecksumError('HMAC mismatch')

def uncompress_fh(ifh, ofh, decomp):
    '''Read `ofh` and write uncompressed data to `ofh`'''

    bs = 256 * 1024
    while True:
        buf = ifh.read(bs)
        if not buf:
            break

        try:
            buf = decomp.decompress(buf)
        except IOError:
            raise ChecksumError('Invalid compressed stream')

        if buf:
            ofh.write(buf)

    if decomp.unused_data:
        raise ChecksumError('Data after end of compressed stream')


class DummyDecompressor(object):
    def __init__(self):
        super(DummyDecompressor, self).__init__()
        self.unused_data = None

    def decompress(self, buf):
        return buf

class DummyCompressor(object):
    def flush(self):
        return ''

    def compress(self, buf):
        return buf


def compress_encrypt_fh(ifh, ofh, passphrase, nonce, compr):
    '''Read `ifh` and write compressed, encrypted data to `ofh`'''

    if isinstance(nonce, unicode):
        nonce = nonce.encode('utf-8')

    bs = 1024 * 1024
    key = sha256(passphrase + nonce)
    cipher = pycryptopp.cipher.aes.AES(key)
    hmac_ = hmac.new(key, digestmod=hashlib.sha256)

    # Write nonce
    ofh.write(struct.pack(b'<B', len(nonce)))
    ofh.write(nonce)
    off = ofh.tell()

    # Reserve space for hmac
    ofh.write(b'0' * 32)

    while True:
        buf = ifh.read(bs)
        if not buf:
            buf = compr.flush()
            buf = cipher.process(buf)
            ofh.write(buf)
            break

        hmac_.update(buf)
        buf = compr.compress(buf)
        if buf:
            buf = cipher.process(buf)
            ofh.write(buf)

    buf = hmac_.digest()
    buf = cipher.process(buf)
    ofh.seek(off)
    ofh.write(buf)

def compress_fh(ifh, ofh, compr):
    '''Read `ifh` and write compressed data to `ofh`'''

    bs = 1024 * 1024
    while True:
        buf = ifh.read(bs)
        if not buf:
            buf = compr.flush()
            ofh.write(buf)
            break

        buf = compr.compress(buf)
        if buf:
            ofh.write(buf)



def decrypt(buf, passphrase):
    '''Decrypt given string'''

    fh = StringIO(buf)

    len_ = struct.unpack(b'<B', fh.read(struct.calcsize(b'<B')))[0]
    nonce = fh.read(len_)

    key = sha256(passphrase + nonce)
    cipher = pycryptopp.cipher.aes.AES(key)
    hmac_ = hmac.new(key, digestmod=hashlib.sha256)

    # Read (encrypted) hmac
    hash_ = fh.read(32) # Length of hash

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
    pass

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
        
def encrypt(buf, passphrase, nonce):
    '''Encrypt given string'''

    if isinstance(nonce, unicode):
        nonce = nonce.encode('utf-8')

    key = sha256(passphrase + nonce)
    cipher = pycryptopp.cipher.aes.AES(key)
    hmac_ = hmac.new(key, digestmod=hashlib.sha256)

    hmac_.update(buf)
    buf = cipher.process(buf)
    hash_ = cipher.process(hmac_.digest())

    return b''.join(
                    (struct.pack(b'<B', len(nonce)),
                    nonce, hash_, buf))


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



    
    