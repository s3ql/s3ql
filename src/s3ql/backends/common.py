'''
common.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import division, print_function, absolute_import

from cStringIO import StringIO
import hmac
import logging
from pycryptopp.cipher import aes
import cPickle as pickle
import time
import hashlib
import zlib
import bz2
import lzma
from base64 import b64decode, b64encode
import struct
from abc import ABCMeta, abstractmethod
import threading
from contextlib import contextmanager
import re
import ConfigParser
from getpass import getpass
import os
import sys
import stat
from ..common import QuietError

aes.start_up_self_test()

log = logging.getLogger("backend")

HMAC_SIZE = 32

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
    '''
    __metaclass__ = ABCMeta

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

    def fetch(self, key):
        """Return data stored under `key`.

        Returns a tuple with the data and metadata. If only the data itself is
        required, ``bucket[key]`` is a more concise notation for
        ``bucket.fetch(key)[0]``.
        """

        with self.open_read(key) as (meta, fh): 
            data = fh.read()
        
        return (data, meta)

    def store(self, key, val, metadata=None):
        """Store data under `key`.

        `metadata` can be a dict of additional attributes to store with the
        object.

        If no metadata is required, one can simply assign to the subscripted
        bucket instead of using this function: ``bucket[key] = val`` is
        equivalent to ``bucket.store(key, val)``.
        """
        
        with self.open_write(key, metadata) as fh:
            fh.write(val)

    @abstractmethod
    def lookup(self, key):
        """Return metadata for given key.

        If the key does not exist, `NoSuchObject` is raised.
        """

        pass
    
    @abstractmethod
    def open_read(self, key):
        """Open object for reading

        Return a tuple of a file-like object and metadata. Bucket
        contents can be read from the file-like object. 
        """
        
        pass
    
    @abstractmethod
    def open_write(self, key, metadata=None):
        """Open object for writing

        `metadata` can be a dict of additional attributes to store with the
        object. Returns a file-like object.
        """
        
        pass
            
    @abstractmethod
    def read_after_create_consistent(self):
        '''Does this backend provide read-after-create consistency?'''
        pass
    
    @abstractmethod
    def read_after_write_consistent(self):
        '''Does this backend provide read-after-write consistency?'''
        pass
        
    @abstractmethod
    def read_after_delete_consistent(self):
        '''Does this backend provide read-after-delete consistency?'''
        pass

    @abstractmethod
    def list_after_delete_consistent(self):
        '''Does this backend provide list-after-delete consistency?'''
        pass
        
    @abstractmethod
    def list_after_create_consistent(self):
        '''Does this backend provide list-after-create consistency?'''
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
    This class adds encryption, compression and integrity
    protection to a plain bucket.
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

        meta_raw = self.bucket.lookup(key)
        return self._get_meta(meta_raw)[0]

    def _get_meta(self, meta_raw, plain=False):
        '''Get metadata & decompressor factory
        
        If the bucket has a password set but the object is not encrypted,
        `ObjectNotEncrypted` is raised unless `plain` is true.
        '''

        convert_legacy_metadata(meta_raw)

        compr_alg = meta_raw['compression']
        encr_alg = meta_raw['encryption']
        encrypted = (encr_alg != 'None')

        if encrypted and not self.passphrase:
                raise ChecksumError('Encrypted object and no passphrase supplied')

        elif self.passphrase and not plain:
            raise ObjectNotEncrypted()

        if compr_alg == 'BZIP2':
            decomp = bz2.BZ2Decompressor
        elif compr_alg == 'LZMA':
            decomp = lzma.LZMADecompressor
        elif compr_alg == 'ZLIB':
            decomp = zlib.decompressobj
        elif compr_alg is 'None':
            decomp = None
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

    def open_read(self, key, plain=False):
        """Fetch data for `key` and write to `fh`

        Return a dictionary with the metadata. If the bucket has a password set
        but the object is not encrypted, `ObjectNotEncrypted` is raised
        unless `plain` is true. 
        """

        meta_raw = self.bucket.lookup(key)
        (metadata, decomp) = self._get_meta(meta_raw, plain)

        fh = self.bucket.open_read(key)
        
        if meta_raw['encryption'] != 'None':
            fh = DecryptFilter(fh, self.passphrase, meta_raw['encryption'])
           
        if decomp:
            fh = DecompressFilter(fh, decomp)
            
        return (fh, metadata)


    def open_write(self, key, metadata=None):
        """Open object for writing

        `metadata` can be a dict of additional attributes to store with the
        object. Returns a file-like object.
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
            compr = None
            meta_raw['compression'] = 'None'

        fh = self.bucket.open_write(key, meta_raw)
        if nonce:
            fh = EncryptFilter(fh, self.passphrase, nonce)
        if compr:
            fh = CompressFilter(fh, compr)
            
        return fh
            
    def read_after_create_consistent(self):
        '''Does this backend provide read-after-create consistency?'''
        return self.bucket.read_after_create_consistent()
    
    def read_after_write_consistent(self):
        '''Does this backend provide read-after-write consistency?'''
        return self.bucket.read_after_write_consistent()
        
    def read_after_delete_consistent(self):
        '''Does this backend provide read-after-delete consistency?'''
        return self.bucket.read_after_delete_consistent()

    def list_after_delete_consistent(self):
        '''Does this backend provide list-after-delete consistency?'''
        return self.bucket.list_after_delete_consistent()
        
    def list_after_create_consistent(self):
        '''Does this backend provide list-after-create consistency?'''
        return self.bucket.list_after_create_consistent()
    
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
    
    def __init__(self, fh, comp):
        '''Initialize
        
        *fh* should be a file-like object. *decomp* should be a
        fresh compressor instance with a *compress* method.
        '''
        super(CompressFilter, self).__init__()
        
        self.fh = fh
        self.comp = comp
    
    def write(self, data):
        '''Write *data*'''
        
        buf = self.compr.compress(data)
        if buf:
            self.fh.write(buf)
            
    def close(self):
        buf = self.compr.flush()
        self.fh.write(buf) 
        self.fh.close()
            
class DecompressFilter(AbstractInputFilter):
    '''Decompress data while reading'''
    
    def __init__(self, fh, decomp):
        '''Initialize
        
        *fh* should be a file-like object. *decomp* should be a
        fresh decompressor instance with a *decompress* method.
        '''
        super(DecompressFilter, self).__init__()
        
        self.fh = fh
        self.decomp = decomp
    
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
            except IOError:
                raise ChecksumError('Invalid compressed stream')
            
        return buf
    
    def close(self):
        self.fh.close()
        

class EncryptFilter(object):
    '''Encrypt data while writing'''
    
    def __init__(self, fh, passphrase, nonce):
        '''Initialize
        
        *fh* should be a file-like object.
        '''
        super(EncryptFilter, self).__init__()
        
        self.fh = fh
        
        if isinstance(nonce, unicode):
            nonce = nonce.encode('utf-8')
    
        self.key = sha256(passphrase + nonce)
        self.cipher = aes.AES(self.key)
        self.hmac = hmac.new(self.key, digestmod=hashlib.sha256)
    
        self.fh.write(struct.pack(b'<B', len(nonce)))
        self.fh.write(nonce)
    
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
            
    def close(self):
        # Packet length of 0 indicates end of stream, only HMAC follows
        buf = struct.pack(b'<I', 0) + self.hmac.digest()
        buf = self.cipher.process(buf)
        self.fh.write(buf)
        self.fh.close()

    
class DecryptFilter(AbstractInputFilter):
    '''Decrypt data while reading
    
    Reader has to read the entire stream in order for HMAC
    checking to work.
    '''
    
    def __init__(self, fh, passphrase, algorithm):
        '''Initialize
        
        *fh* should be a file-like object.
        '''
        super(DecryptFilter, self).__init__()
        
        self.fh = fh
        self.off_size = struct.calcsize(b'<I')
        self.remaining = 0 # Remaining length of current packet

        # Read nonce
        len_ = struct.unpack(b'<B', fh.read(struct.calcsize(b'<B')))[0]
        nonce = fh.read(len_)
                    
        if algorithm == 'AES':
            self.hash = fh.read(HMAC_SIZE)
                    
        elif algorithm == 'AES_v2':
            # Hash is stored at end of stream
            self.hash = None
            
        else:
            raise RuntimeError('Unsupported encryption: %s' % algorithm)
    
        key = sha256(passphrase + nonce)
        self.cipher = aes.AES(key)
        self.hmac = hmac.new(key, digestmod=hashlib.sha256)
    
    def _read(self, size):
        '''Read roughly *size* bytes'''
         
        buf = self.fh.read(size)
        if not buf:
            # For v1 objects, check hash
            if self.hash and self.cipher.process(self.hash) != self.hmac.digest():
                raise ChecksumError('HMAC mismatch')
            else:        
                return ''
        
        plain = self.cipher.process(buf)
         
        # v1 objects: done
        # v2 objects: split into packets and check hash on last packet
        if self.hash:
            self.hmac.update(plain)
            return plain
        
        if len(buf) <= self.remaining:
            self.remaining -= len(buf)
            
        else:
            part1 = plain[:self.remaining]
            part2 = plain[self.remaining + self.off_size:]
            self.remaining = struct.unpack(b'<I', plain[self.remaining
                                                        :self.remaining+self.off_size])[0]
            
            # End of file, read and check HMAC
            if self.remaining == 0:
                self.hmac.update(part1)
                hash_ = part2
                if len(hash_) != HMAC_SIZE:
                    hash_ += self.cipher.process(self.fh.read(HMAC_SIZE - len(hash_)))
                if hash_ != self.hmac.digest():
                    raise ChecksumError('HMAC mismatch')
                return part1
            
            plain = part1 + part2
             
        self.hmac.update(plain)
        return buf                                           

    
    def close(self):
        self.fh.close()


def encrypt(buf, passphrase, nonce):
    '''Encrypt *buf*'''

    if isinstance(nonce, unicode):
        nonce = nonce.encode('utf-8')

    key = sha256(passphrase + nonce)
    cipher = aes.AES(key)
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
    cipher = aes.AES(key)
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


def get_bucket(options):
    '''Return bucket for given storage-url'''

    return get_bucket_factory(options)()

def get_bucket_factory(options):
    '''Return factory producing bucket objects for given storage-url'''

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
        
        if not options.storage_url.startswith(pattern):
            continue
        
        backend_login = backend_login or getopt('backend-login')
        backend_pw = backend_pw or getopt('backend-password')
        bucket_passphrase = bucket_passphrase or getopt('bucket-passphrase')
      
    if not backend_login:
        if sys.stdin.isatty():
            backend_login = getpass("Enter backend login: ") 
        else:
            backend_login = sys.stdin.readline().rstrip()

    if not backend_pw:
        if sys.stdin.isatty():
            backend_pw = getpass("Enter backend password: ") 
        else:
            backend_pw = sys.stdin.readline().rstrip()
                                     
    hit = re.match(r'^([a-zA-Z0-9])://(.+)$', options.storage_url)
    if not hit:
        raise QuietError('Unknown storage url: %s' % options.storage_url)
    
    backend_name = 's3ql.backends.%s' % hit.group(1)
    bucket_name = hit.group(2)
    try:
        __import__(backend_name)
    except ImportError:
        raise QuietError('No such backend: %s' % hit.group(1))
    
    bucket_class = sys.modules[backend_name].getattr('Bucket')
    
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
        
    tmp_bucket = BetterBucket(bucket_passphrase, options.compress, bucket)
    try:
        data_pw = tmp_bucket['s3ql_passphrase']
    except ChecksumError:
        raise QuietError('Wrong bucket passphrase')


    return lambda: BetterBucket(data_pw, options.compress, 
                                bucket_class(bucket_name, backend_login, backend_pw))
    