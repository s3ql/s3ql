'''
$Id$

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function, absolute_import

from cStringIO import StringIO
from s3ql.common import sha256
import tempfile
import hmac
import logging
import pycryptopp
import cPickle as pickle
import time
import hashlib
import os
import bz2
#import lzma
from base64 import b64decode, b64encode
import struct

log = logging.getLogger("backend")

__all__ = [ 'AbstractConnection', 'AbstractBucket', 'ChecksumError' ]

class AbstractConnection(object):
    '''This class contains functionality shared between all backends.'''

    def bucket_exists(self, name):
        """Check if the bucket `name` exists"""

        try:
            self.get_bucket(name)
        except KeyError:
            return False
        else:
            return True

class AbstractBucket(object):
    '''This class contains functionality shared between all backends.
    
    Instances behave more or less like dicts. They raise the
    same exceptions, can be iterated over and indexed into.
    '''

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

        If the key does not exist, KeyError is raised.
        """

        if not isinstance(key, str):
            raise TypeError('key must be of type str')

        meta_raw = self.raw_lookup(key)

        if 'encrypted' in meta_raw:
            if meta_raw['encrypted'] in ('True', 'AES/BZ2', 'AES/LZMA'):
                encrypted = True
            elif meta_raw['encrypted'] == 'False':
                encrypted = False
            else:
                raise RuntimeError('Unsupported compression/encryption')
        else:
            encrypted = False

        if encrypted and not self.passphrase:
            raise ChecksumError('Encrypted object and no passphrase supplied')
        if not encrypted and self.passphrase:
            raise ChecksumError('Passphrase supplied, but object is not encrypted')
        if encrypted and not 'meta' in meta_raw:
            raise ChecksumError('Encrypted object without metadata, unable to verify on lookup.')

        if 'meta' in meta_raw:
            buf = b64decode(meta_raw['meta'])
            if encrypted:
                buf = decrypt(buf, self.passphrase)
            metadata = pickle.loads(buf)
        else:
            metadata = dict()

        return metadata

    def fetch(self, key):
        """Return data stored under `key`.

        Returns a tuple with the data and metadata. If only the data
        itself is required, ``bucket[key]`` is a more concise notation
        for ``bucket.fetch(key)[0]``.
        """

        if not isinstance(key, str):
            raise TypeError('key must be of type str')

        fh = StringIO()
        meta = self.fetch_fh(key, fh)

        return (fh.getvalue(), meta)

    def store(self, key, val, metadata=None):
        """Store data under `key`.

        `metadata` can be a dict of additional attributes to 
        store with the object.

        If no metadata is required, one can simply assign to the
        subscripted bucket instead of using this function:
        ``bucket[key] = val`` is equivalent to ``bucket.store(key,
        val)``.
        """
        if isinstance(val, unicode):
            val = val.encode('us-ascii')

        if not isinstance(key, str):
            raise TypeError('key must be of type str')

        fh = StringIO(val)
        self.store_fh(key, fh, metadata)

    def fetch_fh(self, key, fh):
        """Fetch data for `key` and write to `fh`

        Return a dictionary with the metadata.
        """

        if not isinstance(key, str):
            raise TypeError('key must be of type str')

        if self.passphrase:
            tmp = tempfile.TemporaryFile()
            (fh, tmp) = (tmp, fh)

        meta_raw = self.raw_fetch(key, fh)

        if 'encrypted' in meta_raw:
            if meta_raw['encrypted'] in ('True', 'AES/BZ2'):
                decomp = bz2.BZ2Decompressor()
                encrypted = True
            elif meta_raw['encrypted'] == 'AES/LZMA':
                decomp = lzma.LZMADecompressor()
                encrypted = True
            elif meta_raw['encrypted'] == 'False':
                encrypted = False
            else:
                raise RuntimeError('Unsupported compression/encryption')
        else:
            encrypted = False

        if encrypted and not self.passphrase:
            raise ChecksumError('Encrypted object and no passphrase supplied')
        if not encrypted and self.passphrase:
            raise ChecksumError('Passphrase supplied, but object is not encrypted')

        if 'meta' in meta_raw:
            buf = b64decode(meta_raw['meta'])
            if encrypted:
                buf = decrypt(buf, self.passphrase)
            metadata = pickle.loads(buf)
        else:
            metadata = dict()

        if self.passphrase:
            (fh, tmp) = (tmp, fh)
            tmp.seek(0)
            fh.seek(0)
            decrypt_uncompress_fh(tmp, fh, self.passphrase, decomp)
            tmp.close()

        return metadata

    def store_fh(self, key, fh, metadata=None):
        """Store data in `fh` under `key`
        
        `metadata` can be a dict of additional attributes to 
        store with the object.
        """

        if not isinstance(key, str):
            raise TypeError('key must be of type str')

        # We always store metadata (even if it's just None), so that we can verify that the
        # object has been created by us when we call lookup().
        meta_raw = pickle.dumps(metadata, 2)

        if self.passphrase:
            # We need to generate a temporary copy to determine the
            # size of the object (which needs to transmitted as Content-Length)
            nonce = struct.pack(b'<f', time.time() - time.timezone) + bytes(key)
            fh.seek(0, os.SEEK_END)
            if fh.tell() > 1024 * 512:
                tmp = tempfile.TemporaryFile()
            else:
                tmp = StringIO()
            fh.seek(0)
            compress_encrypt_fh(fh, tmp, self.passphrase, nonce)
            (fh, tmp) = (tmp, fh)
            fh.seek(0)
            meta_raw = encrypt(meta_raw, self.passphrase, nonce)
        else:
            fh.seek(0)

        if self.passphrase:
            # LZMA is not yet working
            self.raw_store(key, fh, { 'meta': b64encode(meta_raw),
                                     'encrypted': 'AES/BZ2' })
        else:
            self.raw_store(key, fh, { 'meta': b64encode(meta_raw),
                                     'encrypted': 'False' })

        if self.passphrase:
            (fh, tmp) = (tmp, fh)
            tmp.close()



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
            raise ChecksumError('Invalid bz2 stream')

        if buf:
            hmac_.update(buf)
            ofh.write(buf)

    if decomp.unused_data:
        raise ChecksumError('Data after end of compressed stream')

    # Decompress hmac
    hash_ = cipher.process(hash_)

    if hash_ != hmac_.digest():
        raise ChecksumError('HMAC mismatch')


def compress_encrypt_fh(ifh, ofh, passphrase, nonce):
    '''Read `ifh` and write compressed, encrypted data to `ofh`'''

    if isinstance(nonce, unicode):
        nonce = nonce.encode('utf-8')

    # LZMA is not yet working
    #compr = lzma.LZMACompressor(options={ 'level': 9 })
    compr = bz2.BZ2Compressor(9)
    bs = 512 * 1024
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
    Raised if there is a checksum error in the data that we received 
    from S3.
    """
    pass


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
