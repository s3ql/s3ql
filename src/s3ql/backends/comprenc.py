'''
comprenc.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

import bz2
import hashlib
import hmac
import io
import logging
import lzma
import shutil
import struct
import time
import zlib
from typing import BinaryIO

import cryptography.hazmat.backends as crypto_backends
import cryptography.hazmat.primitives.ciphers as crypto_ciphers

from .. import BUFSIZE
from ..common import ThawError, freeze_basic_mapping, thaw_basic_mapping
from .common import AbstractBackend, CorruptedObjectError, checksum_basic_mapping

log = logging.getLogger(__name__)

HMAC_SIZE = 32

crypto_backend = crypto_backends.default_backend()


def sha256(s):
    return hashlib.sha256(s).digest()


def aes_encryptor(key):
    '''Return AES cipher in CTR mode for *key*'''

    cipher = crypto_ciphers.Cipher(
        crypto_ciphers.algorithms.AES(key),
        crypto_ciphers.modes.CTR(nonce=bytes(16)),
        backend=crypto_backend,
    )
    return cipher.encryptor()


def aes_decryptor(key):
    '''Return AES cipher in CTR mode for *key*'''

    cipher = crypto_ciphers.Cipher(
        crypto_ciphers.algorithms.AES(key),
        crypto_ciphers.modes.CTR(nonce=bytes(16)),
        backend=crypto_backend,
    )
    return cipher.decryptor()


class ComprencBackend(AbstractBackend):
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

        if compression[0] not in ('bzip2', 'lzma', 'zlib', None) or compression[1] not in range(10):
            raise ValueError('Unsupported compression: %s' % compression)

    @property
    def has_delete_multi(self):
        return self.backend.has_delete_multi

    def reset(self):
        self.backend.reset()

    def lookup(self, key):
        meta_raw = self.backend.lookup(key)
        return self._verify_meta(key, meta_raw)[1]

    def get_size(self, key):
        '''
        This method returns the compressed size, i.e. the storage space
        that's actually occupied by the object.
        '''

        return self.backend.get_size(key)

    def is_temp_failure(self, exc):
        return self.backend.is_temp_failure(exc)

    def _verify_meta(self, key, metadata):
        '''Unwrap and authenticate metadata

        If the backend has a password set but the object is not encrypted,
        `ObjectNotEncrypted` is raised. Returns the object nonce and its
        metadata. If the object is not encrypted, the nonce is `None`.
        '''

        if not isinstance(metadata, dict):
            raise CorruptedObjectError('metadata should be dict, not %s' % type(metadata))

        format_version = metadata.get('format_version', 0)
        if format_version != 2:
            raise CorruptedObjectError('format_version %s unsupported' % format_version)

        for mkey in ('encryption', 'compression', 'data'):
            if mkey not in metadata:
                raise CorruptedObjectError('meta key %s is missing' % mkey)

        encr_alg = metadata['encryption']
        encrypted = encr_alg != 'None'

        if encrypted and self.passphrase is None:
            raise CorruptedObjectError('Encrypted object and no passphrase supplied')

        elif not encrypted and self.passphrase is not None:
            raise ObjectNotEncrypted()

        meta_buf = metadata['data']
        if not encrypted:
            try:
                meta = thaw_basic_mapping(meta_buf)
            except ThawError:
                raise CorruptedObjectError('Invalid metadata')
            return (None, meta)

        # Encrypted
        for mkey in ('nonce', 'signature', 'object_id'):
            if mkey not in metadata:
                raise CorruptedObjectError('meta key %s is missing' % mkey)

        nonce = metadata['nonce']
        stored_key = metadata['object_id']
        meta_key = sha256(self.passphrase + nonce + b'meta')
        meta_sig = checksum_basic_mapping(metadata, meta_key)
        if not hmac.compare_digest(metadata['signature'], meta_sig):
            raise CorruptedObjectError('HMAC mismatch')

        if stored_key != key:
            raise CorruptedObjectError(
                'Object content does not match its key (%s vs %s)' % (stored_key, key)
            )

        decryptor = aes_decryptor(meta_key)
        buf = decryptor.update(meta_buf) + decryptor.finalize()
        meta = thaw_basic_mapping(buf)
        try:
            return (nonce, meta)
        except ThawError:
            raise CorruptedObjectError('Invalid metadata')

    def readinto_fh(self, key: str, fh: BinaryIO):
        '''Transfer data stored under *key* into *fh*, return metadata.

        The data will be inserted at the current offset. If a temporary error (as defined by
        `is_temp_failure`) occurs, the operation is retried.

        If the backend has a password set but the object is not encrypted, `ObjectNotEncrypted` is
        raised.
        '''

        buf1 = io.BytesIO()
        meta_raw = self.backend.readinto_fh(key, buf1)
        (nonce, meta) = self._verify_meta(key, meta_raw)
        compr_alg = meta_raw['compression']
        encr_alg = meta_raw['encryption']
        if nonce:
            data_key = sha256(self.passphrase + nonce)

        # The `payload_offset` key only exists if the storage object was created with on old S3QL
        # version. In order to avoid having to download and re-upload the entire object during the
        # upgrade, the upgrade procedure adds this header to tell us how many bytes at the beginning
        # of the object we have to skip to get to the payload.
        if 'payload_offset' in meta_raw:
            buf1.seek(meta_raw['payload_offset'])
        else:
            buf1.seek(0)

        # If not compressed, decrypt directly into `fh`. Otherwise, use intermediate buffer.
        if compr_alg == 'None':
            buf2 = fh
        else:
            buf2 = io.BytesIO()

        if encr_alg == 'AES_v2':
            decrypt_fh(buf1, buf2, data_key)
        elif encr_alg == 'None':
            shutil.copyfileobj(buf1, buf2, BUFSIZE)
        else:
            raise RuntimeError('Unsupported encryption: %s' % encr_alg)

        if compr_alg == 'None':
            assert buf2 is fh
            return meta

        if compr_alg == 'BZIP2':
            decompressor = bz2.BZ2Decompressor()
        elif compr_alg == 'LZMA':
            decompressor = lzma.LZMADecompressor()
        elif compr_alg == 'ZLIB':
            decompressor = zlib.decompressobj()
        else:
            raise RuntimeError('Unsupported compression: %s' % compr_alg)
        assert buf2 is not fh
        buf2.seek(0)
        decompress_fh(buf2, fh, decompressor)

        return meta

    def open_write(self, key, metadata=None, is_compressed=False):

        if metadata is None:
            metadata = dict()
        elif not isinstance(metadata, dict):
            raise TypeError('*metadata*: expected dict or None, got %s' % type(metadata))

        meta_buf = freeze_basic_mapping(metadata)
        meta_raw = dict(format_version=2)

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

        if self.passphrase is not None:
            nonce = struct.pack('<d', time.time()) + key.encode('utf-8')
            meta_key = sha256(self.passphrase + nonce + b'meta')
            data_key = sha256(self.passphrase + nonce)
            encryptor = aes_encryptor(meta_key)
            meta_raw['encryption'] = 'AES_v2'
            meta_raw['nonce'] = nonce
            meta_raw['data'] = encryptor.update(meta_buf) + encryptor.finalize()
            meta_raw['object_id'] = key
            meta_raw['signature'] = checksum_basic_mapping(meta_raw, meta_key)
        else:
            meta_raw['encryption'] = 'None'
            meta_raw['data'] = meta_buf

        fh = self.backend.open_write(key, meta_raw)

        if self.passphrase is not None:
            fh = EncryptFilter(fh, data_key)
        if compr:
            fh = CompressFilter(fh, compr)

        return fh

    def contains(self, key):
        return self.backend.contains(key)

    def delete(self, key, force=False):
        return self.backend.delete(key, force)

    def delete_multi(self, keys, force=False):
        return self.backend.delete_multi(keys, force=force)

    def list(self, prefix=''):
        return self.backend.list(prefix)

    def close(self):
        self.backend.close()


class CompressFilter:
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
        # There may be errors when calling fh.close(), so we make sure that a
        # repeated call is forwarded to fh.close(), even if we already cleaned
        # up.
        if not self.closed:
            buf = self.compr.flush()
            if buf:
                self.fh.write(buf)
                self.obj_size += len(buf)
            self.closed = True
        self.fh.close()

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()
        return False

    def get_obj_size(self):
        if not self.closed:
            raise RuntimeError('Object must be closed first.')
        return self.obj_size


class EncryptFilter:
    '''Encrypt data while writing'''

    def __init__(self, fh, key):
        '''Initialize

        *fh* should be a file-like object.
        '''
        super().__init__()

        self.fh = fh
        self.obj_size = 0
        self.closed = False
        self.encryptor = aes_encryptor(key)
        self.hmac = hmac.new(key, digestmod=hashlib.sha256)

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
        buf2 = self.encryptor.update(buf)
        assert len(buf2) == len(buf)
        self.fh.write(buf2)
        self.obj_size += len(buf2)

    def close(self):
        # There may be errors when calling fh.close(), so we make sure that a
        # repeated call is forwarded to fh.close(), even if we already cleaned
        # up.
        if not self.closed:
            # Packet length of 0 indicates end of stream, only HMAC follows
            buf = struct.pack(b'<I', 0)
            self.hmac.update(buf)
            buf += self.hmac.digest()
            buf2 = self.encryptor.update(buf)
            assert len(buf) == len(buf2)
            self.fh.write(buf2)
            self.obj_size += len(buf2)
            self.closed = True

        self.fh.close()

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()
        return False

    def get_obj_size(self):
        if not self.closed:
            raise RuntimeError('Object must be closed first.')
        return self.obj_size


def decompress_fh(ifh: BinaryIO, ofh: BinaryIO, decompressor):
    '''Decompress contents of *ifh* into *ofh*'''

    while True:
        buf = ifh.read(BUFSIZE)
        if not buf:
            break
        buf = decompress_buf(decompressor, buf)
        if buf:
            ofh.write(buf)

    if not decompressor.eof:
        raise CorruptedObjectError('Premature end of stream.')
    if decompressor.unused_data:
        raise CorruptedObjectError('Data after end of compressed stream')


def decrypt_fh(ifh: BinaryIO, ofh: BinaryIO, key: bytes):
    '''Decrypt contents of *ifh* into *ofh*'''

    off_size = struct.calcsize(b'<I')
    decryptor = aes_decryptor(key)
    hmac_ = hmac.new(key, digestmod=hashlib.sha256)

    while True:
        buf = ifh.read(off_size)
        if not buf:
            raise CorruptedObjectError('Premature end of stream.')
        buf = decryptor.update(buf)
        hmac_.update(buf)
        assert len(buf) == off_size
        to_read = struct.unpack(b'<I', buf)[0]
        if to_read == 0:
            break
        while to_read:
            buf = ifh.read(min(to_read, BUFSIZE))
            if not buf:
                raise CorruptedObjectError('Premature end of stream.')
            to_read -= len(buf)
            buf = decryptor.update(buf)
            hmac_.update(buf)
            ofh.write(buf)

    buf = ifh.read(HMAC_SIZE)
    buf = decryptor.update(buf)
    if ifh.read(BUFSIZE):
        raise CorruptedObjectError('Extraneous data at end of object')

    if not hmac.compare_digest(buf, hmac_.digest()):
        raise CorruptedObjectError('HMAC mismatch')


def decompress_buf(decomp, buf):
    '''Decompress *buf* using *decomp*

    This method encapsulates exception handling for different
    decompressors.
    '''

    try:
        return decomp.decompress(buf)
    except IOError as exc:
        if exc.args[0].lower().startswith('invalid data stream'):
            raise CorruptedObjectError('Invalid compressed stream')
        raise
    except lzma.LZMAError as exc:
        if exc.args[0].lower().startswith('corrupt input data') or exc.args[0].startswith(
            'Input format not supported'
        ):
            raise CorruptedObjectError('Invalid compressed stream')
        raise
    except zlib.error as exc:
        if exc.args[0].lower().startswith('error -3 while decompressing'):
            raise CorruptedObjectError('Invalid compressed stream')
        raise


class ObjectNotEncrypted(Exception):
    '''
    Raised by the backend if an object was requested from an encrypted
    backend, but the object was stored without encryption.

    We do not want to simply return the unencrypted object, because the
    caller may rely on the objects integrity being cryptographically
    verified.
    '''

    pass
