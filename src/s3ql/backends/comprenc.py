'''
comprenc.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import annotations

import bz2
import hashlib
import hmac
import io
import logging
import lzma
import os
import struct
import time
import zlib
from collections.abc import AsyncIterator, Awaitable
from typing import TYPE_CHECKING, ClassVar

import cryptography.hazmat.backends as crypto_backends
import cryptography.hazmat.primitives.ciphers as crypto_ciphers
import trio

from s3ql.async_bridge import run_async
from s3ql.types import (
    BasicMappingT,
    BinaryInput,
    BinaryOutput,
    CompressorProtocol,
    DecompressorProtocol,
)

from .. import BUFSIZE
from ..common import ThawError, copyfh, freeze_basic_mapping, thaw_basic_mapping
from .common import (
    _FACTORY_SENTINEL,
    AbstractBackend,
    AsyncBackend,
    CorruptedObjectError,
    checksum_basic_mapping,
)

if TYPE_CHECKING:
    from cryptography.hazmat.primitives.ciphers import CipherContext

log = logging.getLogger(__name__)

HMAC_SIZE = 32

crypto_backend = crypto_backends.default_backend()


# Thresholds for using threaded compression/encryption/decompression/decryption
# Yeah, these use different capitalization styles. Historical reasons.
SYNC_COMPRESSION_THRESHOLD = {
    'None': 50 * 1024,
    'zlib': 10 * 1024,
    'bzip2': 250,
    'lzma': 0,
}
SYNC_DECOMPRESSION_THRESHOLD = {
    'None': 50 * 1024,
    'ZLIB': 25 * 1024,
    'BZIP2': 3 * 1024,
    'LZMA': 4 * 1024,
}


def sha256(s: bytes) -> bytes:
    return hashlib.sha256(s).digest()


def aes_encryptor(key: bytes) -> CipherContext:
    '''Return AES cipher in CTR mode for *key*'''

    cipher = crypto_ciphers.Cipher(
        crypto_ciphers.algorithms.AES(key),
        crypto_ciphers.modes.CTR(nonce=bytes(16)),
        backend=crypto_backend,
    )
    return cipher.encryptor()


def aes_decryptor(key: bytes) -> CipherContext:
    '''Return AES cipher in CTR mode for *key*'''

    cipher = crypto_ciphers.Cipher(
        crypto_ciphers.algorithms.AES(key),
        crypto_ciphers.modes.CTR(nonce=bytes(16)),
        backend=crypto_backend,
    )
    return cipher.decryptor()


class AsyncComprencBackend(AsyncBackend):
    '''
    This class adds encryption, compression and integrity protection to a plain
    backend.
    '''

    passphrase: bytes | None
    compression: tuple[str | None, int]
    backend: AsyncBackend

    _max_threads: ClassVar[int] = max(1, (os.cpu_count() or 2) // 2)
    _thread_limiter: ClassVar[trio.CapacityLimiter | None] = None

    @classmethod
    def set_max_threads(cls, n: int) -> None:
        '''Set the maximum number of concurrent compression/encryption threads.'''
        cls._max_threads = n
        if cls._thread_limiter is not None:
            cls._thread_limiter.total_tokens = n

    @classmethod
    def _get_thread_limiter(cls) -> trio.CapacityLimiter:
        if cls._thread_limiter is None:
            cls._thread_limiter = trio.CapacityLimiter(cls._max_threads)
        return cls._thread_limiter

    @classmethod
    async def create(  # type: ignore[override]
        cls,
        passphrase: bytes | None,
        compression: tuple[str | None, int],
        backend: AsyncBackend,
    ) -> AsyncComprencBackend:
        return cls(_FACTORY_SENTINEL, passphrase, compression, backend)

    def __init__(
        self,
        _sentinel: object,
        passphrase: bytes | None,
        compression: tuple[str | None, int],
        backend: AsyncBackend,
    ) -> None:
        if _sentinel is not _FACTORY_SENTINEL:
            raise TypeError(
                "Use 'await AsyncComprencBackend.create(...)' instead of direct instantiation"
            )
        super().__init__(_factory_sentinel=_FACTORY_SENTINEL)

        self.passphrase = passphrase
        self.compression = compression
        self.backend = backend

        if compression[0] not in ('bzip2', 'lzma', 'zlib', None) or compression[1] not in range(10):
            raise ValueError(f'Unsupported compression: {compression}')

    @property
    def has_delete_multi(self) -> bool:
        return self.backend.has_delete_multi

    async def reset(self) -> None:
        await self.backend.reset()

    async def lookup(self, key: str) -> BasicMappingT:
        meta_raw = await self.backend.lookup(key)
        meta = self._verify_meta(key, meta_raw)[1]
        meta.pop('__size', None)
        return meta

    async def get_size(self, key: str) -> int:
        '''
        This method returns the compressed size, i.e. the storage space
        that's actually occupied by the object.
        '''

        return await self.backend.get_size(key)

    def is_temp_failure(self, exc: BaseException) -> bool:
        return self.backend.is_temp_failure(exc)

    def _verify_meta(self, key: str, metadata: BasicMappingT) -> tuple[bytes | None, BasicMappingT]:
        '''Unwrap and authenticate metadata

        If the backend has a password set but the object is not encrypted,
        `ObjectNotEncrypted` is raised. Returns the object nonce and its
        metadata. If the object is not encrypted, the nonce is `None`.
        '''

        if not isinstance(metadata, dict):
            raise CorruptedObjectError('metadata should be dict, not %s' % type(metadata))

        format_version = metadata.get('format_version', 0)
        if format_version != 2:
            raise CorruptedObjectError('format_version %r unsupported' % format_version)

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
        if not isinstance(meta_buf, bytes):
            raise CorruptedObjectError('meta data should be bytes, not %s' % type(meta_buf))
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
        if not isinstance(nonce, bytes):
            raise CorruptedObjectError('nonce should be bytes')
        stored_key = metadata['object_id']
        if not isinstance(stored_key, str):
            raise CorruptedObjectError('object_id should be str')
        signature = metadata['signature']
        if not isinstance(signature, bytes):
            raise CorruptedObjectError('signature should be bytes')
        assert self.passphrase is not None
        meta_key = sha256(self.passphrase + nonce + b'meta')
        meta_sig = checksum_basic_mapping(metadata, meta_key)
        if not hmac.compare_digest(signature, meta_sig):
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

    async def readinto_fh(
        self, key: str, fh: BinaryOutput, size_hint: int | None = None
    ) -> BasicMappingT:
        '''Transfer data stored under *key* into *fh*, return metadata.

        The data will be inserted at the current offset. If a temporary error (as defined by
        `is_temp_failure`) occurs, the operation is retried.

        If the backend has a password set but the object is not encrypted, `ObjectNotEncrypted` is
        raised.

        *size_hint*, if given, is an optional hint about the expected size of the object that
        will be used to decide whether to use threaded decompression/decryption or not.
        '''

        buf = io.BytesIO()
        meta_raw = await self.backend.readinto_fh(key, buf)
        (nonce, meta) = self._verify_meta(key, meta_raw)
        if nonce:
            assert self.passphrase is not None
            data_key = sha256(self.passphrase + nonce)
        else:
            data_key = None
        compr_alg = meta_raw['compression']
        assert isinstance(compr_alg, str)
        encr_alg = meta_raw['encryption']
        assert isinstance(encr_alg, str)
        if encr_alg != 'AES_v2' and encr_alg != 'None':
            raise RuntimeError('Unsupported encryption: %r' % encr_alg)
        if compr_alg not in ('BZIP2', 'LZMA', 'ZLIB', 'None'):
            raise RuntimeError('Unsupported compression: %r' % compr_alg)

        # The `payload_offset` key only exists if the storage object was created with on old S3QL
        # version. In order to avoid having to download and re-upload the entire object during the
        # upgrade, the upgrade procedure adds this header to tell us how many bytes at the beginning
        # of the object we have to skip to get to the payload.
        if 'payload_offset' in meta_raw:
            off = meta_raw['payload_offset']
            assert isinstance(off, int)
            buf.seek(off)
        else:
            buf.seek(0)

        # __size was introduced without bumping the filesystem revision, so some objects
        # may not have it. If it's available, use that instead of the size_hint.
        if v := meta.pop('__size', None):
            assert isinstance(v, int)
            size_hint = v

        # In contrast to write_fh, we potentially use sync decompression/decryption here even
        # if writing to disk. The hope is that small amounts of data will fit into the write
        # cache of the OS and thus not cause significant delays.
        if size_hint is None:
            use_async = True
        else:
            use_async = size_hint > SYNC_DECOMPRESSION_THRESHOLD[compr_alg]

        # If not compressed, decrypt directly into `fh`.
        limiter = self._get_thread_limiter()
        if compr_alg == 'None':
            if data_key is not None:
                if use_async:
                    await trio.to_thread.run_sync(decrypt_fh, buf, fh, data_key, limiter=limiter)
                else:
                    decrypt_fh(buf, fh, data_key)
            else:
                # It would be nice to avoid the pointless copy to buf1 in this case, but this makes
                # the code more complicated and we'd need two requests (first one to determine the
                # compression algorithm, second one to fetch the data).
                if use_async:
                    await trio.to_thread.run_sync(copyfh, buf, fh, limiter=limiter)
                else:
                    copyfh(buf, fh)
        else:
            if use_async:
                await trio.to_thread.run_sync(
                    self._decompress_sync, compr_alg, data_key, buf, fh, limiter=limiter
                )
            else:
                self._decompress_sync(compr_alg, data_key, buf, fh)

        return meta

    def _decompress_sync(
        self,
        compr_alg: str,
        data_key: bytes | None,
        ifh: BinaryInput,
        ofh: BinaryOutput,
    ) -> None:
        if data_key is None:
            buf = ifh
        else:
            buf = io.BytesIO()
            decrypt_fh(ifh, buf, data_key)
            buf.seek(0)

        if compr_alg == 'BZIP2':
            decompressor: DecompressorProtocol = bz2.BZ2Decompressor()
        elif compr_alg == 'LZMA':
            decompressor = lzma.LZMADecompressor()
        elif compr_alg == 'ZLIB':
            decompressor = zlib.decompressobj()
        else:
            raise RuntimeError('Unsupported compression: %r' % compr_alg)

        decompress_fh(buf, ofh, decompressor)

    async def write_fh(
        self,
        key: str,
        fh: BinaryInput,
        len_: int,
        metadata: BasicMappingT | None = None,
        dont_compress: bool = False,
    ) -> int:
        '''Upload *len_* bytes from *fh* under *key*.

        The data will be read at the current offset.

        If a temporary error (as defined by `is_temp_failure`) occurs, the operation is
        retried.  Returns the size of the resulting storage object (which may be less due
        to compression)'''

        meta: BasicMappingT = {'__size': len_}
        # meta: BasicMappingT = {}
        if metadata is not None:
            if '__size' in metadata:
                raise ValueError("Metadata key '__size' is reserved")
            meta.update(metadata)

        if not isinstance(fh, io.BytesIO):
            # If we're potentially reading from disk (which may take time), always use async
            use_async = False
        else:
            if dont_compress or self.compression[0] is None:
                compress_alg = 'None'
            else:
                compress_alg = self.compression[0]
            use_async = len_ > SYNC_COMPRESSION_THRESHOLD[compress_alg]

        if use_async:
            # We need to can't directly return an awaitable, because then run_sync believes
            # we wanted to do something differently.
            uploader = (
                await trio.to_thread.run_sync(
                    self._compress_sync,
                    key,
                    fh,
                    len_,
                    meta,
                    dont_compress,
                    limiter=self._get_thread_limiter(),
                )
            )[0]
        else:
            uploader = self._compress_sync(key, fh, len_, meta, dont_compress)[0]

        return await uploader

    def _compress_sync(
        self,
        key: str,
        fh: BinaryInput,
        len_: int,
        metadata: BasicMappingT,
        dont_compress: bool,
    ) -> tuple[Awaitable[int]]:
        meta_buf = freeze_basic_mapping(metadata)
        meta_raw: BasicMappingT = dict(format_version=2)

        if dont_compress or self.compression[0] is None:
            meta_raw['compression'] = 'None'
        else:
            if self.compression[0] == 'zlib':
                compr: CompressorProtocol = zlib.compressobj(self.compression[1])
                meta_raw['compression'] = 'ZLIB'
            elif self.compression[0] == 'bzip2':
                compr = bz2.BZ2Compressor(self.compression[1])
                meta_raw['compression'] = 'BZIP2'
            elif self.compression[0] == 'lzma':
                compr = lzma.LZMACompressor(preset=self.compression[1])
                meta_raw['compression'] = 'LZMA'
            buf = io.BytesIO()
            compress_fh(fh, buf, compr, len_=len_)
            len_ = buf.tell()
            fh = buf
            fh.seek(0)

        if self.passphrase is None:
            meta_raw['encryption'] = 'None'
            meta_raw['data'] = meta_buf
        else:
            nonce = struct.pack('<d', time.time()) + key.encode('utf-8')
            meta_key = sha256(self.passphrase + nonce + b'meta')
            encryptor = aes_encryptor(meta_key)
            meta_raw['encryption'] = 'AES_v2'
            meta_raw['nonce'] = nonce
            meta_raw['data'] = encryptor.update(meta_buf) + encryptor.finalize()
            meta_raw['object_id'] = key
            meta_raw['signature'] = checksum_basic_mapping(meta_raw, meta_key)
            data_key = sha256(self.passphrase + nonce)
            buf = io.BytesIO()
            encrypt_fh(fh, buf, data_key, len_=len_)
            len_ = buf.tell()
            fh = buf
            fh.seek(0)

        # Delibertely return an Awaitable here, hidden in a tuple so trio.to_thread.run_sync
        # doesn't get confused.
        return (self.backend.write_fh(key, fh, len_, meta_raw),)

    async def contains(self, key: str) -> bool:
        return await self.backend.contains(key)

    async def delete(self, key: str) -> None:
        return await self.backend.delete(key)

    async def delete_multi(self, keys: list[str]) -> None:
        return await self.backend.delete_multi(keys)

    async def list(self, prefix: str = '') -> AsyncIterator[str]:
        async for key in self.backend.list(prefix):
            yield key

    async def close(self) -> None:
        await self.backend.close()


def compress_fh(
    ifh: BinaryInput, ofh: BinaryOutput, compr: CompressorProtocol, len_: int | None = None
) -> None:
    '''Compress *len_* bytes from *ifh* into *ofh* using *compr*'''

    while len_ is None or len_ > 0:
        max_ = BUFSIZE if len_ is None else min(BUFSIZE, len_)
        buf = ifh.read(max_)
        if not buf:
            break
        if len_:
            len_ -= len(buf)
        buf = compr.compress(buf)
        if buf:
            ofh.write(buf)

    buf = compr.flush()
    if buf:
        ofh.write(buf)


def encrypt_fh(ifh: BinaryInput, ofh: BinaryOutput, key: bytes, len_: int | None = None) -> None:
    '''Encrypt contents of *ifh* into *ofh*'''

    encryptor = aes_encryptor(key)
    hmac_ = hmac.new(key, digestmod=hashlib.sha256)

    while len_ is None or len_ > 0:
        max_ = BUFSIZE if len_ is None else min(BUFSIZE, len_)
        buf = ifh.read(max_)
        if not buf:
            break
        if len_:
            len_ -= len(buf)

        header = struct.pack(b'<I', len(buf))
        hmac_.update(header)
        ofh.write(encryptor.update(header))

        hmac_.update(buf)
        ofh.write(encryptor.update(buf))

    # Packet length of 0 indicates end of stream, only HMAC follows
    buf = struct.pack(b'<I', 0)
    hmac_.update(buf)
    ofh.write(encryptor.update(buf))
    ofh.write(encryptor.update(hmac_.digest()))


def decompress_fh(ifh: BinaryInput, ofh: BinaryOutput, decompressor: DecompressorProtocol) -> None:
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


def decrypt_fh(ifh: BinaryInput, ofh: BinaryOutput, key: bytes) -> None:
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


def decompress_buf(decomp: DecompressorProtocol, buf: bytes) -> bytes:
    '''Decompress *buf* using *decomp*

    This method encapsulates exception handling for different
    decompressors.
    '''

    try:
        return decomp.decompress(buf)
    except OSError as exc:
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


# TODO: Remove this after async transition is complete
class ComprencBackend(AbstractBackend):
    '''Synchronous wrapper for AsyncComprencBackend.

    For transitional use only, until all code has been converted to async.
    '''

    async_backend: AsyncComprencBackend

    @staticmethod
    def from_async_backend(async_backend: AsyncComprencBackend) -> ComprencBackend:
        '''Create a ComprencBackend from an AsyncComprencBackend'''

        b = ComprencBackend.__new__(ComprencBackend)
        AbstractBackend.__init__(b, async_backend)
        return b

    def __init__(
        self,
        passphrase: bytes | None,
        compression: tuple[str | None, int],
        backend: AbstractBackend,
    ) -> None:
        async_comprenc = run_async(
            AsyncComprencBackend.create,
            passphrase,
            compression,
            backend.async_backend,
        )
        super().__init__(async_comprenc)

    @property
    def passphrase(self) -> bytes | None:
        return self.async_backend.passphrase

    @passphrase.setter
    def passphrase(self, value: bytes | None) -> None:
        self.async_backend.passphrase = value

    @property
    def compression(self) -> tuple[str | None, int]:
        return self.async_backend.compression

    @property
    def backend(self) -> AbstractBackend:
        '''Access underlying raw backend (wrapped as sync)'''
        return AbstractBackend(self.async_backend.backend)

    def readinto_fh(
        self, key: str, fh: BinaryOutput, size_hint: int | None = None
    ) -> BasicMappingT:
        return run_async(self.async_backend.readinto_fh, key, fh, size_hint)

    def write_fh(
        self,
        key: str,
        fh: BinaryInput,
        len_: int,
        metadata: BasicMappingT | None = None,
        dont_compress: bool = False,
    ) -> int:
        return run_async(self.async_backend.write_fh, key, fh, len_, metadata, dont_compress)
