'''
comprenc.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from ..logging import logging # Ensure use of custom logger class
from .. import BUFSIZE, PICKLE_PROTOCOL
from .common import AbstractBackend, ChecksumError, safe_unpickle
from ..inherit_docstrings import (copy_ancestor_docstring, prepend_ancestor_docstring,
                                  ABCDocstMeta)
from Crypto.Cipher import AES
from Crypto.Util import Counter
from base64 import b64decode, b64encode
import bz2
import hashlib
import hmac
import lzma
import pickle
import io
import struct
import time
import zlib

log = logging.getLogger(__name__)

HMAC_SIZE = 32

def sha256(s):
    return hashlib.sha256(s).digest()

def aes_cipher(key):
    '''Return AES cipher in CTR mode for *key*'''

    return AES.new(key, AES.MODE_CTR,
                   counter=Counter.new(128, initial_value=0))

class ComprencBackend(AbstractBackend, metaclass=ABCDocstMeta):
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

    @property
    @copy_ancestor_docstring
    def has_native_rename(self):
        return self.backend.has_native_rename

    @copy_ancestor_docstring
    def reset(self):
        self.backend.reset()

    @copy_ancestor_docstring
    def lookup(self, key):
        meta_raw = self.backend.lookup(key)
        if 'format_version' not in meta_raw:
            meta_raw = self._convert_legacy_metadata(meta_raw)
        return self._verify_meta(key, meta_raw)[1]

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

    def _verify_meta(self, key, metadata):
        '''Unwrap and authenticate metadata

        If the backend has a password set but the object is not encrypted,
        `ObjectNotEncrypted` is raised. Returns the object nonce and its
        metadata. If the object is not encrypted, the nonce is `None`.
        '''

        if not isinstance(metadata, dict):
            raise MalformedObjectError()
        for mkey in ('encryption', 'compression', 'data'):
            if mkey not in metadata:
                raise MalformedObjectError()

        encr_alg = metadata['encryption']
        encrypted = (encr_alg != 'None')

        if encrypted and self.passphrase is None:
            raise ChecksumError('Encrypted object and no passphrase supplied')

        elif not encrypted and self.passphrase is not None:
            raise ObjectNotEncrypted()

        buf = b64decode(metadata['data'])

        if not encrypted:
            try:
                return (None, safe_unpickle(buf, encoding='latin1'))
            except pickle.UnpicklingError:
                raise ChecksumError('Invalid metadata')

        # Encrypted
        for mkey in ('nonce', 'signature', 'object_id'):
            if mkey not in metadata:
                raise MalformedObjectError()

        nonce = b64decode(metadata['nonce'])
        meta_key = sha256(self.passphrase + nonce + b'meta')
        meta_sig = compute_metadata_signature(meta_key, metadata)
        if not hmac.compare_digest(b64decode(metadata['signature']),
                                   meta_sig):
            raise ChecksumError('HMAC mismatch')

        stored_key = b64decode(metadata['object_id']).decode('utf-8')
        if stored_key != key:
            raise ChecksumError('Object content does not match its key (%s vs %s)'
                                % (stored_key, key))

        buf = b64decode(metadata['data'])
        return (nonce, safe_unpickle(aes_cipher(meta_key).decrypt(buf),
                                     encoding='latin1'))

    @prepend_ancestor_docstring
    def open_read(self, key):
        """
        If the backend has a password set but the object is not encrypted,
        `ObjectNotEncrypted` is raised.
        """

        fh = self.backend.open_read(key)
        checksum_warning = False
        try:
            if 'format_version' in fh.metadata:
                meta_raw = fh.metadata
            else:
                meta_raw = self._convert_legacy_metadata(fh.metadata)

            # Also checks if this is a BetterBucket storage object
            (nonce, meta) = self._verify_meta(key, meta_raw)
            if nonce:
                data_key = sha256(self.passphrase + nonce)

            compr_alg = meta_raw['compression']
            encr_alg = meta_raw['encryption']

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

            # The `payload_offset` key only exists if the storage object was
            # created with on old S3QL version. In order to avoid having to
            # download and re-upload the entire object during the upgrade, the
            # upgrade procedure adds this header to tell us how many bytes at
            # the beginning of the object we have to skip to get to the payload.
            if 'payload_offset' in meta_raw:
                to_skip = meta_raw['payload_offset']
                while to_skip:
                    to_skip -= len(fh.read(to_skip))

            # If we've come this far, we want to emit a warning if the object
            # has not been read completely on close().
            checksum_warning = True

            if encr_alg == 'AES':
                fh = LegacyDecryptDecompressFilter(fh, data_key, decompressor)
                decompressor = None
            elif encr_alg == 'AES_v2':
                fh = DecryptFilter(fh, data_key)
            elif encr_alg != 'None':
                raise RuntimeError('Unsupported encryption: %s' % encr_alg)

            if decompressor:
                fh = DecompressFilter(fh, decompressor)

            fh.metadata = meta
        except:
            fh.close(checksum_warning=checksum_warning)
            raise

        return fh

    @copy_ancestor_docstring
    def open_write(self, key, metadata=None, is_compressed=False):

        if metadata is None:
            metadata = dict()
        elif not isinstance(metadata, dict):
            raise TypeError('*metadata*: expected dict or None, got %s' % type(metadata))

        meta_buf = pickle.dumps(metadata, PICKLE_PROTOCOL)
        meta_raw = dict(format_version=1)

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
            nonce = struct.pack('<f', time.time()) + key.encode('utf-8')
            meta_key = sha256(self.passphrase + nonce + b'meta')
            data_key = sha256(self.passphrase + nonce)
            meta_raw['encryption'] = 'AES_v2'
            meta_raw['nonce'] = b64encode(nonce)
            meta_raw['data'] = b64encode(aes_cipher(meta_key).encrypt(meta_buf))
            meta_raw['object_id'] = b64encode(key.encode('utf-8'))
            meta_sig = compute_metadata_signature(meta_key, meta_raw)
            meta_raw['signature'] = b64encode(meta_sig)
        else:
            meta_raw['encryption'] = 'None'
            meta_raw['data'] = b64encode(meta_buf)

        fh = self.backend.open_write(key, meta_raw)

        if self.passphrase is not None:
            fh = EncryptFilter(fh, data_key)
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
    def update_meta(self, key, metadata):
        if not isinstance(metadata, dict):
            raise TypeError('*metadata*: expected dict, got %s' % type(metadata))
        self._copy_or_rename(src=key, dest=key, rename=False,
                             metadata=metadata)

    @copy_ancestor_docstring
    def copy(self, src, dest, metadata=None):
        if not (metadata is None or isinstance(metadata, dict)):
            raise TypeError('*metadata*: expected dict or None, got %s' % type(metadata))
        self._copy_or_rename(src, dest, rename=False, metadata=metadata)

    @copy_ancestor_docstring
    def rename(self, src, dest, metadata=None):
        if not (metadata is None or isinstance(metadata, dict)):
            raise TypeError('*metadata*: expected dict or None, got %s' % type(metadata))
        self._copy_or_rename(src, dest, rename=True, metadata=metadata)

    def _copy_or_rename(self, src, dest, rename, metadata=None):
        meta_raw = self.backend.lookup(src)
        if 'format_version' not in meta_raw:
            meta_raw = self._convert_legacy_metadata(meta_raw)
        (nonce, meta_old) = self._verify_meta(src, meta_raw)

        if nonce:
            meta_key = sha256(self.passphrase + nonce + b'meta')
            if metadata is not None:
                meta_buf = pickle.dumps(metadata, PICKLE_PROTOCOL)
                meta_raw['data'] = b64encode(aes_cipher(meta_key).encrypt(meta_buf))
            meta_raw['object_id'] = b64encode(dest.encode('utf-8'))
            meta_raw['signature'] = b64encode(compute_metadata_signature(meta_key, meta_raw))
        elif metadata is None:
            # Just copy old metadata
            meta_raw = None
        else:
            meta_buf = pickle.dumps(metadata, PICKLE_PROTOCOL)
            meta_raw['data'] = b64encode(meta_buf)

        if src == dest: # metadata update only
            self.backend.update_meta(src, meta_raw)
        elif rename:
            self.backend.rename(src, dest, metadata=meta_raw)
        else:
            self.backend.copy(src, dest, metadata=meta_raw)

    @copy_ancestor_docstring
    def close(self):
        self.backend.close()

    def _convert_legacy_metadata(self, meta,
                                 LEN_BYTES = struct.calcsize(b'<B'),
                                 TIME_BYTES = struct.calcsize(b'<f')):
        '''Convert metadata to newest format

        This method ensures that we can read objects written
        by older S3QL versions.
        '''

        meta_new = dict(format_version=1)

        if ('encryption' in meta and
            'compression' in meta):
            meta_new['encryption'] = meta['encryption']
            meta_new['compression'] = meta['compression']

        elif 'encrypted' in meta:
            s = meta['encrypted']
            if s == 'True':
                meta_new['encryption'] = 'AES'
                meta_new['compression'] = 'BZIP2'

            elif s == 'False':
                meta_new['encryption'] = 'None'
                meta_new['compression'] = 'None'

            elif s.startswith('AES/'):
                meta_new['encryption'] = 'AES'
                meta_new['compression'] = s[4:]

            elif s.startswith('PLAIN/'):
                meta_new['encryption'] = 'None'
                meta_new['compression'] = s[6:]
            else:
                raise RuntimeError('Unsupported encryption')

            if meta_new['compression'] == 'BZ2':
                meta_new['compression'] = 'BZIP2'

            if meta_new['compression'] == 'NONE':
                meta_new['compression'] = 'None'
        else:
            meta_new['encryption'] = 'None'
            meta_new['compression'] = 'None'

        # Extract metadata (pre 2.x versions use multiple headers)
        if any(k.startswith('meta') for k in meta):
            parts = [ meta[k] for k in sorted(meta.keys())
                      if k.startswith('meta') ]
            meta_new['data'] = ''.join(parts)
        else:
            try:
                meta_new['data'] = meta['data']
            except KeyError:
                raise MalformedObjectError()

        if not self.passphrase:
            return meta_new

        meta_buf = b64decode(meta_new['data'])
        off = 0
        def read(len_):
            nonlocal off
            tmp = meta_buf[off:off+len_]
            off += len_
            return tmp

        len_ = struct.unpack(b'<B', read(LEN_BYTES))[0]
        nonce = read(len_)
        key = sha256(self.passphrase + nonce)
        cipher = aes_cipher(key)
        hmac_ = hmac.new(key, digestmod=hashlib.sha256)
        hash_ = read(HMAC_SIZE)
        meta_buf = meta_buf[off:]
        meta_buf_plain = cipher.decrypt(meta_buf)
        hmac_.update(meta_buf_plain)
        hash_ = cipher.decrypt(hash_)

        if not hmac.compare_digest(hash_, hmac_.digest()):
            raise ChecksumError('HMAC mismatch')

        obj_id = nonce[TIME_BYTES:].decode('utf-8')
        meta_key = sha256(self.passphrase + nonce + b'meta')
        meta_new['nonce'] = b64encode(nonce)
        meta_new['payload_offset'] = LEN_BYTES + len(nonce)
        meta_new['data'] = b64encode(aes_cipher(meta_key).encrypt(meta_buf_plain))
        meta_new['object_id'] = b64encode(obj_id.encode('utf-8'))
        signature = compute_metadata_signature(meta_key, meta_new)
        meta_new['signature'] = b64encode(signature)

        return meta_new

def compute_metadata_signature(key, metadata):
    '''Compute HMAC for metadata dictionary'''

    hmac_ = hmac.new(key, digestmod=hashlib.sha256)
    for mkey in sorted(metadata.keys()):
        assert isinstance(mkey, str)
        if mkey == 'signature':
            continue
        val = metadata[mkey]
        if isinstance(val, str):
            val = val.encode('utf-8')
        elif not isinstance(val, (bytes, bytearray)):
            val = pickle.dumps(val, PICKLE_PROTOCOL)
        hmac_.update(mkey.encode('utf-8') + val)
    return hmac_.digest()

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

class InputFilter(io.RawIOBase):

    # Overwrite default implementation to make sure that we're using a decent
    # blocksize
    def readall(self):
        """Read until EOF, using multiple read() calls."""

        res = bytearray()
        while True:
            data = self.read(BUFSIZE)
            if not data:
                break
            res += data
        return res

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

    def discard_input(self):
        while True:
            buf = self.fh.read(BUFSIZE)
            if not buf:
                break

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

        This method is currently buggy and may also return *more* than *size*
        bytes. Callers should be prepared to handle that. This is because some
        of the used (de)compression modules don't support output limiting.
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
                # Read rest of stream, so that we raise HMAC or MD5 error instead
                # if problem is on lower layer
                self.discard_input()
                raise

        return buf

    def close(self, *a, **kw):
        self.fh.close(*a, **kw)

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()
        return False


class EncryptFilter(object):
    '''Encrypt data while writing'''

    def __init__(self, fh, key):
        '''Initialize

        *fh* should be a file-like object.
        '''
        super().__init__()

        self.fh = fh
        self.obj_size = 0
        self.closed = False
        self.cipher = aes_cipher(key)
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
        buf2 = self.cipher.encrypt(buf)
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
            buf2 = self.cipher.encrypt(buf)
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


class DecryptFilter(InputFilter):
    '''Decrypt data while reading

    Reader has to read the entire stream in order for HMAC
    checking to work.
    '''

    off_size = struct.calcsize(b'<I')

    def __init__(self, fh, key, metadata=None):
        '''Initialize

        *fh* should be a file-like object that may be unbuffered.
        '''
        super().__init__()

        self.fh = fh
        self.remaining = 0 # Remaining length of current packet
        self.metadata = metadata
        self.hmac_checked = False
        self.cipher = aes_cipher(key)
        self.hmac = hmac.new(key, digestmod=hashlib.sha256)

    def _read_and_decrypt(self, size):
        '''Read and decrypt up to *size* bytes'''

        if not isinstance(size, int) or size <= 0:
            raise ValueError("Exact *size* required (got %d)" % size)

        buf = self.fh.read(size)
        if not buf:
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

        # If HMAC has been checked, then we've read the complete file (we don't
        # want to read b'' from the underlying fh repeatedly)
        if self.hmac_checked:
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
                    buf = self._read_and_decrypt(HMAC_SIZE+1)
                    assert buf
                    inbuf += buf

                if len(inbuf) > HMAC_SIZE or self.fh.read(1):
                    # Read rest of stream, so that we raise MD5 error instead
                    # if problem is on lower layer
                    self.discard_input()
                    raise ChecksumError('Extraneous data at end of object')

                if not hmac.compare_digest(inbuf, self.hmac.digest()):
                    raise ChecksumError('HMAC mismatch')

                self.hmac_checked = True
                break

        return outbuf

    def close(self, *a, **kw):
        self.fh.close(*a, **kw)

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

    def __init__(self, fh, key, decomp):
        '''Initialize

        *fh* should be a file-like object and may be unbuffered.
        '''
        super().__init__()

        self.fh = fh
        self.decomp = decomp
        self.hmac_checked = False
        self.cipher = aes_cipher(key)
        self.hmac = hmac.new(key, digestmod=hashlib.sha256)
        self.hash = fh.read(HMAC_SIZE)

    def discard_input(self):
        while True:
            buf = self.fh.read(BUFSIZE)
            if not buf:
                break

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

    def close(self, *a, **kw):
        self.fh.close(*a, **kw)

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
        if (exc.args[0].lower().startswith('corrupt input data')
            or exc.args[0].startswith('Input format not supported')):
            raise ChecksumError('Invalid compressed stream')
        raise
    except zlib.error as exc:
        if exc.args[0].lower().startswith('error -3 while decompressing'):
            raise ChecksumError('Invalid compressed stream')
        raise

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
    Raised by ComprencBackend when trying to access an object that
    wasn't stored by ComprencBackend, i.e. has no information about
    encryption or compression.
    '''

    pass
