'''
storjs3.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

from ..logging import logging, QuietError # Ensure use of custom logger class
from . import s3c
from .s3c import get_S3Error
from .common import NoSuchObject, retry
from ..inherit_docstrings import copy_ancestor_docstring
from xml.sax.saxutils import escape as xml_escape
import re
import time
import urllib.parse
import hashlib
import hmac

log = logging.getLogger(__name__)

OBJ_TRANSLATED_RE = re.compile(r'^.*/-?[0-9]+$')

OBJ_DATA_RE = re.compile(r'^(s3ql_data_)([0-9]+)$')
OBJ_SEQ_NO_RE = re.compile(r'^(s3ql_seq_no_)([0-9]+)$')

OBJ_METADATA_BAK_RE = re.compile(r'^(s3ql_metadata_bak_)([0-9]+)$')
OBJ_METADATA_RE = re.compile(r'^s3ql_metadata$')

OBJ_PASS_BAK_RE = re.compile(r'^(s3ql_passphrase_bak)([0-9])$')
OBJ_PASS_RE = re.compile(r'^s3ql_passphrase$')

PFX_DATA_RE = re.compile(r'^(s3ql_data)(_)$')
PFX_SEQ_NO_RE = re.compile(r'^(s3ql_seq_no)(_)$')
PFX_METADATA_RE = re.compile(r'^s3ql_metadata$')
PFX_PASS_RE = re.compile(r'^s3ql_passphrase$')

OBJ_TEST_RE = re.compile(r'^random\\(\'name\'\s)([0-9]+)$')

class Backend(s3c.Backend):
    """A backend for Storj S3 gateway-st/mt

    Uses some quirks for placing data/seq/metadata objects in the storj bucket.
    This is needed for gateway-st/gateway-mt limited ListObjectsV2 S3-API to work correctly
    with buckets that contains more than 100K objects.
    """

    def __init__(self, options):
        super().__init__(options)

    def _translate_s3_key_to_storj(self, key):
        '''convert object key to the form suitable for use with storj s3 bucket'''
        #check wether key is in already translated form
        match = OBJ_TRANSLATED_RE.match(key)
        if match is not None:
            log.info('skipping already translated key: %s', key)
            return key
        #match sql_data or s3ql_seq_no keys
        match = OBJ_DATA_RE.match(key)
        if match is not None:
            return "s3ql_data/" + match.group(2)
        match = OBJ_SEQ_NO_RE.match(key)
        if match is not None:
            result = "s3ql_seq_no/" + match.group(2)
            log.info('translated seq_no key: %s', result)
            return result
        #match metadata keys
        match = OBJ_METADATA_BAK_RE.match(key)
        if match is not None:
            result = "s3ql_metadata_store/" + match.group(2)
            log.info('translated metadata key: %s', result)
            return result
        match = OBJ_METADATA_RE.match(key)
        if match is not None:
            result = "s3ql_metadata_store/-1"
            log.info('translated metadata key: %s', result)
            return result
        #match s3ql_passphrase keys
        match = OBJ_PASS_BAK_RE.match(key)
        if match is not None:
            result = "s3ql_passphrase_store/" + match.group(2)
            log.info('translated passphrase key: %s', result)
            return result
        match = OBJ_PASS_RE.match(key)
        if match is not None:
            result = "s3ql_passphrase_store/0"
            log.info('translated passphrase key: %s', result)
            return result
        #fix for test-pattern
        match = OBJ_TEST_RE.match(key)
        if match is not None:
            result = "random\\\'name\'/" + match.group(2)
            log.info('translated test key: %s', result)
            return result
        raise RuntimeError(f'Failed to translate unsupported key to storj form: {key}')

    def _translate_storj_key_to_s3(self, key):
        '''convert object key from storj form to normal s3 form'''

    def _translate_s3_prefix_to_storj(self, prefix):
        '''convert ListObjectsV2 search prefix to the form suitable for use with storj s3 bucket'''
        #empty prefix
        if prefix == '':
            log.info('list with empty prefix')
            return prefix
        #match sql_data or s3ql_seq_no prefixes
        match = PFX_DATA_RE.match(prefix)
        if match is not None:
            result = "s3ql_data/"
            log.info('translated data prefix: %s', result)
            return result
        match = PFX_SEQ_NO_RE.match(prefix)
        if match is not None:
            result = "s3ql_seq_no/"
            log.info('translated seq_no prefix: %s', result)
            return result
        #match metadata keys
        match = PFX_METADATA_RE.match(prefix)
        if match is not None:
            result = "s3ql_metadata_store/"
            log.info('translated metadata prefix: %s', result)
            return result
        #match s3ql_passphrase keys
        match = PFX_PASS_RE.match(prefix)
        if match is not None:
            result = "s3ql_passphrase_store/"
            log.info('translated passphrase prefix: %s', result)
            return result
        raise RuntimeError(f'Failed to translate unsupported prefix to storj form: {prefix}')

    @copy_ancestor_docstring
    def delete(self, key, force=False):
        key_t = self._translate_s3_key_to_storj(key)
        return super().delete(key_t, force)

    @copy_ancestor_docstring
    def list(self, prefix=''):
        prefix_t = self._translate_s3_prefix_to_storj(prefix)
        #needs back-conversion for list items
        return super().list(prefix_t)

    @copy_ancestor_docstring
    def lookup(self, key):
        key_t = self._translate_s3_key_to_storj(key)
        return super().lookup(key_t)

    @copy_ancestor_docstring
    def get_size(self, key):
        key_t = self._translate_s3_key_to_storj(key)
        return super().get_size(key_t)

    @copy_ancestor_docstring
    def open_read(self, key):
        key_t = self._translate_s3_key_to_storj(key)
        return super().open_read(key_t)

    @copy_ancestor_docstring
    def open_write(self, key, metadata=None, is_compressed=False, extra_headers=None):
        key_t = self._translate_s3_key_to_storj(key)
        return super().open_write(key_t, metadata, is_compressed, extra_headers)

    @copy_ancestor_docstring
    def copy(self, src, dest, metadata=None, extra_headers=None):
        src_t = self._translate_s3_key_to_storj(src)
        dest_t = self._translate_s3_key_to_storj(dest)
        return super().copy(src_t, dest_t, metadata, extra_headers)

#not needed, will call copy from THIS class
#def update_meta(self, key, metadata):

#not needed
#def close(self):
