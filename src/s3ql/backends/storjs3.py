'''
storjs3.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

import logging
import re
import base64

from ..logging import QuietError
from . import s3c

log = logging.getLogger(__name__)

OBJ_DATA_TRANSLATED_RE = re.compile(r'^(.*)(s3ql_data)/([0-9]+)$')
OBJ_OTHER_TRANSLATED_RE = re.compile(r'^(.*)(s3ql_other)/([a-zA-Z0-9_\-=]*)$')

OBJ_DATA_RE = re.compile(r'^(s3ql_data_)([0-9]+)$')
PFX_DATA = "s3ql_data_"

PFX_DATA_TRANSLATED = "s3ql_data/"
PFX_OTHER_TRANSLATED = "s3ql_other/"

def STR_ENCODE(key):
    return base64.urlsafe_b64encode(key.encode()).decode()

def STR_DECODE(b64key):
    return base64.urlsafe_b64decode(b64key.encode()).decode()

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
        #try to convert binary string key to string in a same manner as in s3c class
        if not isinstance(key, str):
            log.info('key is not a string: %s', key)
            key='%s' % key
        #check wether key is already in storj format, needed if backend methods called recursively from ObjectR or ObjectW classes
        match = OBJ_DATA_TRANSLATED_RE.match(key)
        if match is not None:
            log.info('skipping already translated data key: %s', key)
            return key
        match = OBJ_OTHER_TRANSLATED_RE.match(key)
        if match is not None:
            log.info('skipping already translated key: %s', key)
            return key
        #match sql_data keys
        match = OBJ_DATA_RE.match(key)
        if match is not None:
            return PFX_DATA_TRANSLATED + match.group(2)
        #for all other cases:
        #urlencode key it to remove forward slashes and any possible match with "s3ql_data" word
        #place it into s3ql_other prefix and issue log message
        result = PFX_OTHER_TRANSLATED + STR_ENCODE(key)
        log.info('translated s3 key %s: %s', key, result)
        return result


    def _translate_other_key_to_s3(self, key):
        '''convert object key from storj form to normal s3 form'''
        match = OBJ_OTHER_TRANSLATED_RE.match(key)
        if match is not None:
            result = STR_DECODE(match.group(3))
            log.info('translated storj key %s: %s', key, result)
            return result
        raise RuntimeError(f'Failed to translate key to s3 form: {key}')

    def _translate_data_key_to_s3(self, key):
        '''convert object key from storj form to normal s3 form'''
        match = OBJ_DATA_TRANSLATED_RE.match(key)
        if match is not None:
            result = PFX_DATA + match.group(3)
            #log.info('translated storj key %s: %s', key, result)
            return result
        raise RuntimeError(f'Failed to translate data key to s3 form: {key}')

    @copy_ancestor_docstring
    def list(self, prefix=''):
        #try to convert binary string key to string in a same manner as in s3c class
        if not isinstance(prefix, str):
            log.info('filter prefix is not a string: %s', prefix)
            prefix='%s' % prefix
        log.info('list requested for prefix: %s', prefix)
        #list s3ql_data segments for any partial s3ql_data_ or empty searches
        if PFX_DATA.startswith(prefix):
            log.info('running list for %s sub-prefix', PFX_DATA_TRANSLATED)
            inner_list = super().list(PFX_DATA_TRANSLATED)
            for el in inner_list:
                yield self._translate_data_key_to_s3(el)
        #iterate over sql_other store, if search prefix not exactly "sql_data_"
        if prefix != PFX_DATA:
            #get inner list generator for s3ql_other/ prefix
            log.info('running list for %s sub-prefix with manual filtering', PFX_OTHER_TRANSLATED)
            inner_list = super().list(PFX_OTHER_TRANSLATED)
            #translate keys for s3 form and filter against requested prefix manually
            for el in inner_list:
                el_t = self._translate_other_key_to_s3(el)
                if not el_t.startswith(prefix):
                    continue
                yield el_t

    @copy_ancestor_docstring
    def delete(self, key, force=False):
        key_t = self._translate_s3_key_to_storj(key)
        return super().delete(key_t, force)

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
