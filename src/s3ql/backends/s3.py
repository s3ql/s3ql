'''
s3.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

import logging
import re
from xml.sax.saxutils import escape as xml_escape

from ..logging import QuietError
from . import s3c4
from .common import retry
from .s3c import get_S3Error

log = logging.getLogger(__name__)

# Maximum number of keys that can be deleted at once
MAX_KEYS = 1000

# Pylint goes berserk with false positives
# pylint: disable=E1002,E1101


class Backend(s3c4.Backend):
    """A backend to store data in Amazon S3

    This class uses standard HTTP connections to connect to S3.
    """

    known_options = (s3c4.Backend.known_options | {'sse', 'rrs', 'ia', 'oia', 'it'}) - {
        'dumb-copy',
        'disable-expect100',
        'sig-region',
    }

    def __init__(self, options):
        self.region = None
        super().__init__(options)
        self._set_storage_options(self._extra_put_headers)
        self.sig_region = self.region

    def _parse_storage_url(self, storage_url, ssl_context):
        hit = re.match(r'^s3s?://([^/]+)/([^/]+)(?:/(.*))?$', storage_url)
        if not hit:
            raise QuietError('Invalid storage URL', exitcode=2)

        self.region = hit.group(1)
        bucket_name = hit.group(2)

        # http://docs.amazonwebservices.com/AmazonS3/2006-03-01/dev/BucketRestrictions.html
        if not re.match('^[a-z0-9][a-z0-9.-]{1,60}[a-z0-9]$', bucket_name):
            raise QuietError('Invalid bucket name.', exitcode=2)

        if self.region.startswith('cn-'):
            hostname = 's3.%s.amazonaws.com.cn' % self.region
        else:
            hostname = 's3.%s.amazonaws.com' % self.region

        prefix = hit.group(3) or ''
        port = 443 if ssl_context else 80
        return (hostname, port, bucket_name, prefix)

    def __str__(self):
        return 'Amazon S3 bucket %s, prefix %s' % (self.bucket_name, self.prefix)

    @property
    def has_delete_multi(self):
        return True

    def delete_multi(self, keys):
        log.debug('started with %s', keys)

        while len(keys) > 0:
            tmp = keys[:MAX_KEYS]
            try:
                self._delete_multi(tmp)
            finally:
                keys[:MAX_KEYS] = tmp

    def _set_storage_options(self, headers):
        if 'sse' in self.options:
            headers['x-amz-server-side-encryption'] = 'AES256'

        if 'ia' in self.options:
            sc = 'STANDARD_IA'
        elif 'oia' in self.options:
            sc = 'ONEZONE_IA'
        elif 'rrs' in self.options:
            sc = 'REDUCED_REDUNDANCY'
        elif 'it' in self.options:
            sc = 'INTELLIGENT_TIERING'
        else:
            sc = 'STANDARD'
        headers['x-amz-storage-class'] = sc

    @retry
    def _delete_multi(self, keys):
        body = ['<Delete>']
        esc_prefix = xml_escape(self.prefix)
        for key in keys:
            body.append('<Object><Key>%s%s</Key></Object>' % (esc_prefix, xml_escape(key)))
        body.append('</Delete>')
        body = '\n'.join(body).encode('utf-8')
        headers = {'content-type': 'text/xml; charset=utf-8'}

        resp = self._do_request('POST', '/', subres='delete', body=body, headers=headers)
        try:
            root = self._parse_xml_response(resp)
            ns_p = self.xml_ns_prefix

            error_tags = root.findall(ns_p + 'Error')
            if not error_tags:
                # No errors occurred, everything has been deleted
                del keys[:]
                return

            # Some errors occurred, so we need to determine what has
            # been deleted and what hasn't
            offset = len(self.prefix)
            for tag in root.findall(ns_p + 'Deleted'):
                fullkey = tag.find(ns_p + 'Key').text
                assert fullkey.startswith(self.prefix)
                keys.remove(fullkey[offset:])

            if log.isEnabledFor(logging.DEBUG):
                for errtag in error_tags:
                    log.debug(
                        'Delete %s failed with %s',
                        errtag.findtext(ns_p + 'Key')[offset:],
                        errtag.findtext(ns_p + 'Code'),
                    )

            errcode = error_tags[0].findtext(ns_p + 'Code')
            errmsg = error_tags[0].findtext(ns_p + 'Message')
            errkey = error_tags[0].findtext(ns_p + 'Key')[offset:]

            if errcode == 'NoSuchKeyError':
                pass
            else:
                raise get_S3Error(errcode, 'Error deleting %s: %s' % (errkey, errmsg))

        except:
            self.conn.discard()
