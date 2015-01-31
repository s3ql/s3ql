'''
s3.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from ..logging import logging, QuietError # Ensure use of custom logger class
from . import s3c
from .s3c import get_S3Error
from .common import NoSuchObject, retry
from ..inherit_docstrings import copy_ancestor_docstring
from xml.sax.saxutils import escape as xml_escape
import re

log = logging.getLogger(__name__)

# Maximum number of keys that can be deleted at once
MAX_KEYS = 1000

# Pylint goes berserk with false positives
#pylint: disable=E1002,E1101

class Backend(s3c.Backend):
    """A backend to store data in Amazon S3

    This class uses standard HTTP connections to connect to S3.

    The backend guarantees get after create consistency, i.e. a newly created
    object will be immediately retrievable. Additional consistency guarantees
    may or may not be available and can be queried for with instance methods.
    """

    known_options = ((s3c.Backend.known_options | { 'sse', 'rrs' })
                     - {'dumb-copy', 'disable-expect100'})

    def __init__(self, storage_url, login, password, options):
        super().__init__(storage_url, login, password, options)

    @staticmethod
    def _parse_storage_url(storage_url, ssl_context):
        hit = re.match(r'^s3s?://([^/]+)(?:/(.*))?$', storage_url)
        if not hit:
            raise QuietError('Invalid storage URL', exitcode=2)

        bucket_name = hit.group(1)

        # http://docs.amazonwebservices.com/AmazonS3/2006-03-01/dev/BucketRestrictions.html
        if not re.match('^[a-z0-9][a-z0-9.-]{1,60}[a-z0-9]$', bucket_name):
            raise QuietError('Invalid bucket name.', exitcode=2)

        # Dots in the bucket cause problems with SSL certificate validation,
        # because server certificate is for *.s3.amazonaws.com (which does not
        # match e.g. a.b.s3.amazonaws.com). However, when using path based
        # bucket selection, AWS issues a redirect to the former
        # location. Therefore, S3 buckets with dots in their names cannot be
        # accessed securely. This can only be fixed by Amazon by allowing
        # path-based bucket selection, or providing a valid SSL certificate for
        # endpoints that also matches if the bucket name contains dots.
        if '.' in bucket_name and ssl_context:
            raise QuietError('Buckets with dots in the name cannot be accessed over SSL.\n'
                             'This is purely Amazon\'s fault, see '
                             'https://forums.aws.amazon.com/thread.jspa?threadID=130560',
                             exitcode=2)
        hostname = '%s.s3.amazonaws.com' % bucket_name

        prefix = hit.group(2) or ''
        port = 443 if ssl_context else 80
        return (hostname, port, bucket_name, prefix)

    def __str__(self):
        return 'Amazon S3 bucket %s, prefix %s' % (self.bucket_name, self.prefix)

    @copy_ancestor_docstring
    def delete_multi(self, keys, force=False):
        log.debug('started with %s', keys)

        while len(keys) > 0:
            tmp = keys[:MAX_KEYS]
            try:
                self._delete_multi(tmp, force=force)
            finally:
                keys[:MAX_KEYS] = tmp

    @copy_ancestor_docstring
    def copy(self, src, dest, metadata=None):
        extra_headers = {}
        if 'sse' in self.options:
            extra_headers['x-amz-server-side-encryption'] = 'AES256'
        if 'rrs' in self.options:
            extra_headers['x-amz-storage-class'] = 'REDUCED_REDUNDANCY'
        return super().copy(src, dest, metadata=metadata,
                            extra_headers=extra_headers)

    @copy_ancestor_docstring
    def open_write(self, key, metadata=None, is_compressed=False):
        extra_headers = {}
        if 'sse' in self.options:
            extra_headers['x-amz-server-side-encryption'] = 'AES256'
        if 'rrs' in self.options:
            extra_headers['x-amz-storage-class'] = 'REDUCED_REDUNDANCY'
        return super().open_write(key, metadata=metadata, is_compressed=is_compressed,
                                  extra_headers=extra_headers)

    @retry
    def _delete_multi(self, keys, force=False):

        body = [ '<Delete>' ]
        esc_prefix = xml_escape(self.prefix)
        for key in keys:
            body.append('<Object><Key>%s%s</Key></Object>' % (esc_prefix, xml_escape(key)))
        body.append('</Delete>')
        body = '\n'.join(body).encode('utf-8')
        headers = { 'content-type': 'text/xml; charset=utf-8' }

        resp = self._do_request('POST', '/', subres='delete', body=body, headers=headers)
        try:
            root = self._parse_xml_response(resp)
            ns_p = self.xml_ns_prefix

            error_tags = root.findall(ns_p + 'Error')
            if not error_tags:
                # No errors occured, everything has been deleted
                del keys[:]
                return

            # Some errors occured, so we need to determine what has
            # been deleted and what hasn't
            offset = len(self.prefix)
            for tag in root.findall(ns_p + 'Deleted'):
                fullkey = tag.find(ns_p + 'Key').text
                assert fullkey.startswith(self.prefix)
                keys.remove(fullkey[offset:])

            if log.isEnabledFor(logging.DEBUG):
                for errtag in error_tags:
                    log.debug('Delete %s failed with %s',
                              errtag.findtext(ns_p + 'Key')[offset:],
                              errtag.findtext(ns_p + 'Code'))

            # If *force*, just modify the passed list and return without
            # raising an exception, otherwise raise exception for the first error
            if force:
                return

            errcode = error_tags[0].findtext(ns_p + 'Code')
            errmsg = error_tags[0].findtext(ns_p + 'Message')
            errkey = error_tags[0].findtext(ns_p + 'Key')[offset:]

            if errcode == 'NoSuchKeyError':
                raise NoSuchObject(errkey)
            else:
                raise get_S3Error(errcode, 'Error deleting %s: %s' % (errkey, errmsg))

        except:
            self.conn.discard()
