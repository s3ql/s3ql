'''
backends/gs.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from ..logging import logging, QuietError # Ensure use of custom logger class
from . import s3c
from .s3c import C_DAY_NAMES, C_MONTH_NAMES, HTTPError, S3Error
from .common import AuthenticationError, retry, NoSuchObject
from .. import oauth_client
from ..inherit_docstrings import copy_ancestor_docstring
from dugong import CaseInsensitiveDict, HTTPConnection
from urllib.parse import urlencode
import re
import json
import threading
import time

# Pylint goes berserk with false positives
#pylint: disable=E1002,E1101,W0201

log = logging.getLogger(__name__)

class Backend(s3c.Backend):
    """A backend to store data in Google Storage

    This class uses standard HTTP connections to connect to GS.

    The backend guarantees immediate get consistency and eventual list
    consistency.
    """

    use_expect_100c = False
    xml_ns_prefix = '{http://doc.s3.amazonaws.com/2006-03-01}'

    # We don't want to request an access token for each instance,
    # because there is a limit on the total number of valid tokens.
    # This class variable holds the mapping from refresh tokens to
    # access tokens.
    access_token = dict()
    _refresh_lock = threading.Lock()

    def __init__(self, storage_url, gs_key, gs_secret, ssl_context=None, proxy=None):
        super().__init__(storage_url, gs_key, gs_secret, ssl_context=ssl_context,
                         proxy=proxy)

        self.use_oauth2 = (gs_key == 'oauth2')

        if self.use_oauth2:
            self.hdr_prefix = 'x-goog-'

    @staticmethod
    def _parse_storage_url(storage_url, ssl_context):
        # Special case for unit testing against local mock server
        hit = re.match(r'^gs://!unittest!'
                       r'([^/:]+)' # Hostname
                       r':([0-9]+)' # Port
                       r'/([^/]+)' # Bucketname
                       r'(?:/(.*))?$', # Prefix
                       storage_url)
        if hit:
            hostname = hit.group(1)
            port = int(hit.group(2))
            bucket_name = hit.group(3)
            prefix = hit.group(4) or ''
            return (hostname, port, bucket_name, prefix)

        hit = re.match(r'^gs://([^/]+)(?:/(.*))?$', storage_url)
        if not hit:
            raise QuietError('Invalid storage URL', exitcode=2)

        bucket_name = hit.group(1)

        # Dots in the bucket cause problems with SSL certificate validation,
        # because server certificate is for *.commondatastorage.googleapis.com
        # (which does not match e.g. a.b.commondatastorage.googleapis.com)
        if '.' in bucket_name and ssl_context:
            hostname = 'commondatastorage.googleapis.com'
        else:
            hostname = '%s.commondatastorage.googleapis.com' % bucket_name

        prefix = hit.group(2) or ''
        port = 443 if ssl_context else 80
        return (hostname, port, bucket_name, prefix)

    def __str__(self):
        return 'Google Storage bucket %s, prefix %s' % (self.bucket_name, self.prefix)

    def _authorize_request(self, method, path, headers, subres):
        '''Add authorization information to *headers*'''

        if not self.use_oauth2:
            return super()._authorize_request(method, path, headers, subres)

        headers['Authorization'] = 'Bearer ' + self.access_token[self.password]

        now = time.gmtime()
        headers['Date'] = ('%s, %02d %s %04d %02d:%02d:%02d GMT'
                           % (C_DAY_NAMES[now.tm_wday],
                              now.tm_mday,
                              C_MONTH_NAMES[now.tm_mon - 1],
                              now.tm_year, now.tm_hour,
                              now.tm_min, now.tm_sec))

    def _get_access_token(self):
        log.info('Requesting new access token')

        headers = CaseInsensitiveDict()
        headers['Content-Type'] = 'application/x-www-form-urlencoded; charset=utf-8'

        body = urlencode({'client_id': oauth_client.CLIENT_ID,
                          'client_secret': oauth_client.CLIENT_SECRET,
                          'refresh_token': self.password,
                          'grant_type': 'refresh_token' })

        conn = HTTPConnection('accounts.google.com', 443, proxy=self.proxy,
                              ssl_context=self.ssl_context)
        try:

            conn.send_request('POST', '/o/oauth2/token', headers=headers,
                              body=body.encode('utf-8'))
            resp = conn.read_response()

            resp_json = None
            if 'Content-Type' in resp.headers:
                hit = re.match(r'application/json(?:; charset="(.+)")?$',
                               resp.headers['Content-Type'], re.IGNORECASE)
                if hit:
                    charset = hit.group(1) or 'utf-8'
                    body = conn.readall().decode(charset)
                    resp_json = json.loads(body)

            if 'error' in resp_json:
                raise AuthenticationError(resp_json['error'])

            if resp.status > 299 or resp.status < 200:
                raise HTTPError(resp.status, resp.reason, resp.headers)

            if 'access_token' not in resp_json:
                raise RuntimeError('Unable to parse server response')

            self.access_token[self.password] = resp_json['access_token']

        finally:
            conn.disconnect()

    def _do_request(self, method, path, subres=None, query_string=None,
                    headers=None, body=None):

        # When using OAuth2 and we have an access token, try to use
        # it (and invalidate if expired)
        if self.use_oauth2 and self.password in self.access_token:
            try:
                return super()._do_request(method, path, subres=subres, headers=headers,
                                           query_string=query_string, body=body)
            except HTTPError as exc:
                if exc.status != 401:
                    raise
            except S3Error as exc:
                if exc.code != 'AuthenticationRequired':
                    raise
            try:
                del self.access_token[self.password]
            except KeyError: # Mind multithreading..
                pass

        # If we use OAuth2 and don't have an access token, retrieve
        # one
        if self.use_oauth2 and self.password not in self.access_token:
            # Grab lock to prevent multiple threads from refreshing the token
            with self._refresh_lock:
                if self.password not in self.access_token:
                    self._get_access_token()

        # Try request. If we are using OAuth2 and this fails, propagate
        # the error (because we have just refreshed the access token)
        return super()._do_request(method, path, subres=subres, headers=headers,
                                   query_string=query_string, body=body)

    # Overwrite, because Google Storage does not return errors after
    # 200 OK.
    @retry
    @copy_ancestor_docstring
    def copy(self, src, dest, metadata=None):
        log.debug('copy(%s, %s): start', src, dest)

        if not (metadata is None or isinstance(metadata, dict)):
            raise TypeError('*metadata*: expected dict or None, got %s' % type(metadata))

        headers = CaseInsensitiveDict()
        headers[self.hdr_prefix + 'copy-source'] = \
            '/%s/%s%s' % (self.bucket_name, self.prefix, src)

        if metadata is None:
            headers[self.hdr_prefix + 'metadata-directive'] = 'COPY'
        else:
            headers[self.hdr_prefix + 'metadata-directive'] = 'REPLACE'
            self._add_meta_headers(headers, metadata)

        try:
            self._do_request('PUT', '/%s%s' % (self.prefix, dest), headers=headers)
            self.conn.discard()
        except s3c.NoSuchKeyError:
            raise NoSuchObject(src)

