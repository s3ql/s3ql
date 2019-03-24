'''
backends/gs.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

from ..logging import logging, QuietError # Ensure use of custom logger class
from .common import (AbstractBackend, NoSuchObject, retry, AuthorizationError,
                     AuthenticationError, DanglingStorageURLError,
                     get_proxy, get_ssl_context, CorruptedObjectError,
                     checksum_basic_mapping)
from ..common import OAUTH_CLIENT_ID, OAUTH_CLIENT_SECRET
from .. import BUFSIZE
from ..inherit_docstrings import (copy_ancestor_docstring, prepend_ancestor_docstring,
                                  ABCDocstMeta)
from dugong import (HTTPConnection, is_temp_network_error, BodyFollowing, CaseInsensitiveDict,
                    ConnectionClosed)
from base64 import b64encode, b64decode
from itertools import count
from ast import literal_eval

import hashlib
import urllib.parse
import re
import tempfile
import os
import dugong
import json
import threading
import ssl
from typing import Optional, Dict, Any

try:
    import google.auth as g_auth
except ImportError:
    g_auth = None


log = logging.getLogger(__name__)

# Used only by adm.py
UPGRADE_MODE=False


class ServerResponseError(Exception):
    '''Raised if the server response cannot be parsed.

    For HTTP errors (i.e., non 2xx response codes), RequestError should
    always be used instead (since in that case the response body can
    not be expected to have any specific format).
    '''

    def __init__(self, resp: dugong.HTTPResponse, error: str,
                 body: str):
        self.resp = resp
        self.error = error
        self.body = body

    def __str__(self):
        return '<ServerResponseError: %s>' % self.error


class RequestError(Exception):
    '''
    An error returned by the server.
    '''

    def __init__(self, code: str, reason: str, message: Optional[str] = None,
                 body: Optional[str] = None):
        super().__init__()
        self.code = code
        self.reason = reason
        self.message = message
        self.body = body

    def __str__(self) -> str:
        if self.message:
            return '<RequestError, code=%d, reason=%r, message=%r>' % (
                self.code, self.reason, self.message)
        elif self.body:
            return '<RequestError, code=%d, reason=%r, with body data>' % (
                self.code, self.reason)
        else:
            return '<RequestError, code=%d, reason=%r>' % (
                self.code, self.reason)


class AccessTokenExpired(Exception):
    '''
    Raised if the access token has expired.
    '''


class Backend(AbstractBackend, metaclass=ABCDocstMeta):
    """A backend to store data in Google Storage"""

    known_options = {'ssl-ca-path', 'tcp-timeout'}

    # We don't want to request an access token for each instance,
    # because there is a limit on the total number of valid tokens.
    # This class variable holds the mapping from refresh tokens to
    # access tokens.
    access_token = dict()
    _refresh_lock = threading.Lock()
    adc = None

    def __init__(self, options):
        super().__init__()

        self.ssl_context = get_ssl_context(
            options.backend_options.get('ssl-ca-path', None)) # type: Optional[ssl.Context]
        self.options = options.backend_options  # type: Dict[str, str]
        self.proxy = get_proxy(ssl=True) # type: str
        self.login = options.backend_login  # type: str
        self.refresh_token = options.backend_password # type: str

        if self.login == 'adc':
            if g_auth is None:
                raise QuietError('ADC authentification requires the google.auth module')
            elif self.adc is None:
                import google.auth.transport.urllib3
                import urllib3

                # Deliberately ignore proxy and SSL context when attemping
                # to connect to Compute Engine Metadata server.
                requestor = google.auth.transport.urllib3.Request(
                    urllib3.PoolManager())
                try:
                    credentials, _ = g_auth.default(
                        request=requestor,
                        scopes=['https://www.googleapis.com/auth/devstorage.full_control'])
                except g_auth.exceptions.DefaultCredentialsError as exc:
                    raise QuietError('ADC found no valid credential sources: ' + str(exc))
                type(self).adc = (credentials, requestor)
        elif self.login != 'oauth2':
            raise QuietError("Google Storage backend requires OAuth2 or ADC authentication")

        # Special case for unit testing against local mock server
        hit = re.match(r'^gs://!unittest!'
                       r'([^/:]+)' # Hostname
                       r':([0-9]+)' # Port
                       r'/([^/]+)' # Bucketname
                       r'(?:/(.*))?$', # Prefix
                       options.storage_url)
        if hit:
            self.hostname = hit.group(1)
            self.port = int(hit.group(2))
            self.bucket_name = hit.group(3)
            self.prefix = hit.group(4) or ''
        else:
            hit = re.match(r'^gs://([^/]+)(?:/(.*))?$', options.storage_url)
            if not hit:
                raise QuietError('Invalid storage URL', exitcode=2)

            self.bucket_name = hit.group(1)
            self.hostname = 'www.googleapis.com'
            self.prefix = hit.group(2) or ''
            self.port = 443

        self.conn = self._get_conn()

        # Check if bucket exists and/or credentials are correct
        path = '/storage/v1/b/' + urllib.parse.quote(self.bucket_name, safe='')
        try:
            resp = self._do_request('GET', path)
        except RequestError as exc:
            if exc.code == 404:
                raise DanglingStorageURLError("Bucket '%s' does not exist" %
                                              self.bucket_name)
            exc = _map_request_error(exc, None)
            if exc:
                raise exc
            raise
        self._parse_json_response(resp)

    @property
    @copy_ancestor_docstring
    def has_native_rename(self):
        return False

    @copy_ancestor_docstring
    def reset(self):
        if (self.conn is not None and
            (self.conn.response_pending() or self.conn._out_remaining)):
            log.debug('Resetting state of http connection %d', id(self.conn))
            self.conn.disconnect()

    def _get_conn(self):
        '''Return connection to server'''

        conn =  HTTPConnection(self.hostname, self.port, proxy=self.proxy,
                               ssl_context=self.ssl_context)
        conn.timeout = int(self.options.get('tcp-timeout', 20))
        return conn


    @copy_ancestor_docstring
    def is_temp_failure(self, exc): #IGNORE:W0613
        if is_temp_network_error(exc) or isinstance(exc, ssl.SSLError):
            # We probably can't use the connection anymore, so use this
            # opportunity to reset it
            self.conn.reset()
            return True

        elif isinstance(exc, RequestError) and (
                500 <= exc.code <= 599 or exc.code == 408):
            return True

        elif isinstance(exc, AccessTokenExpired):
            return True

        # Not clear at all what is happening here, but in doubt we retry
        elif isinstance(exc, ServerResponseError):
            return True

        if g_auth and isinstance(exc, g_auth.exceptions.TransportError):
            return True

        return False

    def _assert_empty_response(self, resp):
        '''Assert that current response body is empty'''

        buf = self.conn.read(2048)
        if not buf:
            return # expected

        body = '\n'.join('%s: %s' % x for x in resp.headers.items())

        hit = re.search(r'; charset="(.+)"$',
                        resp.headers.get('Content-Type', ''),
                        re.IGNORECASE)
        if hit:
            charset = hit.group(1)
            body += '\n' + buf.decode(charset, errors='backslashreplace')

        log.warning('Expected empty response body, but got data - this is odd.')
        raise ServerResponseError(resp, error='expected empty response',
                                  body=body)

    @retry
    @copy_ancestor_docstring
    def delete(self, key, force=False, is_retry=False):
        log.debug('started with %s', key)
        path = '/storage/v1/b/%s/o/%s' % (
            urllib.parse.quote(self.bucket_name, safe=''),
            urllib.parse.quote(self.prefix + key, safe=''))
        try:
            resp = self._do_request('DELETE', path)
            self._assert_empty_response(resp)
        except RequestError as exc:
            exc = _map_request_error(exc, key)
            if isinstance(exc, NoSuchObject) and (force or is_retry):
                pass
            elif exc:
                raise exc
            else:
                raise


    @copy_ancestor_docstring
    def list(self, prefix=''):
        prefix = self.prefix + prefix
        strip = len(self.prefix)
        page_token = None
        while True:
            (els, page_token) = self._list_page(prefix, page_token)
            for el in els:
                yield el[strip:]
            if page_token is None:
                break

    @retry
    def _list_page(self, prefix, page_token=None, batch_size=1000):

        # Limit maximum number of results since we read everything
        # into memory (because Python JSON doesn't have a streaming API)
        query_string = { 'prefix': prefix, 'maxResults': str(batch_size) }
        if page_token:
            query_string['pageToken'] = page_token

        path = '/storage/v1/b/%s/o' % (
            urllib.parse.quote(self.bucket_name, safe=''),)

        try:
            resp = self._do_request('GET', path, query_string=query_string)
        except RequestError as exc:
            exc = _map_request_error(exc, None)
            if exc:
                raise exc
            raise
        json_resp = self._parse_json_response(resp)
        page_token = json_resp.get('nextPageToken', None)

        if 'items' not in json_resp:
            assert page_token is None
            return ((), None)

        return ([ x['name'] for x in json_resp['items'] ], page_token)


    @retry
    @copy_ancestor_docstring
    def lookup(self, key):
        log.debug('started with %s', key)
        return _unwrap_user_meta(self._get_gs_meta(key))

    def _get_gs_meta(self, key):

        path = '/storage/v1/b/%s/o/%s' % (
            urllib.parse.quote(self.bucket_name, safe=''),
            urllib.parse.quote(self.prefix + key, safe=''))
        try:
            resp = self._do_request('GET', path)
        except RequestError as exc:
            exc = _map_request_error(exc, key)
            if exc:
                raise exc
            raise
        return self._parse_json_response(resp)

    @retry
    @copy_ancestor_docstring
    def get_size(self, key):
        json_resp = self._get_gs_meta(key)
        return json_resp['size']

    @retry
    @copy_ancestor_docstring
    def open_read(self, key):
        gs_meta = self._get_gs_meta(key)

        path = '/storage/v1/b/%s/o/%s' % (
            urllib.parse.quote(self.bucket_name, safe=''),
            urllib.parse.quote(self.prefix + key, safe=''))
        try:
            resp = self._do_request('GET', path, query_string={'alt': 'media'})
        except RequestError as exc:
            exc = _map_request_error(exc, key)
            if exc:
                raise exc
            raise

        return ObjectR(key, resp, self, gs_meta)

    @prepend_ancestor_docstring
    def open_write(self, key, metadata=None, is_compressed=False):
        """
        The returned object will buffer all data and only start the upload
        when its `close` method is called.
        """

        return ObjectW(key, self, metadata)

    @retry
    def write_fh(self, fh, key: str, md5: bytes,
                 metadata: Optional[Dict[str, Any]] = None,
                 size: Optional[int] = None):
        '''Write data from byte stream *fh* into *key*.

        *fh* must be seekable. If *size* is None, *fh* must also implement
        `fh.fileno()` so that the size can be determined through `os.fstat`.

        *md5* must be the (binary) md5 checksum of the data.
        '''

        metadata = json.dumps({
            'metadata': _wrap_user_meta(metadata if metadata else {}),
            'md5Hash': b64encode(md5).decode(),
            'name': self.prefix + key,
        })

        # Google Storage uses Content-Length to read the object data, so we
        # don't have to worry about the boundary occurring in the object data.
        boundary = 'foo_bar_baz'
        headers = CaseInsensitiveDict()
        headers['Content-Type'] = 'multipart/related; boundary=%s' % boundary

        body_prefix = '\n'.join(('--' + boundary,
                                 'Content-Type: application/json; charset=UTF-8',
                                 '', metadata,
                                 '--' + boundary,
                                 'Content-Type: application/octet-stream',
                                 '', '')).encode()
        body_suffix = ('\n--%s--\n' % boundary).encode()

        body_size = len(body_prefix) + len(body_suffix)
        if size is not None:
            body_size += size
        else:
            body_size += os.fstat(fh.fileno()).st_size

        path = '/upload/storage/v1/b/%s/o' % (
            urllib.parse.quote(self.bucket_name, safe=''),)
        query_string = {'uploadType': 'multipart'}
        try:
            resp = self._do_request('POST', path, query_string=query_string,
                                    headers=headers, body=BodyFollowing(body_size))
        except RequestError as exc:
            exc = _map_request_error(exc, key)
            if exc:
                raise exc
            raise

        assert resp.status == 100
        fh.seek(0)

        md5_run = hashlib.md5()
        try:
            self.conn.write(body_prefix)
            while True:
                buf = fh.read(BUFSIZE)
                if not buf:
                    break
                self.conn.write(buf)
                md5_run.update(buf)
            self.conn.write(body_suffix)
        except ConnectionClosed:
            # Server closed connection while we were writing body data -
            # but we may still be able to read an error response
            try:
                resp = self.conn.read_response()
            except ConnectionClosed: # No server response available
                pass
            else:
                log.warning('Server broke connection during upload, signaled '
                            '%d %s', resp.status, resp.reason)
            # Re-raise first ConnectionClosed exception
            raise

        if md5_run.digest() != md5:
            raise ValueError('md5 passed to write_fd does not match fd data')

        resp = self.conn.read_response()
        if resp.status != 200:
            exc = self._parse_error_response(resp)
            # If we're really unlucky, then the token has expired while we
            # were uploading data.
            if exc.message == 'Invalid Credentials':
                raise AccessTokenExpired()
            raise _map_request_error(exc, key) or exc
        self._parse_json_response(resp)

    @copy_ancestor_docstring
    def update_meta(self, key, metadata):

        headers = CaseInsensitiveDict()
        headers['Content-Type'] = 'application/json; charset="utf-8"'
        body = json.dumps({ 'metadata': _wrap_user_meta(metadata),
                            'acl': [] }).encode()

        path = '/storage/v1/b/%s/o/%s' % (
            urllib.parse.quote(self.bucket_name, safe=''),
            urllib.parse.quote(self.prefix + key, safe=''))
        try:
            resp = self._do_request('PUT', path, headers=headers, body=body)
        except RequestError as exc:
            exc = _map_request_error(exc, key)
            if exc:
                raise exc
            raise

        self._parse_json_response(resp)


    @copy_ancestor_docstring
    def close(self):
        self.conn.disconnect()

    def __str__(self):
        return '<gs.Backend, name=%s, prefix=%s>' % (self.bucket_name, self.prefix)

    # This method uses a different HTTP connection than its callers, but shares
    # the same retry logic. It is therefore possible that errors with this
    # connection cause the other connection to be reset - but this should not
    # be a problem, because there can't be a pending request if we don't have
    # a valid access token.
    def _get_access_token(self):
        log.info('Requesting new access token')

        if self.adc:
            try:
                self.adc[0].refresh(self.adc[1])
            except g_auth.exceptions.RefreshError as exc:
                raise AuthenticationError(
                    'Failed to refresh credentials: '  + str(exc))
            self.access_token[self.refresh_token] = self.adc[0].token
            return

        headers = CaseInsensitiveDict()
        headers['Content-Type'] = 'application/x-www-form-urlencoded; charset=utf-8'

        body = urllib.parse.urlencode({
            'client_id': OAUTH_CLIENT_ID,
            'client_secret': OAUTH_CLIENT_SECRET,
            'refresh_token': self.refresh_token,
            'grant_type': 'refresh_token' })

        conn = HTTPConnection('accounts.google.com', 443, proxy=self.proxy,
                              ssl_context=self.ssl_context)
        try:

            conn.send_request('POST', '/o/oauth2/token', headers=headers,
                              body=body.encode('utf-8'))
            resp = conn.read_response()
            json_resp = self._parse_json_response(resp, conn)

            if resp.status > 299 or resp.status < 200:
                assert 'error' in json_resp
            if 'error' in json_resp:
                raise AuthenticationError(json_resp['error'])
            else:
                self.access_token[self.refresh_token] = json_resp['access_token']
        finally:
            conn.disconnect()

    def _parse_error_response(self, resp, conn=None):
        '''Return exception corresponding to server response.'''

        try:
            json_resp = self._parse_json_response(resp, conn)
        except ServerResponseError as exc:
            # Error messages may come from intermediate proxies and thus may not
            # be in JSON.
            log.debug('Server response not JSON - intermediate proxy failure?')
            return RequestError(code=resp.status, reason=resp.reason,
                                body=exc.body)

        try:
            message = json_resp['error']['message']
            body = None
        except KeyError:
            log.warning('Did not find error.message element in JSON '
                        'error response. This is odd.')
            message = None
            body = str(json_resp)

        return RequestError(code=resp.status, reason=resp.reason, message=message,
                            body=body)


    def _parse_json_response(self, resp, conn=None):

        if conn is None:
            conn = self.conn

        # Note that even though the final server backend may guarantee to always
        # deliver a JSON document body with a detailed error message, we may
        # also get errors from intermediate proxies.
        content_type = resp.headers.get('Content-Type', None)
        if content_type:
            hit = re.match(r'application/json(?:; charset="(.+)")?$',
                           resp.headers['Content-Type'], re.IGNORECASE)
        if not content_type or not hit:
            raise ServerResponseError(resp, error='expected json, got %s' % content_type,
                                      body=self._dump_body(resp))
        charset = hit.group(1)

        body = conn.readall()
        try:
            body_text = body.decode(charset)
        except UnicodeDecodeError as exc:
            log.warning('Unable to decode JSON response as Unicode (%s) '
                        '- this is odd.', str(exc))
            raise ServerResponseError(resp, error=str(exc),
                                      body=body.decode(charset, errors='backslashreplace'))

        try:
            resp_json = json.loads(body_text)
        except json.JSONDecodeError as exc:
            log.warning('Unable to decode JSON response (%s) - this is odd.', str(exc))
            raise ServerResponseError(resp, error=str(exc), body=body_text)

        return resp_json

    def _dump_body(self, resp):
        '''Return truncated string representation of response body.'''

        is_truncated = False
        try:
            body = self.conn.read(2048)
            if self.conn.read(1):
                is_truncated = True
                self.conn.discard()
        except dugong.UnsupportedResponse:
            log.warning('Unsupported response, trying to retrieve data from raw socket!')
            body = self.conn.read_raw(2048)
            self.conn.close()

        hit = re.search(r'; charset="(.+)"$',
                        resp.headers.get('Content-Type', ''),
                        re.IGNORECASE)
        if hit:
            charset = hit.group(1)
        else:
            charset = 'utf-8'

        body = body.decode(charset, errors='backslashreplace')

        if is_truncated:
            body += '... [truncated]'

        return body

    def _do_request(self, method, path, query_string=None, headers=None, body=None):
        '''Send request, read and return response object'''

        log.debug('started with %s %s, qs=%s', method, path, query_string)

        if headers is None:
            headers = CaseInsensitiveDict()

        expect100 = isinstance(body, BodyFollowing)
        headers['host'] = self.hostname
        if query_string:
            s = urllib.parse.urlencode(query_string, doseq=True)
            path += '?%s' % s

        # If we have an access token, try to use it.
        token = self.access_token.get(self.refresh_token, None)
        if token is not None:
            headers['Authorization'] = 'Bearer ' + token
            self.conn.send_request(method, path, body=body, headers=headers,
                                   expect100=expect100)
            resp = self.conn.read_response()
            if ((expect100 and resp.status == 100) or
                (not expect100 and 200 <= resp.status <= 299)):
                return resp
            elif resp.status != 401:
                raise self._parse_error_response(resp)

        # If we reach this point, then the access token must have
        # expired, so we try to get a new one. We use a lock to prevent
        # multiple threads from refreshing the token simultaneously.
        with self._refresh_lock:
            # Don't refresh if another thread has already done so while
            # we waited for the lock.
            if token is None or self.access_token.get(self.refresh_token, None) == token:
                self._get_access_token()

        # Try request again. If this still fails, propagate the error
        # (because we have just refreshed the access token).
        # FIXME: We can't rely on this if e.g. the system hibernated
        # after refreshing the token, but before reaching this line.
        headers['Authorization'] = 'Bearer ' + self.access_token[self.refresh_token]
        self.conn.send_request(method, path, body=body, headers=headers,
                               expect100=expect100)
        resp = self.conn.read_response()
        if ((expect100 and resp.status == 100) or
            (not expect100 and 200 <= resp.status <= 299)):
            return resp
        else:
            raise self._parse_error_response(resp)

    @retry
    @copy_ancestor_docstring
    def copy(self, src, dest, metadata=None):
        log.debug('started with %s, %s', src, dest)

        if not (metadata is None or isinstance(metadata, dict)):
            raise TypeError('*metadata*: expected dict or None, got %s' % type(metadata))

        headers = CaseInsensitiveDict()

        if metadata is not None:
            headers['Content-Type'] = 'application/json; charset="utf-8"'
            body = json.dumps({'metadata': _wrap_user_meta(metadata)}).encode()
        else:
            body = None

        path = '/storage/v1/b/%s/o/%s/rewriteTo/b/%s/o/%s' % (
            urllib.parse.quote(self.bucket_name, safe=''),
            urllib.parse.quote(self.prefix + src, safe=''),
            urllib.parse.quote(self.bucket_name, safe=''),
            urllib.parse.quote(self.prefix + dest, safe=''))
        try:
            resp = self._do_request('POST', path, headers=headers, body=body)
        except RequestError as exc:
            exc = _map_request_error(exc, src)
            if exc:
                raise exc
            raise

        json_resp = self._parse_json_response(resp)
        assert json_resp['done']
        assert 'rewriteToken' not in json_resp


def _map_request_error(exc: RequestError, key: str):
    '''Map RequestError to more general exception if possible'''

    if exc.code == 404 and key:
        return NoSuchObject(key)
    elif exc.message == 'Forbidden':
        return AuthorizationError(exc.message)
    elif exc.message in ('Login Required', 'Invalid Credentials'):
        return AuthenticationError(exc.message)

    return None


def _wrap_user_meta(user_meta):

    obj_meta = dict()
    for (key, val) in user_meta.items():
        if not isinstance(key, str):
            raise TypeError('metadata keys must be str, not %s' % type(key))
        if (not isinstance(val, (str, bytes, int, float, complex, bool))
            and val is not None):
            raise TypeError('value for key %s (%s) is not elementary' % (key, val))
        if isinstance(val, (bytes, bytearray)):
            val = b64encode(val)

        obj_meta[key] = repr(val)

    return obj_meta


def _unwrap_user_meta(json_resp):
    '''Extract user metadata from JSON object metadata'''

    meta_raw = json_resp.get('metadata', None)
    if meta_raw is None:
        return {}

    # Detect Legacy format.
    if (meta_raw.get('format', None) == 'raw2' and
        'md5' in meta_raw and
        all(key in ('format', 'md5') or re.match(r'^\d\d\d$', key)
            for key in meta_raw.keys())):
        parts = []
        for i in count():
            part = meta_raw.get('%03d' % i, None)
            if part is None:
                break
            parts.append(part)
        buf = ''.join(parts)
        meta = literal_eval('{ %s }' % buf)
        for (k,v) in meta.items():
            if isinstance(v, bytes):
                meta[k] = b64decode(v)

        # TODO: Remove next block on next file system revision bump.
        # Metadata MD5 headers were created by old S3QL versions where the
        # Google Storage backend shared code with the S3C backend (which
        # supports plain HTTP connections).  There's no need to validate them
        # here since Google Storage always uses TLS. However, we retain the code
        # for now since the metadata format was used to detect an old filesystem
        # revision.
        stored_md5 = meta_raw.get('md5', None)
        new_md5 = b64encode(checksum_basic_mapping(meta)).decode('ascii')
        if stored_md5 != new_md5:
            if UPGRADE_MODE:
                old_md5 = b64encode(UPGRADE_MODE(meta)).decode('ascii')
                if stored_md5 == old_md5:
                    meta['needs_reupload'] = True
                else:
                    raise CorruptedObjectError(
                        'Metadata MD5 mismatch for %s (%s vs %s (old) or %s (new))'
                        % (json_resp.get('name', None), stored_md5, old_md5, new_md5))
            else:
                raise CorruptedObjectError(
                    'Metadata MD5 mismatch for %s (%s vs %s)'
                    % (json_resp.get('name', None), stored_md5, new_md5))
        elif UPGRADE_MODE:
            meta['needs_reupload'] = False

        return meta

    meta = {}
    for (k,v) in meta_raw.items():
        try:
            v2 = literal_eval(v)
        except ValueError as exc:
            raise CorruptedObjectError('Invalid metadata value: ' + str(exc))
        if isinstance(v2, bytes):
            meta[k] = b64decode(v2)
        else:
            meta[k] = v2

    return meta


class ObjectR(object):
    '''A GS object open for reading'''

    def __init__(self, key, resp, backend, gs_meta):
        self.key = key
        self.closed = False
        self.md5_checked = False
        self.backend = backend
        self.resp = resp
        self.metadata = _unwrap_user_meta(gs_meta)
        self.md5_want = b64decode(gs_meta['md5Hash'])
        self.md5 = hashlib.md5()

    def read(self, size=None):
        '''Read up to *size* bytes of object data

        For integrity checking to work, this method has to be called until
        it returns an empty string, indicating that all data has been read
        (and verified).
        '''

        if size == 0:
            return b''

        # This may raise an exception, in which case we probably can't re-use
        # the connection. However, we rely on the caller to still close the
        # file-like object, so that we can do cleanup in close().
        buf = self.backend.conn.read(size)
        self.md5.update(buf)

        # Check MD5 on EOF (size == None implies EOF)
        if (not buf or size is None) and not self.md5_checked:
            self.md5_checked = True
            if self.md5_want != self.md5.digest():
                log.warning('MD5 mismatch for %s: %s vs %s',
                            self.key, b64encode(self.md5_want),
                            b64encode(self.md5.digest()))
                raise ServerResponseError(error='md5Hash mismatch',
                                          body=b'<binary blob>',
                                          resp=self.resp)

        return buf

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()
        return False

    def close(self, checksum_warning=True):
        '''Close object

        If *checksum_warning* is true, this will generate a warning message if
        the object has not been fully read (because in that case the MD5
        checksum cannot be checked).
        '''

        if self.closed:
            return
        self.closed = True

        # If we have not read all the data, close the entire
        # connection (otherwise we loose synchronization)
        if not self.md5_checked:
            if checksum_warning:
                log.warning("Object closed prematurely, can't check MD5, and have to "
                            "reset connection")
            self.backend.conn.disconnect()


class ObjectW(object):
    '''An GS object open for writing

    All data is first cached in memory, upload only starts when
    the close() method is called.
    '''

    def __init__(self, key, backend, metadata):
        self.key = key
        self.backend = backend
        self.metadata = metadata
        self.closed = False
        self.obj_size = 0
        self.md5 = hashlib.md5()

        # According to http://docs.python.org/3/library/functions.html#open
        # the buffer size is typically ~8 kB. We process data in much
        # larger chunks, so buffering would only hurt performance.
        self.fh = tempfile.TemporaryFile(buffering=0)

    def write(self, buf):
        '''Write object data'''

        self.fh.write(buf)
        self.md5.update(buf)
        self.obj_size += len(buf)

    def close(self):
        '''Close object and upload data'''

        if self.closed:
            return

        self.backend.write_fh(self.fh, self.key, self.md5.digest(),
                              self.metadata, size=self.obj_size)
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


def md5sum_b64(buf):
    '''Return base64 encoded MD5 sum'''

    return b64encode(hashlib.md5(buf).digest()).decode('ascii')
