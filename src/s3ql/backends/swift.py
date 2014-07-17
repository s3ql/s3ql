'''
swift.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from ..logging import logging # Ensure use of custom logger class
from ..common import QuietError, PICKLE_PROTOCOL, BUFSIZE
from .common import (AbstractBackend, NoSuchObject, retry, AuthorizationError,
    DanglingStorageURLError, ChecksumError, retry_generator)
from .s3c import HTTPError, ObjectR, ObjectW, md5sum_b64
from ..inherit_docstrings import (copy_ancestor_docstring, prepend_ancestor_docstring,
                                  ABCDocstMeta)
from dugong import (HTTPConnection, BodyFollowing, is_temp_network_error, CaseInsensitiveDict,
                    ConnectionClosed)
from urllib.parse import urlsplit
from base64 import b64encode, b64decode
import json
import shutil
import re
import os
import pickle
import urllib.parse

log = logging.getLogger(__name__)

#: Suffix to use when creating temporary objects
TEMP_SUFFIX = '_tmp$oentuhuo23986konteuh1062$'

class Backend(AbstractBackend, metaclass=ABCDocstMeta):
    """A backend to store data in OpenStack Swift

    The backend guarantees get after create consistency, i.e. a newly created
    object will be immediately retrievable.

    If the *expect_100c* attribute is True, the 'Expect: 100-continue' header is
    used to check for error codes before uploading payload data.
    """

    use_expect_100c = True

    def __init__(self, storage_url, login, password, ssl_context=None, proxy=None):
        # Unused argument
        #pylint: disable=W0613

        super().__init__()

        (host, port, container_name, prefix) = self._parse_storage_url(storage_url, ssl_context)

        self.hostname = host
        self.port = port
        self.container_name = container_name
        self.prefix = prefix
        self.password = password
        self.login = login
        self.auth_token = None
        self.auth_prefix = None
        self.conn = None
        self.proxy = proxy
        self.ssl_context = ssl_context

        self._container_exists()

    def __str__(self):
        return 'swift container %s, prefix %s' % (self.container_name, self.prefix)

    @retry
    def _container_exists(self):
        '''Make sure that the container exists'''

        try:
            self._do_request('GET', '/', query_string={'limit': 1 })
            self.conn.discard()
        except HTTPError as exc:
            if exc.status == 404:
                raise DanglingStorageURLError(self.container_name)
            raise

    @staticmethod
    def _parse_storage_url(storage_url, ssl_context):
        '''Extract information from storage URL

        Return a tuple *(host, port, container_name, prefix)* .
        '''

        hit = re.match(r'^[a-zA-Z0-9]+://' # Backend
                       r'([^/:]+)' # Hostname
                       r'(?::([0-9]+))?' # Port
                       r'/([^/]+)' # Bucketname
                       r'(?:/(.*))?$', # Prefix
                       storage_url)
        if not hit:
            raise QuietError('Invalid storage URL', exitcode=2)

        hostname = hit.group(1)
        if hit.group(2):
            port = int(hit.group(2))
        elif ssl_context:
            port = 443
        else:
            port = 80
        containername = hit.group(3)
        prefix = hit.group(4) or ''

        return (hostname, port, containername, prefix)

    @copy_ancestor_docstring
    def is_temp_failure(self, exc): #IGNORE:W0613
        if isinstance(exc, AuthenticationExpired):
            return True

        elif isinstance(exc, HTTPError) and exc.status >= 500 and exc.status <= 599:
            return True

        elif is_temp_network_error(exc):
            return True

        return False

    @retry
    def _get_conn(self):
        '''Obtain connection to server and authentication token'''

        log.debug('_get_conn(): start')

        conn = HTTPConnection(self.hostname, self.port, proxy=self.proxy,
                              ssl_context=self.ssl_context)

        headers = CaseInsensitiveDict()
        headers['X-Auth-User'] = self.login
        headers['X-Auth-Key'] = self.password

        for auth_path in ('/v1.0', '/auth/v1.0'):
            log.debug('_get_conn(): GET %s', auth_path)
            conn.send_request('GET', auth_path, headers=headers)
            resp = conn.read_response()

            if resp.status in (404, 412):
                log.debug('_refresh_auth(): auth to %s failed, trying next path', auth_path)
                conn.discard()
                continue

            elif resp.status == 401:
                conn.disconnect()
                raise AuthorizationError(resp.reason)

            elif resp.status > 299 or resp.status < 200:
                conn.disconnect()
                raise HTTPError(resp.status, resp.reason, resp.headers)

            # Pylint can't infer SplitResult Types
            #pylint: disable=E1103
            self.auth_token = resp.headers['X-Auth-Token']
            o = urlsplit(resp.headers['X-Storage-Url'])
            self.auth_prefix = urllib.parse.unquote(o.path)
            conn.disconnect()

            return HTTPConnection(o.hostname, o.port, proxy=self.proxy,
                                  ssl_context=self.ssl_context)

        raise RuntimeError('No valid authentication path found')

    def _do_request(self, method, path, subres=None, query_string=None,
                    headers=None, body=None):
        '''Send request, read and return response object

        This method modifies the *headers* dictionary.
        '''

        log.debug('_do_request(): start with parameters (%r, %r, %r, %r, %r, %r)',
                  method, path, subres, query_string, headers, body)

        if headers is None:
            headers = CaseInsensitiveDict()

        if isinstance(body, (bytes, bytearray, memoryview)):
            headers['Content-MD5'] = md5sum_b64(body)

        if self.conn is None:
            log.debug('_do_request(): no active connection, calling _get_conn()')
            self.conn =  self._get_conn()

        # Construct full path
        path = urllib.parse.quote('%s/%s%s' % (self.auth_prefix, self.container_name, path))
        if query_string:
            s = urllib.parse.urlencode(query_string, doseq=True)
            if subres:
                path += '?%s&%s' % (subres, s)
            else:
                path += '?%s' % s
        elif subres:
            path += '?%s' % subres

        # We can probably remove the assertions at some point and
        # call self.conn.read_response() directly
        def read_response():
            resp = self.conn.read_response()
            assert resp.method == method
            assert resp.path == path
            return resp

        headers['X-Auth-Token'] = self.auth_token
        try:
            resp = None
            log.debug('_send_request(): %s %s', method, path)
            if body is None or isinstance(body, (bytes, bytearray, memoryview)):
                self.conn.send_request(method, path, body=body, headers=headers)
            else:
                body_len = os.fstat(body.fileno()).st_size
                self.conn.send_request(method, path, expect100=self.use_expect_100c,
                                       headers=headers, body=BodyFollowing(body_len))

                if self.use_expect_100c:
                    resp = read_response()
                    if resp.status == 100:
                        resp = None
                    # Otherwise fall through below

                try:
                    shutil.copyfileobj(body, self.conn, BUFSIZE)
                except ConnectionClosed:
                    # Server closed connection while we were writing body data -
                    # but we may still be able to read an error response
                    try:
                        resp = read_response()
                    except ConnectionClosed: # No server response available
                        pass
                    else:
                        if resp.status < 400:
                            log.warning('Server broke connection during upload, but signaled '
                                        '%d %s', resp.status, resp.reason)
                            resp = None

                    # Re-raise original error
                    if resp is None:
                        raise

            if resp is None:
                resp = read_response()

        except Exception as exc:
            if is_temp_network_error(exc):
                # We probably can't use the connection anymore
                self.conn.disconnect()
            raise

        # Success
        if resp.status >= 200 and resp.status <= 299:
            return resp

        # Expired auth token
        if resp.status == 401:
            log.info('OpenStack auth token seems to have expired, requesting new one.')
            self.conn.disconnect()
            # Force constructing a new connection with a new token, otherwise
            # the connection will be reestablished with the same token.
            self.conn = None
            raise AuthenticationExpired(resp.reason)

        # If method == HEAD, server must not return response body
        # even in case of errors
        self.conn.discard()
        if method.upper() == 'HEAD':
            raise HTTPError(resp.status, resp.reason, resp.headers)
        else:
            raise HTTPError(resp.status, resp.reason, resp.headers)


    def _assert_empty_response(self, resp):
        '''Assert that current response body is empty'''

        buf = self.conn.read(2048)
        if not buf:
            return # expected

        # Log the problem
        self.conn.discard()
        log.error('Unexpected server response. Expected nothing, got:\n'
                  '%d %s\n%s\n\n%s', resp.status, resp.reason,
                  '\n'.join('%s: %s' % x for x in resp.headers.items()),
                  buf)
        raise RuntimeError('Unexpected server response')


    def _dump_response(self, resp, body=None):
        '''Return string representation of server response

        Only the beginning of the response body is read, so this is
        mostly useful for debugging.
        '''

        if body is None:
            body = self.conn.read(2048)
            if body:
                self.conn.discard()
        else:
            body = body[:2048]

        return '%d %s\n%s\n\n%s' % (resp.status, resp.reason,
                                    '\n'.join('%s: %s' % x for x in resp.headers.items()),
                                    body.decode('utf-8', errors='backslashreplace'))

    @retry
    @copy_ancestor_docstring
    def lookup(self, key):
        log.debug('lookup(%s)', key)
        if key.endswith(TEMP_SUFFIX):
            raise ValueError('Keys must not end with %s' % TEMP_SUFFIX)

        try:
            resp = self._do_request('HEAD', '/%s%s' % (self.prefix, key))
            self._assert_empty_response(resp)
        except HTTPError as exc:
            if exc.status == 404:
                raise NoSuchObject(key)
            else:
                raise

        return extractmeta(resp)

    @retry
    @copy_ancestor_docstring
    def get_size(self, key):
        if key.endswith(TEMP_SUFFIX):
            raise ValueError('Keys must not end with %s' % TEMP_SUFFIX)
        log.debug('get_size(%s)', key)

        try:
            resp = self._do_request('HEAD', '/%s%s' % (self.prefix, key))
            self._assert_empty_response(resp)
        except HTTPError as exc:
            if exc.status == 404:
                raise NoSuchObject(key)
            else:
                raise

        try:
            return int(resp.headers['Content-Length'])
        except KeyError:
            raise RuntimeError('HEAD request did not return Content-Length')

    @retry
    @copy_ancestor_docstring
    def open_read(self, key):
        if key.endswith(TEMP_SUFFIX):
            raise ValueError('Keys must not end with %s' % TEMP_SUFFIX)
        try:
            resp = self._do_request('GET', '/%s%s' % (self.prefix, key))
        except HTTPError as exc:
            if exc.status == 404:
                raise NoSuchObject(key)
            raise

        return ObjectR(key, resp, self, extractmeta(resp))

    def _add_meta_headers(self, headers, metadata):

        # We don't store the metadata keys directly, because HTTP headers
        # are case insensitive (so the server may change capitalization)
        # and we may run into length restrictions.
        meta_buf = b64encode(pickle.dumps(metadata, PICKLE_PROTOCOL)).decode('us-ascii')

        chunksize = 255
        i = 0
        headers['X-Object-Meta-Format'] = 'pickle'
        while i*chunksize < len(meta_buf):
            headers['X-Object-Meta-Data-%02d' % i] = meta_buf[i*chunksize:(i+1)*chunksize]
            i += 1

    @prepend_ancestor_docstring
    def open_write(self, key, metadata=None, is_compressed=False):
        """
        The returned object will buffer all data and only start the upload
        when its `close` method is called.
        """
        log.debug('open_write(%s): start', key)

        if key.endswith(TEMP_SUFFIX):
            raise ValueError('Keys must not end with %s' % TEMP_SUFFIX)

        if metadata is None:
            metadata = dict()
        elif not isinstance(metadata, dict):
            raise TypeError('*metadata*: expected dict or None, got %s' % type(metadata))

        headers = CaseInsensitiveDict()
        self._add_meta_headers(headers, metadata)

        return ObjectW(key, self, headers)

    @copy_ancestor_docstring
    def clear(self):

        # We have to cache keys, because otherwise we can't use the
        # http connection to delete keys.
        for (no, s3key) in enumerate(list(self)):
            if no != 0 and no % 1000 == 0:
                log.info('clear(): deleted %d objects so far..', no)

            log.debug('clear(): deleting key %s', s3key)

            # Ignore missing objects when clearing backend
            self.delete(s3key, True)

    @retry
    @copy_ancestor_docstring
    def delete(self, key, force=False):
        if key.endswith(TEMP_SUFFIX):
            raise ValueError('Keys must not end with %s' % TEMP_SUFFIX)
        log.debug('delete(%s)', key)
        try:
            resp = self._do_request('DELETE', '/%s%s' % (self.prefix, key))
            self._assert_empty_response(resp)
        except HTTPError as exc:
            if exc.status == 404 and not force:
                raise NoSuchObject(key)
            elif exc.status != 404:
                raise

    @retry
    @copy_ancestor_docstring
    def copy(self, src, dest, metadata=None):
        log.debug('copy(%s, %s): start', src, dest)
        if dest.endswith(TEMP_SUFFIX) or src.endswith(TEMP_SUFFIX):
            raise ValueError('Keys must not end with %s' % TEMP_SUFFIX)
        if not (metadata is None or isinstance(metadata, dict)):
            raise TypeError('*metadata*: expected dict or None, got %s' % type(metadata))

        headers = CaseInsensitiveDict()
        headers['X-Copy-From'] = '/%s/%s%s' % (self.container_name, self.prefix, src)

        if metadata is not None:
            # We can't do a direct copy, because during copy we can only update the
            # metadata, but not replace it. Therefore, we have to make a full copy
            # followed by a separate request to replace the metadata. To avoid an
            # inconsistent intermediate state, we use a temporary object.
            final_dest = dest
            dest = final_dest + TEMP_SUFFIX
            headers['X-Delete-After'] = '600'

        try:
            self._do_request('PUT', '/%s%s' % (self.prefix, dest), headers=headers)
            self.conn.discard()
        except HTTPError as exc:
            if exc.status == 404:
                raise NoSuchObject(src)
            raise

        if metadata is None:
            return

        # Update metadata
        headers = CaseInsensitiveDict()
        self._add_meta_headers(headers, metadata)
        self._do_request('POST', '/%s%s' % (self.prefix, dest), headers=headers)
        self.conn.discard()

        # Rename object
        headers = CaseInsensitiveDict()
        headers['X-Copy-From'] = '/%s/%s%s' % (self.container_name, self.prefix, dest)
        self._do_request('PUT', '/%s%s' % (self.prefix, final_dest), headers=headers)
        self.conn.discard()

    @copy_ancestor_docstring
    def update_meta(self, key, metadata):
        log.debug('start for %s', key)
        if not isinstance(metadata, dict):
            raise TypeError('*metadata*: expected dict, got %s' % type(metadata))

        headers = CaseInsensitiveDict()
        self._add_meta_headers(headers, metadata)
        self._do_request('POST', '/%s%s' % (self.prefix, key), headers=headers)
        self.conn.discard()

    @retry_generator
    @copy_ancestor_docstring
    def list(self, prefix='', start_after='', batch_size=5000):
        log.debug('list(%s, %s): start', prefix, start_after)

        keys_remaining = True
        marker = start_after
        prefix = self.prefix + prefix

        while keys_remaining:
            log.debug('list(%s): requesting with marker=%s', prefix, marker)

            try:
                resp = self._do_request('GET', '/', query_string={'prefix': prefix,
                                                                  'format': 'json',
                                                                  'marker': marker,
                                                                  'limit': batch_size })
            except HTTPError as exc:
                if exc.status == 404:
                    raise DanglingStorageURLError(self.container_name)
                raise

            if resp.status == 204:
                return

            hit = re.match('application/json; charset="?(.+?)"?$',
                           resp.headers['content-type'])
            if not hit:
                log.error('Unexpected server response. Expected json, got:\n%s',
                          self._dump_response(resp))
                raise RuntimeError('Unexpected server reply')

            strip = len(self.prefix)
            count = 0
            try:
                # JSON does not have a streaming API, so we just read
                # the entire response in memory.
                for dataset in json.loads(self.conn.read().decode(hit.group(1))):
                    count += 1
                    marker = dataset['name']
                    if marker.endswith(TEMP_SUFFIX):
                        continue
                    yield marker[strip:]

            except GeneratorExit:
                self.conn.discard()
                break

            keys_remaining = count == batch_size

    @copy_ancestor_docstring
    def close(self):
        self.conn.disconnect()

    @copy_ancestor_docstring
    def reset(self):
        if self.conn.response_pending() or self.conn._out_remaining:
            log.debug('Resetting state of http connection %d', id(self.conn))
            self.conn.disconnect()

def extractmeta(resp):
    '''Extract metadata from HTTP response object'''

    meta = dict()
    format_ = 'raw'
    for (name, val) in resp.headers.items():
        # HTTP headers are case-insensitive and pre 2.x S3QL versions metadata
        # names verbatim (and thus loose capitalization info), so we force lower
        # case (since we know that this is the original capitalization).
        name = name.lower()

        hit = re.match(r'^X-Object-Meta-(.+)$', name, re.IGNORECASE)
        if not hit:
            continue

        if hit.group(1).lower() == 'format':
            format_ = val
        else:
            log.debug('read %s: %s', hit.group(1), val)
            meta[hit.group(1)] = val

    if format_ == 'pickle':
        buf = ''.join(meta[x] for x in sorted(meta)
                      if x.lower().startswith('data-'))
        try:
            return pickle.loads(b64decode(buf))
        except pickle.UnpicklingError as exc:
            if (isinstance(exc.args[0], str)
                and exc.args[0].startswith('invalid load key')):
                raise ChecksumError('Invalid metadata')
            raise
    else:
        return meta


class AuthenticationExpired(Exception):
    '''Raised if the provided Authentication Token has expired'''

    def __init__(self, msg):
        super().__init__()
        self.msg = msg

    def __str__(self):
        return 'Auth token expired. Server said: %s' % self.msg
