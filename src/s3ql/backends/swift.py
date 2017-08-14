'''
swift.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

from ..logging import logging, QuietError, LOG_ONCE # Ensure use of custom logger class
from .. import BUFSIZE
from .common import (AbstractBackend, NoSuchObject, retry, AuthorizationError,
                     DanglingStorageURLError, retry_generator, get_proxy,
                     get_ssl_context)
from .s3c import HTTPError, ObjectR, ObjectW, md5sum_b64, BadDigestError
from . import s3c
from ..inherit_docstrings import (copy_ancestor_docstring, prepend_ancestor_docstring,
                                  ABCDocstMeta)
from dugong import (HTTPConnection, BodyFollowing, is_temp_network_error, CaseInsensitiveDict,
                    ConnectionClosed)
from urllib.parse import urlsplit
import json
import shutil
import re
import os
import urllib.parse
import ssl
from distutils.version import LooseVersion

log = logging.getLogger(__name__)

#: Suffix to use when creating temporary objects
TEMP_SUFFIX = '_tmp$oentuhuo23986konteuh1062$'

class Backend(AbstractBackend, metaclass=ABCDocstMeta):
    """A backend to store data in OpenStack Swift

    The backend guarantees get after create consistency, i.e. a newly created
    object will be immediately retrievable.
    """

    hdr_prefix = 'X-Object-'
    known_options = {'no-ssl', 'ssl-ca-path', 'tcp-timeout',
                     'disable-expect100', 'no-feature-detection'}

    _add_meta_headers = s3c.Backend._add_meta_headers
    _extractmeta = s3c.Backend._extractmeta
    _assert_empty_response = s3c.Backend._assert_empty_response
    _dump_response = s3c.Backend._dump_response
    clear = s3c.Backend.clear
    reset = s3c.Backend.reset

    def __init__(self, storage_url, login, password, options):
        super().__init__()
        self.options = options
        self.hostname = None
        self.port = None
        self.container_name = None
        self.prefix = None
        self.auth_token = None
        self.auth_prefix = None
        self.conn = None
        self.password = password
        self.login = login
        self.features = Features()

        # We may need the context even if no-ssl has been specified,
        # because no-ssl applies only to the authentication URL.
        self.ssl_context = get_ssl_context(options.get('ssl-ca-path', None))

        self._parse_storage_url(storage_url, self.ssl_context)
        self.proxy = get_proxy(self.ssl_context is not None)
        self._container_exists()

    def __str__(self):
        return 'swift container %s, prefix %s' % (self.container_name, self.prefix)

    @property
    @copy_ancestor_docstring
    def has_native_rename(self):
        return False

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

    def _parse_storage_url(self, storage_url, ssl_context):
        '''Init instance variables from storage url'''

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

        self.hostname = hostname
        self.port = port
        self.container_name = containername
        self.prefix = prefix

    @copy_ancestor_docstring
    def is_temp_failure(self, exc): #IGNORE:W0613
        if isinstance(exc, AuthenticationExpired):
            return True

        # In doubt, we retry on 5xx (Server error). However, there are some
        # codes where retry is definitely not desired. For 4xx (client error) we
        # do not retry in general, but for 408 (Request Timeout) RFC 2616
        # specifies that the client may repeat the request without
        # modifications.
        elif (isinstance(exc, HTTPError) and
              ((500 <= exc.status <= 599
                and exc.status not in (501,505,508,510,511,523))
               or exc.status == 408
               or 'client disconnected' in exc.msg.lower())):
            return True

        elif is_temp_network_error(exc):
            return True

        # Temporary workaround for
        # https://bitbucket.org/nikratio/s3ql/issues/87 and
        # https://bitbucket.org/nikratio/s3ql/issues/252
        elif (isinstance(exc, ssl.SSLError) and
              (str(exc).startswith('[SSL: BAD_WRITE_RETRY]') or
               str(exc).startswith('[SSL: BAD_LENGTH]'))):
            return True

        return False

    def _get_conn(self):
        '''Obtain connection to server and authentication token'''

        log.debug('started')

        if 'no-ssl' in self.options:
            ssl_context = None
        else:
            ssl_context = self.ssl_context

        headers = CaseInsensitiveDict()
        headers['X-Auth-User'] = self.login
        headers['X-Auth-Key'] = self.password

        with HTTPConnection(self.hostname, self.port, proxy=self.proxy,
                              ssl_context=ssl_context) as conn:
            conn.timeout = int(self.options.get('tcp-timeout', 20))

            for auth_path in ('/v1.0', '/auth/v1.0'):
                log.debug('GET %s', auth_path)
                conn.send_request('GET', auth_path, headers=headers)
                resp = conn.read_response()

                if resp.status in (404, 412):
                    log.debug('auth to %s failed, trying next path', auth_path)
                    conn.discard()
                    continue

                elif resp.status == 401:
                    raise AuthorizationError(resp.reason)

                elif resp.status > 299 or resp.status < 200:
                    raise HTTPError(resp.status, resp.reason, resp.headers)

                # Pylint can't infer SplitResult Types
                #pylint: disable=E1103
                self.auth_token = resp.headers['X-Auth-Token']
                o = urlsplit(resp.headers['X-Storage-Url'])
                self.auth_prefix = urllib.parse.unquote(o.path)
                if o.scheme == 'https':
                    ssl_context = self.ssl_context
                elif o.scheme == 'http':
                    ssl_context = None
                else:
                    # fall through to scheme used for authentication
                    pass

                self._detect_features(o.hostname, o.port)

                conn =  HTTPConnection(o.hostname, o.port, proxy=self.proxy,
                                       ssl_context=ssl_context)
                conn.timeout = int(self.options.get('tcp-timeout', 20))
                return conn

            raise RuntimeError('No valid authentication path found')

    def _do_request(self, method, path, subres=None, query_string=None,
                    headers=None, body=None):
        '''Send request, read and return response object

        This method modifies the *headers* dictionary.
        '''

        log.debug('started with %r, %r, %r, %r, %r, %r',
                  method, path, subres, query_string, headers, body)

        if headers is None:
            headers = CaseInsensitiveDict()

        if isinstance(body, (bytes, bytearray, memoryview)):
            headers['Content-MD5'] = md5sum_b64(body)

        if self.conn is None:
            log.debug('no active connection, calling _get_conn()')
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

        headers['X-Auth-Token'] = self.auth_token
        try:
            resp = self._do_request_inner(method, path, body=body, headers=headers)
        except Exception as exc:
            if is_temp_network_error(exc) or isinstance(exc, ssl.SSLError):
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

    # Including this code directly in _do_request would be very messy since
    # we can't `return` the response early, thus the separate method
    def _do_request_inner(self, method, path, body, headers):
        '''The guts of the _do_request method'''

        log.debug('started with %s %s', method, path)
        use_expect_100c = not self.options.get('disable-expect100', False)

        if body is None or isinstance(body, (bytes, bytearray, memoryview)):
            self.conn.send_request(method, path, body=body, headers=headers)
            return self.conn.read_response()

        body_len = os.fstat(body.fileno()).st_size
        self.conn.send_request(method, path, expect100=use_expect_100c,
                               headers=headers, body=BodyFollowing(body_len))

        if use_expect_100c:
            log.debug('waiting for 100-continue')
            resp = self.conn.read_response()
            if resp.status != 100:
                return resp

        log.debug('writing body data')
        try:
            shutil.copyfileobj(body, self.conn, BUFSIZE)
        except ConnectionClosed:
            log.debug('interrupted write, server closed connection')
            # Server closed connection while we were writing body data -
            # but we may still be able to read an error response
            try:
                resp = self.conn.read_response()
            except ConnectionClosed: # No server response available
                log.debug('no response available in  buffer')
                pass
            else:
                if resp.status >= 400: # error response
                    return resp
                log.warning('Server broke connection during upload, but signaled '
                            '%d %s', resp.status, resp.reason)

            # Re-raise original error
            raise

        return self.conn.read_response()

    @retry
    @copy_ancestor_docstring
    def lookup(self, key):
        log.debug('started with %s', key)
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

        return self._extractmeta(resp, key)

    @retry
    @copy_ancestor_docstring
    def get_size(self, key):
        if key.endswith(TEMP_SUFFIX):
            raise ValueError('Keys must not end with %s' % TEMP_SUFFIX)
        log.debug('started with %s', key)

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

        try:
            meta = self._extractmeta(resp, key)
        except BadDigestError:
            # If there's less than 64 kb of data, read and throw
            # away. Otherwise re-establish connection.
            if resp.length is not None and resp.length < 64*1024:
                self.conn.discard()
            else:
                self.conn.disconnect()
            raise

        return ObjectR(key, resp, self, meta)

    @prepend_ancestor_docstring
    def open_write(self, key, metadata=None, is_compressed=False):
        """
        The returned object will buffer all data and only start the upload
        when its `close` method is called.
        """
        log.debug('started with %s', key)

        if key.endswith(TEMP_SUFFIX):
            raise ValueError('Keys must not end with %s' % TEMP_SUFFIX)

        headers = CaseInsensitiveDict()
        if metadata is None:
            metadata = dict()
        self._add_meta_headers(headers, metadata, chunksize=self.features.max_meta_len)

        return ObjectW(key, self, headers)

    @retry
    @copy_ancestor_docstring
    def delete(self, key, force=False, is_retry=False):
        if key.endswith(TEMP_SUFFIX):
            raise ValueError('Keys must not end with %s' % TEMP_SUFFIX)
        log.debug('started with %s', key)
        try:
            resp = self._do_request('DELETE', '/%s%s' % (self.prefix, key))
            self._assert_empty_response(resp)
        except HTTPError as exc:
            # Server may have deleted the object even though we did not
            # receive the response.
            if exc.status == 404 and not (force or is_retry):
                raise NoSuchObject(key)
            elif exc.status != 404:
                raise

    # We cannot wrap the entire copy() method into a retry() decorator, because
    # copy() issues multiple requests. If the server happens to regularly close
    # the connection after a request (even though it was processed correctly),
    # we'd never make any progress if we always restart from the first request.
    # We experimented with adding a retry(fn, args, kwargs) function to wrap
    # individual calls, but this doesn't really improve the code because we
    # typically also have to wrap e.g. a discard() or assert_empty() call, so we
    # end up with having to create an additional function anyway. Having
    # anonymous blocks in Python would be very nice here.
    @retry
    def _copy_helper(self, method, path, headers):
        self._do_request(method, path, headers=headers)
        self.conn.discard()

    @copy_ancestor_docstring
    def copy(self, src, dest, metadata=None):
        log.debug('started with %s, %s', src, dest)
        if dest.endswith(TEMP_SUFFIX) or src.endswith(TEMP_SUFFIX):
            raise ValueError('Keys must not end with %s' % TEMP_SUFFIX)

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
            self._copy_helper('PUT', '/%s%s' % (self.prefix, dest), headers)
        except HTTPError as exc:
            if exc.status == 404:
                raise NoSuchObject(src)
            raise

        if metadata is None:
            return

        # Update metadata
        headers = CaseInsensitiveDict()
        self._add_meta_headers(headers, metadata, chunksize=self.features.max_meta_len)
        self._copy_helper('POST', '/%s%s' % (self.prefix, dest), headers)

        # Rename object
        headers = CaseInsensitiveDict()
        headers['X-Copy-From'] = '/%s/%s%s' % (self.container_name, self.prefix, dest)
        self._copy_helper('PUT', '/%s%s' % (self.prefix, final_dest), headers)

    @retry
    @copy_ancestor_docstring
    def update_meta(self, key, metadata):
        log.debug('started with %s', key)
        headers = CaseInsensitiveDict()
        self._add_meta_headers(headers, metadata, chunksize=self.features.max_meta_len)
        self._do_request('POST', '/%s%s' % (self.prefix, key), headers=headers)
        self.conn.discard()

    @retry_generator
    @copy_ancestor_docstring
    def list(self, prefix='', start_after='', batch_size=5000):
        log.debug('started with %s, %s', prefix, start_after)

        keys_remaining = True
        marker = self.prefix + start_after
        prefix = self.prefix + prefix

        while keys_remaining:
            log.debug('requesting with marker=%s', marker)

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

    def _detect_features(self, hostname, port):
        '''Try to figure out the Swift version and supported features by
        examining the /info endpoint of the storage server.

        See https://docs.openstack.org/swift/latest/middleware.html#discoverability
        '''

        if 'no-feature-detection' in self.options:
            log.debug('Skip feature detection')
            return

        ssl_context = self.ssl_context
        if 'no-ssl' in self.options:
            ssl_context = None

        if not port:
            port = 443 if ssl_context else 80

        detected_features = Features()

        with HTTPConnection(hostname, port, proxy=self.proxy,
                            ssl_context=ssl_context) as conn:
            conn.timeout = int(self.options.get('tcp-timeout', 20))

            log.debug('GET /info')
            conn.send_request('GET', '/info')
            resp = conn.read_response()

            # 200, 401, 403 and 404 are all OK since the /info endpoint
            # may not be accessible (misconfiguration) or may not
            # exist (old Swift version).
            if resp.status not in (200, 401, 403, 404):
                log.error("Wrong server response.\n%s",
                          self._dump_response(resp, body=conn.read(2048)))
                raise HTTPError(resp.status, resp.reason, resp.headers)

            if resp.status is 200:
                hit = re.match('^application/json(;\s*charset="?(.+?)"?)?$',
                resp.headers['content-type'])
                if not hit:
                    log.error("Wrong server response. Expected json. Got: \n%s",
                              self._dump_response(resp, body=conn.read(2048)))
                    raise RuntimeError('Unexpected server reply')

                info = json.loads(conn.readall().decode(hit.group(2) or 'utf-8'))
                swift_info = info.get('swift', {})

                log.debug('%s:%s/info returns %s', hostname, port, info)

                swift_version_string = swift_info.get('version', None)
                if swift_version_string and \
                    LooseVersion(swift_version_string) >= LooseVersion('2.8'):
                    detected_features.has_copy = True

                # Default metadata value length constrain is 256 bytes
                # but the provider could configure another value.
                # We only decrease the chunk size since 255 is a big enough chunk size.
                max_meta_len = swift_info.get('max_meta_value_length', None)
                if isinstance(max_meta_len, int) and max_meta_len < 256:
                    detected_features.max_meta_len = max_meta_len

                if info.get('bulk_delete', False):
                    detected_features.has_bulk_delete = True
                    # The block cache removal queue has a capacity of 1000.
                    # We do not need bigger values than that (the default maximum is 10000).
                    detected_features.max_deletes = max(1, min(1000,
                        int(info['bulk_delete'].get('max_deletes_per_request', 1000))))

                log.info('Detected Swift features for %s:%s: %s',
                         hostname, port, detected_features, extra=LOG_ONCE)
            else:
                log.debug('%s:%s/info not found or not accessible. Skip feature detection.',
                          hostname, port)

        self.features = detected_features

class AuthenticationExpired(Exception):
    '''Raised if the provided Authentication Token has expired'''

    def __init__(self, msg):
        super().__init__()
        self.msg = msg

    def __str__(self):
        return 'Auth token expired. Server said: %s' % self.msg

class Features:
    """Set of configurable features for Swift servers.

    Swift is deployed in many different versions and configurations.
    To be able to use advanced features like bulk delete we need to
    make sure that the Swift server we are using can handle them.

    This is a value object."""

    __slots__ = ['has_copy', 'has_bulk_delete', 'max_deletes', 'max_meta_len']

    def __init__(self, has_copy=False, has_bulk_delete=False, max_deletes=1000,
                 max_meta_len=255):
        self.has_copy = has_copy
        self.has_bulk_delete = has_bulk_delete
        self.max_deletes = max_deletes
        self.max_meta_len = max_meta_len

    def __str__(self):
        features = []
        if self.has_copy:
            features.append('copy via COPY')
        if self.has_bulk_delete:
            features.append('Bulk delete %d keys at a time' % self.max_deletes)
        features.append('maximum meta value length is %d bytes' % self.max_meta_len)
        return ', '.join(features)

    def __repr__(self):
        init_kwargs = [p + '=' + repr(getattr(self, p)) for p in self.__slots__]
        return 'Features(%s)' % ', '.join(init_kwargs)

    def __hash__(self):
        return hash(repr(self))

    def __eq__(self, other):
        return repr(self) == repr(other)

    def __ne__(self, other):
        return repr(self) != repr(other)
