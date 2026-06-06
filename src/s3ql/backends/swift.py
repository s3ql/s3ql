'''
swift.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

import binascii
import hashlib
import json
import logging
import re
import ssl
import urllib.parse
from ast import literal_eval
from base64 import b64decode, b64encode
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from itertools import count
from typing import Any, Optional
from urllib.parse import quote, unquote, urlsplit

import trio

from s3ql.http import (
    CaseInsensitiveDict,
    HTTPConnection,
    HTTPResponse,
    Pool,
    fh_size,
    is_temp_network_error,
)
from s3ql.types import BackendOptionsProtocol, BinaryInput, BinaryOutput

from ..logging import LOG_ONCE, QuietError
from .common import (
    _FACTORY_SENTINEL,
    AuthorizationError,
    CorruptedObjectError,
    DanglingStorageURLError,
    NoSuchObject,
    ServerResponseError,
    assert_empty_response,
    checksum_basic_mapping,
    dump_response,
    get_proxy,
    get_ssl_context,
    hash_fh,
    log_delete_progress,
    retry,
)
from .common import (
    AsyncBackend as AsyncBackendBase,
)
from .s3c import BadDigestError, HTTPError

log = logging.getLogger(__name__)

#: Suffix to use when creating temporary objects
TEMP_SUFFIX = '_tmp$oentuhuo23986konteuh1062$'


@dataclass
class _AuthResult:
    '''Everything an `_authenticate()` call discovers about the storage endpoint.

    *storage_host*, *storage_port* and *ssl_context* identify the server that
    subsequent storage requests target, while *token* and *prefix* are the auth
    token and URL path prefix to use for those requests.
    '''

    storage_host: str
    storage_port: int | None
    ssl_context: ssl.SSLContext | None
    token: str
    prefix: str


class AsyncBackend(AsyncBackendBase):
    """A backend to store data in OpenStack Swift"""

    hdr_prefix = 'X-Object-'
    known_options = {
        'no-ssl',
        'ssl-ca-path',
        'tcp-timeout',
        'disable-expect100',
        'no-feature-detection',
    }

    @classmethod
    async def create(
        cls: type['AsyncBackend'],
        options: BackendOptionsProtocol,
        max_connections: int = 1,
    ) -> 'AsyncBackend':
        '''Create a new Swift backend instance.

        Authentication happens lazily on the first request; the
        `_container_exists()` check below triggers it.
        '''
        backend = cls(options=options, max_connections=max_connections)
        await backend._container_exists()
        return backend

    def _add_meta_headers(self, headers, metadata, chunksize=255):
        hdr_count = 0
        length = 0
        for key in metadata:
            if not isinstance(key, str):
                raise ValueError('dict keys must be str, not %s' % type(key))
            val = metadata[key]
            if not isinstance(val, (str, bytes, int, float, complex, bool)) and val is not None:
                raise ValueError('value for key %s (%s) is not elementary' % (key, val))

            if isinstance(val, (bytes, bytearray)):
                val = b64encode(val)

            buf = '%s: %s,' % (repr(key), repr(val))
            buf = quote(buf, safe='!@#$^*()=+/?-_\'"><\\| `.,;:~')
            if len(buf) < chunksize:
                headers['%smeta-%03d' % (self.hdr_prefix, hdr_count)] = buf
                hdr_count += 1
                length += 4 + len(buf)
            else:
                i = 0
                while i * chunksize < len(buf):
                    k = '%smeta-%03d' % (self.hdr_prefix, hdr_count)
                    v = buf[i * chunksize : (i + 1) * chunksize]
                    headers[k] = v
                    i += 1
                    hdr_count += 1
                    length += 4 + len(v)

            if length > 2048:
                raise ValueError('Metadata too large')

        assert hdr_count <= 999
        md5 = b64encode(checksum_basic_mapping(metadata)).decode('ascii')
        headers[self.hdr_prefix + 'meta-format'] = 'raw2'
        headers[self.hdr_prefix + 'meta-md5'] = md5

    def _extractmeta(self, resp, obj_key):
        '''Extract metadata from HTTP response object'''

        format_ = resp.headers.get('%smeta-format' % self.hdr_prefix, 'raw')
        if format_ != 'raw2':  # Current
            raise CorruptedObjectError('Invalid metadata format: %s' % format_)

        parts = []
        for i in count():
            # Headers is an email.message object, so indexing it
            # would also give None instead of KeyError
            part = resp.headers.get('%smeta-%03d' % (self.hdr_prefix, i), None)
            if part is None:
                break
            parts.append(part)
        buf = unquote(''.join(parts))
        meta = literal_eval('{ %s }' % buf)

        # Decode bytes values
        for k, v in meta.items():
            if not isinstance(v, bytes):
                continue
            try:
                meta[k] = b64decode(v)
            except binascii.Error:
                # This should trigger a MD5 mismatch below
                meta[k] = None

        # Check MD5. There is a case to be made for treating a mismatch as a
        # `CorruptedObjectError` rather than a `BadDigestError`, because the MD5
        # sum is not calculated on-the-fly by the server but stored with the
        # object, and therefore does not actually verify what the server has
        # sent over the wire. However, it seems more likely for the data to get
        # accidentally corrupted in transit than to get accidentally corrupted
        # on the server (which hopefully checksums its storage devices).
        md5 = b64encode(checksum_basic_mapping(meta)).decode('ascii')
        if md5 != resp.headers.get('%smeta-md5' % self.hdr_prefix, None):
            log.warning('MD5 mismatch in metadata for %s', obj_key)

            # When trying to read file system revision 23 or earlier, we will
            # get a MD5 error because the checksum was calculated
            # differently. In order to get a better error message, we special
            # case the s3ql_passphrase and s3ql_metadata object (which are only
            # retrieved once at program start).
            if obj_key in ('s3ql_passphrase', 's3ql_metadata'):
                raise CorruptedObjectError('Meta MD5 for %s does not match' % obj_key)
            raise BadDigestError('BadDigest', 'Meta MD5 for %s does not match' % obj_key)

        return meta

    def __init__(self, *, options: BackendOptionsProtocol, max_connections: int = 1) -> None:
        '''Initialize swift backend - use AsyncBackend.create() instead.'''
        super().__init__(_factory_sentinel=_FACTORY_SENTINEL)
        self.options = options.backend_options
        self.auth_token: str | None = None
        self.auth_prefix: str | None = None

        # Built lazily by `_borrow_conn()` once authentication succeeds.
        self._pool: Pool | None = None
        self._max_connections = max_connections
        # Serialises reauthentication after an expired token so concurrent tasks
        # do not redundantly hit the auth endpoint or stomp on each other's pool.
        self._auth_lock = trio.Lock()
        self.password = options.backend_password
        self.login = options.backend_login
        self.features = Features()

        # We may need the context even if no-ssl has been specified,
        # because no-ssl applies only to the authentication URL.
        self.ssl_context = get_ssl_context(self.options.get('ssl-ca-path', None))  # type: ignore[arg-type]

        # Note: this is intentionally a separate method (rather than inlining the
        # parsing logic), because derived backends (e.g. swiftks://) override it.
        self._parse_storage_url(options.storage_url, self.ssl_context)

        self.proxy = get_proxy(self.ssl_context is not None)

    def _parse_storage_url(self, storage_url: str, ssl_context: ssl.SSLContext | None) -> None:
        '''Parse storage URL and set instance attributes.'''
        hit = re.match(
            r'^[a-zA-Z0-9]+://'  # Backend
            r'([^/:]+)'  # Hostname
            r'(?::([0-9]+))?'  # Port
            r'/([^/]+)'  # Bucketname
            r'(?:/(.*))?$',  # Prefix
            storage_url,
        )
        if not hit:
            raise QuietError('Invalid storage URL', exitcode=2)

        self.hostname = hit.group(1)
        if hit.group(2):
            self.port = int(hit.group(2))
        elif ssl_context:
            self.port = 443
        else:
            self.port = 80
        self.container_name = hit.group(3)
        self.prefix = hit.group(4) or ''

    def __str__(self):
        return 'swift container %s, prefix %s' % (self.container_name, self.prefix)

    @retry
    async def _container_exists(self):
        '''Make sure that the container exists'''

        try:
            async with self._borrow_conn() as conn:
                await self._do_request(conn, 'GET', '/', query_string={'limit': '1'})
                await conn.discard()
        except HTTPError as exc:
            if exc.status == 404:
                raise DanglingStorageURLError(self.container_name)
            raise

    def is_temp_failure(self, exc):  # IGNORE:W0613
        if isinstance(exc, AuthenticationExpired):  # noqa: SIM114
            return True

        # In doubt, we retry on 5xx (Server error). However, there are some
        # codes where retry is definitely not desired. For 4xx (client error) we
        # do not retry in general, but for 408 (Request Timeout) RFC 2616
        # specifies that the client may repeat the request without
        # modifications. We also retry on 429 (Too Many Requests).
        elif isinstance(exc, HTTPError) and (
            (500 <= exc.status <= 599 and exc.status not in (501, 505, 508, 510, 511, 523))
            or exc.status in (408, 429)
            or 'client disconnected' in exc.msg.lower()
        ):
            return True

        elif is_temp_network_error(exc):
            # The pool drops the affected connection automatically.
            return True

        # Temporary workaround for
        # https://bitbucket.org/nikratio/s3ql/issues/87 and
        # https://bitbucket.org/nikratio/s3ql/issues/252
        elif (
            isinstance(exc, ssl.SSLError)
            and (
                str(exc).startswith('[SSL: BAD_WRITE_RETRY]')
                or str(exc).startswith('[SSL: BAD_LENGTH]')
            )
            or isinstance(exc, ServerResponseError)
        ):
            return True

        return False

    @property
    def max_connections(self) -> int:
        return self._max_connections

    @asynccontextmanager
    async def _borrow_conn(self) -> AsyncIterator[HTTPConnection]:
        '''Borrow a connection from the storage pool, authenticating if needed.

        Concurrent callers serialise on `self._auth_lock`; the first one to win
        the lock authenticates, the rest see the populated pool and proceed.
        '''
        if self._pool is None:
            async with self._auth_lock:
                if self._pool is None:
                    log.debug('no active pool, authenticating')
                    auth = await self._authenticate()
                    self.auth_token = auth.token
                    self.auth_prefix = auth.prefix
                    self.features = await self._detect_features(
                        auth.storage_host, auth.storage_port, auth.ssl_context
                    )
                    self._pool = Pool(
                        auth.storage_host,
                        port=auth.storage_port,
                        ssl_context=auth.ssl_context,
                        proxy=self.proxy,
                        max_connections=self._max_connections,
                        timeout=int(self.options.get('tcp-timeout', 20)),
                    )
        async with self._pool.get() as conn:
            yield conn

    async def _authenticate(self) -> _AuthResult:
        '''Authenticate to the Swift v1 auth endpoint.

        Returns an `_AuthResult` describing the storage endpoint and the auth
        token to use for subsequent requests.
        '''

        log.debug('started')

        if 'no-ssl' in self.options:
            ssl_context = None
        else:
            ssl_context = self.ssl_context

        headers = CaseInsensitiveDict()
        headers['X-Auth-User'] = self.login
        headers['X-Auth-Key'] = self.password

        async with HTTPConnection(
            self.hostname, self.port, proxy=self.proxy, ssl_context=ssl_context
        ) as conn:
            conn.timeout = int(self.options.get('tcp-timeout', 20))

            for auth_path in ('/v1.0', '/auth/v1.0'):
                log.debug('GET %s', auth_path)
                resp = await conn.send_request('GET', auth_path, headers=headers)

                if resp.status in (404, 412):
                    log.debug('auth to %s failed, trying next path', auth_path)
                    await conn.discard()
                    continue

                elif resp.status == 401:
                    raise AuthorizationError(resp.reason)

                elif resp.status > 299 or resp.status < 200:
                    raise HTTPError(resp.status, resp.reason, resp.headers)

                # Pylint can't infer SplitResult Types
                # pylint: disable=E1103
                token = resp.headers['X-Auth-Token']
                o = urlsplit(resp.headers['X-Storage-Url'])
                if o.scheme == 'https':
                    ssl_context = self.ssl_context
                elif o.scheme == 'http':
                    ssl_context = None
                else:
                    # fall through to scheme used for authentication
                    pass

                assert o.hostname is not None
                return _AuthResult(
                    storage_host=o.hostname,
                    storage_port=o.port,
                    ssl_context=ssl_context,
                    token=token,
                    prefix=urllib.parse.unquote(o.path),
                )

            raise RuntimeError('No valid authentication path found')

    async def _do_request(
        self,
        conn: HTTPConnection,
        method: str,
        path: str,
        subres: Optional[str] = None,
        query_string: Optional[dict[str, str]] = None,
        headers: Optional[CaseInsensitiveDict] = None,
        body: Sequence[bytes | BinaryInput] = (),
        md5_hex: Optional[str] = None,
    ) -> HTTPResponse:
        '''Send request on *conn*, return the response object.

        Modifies *headers* by adding the auth token. If *md5_hex* is
        supplied, the server's ETag is verified against it after a
        successful upload.
        '''

        log.debug(
            'started with %r, %r, %r, %r, %r, %r', method, path, subres, query_string, headers, body
        )

        if headers is None:
            headers = CaseInsensitiveDict()

        # Snapshot the auth state this request uses, so a 401 can be attributed to
        # this token and a concurrent reauthentication is not redone.
        auth_token = self.auth_token
        auth_prefix = self.auth_prefix

        # Construct full path
        path = urllib.parse.quote('%s/%s%s' % (auth_prefix, self.container_name, path))
        if query_string:
            s = urllib.parse.urlencode(query_string, doseq=True)
            if subres:
                path += '?%s&%s' % (subres, s)
            else:
                path += '?%s' % s
        elif subres:
            path += '?%s' % subres

        headers['X-Auth-Token'] = auth_token

        resp = await conn.send_request(
            method,
            path,
            headers=headers,
            body=body,
            expect100=False if self.options.get('disable-expect100', False) else None,
        )

        # Success
        if 200 <= resp.status <= 299:
            return resp

        # Expired auth token
        if resp.status == 401:
            await self._do_authentication_expired(resp.reason, auth_token)
            # raises AuthenticationExpired

        # If method == HEAD, server must not return response body
        # even in case of errors
        await conn.discard()
        raise HTTPError(resp.status, resp.reason, resp.headers)

    @retry
    async def lookup(self, key):
        log.debug('started with %s', key)
        if key.endswith(TEMP_SUFFIX):
            raise ValueError('Keys must not end with %s' % TEMP_SUFFIX)

        try:
            async with self._borrow_conn() as conn:
                resp = await self._do_request(conn, 'HEAD', '/%s%s' % (self.prefix, key))
                await assert_empty_response(conn, resp)
        except HTTPError as exc:
            if exc.status == 404:
                raise NoSuchObject(key)
            raise
        return self._extractmeta(resp, key)

    @retry
    async def get_size(self, key):
        if key.endswith(TEMP_SUFFIX):
            raise ValueError('Keys must not end with %s' % TEMP_SUFFIX)
        log.debug('started with %s', key)

        try:
            async with self._borrow_conn() as conn:
                resp = await self._do_request(conn, 'HEAD', '/%s%s' % (self.prefix, key))
                await assert_empty_response(conn, resp)
        except HTTPError as exc:
            if exc.status == 404:
                raise NoSuchObject(key)
            raise

        try:
            return int(resp.headers['Content-Length'])
        except KeyError:
            raise RuntimeError('HEAD request did not return Content-Length')

    async def readinto_fh(self, key: str, fh: BinaryOutput):
        '''Transfer data stored under *key* into *fh*, return metadata.

        The data will be inserted at the current offset. If a temporary error (as defined by
        `is_temp_failure`) occurs, the operation is retried.
        '''

        return await self._readinto_fh(key, fh, fh.tell())

    @retry
    async def _readinto_fh(self, key: str, fh: BinaryOutput, off: int):
        if key.endswith(TEMP_SUFFIX):
            raise ValueError('Keys must not end with %s' % TEMP_SUFFIX)
        async with self._borrow_conn() as conn:
            try:
                resp = await self._do_request(conn, 'GET', '/%s%s' % (self.prefix, key))
            except HTTPError as exc:
                if exc.status == 404:
                    raise NoSuchObject(key)
                raise

            etag = resp.headers['ETag'].strip('"')

            try:
                meta = self._extractmeta(resp, key)
            except BadDigestError:
                # Small bodies are cheap to drain so we can keep the keep-alive connection.
                # Larger ones aren't worth the bandwidth — leave the conn mid-cycle so the pool
                # drops it on exit.
                if resp.length is not None and resp.length < 64 * 1024:
                    await conn.discard()
                raise

            md5 = hashlib.md5()
            fh.seek(off)
            await conn.readinto_fh(fh, update=md5.update)

        if etag != md5.hexdigest():
            log.warning('MD5 mismatch for %s: %s vs %s', key, etag, md5.hexdigest())
            raise BadDigestError('BadDigest', 'ETag header does not agree with calculated MD5')

        return meta

    @retry
    async def write_fh(
        self,
        key: str,
        fh: BinaryInput,
        metadata: Optional[dict[str, Any]] = None,
    ):
        '''Upload the full contents of *fh* under *key*.

        If a temporary error (as defined by `is_temp_failure`) occurs, the operation is
        retried. Returns the size of the resulting storage object.
        '''

        if key.endswith(TEMP_SUFFIX):
            raise ValueError('Keys must not end with %s' % TEMP_SUFFIX)

        size = fh_size(fh)
        headers = CaseInsensitiveDict()
        self._add_meta_headers(headers, metadata or {}, chunksize=self.features.max_meta_len)

        headers['Content-Type'] = 'application/octet-stream'

        if self.ssl_context:
            md5_hex: str | None = None
        else:
            md5_hex = await hash_fh(fh, 'md5')
            headers['Content-MD5'] = b64encode(bytes.fromhex(md5_hex)).decode('ascii')

        try:
            async with self._borrow_conn() as conn:
                resp = await self._do_request(
                    conn,
                    'PUT',
                    '/%s%s' % (self.prefix, key),
                    headers=headers,
                    body=[fh],
                    md5_hex=md5_hex,
                )
                await assert_empty_response(conn, resp)
        except BadDigestError:
            # Object was corrupted in transit, make sure to delete it
            await self.delete(key)
            raise

        return size

    @retry
    async def delete(self, key):
        if key.endswith(TEMP_SUFFIX):
            raise ValueError('Keys must not end with %s' % TEMP_SUFFIX)
        log.debug('started with %s', key)
        try:
            async with self._borrow_conn() as conn:
                resp = await self._do_request(conn, 'DELETE', '/%s%s' % (self.prefix, key))
                await assert_empty_response(conn, resp)
        except HTTPError as exc:
            if exc.status == 404:
                pass
            else:
                raise

    @retry
    async def _delete_multi(self, keys):
        """Doing bulk delete of multiple objects at a time.

        This is a feature of the configurable middleware "Bulk" so it can only
        be used after the middleware was detected. (Introduced in Swift 1.8.0.rc1)

        See https://docs.openstack.org/swift/latest/middleware.html#bulk-delete
        and https://github.com/openstack/swift/blob/master/swift/common/middleware/bulk.py

        A request example:
        'POST /?bulk-delete
        Content-type: text/plain; charset=utf-8
        Accept: application/json

        /container/prefix_key1
        /container/prefix_key2'

        A successful response:
        'HTTP/1.1 200 OK
        Content-Type: application/json

        {"Number Not Found": 0,
         "Response Status": "200 OK",
         "Response Body": "",
         "Errors": [],
         "Number Deleted": 2}'

        An error response:
        'HTTP/1.1 200 OK
        Content-Type: application/json

        {"Number Not Found": 0,
         "Response Status": "500 Internal Server Error",
         "Response Body": "An error description",
         "Errors": [
            ['/container/prefix_key2', 'An error description']
         ],
         "Number Deleted": 1}'

        Response when some objects where not found:
        'HTTP/1.1 200 OK
        Content-Type: application/json

        {"Number Not Found": 1,
         "Response Status": "400 Bad Request",
         "Response Body": "Invalid bulk delete.",
         "Errors": [],
         "Number Deleted": 1}'
        """

        lines = []
        esc_prefix = "/%s/%s" % (
            urllib.parse.quote(self.container_name),
            urllib.parse.quote(self.prefix),
        )
        for key in keys:
            lines.append('%s%s' % (esc_prefix, urllib.parse.quote(key)))
        body = '\n'.join(lines).encode('utf-8')
        headers = CaseInsensitiveDict(
            {'content-type': 'text/plain; charset=utf-8', 'accept': 'application/json'}
        )

        async with self._borrow_conn() as conn:
            # Snapshot the token this request uses, so a 401 embedded in the bulk-delete
            # response body can be attributed to it rather than to a peer's fresh token.
            auth_token = self.auth_token
            resp = await self._do_request(
                conn, 'POST', '/', subres='bulk-delete', body=[body], headers=headers
            )

            # bulk deletes should always return 200
            if resp.status != 200:
                raise HTTPError(resp.status, resp.reason, resp.headers)

            hit = re.match(
                r'^application/json(;\s*charset="?(.+?)"?)?$', resp.headers['content-type']
            )
            if not hit:
                log.error(
                    'Unexpected server response. Expected json, got:\n%s',
                    await dump_response(conn, resp),
                )
                raise RuntimeError('Unexpected server reply')

            # there might be an arbitrary amount of whitespace before the
            # JSON response (to keep the connection from timing out)
            # but json.loads discards these whitespace characters automatically
            resp_dict = json.loads((await conn.readall()).decode(hit.group(2) or 'utf-8'))

        log.debug('Response %s', resp_dict)

        try:
            resp_status_code, resp_status_text = _split_response_status(
                resp_dict['Response Status']
            )
        except ValueError:
            raise RuntimeError('Unexpected server reply')

        if resp_status_code == 200 or (
            resp_status_code in (400, 404)
            and len(resp_dict['Errors']) == 0
            and resp_dict['Number Not Found'] > 0
        ):
            # No/only 404 errors occurred.
            # We remove all keys since we don't know which ones have been deleted.
            # (Swift only returns a counter of deleted objects, not the list of deleted objects.)
            # Swift returns a 400 error, but we also accept the more appropriate 404 error code.
            del keys[:]
            return

        # Some errors occurred, so we need to determine what has
        # been deleted and what hasn't
        failed_keys = []
        offset = len(esc_prefix)
        for error in resp_dict['Errors']:
            fullkey = error[0]
            # strangely the name is url encoded in JSON
            assert fullkey.startswith(esc_prefix)
            key = urllib.parse.unquote(fullkey[offset:])
            failed_keys.append(key)
            log.debug('Delete %s failed with %s', key, error[1])
        for key in keys[:]:
            if key not in failed_keys:
                keys.remove(key)

        # At this point it is clear that the server has sent some kind of error response.
        # Swift is not very consistent in returning errors.
        # We need to jump through these hoops to get something meaningful.

        error_msg = resp_dict['Response Body']
        error_code = resp_status_code
        if not error_msg:
            if len(resp_dict['Errors']) > 0:
                error_code, error_msg = _split_response_status(resp_dict['Errors'][0][1])
            else:
                error_msg = resp_status_text

        if error_code == 401:
            # Expired auth token
            await self._do_authentication_expired(error_msg, auth_token)
            # raises AuthenticationExpired
        if 'Invalid bulk delete.' in error_msg:
            error_code = 400
            # change error message to something more meaningful
            error_msg = 'Sent a bulk delete with an empty list of keys to delete'
        elif 'Max delete failures exceeded' in error_msg:
            error_code = 502
        elif 'Maximum Bulk Deletes: ' in error_msg:
            # Sent more keys in one bulk delete than allowed
            error_code = 413
        elif 'Invalid File Name' in error_msg:
            # get returned when file name is too long
            error_code = 422

        raise HTTPError(error_code, error_msg)

    async def delete_multi(self, keys, *, log_progress=False):
        log.debug('started with %s', keys)

        if self.features.has_bulk_delete:
            total = len(keys)
            while len(keys) > 0:
                tmp = keys[: self.features.max_deletes]
                try:
                    await self._delete_multi(tmp)
                finally:
                    keys[: self.features.max_deletes] = tmp
                if log_progress:
                    log_delete_progress(total - len(keys), total)
        else:
            await super().delete_multi(keys, log_progress=log_progress)

    async def list(self, prefix='') -> AsyncIterator[str]:
        prefix = self.prefix + prefix
        strip = len(self.prefix)
        page_token = None
        while True:
            (els, page_token) = await self._list_page(prefix, page_token)
            for el in els:
                yield el[strip:]
            if page_token is None:
                break

    @retry
    async def _list_page(self, prefix, page_token=None, batch_size=1000) -> tuple:
        # Limit maximum number of results since we read everything
        # into memory (because Python JSON doesn't have a streaming API)
        query_string = {'prefix': prefix, 'limit': str(batch_size), 'format': 'json'}
        if page_token:
            query_string['marker'] = page_token

        async with self._borrow_conn() as conn:
            try:
                resp = await self._do_request(conn, 'GET', '/', query_string=query_string)
            except HTTPError as exc:
                if exc.status == 404:
                    raise DanglingStorageURLError(self.container_name)
                raise

            if resp.status == 204:
                return ([], None)

            hit = re.match('application/json; charset="?(.+?)"?$', resp.headers['content-type'])
            if not hit:
                log.error(
                    'Unexpected server response. Expected json, got:\n%s',
                    await dump_response(conn, resp),
                )
                raise RuntimeError('Unexpected server reply')

            body = await conn.readall()

        names = []
        last_name = None
        count = 0
        for dataset in json.loads(body.decode(hit.group(1))):
            count += 1
            name = dataset['name']
            last_name = name
            if name.endswith(TEMP_SUFFIX):
                continue
            names.append(name)

        if last_name is not None and count == batch_size:
            page_token = last_name
        else:
            page_token = None

        return (names, page_token)

    async def close(self):
        if self._pool is not None:
            await self._pool.aclose()

    async def _detect_features(self, hostname, port, ssl_context) -> 'Features':
        '''Figure out the Swift version and supported features by examining the
        /info endpoint of the storage server, and return them as a `Features`.

        See https://docs.openstack.org/swift/latest/middleware.html#discoverability
        '''

        if 'no-feature-detection' in self.options:
            log.debug('Skip feature detection')
            return Features()

        if not port:
            port = 443 if ssl_context else 80

        detected_features = Features()

        async with HTTPConnection(
            hostname, port, proxy=self.proxy, ssl_context=ssl_context
        ) as conn:
            conn.timeout = int(self.options.get('tcp-timeout', 20))

            log.debug('GET /info')
            resp = await conn.send_request('GET', '/info')

            # 200, 401, 403 and 404 are all OK since the /info endpoint
            # may not be accessible (misconfiguration) or may not
            # exist (old Swift version).
            if resp.status not in (200, 401, 403, 404):
                log.error(
                    "Wrong server response.\n%s",
                    await dump_response(conn, resp, body=await conn.read()),
                )
                raise HTTPError(resp.status, resp.reason, resp.headers)

            if resp.status == 200:
                hit = re.match(
                    r'^application/json(;\s*charset="?(.+?)"?)?$', resp.headers['content-type']
                )
                if not hit:
                    log.error(
                        "Wrong server response. Expected json. Got: \n%s",
                        await dump_response(conn, resp, body=await conn.read()),
                    )
                    raise RuntimeError('Unexpected server reply')

                info = json.loads((await conn.readall()).decode(hit.group(2) or 'utf-8'))
                swift_info = info.get('swift', {})

                log.debug('%s:%s/info returns %s', hostname, port, info)

                # Default metadata value length constrain is 256 bytes
                # but the provider could configure another value.
                # We only decrease the chunk size since 255 is a big enough chunk size.
                max_meta_len = swift_info.get('max_meta_value_length', None)
                if isinstance(max_meta_len, int) and max_meta_len < 256:
                    detected_features.max_meta_len = max_meta_len

                if info.get('bulk_delete', False):
                    detected_features.has_bulk_delete = True
                    bulk_delete = info['bulk_delete']
                    assert bulk_delete.get('max_failed_deletes', 1000) <= bulk_delete.get(
                        'max_deletes_per_request', 10000
                    )
                    assert bulk_delete.get('max_failed_deletes', 1000) > 0
                    # The block cache removal queue has a capacity of 1000.
                    # We do not need bigger values than that.
                    # We use max_failed_deletes instead of max_deletes_per_request
                    # because then we can be sure even when all our delete requests
                    # get rejected we get a complete error list back from the server.
                    # If we set the value higher, _delete_multi() would maybe
                    # delete some entries from the *keys* list that did not get
                    # deleted and would miss them in a retry.
                    detected_features.max_deletes = min(
                        1000, int(bulk_delete.get('max_failed_deletes', 1000))
                    )

                log.info(
                    'Detected Swift features for %s:%s: %s',
                    hostname,
                    port,
                    detected_features,
                    extra=LOG_ONCE,
                )
            else:
                log.debug(
                    '%s:%s/info not found or not accessible. Skip feature detection.',
                    hostname,
                    port,
                )

        return detected_features

    async def _do_authentication_expired(self, reason: str, auth_token: str | None) -> None:
        '''Handle an expired auth token observed while using *auth_token*.

        Always raises `AuthenticationExpired`. Reauthentication is serialised on
        `_auth_lock`: the cached pool is dropped only while *auth_token* is still the
        current token, so a stale 401 from a concurrent task cannot discard a pool that
        a peer has already rebuilt with a fresh token.
        '''
        log.info('OpenStack auth token seems to have expired, requesting new one.')
        async with self._auth_lock:
            if self.auth_token == auth_token:
                self._pool = None
        raise AuthenticationExpired(reason)


def _split_response_status(line):
    '''Splits a HTTP response line into status code (integer)
    and status text.

    Returns 2-tuple (int, string)

    Raises ValueError when line is not parsable'''
    hit = re.match(r'^([0-9]{3})\s+(.*)$', line)
    if not hit:
        log.error('Expected valid Response Status, got: %s', line)
        raise ValueError('Expected valid Response Status, got: %s' % line)
    return (int(hit.group(1)), hit.group(2))


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

    __slots__ = ['has_bulk_delete', 'max_deletes', 'max_meta_len']

    def __init__(self, has_bulk_delete=False, max_deletes=1000, max_meta_len=255):
        self.has_bulk_delete = has_bulk_delete
        self.max_deletes = max_deletes
        self.max_meta_len = max_meta_len

    def __str__(self):
        features = []
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
