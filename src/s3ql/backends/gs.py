'''
backends/gs.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import annotations

import builtins
import json
import logging
import re
import ssl
import threading
import urllib.parse
from ast import literal_eval
from base64 import b64decode, b64encode
from collections.abc import AsyncIterator
from itertools import count

import google.auth as g_auth

from s3ql.http import (
    BodyFollowing,
    CaseInsensitiveDict,
    ConnectionClosed,
    HTTPConnection,
    HTTPResponse,
    UnsupportedResponse,
    is_temp_network_error,
)
from s3ql.types import BackendOptionsProtocol, BasicMappingT, BinaryInput, BinaryOutput

from ..common import OAUTH_CLIENT_ID, OAUTH_CLIENT_SECRET
from ..logging import QuietError
from .common import (
    _FACTORY_SENTINEL,
    AuthenticationError,
    AuthorizationError,
    CorruptedObjectError,
    DanglingStorageURLError,
    NoSuchObject,
    copy_from_http,
    copy_to_http,
    get_proxy,
    get_ssl_context,
    retry,
)
from .common import (
    AsyncBackend as AsyncBackendBase,
)

log = logging.getLogger(__name__)


class ServerResponseError(Exception):
    '''Raised if the server response cannot be parsed.

    For HTTP errors (i.e., non 2xx response codes), RequestError should
    always be used instead (since in that case the response body can
    not be expected to have any specific format).
    '''

    def __init__(self, resp: HTTPResponse, error: str) -> None:
        self.resp = resp
        self.error = error

    def __str__(self) -> str:
        return '<ServerResponseError: %s>' % self.error


class RequestError(Exception):
    '''
    An error returned by the server.
    '''

    def __init__(
        self, code: int, reason: str, message: str | None = None, body: str | None = None
    ) -> None:
        super().__init__()
        self.code = code
        self.reason = reason
        self.message = message
        self.body = body

    def __str__(self) -> str:
        if self.message:
            return '<RequestError, code=%s, reason=%r, message=%r>' % (
                self.code,
                self.reason,
                self.message,
            )
        elif self.body:
            return '<RequestError, code=%d, reason=%r, with body data>' % (self.code, self.reason)
        else:
            return '<RequestError, code=%d, reason=%r>' % (self.code, self.reason)


class AccessTokenExpired(Exception):
    '''
    Raised if the access token has expired.
    '''


def _parse_error_response(resp: HTTPResponse, body: bytes) -> RequestError:
    '''Return exception corresponding to server response.'''

    try:
        json_resp = _parse_json_response(resp, body)
    except ServerResponseError:
        # Error messages may come from intermediate proxies and thus may not be in JSON.
        log.debug('Server response not JSON - intermediate proxy failure?')
        return RequestError(code=resp.status, reason=resp.reason)

    try:
        error_obj = json_resp.get('error')
        if isinstance(error_obj, dict):
            message = str(error_obj.get('message', ''))
        else:
            raise KeyError('error')
    except KeyError:
        log.warning('Did not find error.message element in JSON error response.')
        message = str(json_resp)

    return RequestError(code=resp.status, reason=resp.reason, message=message)


def _parse_json_response(resp: HTTPResponse, body: bytes) -> dict[str, object]:
    # Note that even though the final server backend may guarantee to always deliver a JSON document
    # body with a detailed error message, we may also get errors from intermediate proxies.
    content_type = resp.headers.get('Content-Type', None)
    if content_type:
        hit = re.match(
            r'application/json(?:; charset="(.+)")?$',
            resp.headers['Content-Type'],
            re.IGNORECASE,
        )
    if not content_type or not hit:
        raise ServerResponseError(resp, error='expected json, got %s' % content_type)
    charset = hit.group(1)

    try:
        body_text = body.decode(charset)
    except UnicodeDecodeError as exc:
        log.warning('Unable to decode JSON response as Unicode (%s)', str(exc))
        raise ServerResponseError(resp, error=str(exc))

    try:
        resp_json: dict[str, object] = json.loads(body_text)
    except json.JSONDecodeError as exc:
        log.warning('Unable to decode JSON response (%s)', str(exc))
        raise ServerResponseError(resp, error=str(exc))

    return resp_json


class AsyncBackend(AsyncBackendBase):
    """A backend to store data in Google Storage"""

    known_options: set[str] = {'ssl-ca-path', 'tcp-timeout'}

    # We don't want to request an access token for each instance,
    # because there is a limit on the total number of valid tokens.
    # This class variable holds the mapping from refresh tokens to
    # access tokens.
    access_token: dict[str, str] = dict()
    _refresh_lock: threading.Lock = threading.Lock()
    adc: tuple[object, object] | None = None

    ssl_context: ssl.SSLContext
    options: dict[str, str]
    proxy: tuple[str, int] | None
    login: str
    refresh_token: str
    hostname: str
    port: int
    bucket_name: str
    prefix: str
    conn: HTTPConnection

    @classmethod
    async def create(cls, options: BackendOptionsProtocol) -> AsyncBackend:
        backend = cls(_FACTORY_SENTINEL, options)
        await backend._check_bucket()
        return backend

    def __init__(self, _sentinel: object, options: BackendOptionsProtocol) -> None:
        if _sentinel is not _FACTORY_SENTINEL:
            raise TypeError("Use 'await AsyncBackend.create(...)' instead of direct instantiation")
        super().__init__(_factory_sentinel=_FACTORY_SENTINEL)

        ssl_ca_path = options.backend_options.get('ssl-ca-path', None)
        self.ssl_context = get_ssl_context(
            ssl_ca_path if isinstance(ssl_ca_path, str) or ssl_ca_path is None else None
        )
        self.options = options.backend_options  # type: ignore[assignment]
        self.proxy = get_proxy(ssl=True)
        self.login = options.backend_login
        self.refresh_token = options.backend_password

        if self.login == 'adc':
            if self.adc is None:
                import google.auth.transport.urllib3
                import urllib3

                # Deliberately ignore proxy and SSL context when attempting
                # to connect to Compute Engine Metadata server.
                requester = google.auth.transport.urllib3.Request(urllib3.PoolManager())
                try:
                    credentials, _ = g_auth.default(
                        request=requester,
                        scopes=['https://www.googleapis.com/auth/devstorage.full_control'],
                    )
                except g_auth.exceptions.DefaultCredentialsError as exc:
                    raise QuietError('ADC found no valid credential sources: ' + str(exc))
                type(self).adc = (credentials, requester)
        elif self.login != 'oauth2':
            raise QuietError("Google Storage backend requires OAuth2 or ADC authentication")

        # Special case for unit testing against local mock server
        hit = re.match(
            r'^gs://!unittest!'
            r'([^/:]+)'  # Hostname
            r':([0-9]+)'  # Port
            r'/([^/]+)'  # Bucketname
            r'(?:/(.*))?$',  # Prefix
            options.storage_url,
        )
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

    @retry
    async def _check_bucket(self) -> None:
        '''Check if bucket exists and/or credentials are correct.'''

        path = '/storage/v1/b/' + urllib.parse.quote(self.bucket_name, safe='')
        try:
            resp = await self._do_request('GET', path)
        except RequestError as exc:
            if exc.code == 404:
                raise DanglingStorageURLError("Bucket '%s' does not exist" % self.bucket_name)
            mapped_exc = _map_request_error(exc, None)
            if mapped_exc:
                raise mapped_exc
            raise
        _parse_json_response(resp, await self.conn.co_readall())

    async def reset(self) -> None:
        if self.conn is not None and (self.conn.response_pending() or self.conn._out_remaining):
            log.debug('Resetting state of http connection %d', id(self.conn))
            self.conn.disconnect()

    def _get_conn(self) -> HTTPConnection:
        '''Return connection to server'''

        conn = HTTPConnection(
            self.hostname, self.port, proxy=self.proxy, ssl_context=self.ssl_context
        )
        conn.timeout = int(self.options.get('tcp-timeout', 20))
        return conn

    def is_temp_failure(self, exc: BaseException) -> bool:
        if is_temp_network_error(exc) or isinstance(exc, ssl.SSLError):
            # We probably can't use the connection anymore, so disconnect (can't use reset, since
            # this would immediately attempt to reconnect, circumventing retry logic)
            self.conn.disconnect()
            return True

        elif isinstance(exc, RequestError) and (500 <= exc.code <= 599 or exc.code == 408):
            return True

        elif isinstance(exc, AccessTokenExpired):
            # Delete may fail if multiple threads encounter the same error
            self.access_token.pop(self.refresh_token, None)
            return True

        # Not clear at all what is happening here, but in doubt we retry
        elif isinstance(exc, ServerResponseError):
            return True

        if isinstance(exc, g_auth.exceptions.TransportError):  # noqa: SIM103 # auto-added, needs manual check!
            return True

        return False

    async def _assert_empty_response(self, resp: HTTPResponse) -> None:
        '''Assert that current response body is empty'''

        buf = await self.conn.co_read(2048)
        if not buf:
            return  # expected

        body = '\n'.join('%s: %s' % x for x in resp.headers.items())

        hit = re.search(r'; charset="(.+)"$', resp.headers.get('Content-Type', ''), re.IGNORECASE)
        if hit:
            charset = hit.group(1)
            body += '\n' + buf.decode(charset, errors='backslashreplace')

        log.warning('Expected empty response body, but got data - this is odd.')
        raise ServerResponseError(resp, error='expected empty response')

    @retry
    async def delete(self, key: str) -> None:
        log.debug('started with %s', key)
        path = '/storage/v1/b/%s/o/%s' % (
            urllib.parse.quote(self.bucket_name, safe=''),
            urllib.parse.quote(self.prefix + key, safe=''),
        )
        try:
            resp = await self._do_request('DELETE', path)
            await self._assert_empty_response(resp)
        except RequestError as exc:
            mapped_exc = _map_request_error(exc, key)
            if isinstance(mapped_exc, NoSuchObject):
                pass
            elif mapped_exc:
                raise mapped_exc
            else:
                raise

    async def list(self, prefix: str = '') -> AsyncIterator[str]:
        full_prefix = self.prefix + prefix
        strip = len(self.prefix)
        page_token: str | None = None
        while True:
            result = await self._list_page(full_prefix, page_token)
            els: builtins.list[str] = result[0]
            page_token = result[1]
            for el in els:
                yield el[strip:]
            if page_token is None:
                break

    @retry
    async def _list_page(
        self, prefix: str, page_token: str | None = None, batch_size: int = 1000
    ) -> tuple[builtins.list[str], str | None]:
        # Limit maximum number of results since we read everything
        # into memory (because Python JSON doesn't have a streaming API)
        query_string: dict[str, str] = {'prefix': prefix, 'maxResults': str(batch_size)}
        if page_token:
            query_string['pageToken'] = page_token

        path = '/storage/v1/b/%s/o' % (urllib.parse.quote(self.bucket_name, safe=''),)

        try:
            resp = await self._do_request('GET', path, query_string=query_string)
        except RequestError as exc:
            mapped_exc = _map_request_error(exc, None)
            if mapped_exc:
                raise mapped_exc
            raise
        json_resp = _parse_json_response(resp, await self.conn.co_readall())
        next_token_obj = json_resp.get('nextPageToken', None)
        next_token: str | None = str(next_token_obj) if next_token_obj is not None else None

        if 'items' not in json_resp:
            assert next_token is None
            return ([], None)

        items = json_resp['items']
        assert isinstance(items, list)
        return ([str(x['name']) for x in items], next_token)  # type: ignore[index]

    @retry
    async def lookup(self, key: str) -> BasicMappingT:
        log.debug('started with %s', key)
        return _unwrap_user_meta(await self._get_gs_meta(key))

    async def _get_gs_meta(self, key: str) -> dict[str, object]:
        path = '/storage/v1/b/%s/o/%s' % (
            urllib.parse.quote(self.bucket_name, safe=''),
            urllib.parse.quote(self.prefix + key, safe=''),
        )
        try:
            resp = await self._do_request('GET', path)
        except RequestError as exc:
            mapped_exc = _map_request_error(exc, key)
            if mapped_exc:
                raise mapped_exc
            raise
        return _parse_json_response(resp, await self.conn.co_readall())

    @retry
    async def get_size(self, key: str) -> int:
        json_resp = await self._get_gs_meta(key)
        return json_resp['size']  # type: ignore[return-value]

    async def readinto_fh(self, key: str, fh: BinaryOutput) -> BasicMappingT:
        '''Transfer data stored under *key* into *fh*, return metadata.

        The data will be inserted at the current offset. If a temporary error (as defined by
        `is_temp_failure`) occurs, the operation is retried.
        '''

        return await self._readinto_fh(key, fh, fh.tell())

    @retry
    async def _readinto_fh(self, key: str, fh: BinaryOutput, off: int) -> BasicMappingT:
        gs_meta = await self._get_gs_meta(key)
        metadata = _unwrap_user_meta(gs_meta)

        path = '/storage/v1/b/%s/o/%s' % (
            urllib.parse.quote(self.bucket_name, safe=''),
            urllib.parse.quote(self.prefix + key, safe=''),
        )
        try:
            await self._do_request('GET', path, query_string={'alt': 'media'})
        except RequestError as exc:
            mapped_exc = _map_request_error(exc, key)
            if mapped_exc:
                raise mapped_exc
            raise

        await copy_from_http(self.conn, fh)

        return metadata

    async def write_fh(
        self,
        key: str,
        fh: BinaryInput,
        len_: int,
        metadata: BasicMappingT | None = None,
    ) -> int:
        '''Upload *len_* bytes from *fh* under *key*.

        The data will be read at the current offset.

        If a temporary error (as defined by `is_temp_failure`) occurs, the operation is
        retried. Returns the size of the resulting storage object.
        '''

        off = fh.tell()
        return await self._write_fh(key, fh, off, len_, metadata or {})

    @retry
    async def _write_fh(
        self, key: str, fh: BinaryInput, off: int, len_: int, metadata: BasicMappingT
    ) -> int:
        metadata_s = json.dumps(
            {
                'metadata': _wrap_user_meta(metadata),
                'name': self.prefix + key,
            }
        )

        # Google Storage uses Content-Length to read the object data, so we
        # don't have to worry about the boundary occurring in the object data.
        boundary = 'foo_bar_baz'
        headers = CaseInsensitiveDict()
        headers['Content-Type'] = 'multipart/related; boundary=%s' % boundary

        body_prefix = '\n'.join(
            (
                '--' + boundary,
                'Content-Type: application/json; charset=UTF-8',
                '',
                metadata_s,
                '--' + boundary,
                'Content-Type: application/octet-stream',
                '',
                '',
            )
        ).encode()
        body_suffix = ('\n--%s--\n' % boundary).encode()

        body_size = len(body_prefix) + len(body_suffix) + len_

        path = '/upload/storage/v1/b/%s/o' % (urllib.parse.quote(self.bucket_name, safe=''),)
        query_string = {'uploadType': 'multipart'}
        try:
            resp = await self._do_request(
                'POST',
                path,
                query_string=query_string,
                headers=headers,
                body=BodyFollowing(body_size),
            )
        except RequestError as exc:
            mapped_exc = _map_request_error(exc, key)
            if mapped_exc:
                raise mapped_exc
            raise

        assert resp.status == 100
        fh.seek(off)

        try:
            await self.conn.co_write(body_prefix)
            await copy_to_http(fh, self.conn, len_)
            await self.conn.co_write(body_suffix)
        except ConnectionClosed:
            # Server closed connection while we were writing body data -
            # but we may still be able to read an error response
            try:
                resp = await self.conn.co_read_response()
            except ConnectionClosed:  # No server response available
                pass
            else:
                log.warning(
                    'Server broke connection during upload, signaled %d %s',
                    resp.status,
                    resp.reason,
                )
            # Re-raise first ConnectionClosed exception
            raise

        resp = await self.conn.co_read_response()
        # If we're really unlucky, then the token has expired while we were uploading data.
        if resp.status == 401:
            await self.conn.co_discard()
            raise AccessTokenExpired()
        elif resp.status != 200:
            exc_ = _parse_error_response(resp, await self.conn.co_readall())
            raise _map_request_error(exc_, key) or exc_
        _parse_json_response(resp, await self.conn.co_readall())

        return body_size

    async def close(self) -> None:
        if self.conn is not None:
            self.conn.disconnect()

    def __str__(self) -> str:
        return '<gs.Backend, name=%s, prefix=%s>' % (self.bucket_name, self.prefix)

    # This method uses a different HTTP connection than its callers, but shares
    # the same retry logic. It is therefore possible that errors with this
    # connection cause the other connection to be reset - but this should not
    # be a problem, because there can't be a pending request if we don't have
    # a valid access token.
    async def _get_access_token(self) -> None:
        log.info('Requesting new access token')

        if self.adc:
            try:
                self.adc[0].refresh(self.adc[1])  # type: ignore[attr-defined]
            except g_auth.exceptions.RefreshError as exc:
                raise AuthenticationError('Failed to refresh credentials: ' + str(exc))
            self.access_token[self.refresh_token] = self.adc[0].token  # type: ignore[attr-defined]
            return

        headers = CaseInsensitiveDict()
        headers['Content-Type'] = 'application/x-www-form-urlencoded; charset=utf-8'

        body = urllib.parse.urlencode(
            {
                'client_id': OAUTH_CLIENT_ID,
                'client_secret': OAUTH_CLIENT_SECRET,
                'refresh_token': self.refresh_token,
                'grant_type': 'refresh_token',
            }
        )

        conn = HTTPConnection(
            'accounts.google.com', 443, proxy=self.proxy, ssl_context=self.ssl_context
        )
        try:
            await conn.co_send_request(
                'POST', '/o/oauth2/token', headers=headers, body=body.encode('utf-8')
            )
            resp = await conn.co_read_response()
            json_resp = _parse_json_response(resp, await conn.co_readall())

            if resp.status > 299 or resp.status < 200:
                assert 'error' in json_resp
            if 'error' in json_resp:
                raise AuthenticationError(str(json_resp['error']))
            else:
                self.access_token[self.refresh_token] = str(json_resp['access_token'])
        finally:
            conn.disconnect()

    async def _dump_body(self, resp: HTTPResponse) -> str:
        '''Return truncated string representation of response body.'''

        is_truncated = False
        try:
            body = await self.conn.co_read(2048)
            if await self.conn.co_read(1):
                is_truncated = True
                await self.conn.co_discard()
        except UnsupportedResponse:
            log.warning('Unsupported response, trying to retrieve data from raw socket!')
            body = self.conn.read_raw(2048)
            self.conn.close()

        hit = re.search(r'; charset="(.+)"$', resp.headers.get('Content-Type', ''), re.IGNORECASE)
        if hit:
            charset = hit.group(1)
        else:
            charset = 'utf-8'

        body = body.decode(charset, errors='backslashreplace')

        if is_truncated:
            body += '... [truncated]'

        return body

    async def _do_request(
        self,
        method: str,
        path: str,
        query_string: dict[str, str] | None = None,
        headers: CaseInsensitiveDict | None = None,
        body: bytes | BodyFollowing | None = None,
    ) -> HTTPResponse:
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
            await self.conn.co_send_request(
                method, path, body=body, headers=headers, expect100=expect100
            )
            resp = await self.conn.co_read_response()
            if (expect100 and resp.status == 100) or (not expect100 and 200 <= resp.status <= 299):
                return resp
            elif resp.status != 401:
                raise _parse_error_response(resp, await self.conn.co_readall())
            await self.conn.co_discard()

        # If we reach this point, then the access token must have
        # expired, so we try to get a new one. We use a lock to prevent
        # multiple threads from refreshing the token simultaneously.
        with self._refresh_lock:
            # Don't refresh if another thread has already done so while
            # we waited for the lock.
            if token is None or self.access_token.get(self.refresh_token, None) == token:
                await self._get_access_token()

        # Try request again. If this still fails, propagate the error
        # (because we have just refreshed the access token).
        # FIXME: We can't rely on this if e.g. the system hibernated
        # after refreshing the token, but before reaching this line.
        headers['Authorization'] = 'Bearer ' + self.access_token[self.refresh_token]
        await self.conn.co_send_request(
            method, path, body=body, headers=headers, expect100=expect100
        )
        resp = await self.conn.co_read_response()
        if (expect100 and resp.status == 100) or (not expect100 and 200 <= resp.status <= 299):
            return resp
        else:
            raise _parse_error_response(resp, await self.conn.co_readall())


def _map_request_error(exc: RequestError, key: str | None) -> Exception | None:
    '''Map RequestError to more general exception if possible'''

    if exc.code == 404 and key:
        return NoSuchObject(key)
    elif exc.message == 'Forbidden':
        return AuthorizationError(exc.message)
    elif exc.message in ('Login Required', 'Invalid Credentials'):
        return AuthenticationError(exc.message)

    return None


def _wrap_user_meta(user_meta: BasicMappingT) -> dict[str, str]:
    obj_meta: dict[str, str] = dict()
    for key, val in user_meta.items():
        if not isinstance(key, str):
            raise TypeError('metadata keys must be str, not %s' % type(key))
        if not isinstance(val, (str, bytes, int, float, complex, bool)) and val is not None:
            raise TypeError('value for key %s (%s) is not elementary' % (key, val))
        if isinstance(val, (bytes, bytearray)):
            val = b64encode(val)

        obj_meta[key] = repr(val)

    return obj_meta


def _unwrap_user_meta(json_resp: dict[str, object]) -> BasicMappingT:
    '''Extract user metadata from JSON object metadata'''

    meta_raw_obj = json_resp.get('metadata')
    if meta_raw_obj is None:
        return {}

    assert isinstance(meta_raw_obj, dict)
    meta_raw: dict[str, str] = meta_raw_obj  # type: ignore[assignment]

    # Detect Legacy format.
    if (
        meta_raw.get('format') == 'raw2'
        and 'md5' in meta_raw
        and all(key in ('format', 'md5') or re.match(r'^\d\d\d$', key) for key in meta_raw)
    ):
        parts: list[str] = []
        for i in count():
            part = meta_raw.get('%03d' % i)
            if part is None:
                break
            parts.append(part)
        buf = ''.join(parts)
        meta: BasicMappingT = literal_eval('{ %s }' % buf)
        for k, v in meta.items():
            if isinstance(v, bytes):
                meta[k] = b64decode(v)

        return meta

    meta = {}
    for k, v in meta_raw.items():
        try:
            v2 = literal_eval(v)
        except ValueError as exc:
            raise CorruptedObjectError('Invalid metadata value: ' + str(exc))
        if isinstance(v2, bytes):
            meta[k] = b64decode(v2)
        else:
            meta[k] = v2

    return meta
