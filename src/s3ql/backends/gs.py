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
import urllib.parse
from ast import literal_eval
from base64 import b64decode, b64encode
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime, timezone
from itertools import count

import google.auth as g_auth
import google.auth.exceptions  # noqa: F401
import trio

from s3ql.http import (
    BodyT,
    CaseInsensitiveDict,
    HTTPConnection,
    HTTPResponse,
    Pool,
    fh_size,
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
    ServerResponseError,
    assert_empty_response,
    get_proxy,
    get_ssl_context,
    retry,
)
from .common import (
    AsyncBackend as AsyncBackendBase,
)

log = logging.getLogger(__name__)


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
    Raised if the server rejected the access token as expired.
    '''


@dataclass(slots=True)
class _Token:
    '''Cached OAuth2 bearer token with its expiry deadline.

    *deadline* is a `trio.current_time()` value already adjusted by the
    safety margin; once the current time crosses it, the token must be
    refreshed before use.
    '''

    bearer: str
    deadline: float


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
            message = str(error_obj.get('message') or '')  # type: ignore[arg-type]  # dict narrowed from object
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
        # Both the real GCS server (no quotes) and various intermediaries
        # (sometimes quoted) appear in the wild — accept either form.
        hit = re.match(
            r'application/json(?:;\s*charset="?(.+?)"?)?$',
            resp.headers['Content-Type'],
            re.IGNORECASE,
        )
    if not content_type or not hit:
        raise ServerResponseError(resp, error='expected json, got %s' % content_type)
    charset = hit.group(1) or 'utf-8'

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

    # Refresh access tokens at most this far before their nominal expiry.
    # Protects against clock skew between us and the OAuth endpoint, and
    # against tokens expiring mid-request.
    _TOKEN_SAFETY_MARGIN = 300.0

    # Conservative fallback lifetime if the server omits `expires_in` or
    # the ADC credential has no usable expiry. Google access tokens are
    # nominally valid for 3600s, so this still leaves us a fresh token
    # most of the time.
    _TOKEN_DEFAULT_LIFETIME = 30 * 60.0

    # We don't want to request an access token for each instance,
    # because there is a limit on the total number of valid tokens.
    # This class variable holds the mapping from refresh tokens to
    # access tokens.
    _tokens: dict[str, _Token] = dict()
    _refresh_lock: trio.Lock = trio.Lock()
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
    _pool: Pool

    @classmethod
    async def create(
        cls, options: BackendOptionsProtocol, max_connections: int = 1
    ) -> AsyncBackend:
        backend = cls(_FACTORY_SENTINEL, options, max_connections)
        await backend._check_bucket()
        return backend

    def __init__(
        self,
        _sentinel: object,
        options: BackendOptionsProtocol,
        max_connections: int = 1,
    ) -> None:
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

        self._max_connections = max_connections
        self._pool = self._make_pool()

    @retry
    async def _check_bucket(self) -> None:
        '''Check if bucket exists and/or credentials are correct.'''

        path = '/storage/v1/b/' + urllib.parse.quote(self.bucket_name, safe='')
        try:
            async with self._pool.get() as conn:
                resp = await self._do_request(conn, 'GET', path)
                _parse_json_response(resp, await conn.readall())
        except RequestError as exc:
            if exc.code == 404:
                raise DanglingStorageURLError("Bucket '%s' does not exist" % self.bucket_name)
            mapped_exc = _map_request_error(exc, None)
            if mapped_exc:
                raise mapped_exc
            raise

    def _make_pool(self) -> Pool:
        '''Return a fresh connection pool for the current host/port.'''

        return Pool(
            self.hostname,
            self.port,
            proxy=self.proxy,
            ssl_context=self.ssl_context,
            max_connections=self._max_connections,
            timeout=int(self.options.get('tcp-timeout', 20)),
        )

    def is_temp_failure(self, exc: BaseException) -> bool:
        if is_temp_network_error(exc) or isinstance(exc, ssl.SSLError):
            # The pool drops the affected connection automatically.
            return True

        elif isinstance(exc, RequestError) and (500 <= exc.code <= 599 or exc.code == 408):
            return True

        elif isinstance(exc, AccessTokenExpired):
            # Concurrent failures may race to remove the same entry.
            self._tokens.pop(self.refresh_token, None)
            return True

        # Not clear at all what is happening here, but in doubt we retry
        elif isinstance(exc, ServerResponseError):
            return True

        return isinstance(exc, g_auth.exceptions.TransportError)

    @retry
    async def delete(self, key: str) -> None:
        log.debug('started with %s', key)
        path = '/storage/v1/b/%s/o/%s' % (
            urllib.parse.quote(self.bucket_name, safe=''),
            urllib.parse.quote(self.prefix + key, safe=''),
        )
        try:
            async with self._pool.get() as conn:
                resp = await self._do_request(conn, 'DELETE', path)
                await assert_empty_response(conn, resp)
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
            async with self._pool.get() as conn:
                resp = await self._do_request(conn, 'GET', path, query_string=query_string)
                json_resp = _parse_json_response(resp, await conn.readall())
        except RequestError as exc:
            mapped_exc = _map_request_error(exc, None)
            if mapped_exc:
                raise mapped_exc
            raise
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
            async with self._pool.get() as conn:
                resp = await self._do_request(conn, 'GET', path)
                return _parse_json_response(resp, await conn.readall())
        except RequestError as exc:
            mapped_exc = _map_request_error(exc, key)
            if mapped_exc:
                raise mapped_exc
            raise

    @retry
    async def get_size(self, key: str) -> int:
        json_resp = await self._get_gs_meta(key)
        return int(json_resp['size'])  # type: ignore[call-overload] # GCS returns size as a string

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
            async with self._pool.get() as conn:
                await self._do_request(conn, 'GET', path, query_string={'alt': 'media'})
                await conn.readinto_fh(fh)
        except RequestError as exc:
            mapped_exc = _map_request_error(exc, key)
            if mapped_exc:
                raise mapped_exc
            raise

        return metadata

    @retry
    async def write_fh(
        self,
        key: str,
        fh: BinaryInput,
        metadata: BasicMappingT | None = None,
    ) -> int:
        '''Upload the full contents of *fh* under *key*.

        If a temporary error (as defined by `is_temp_failure`) occurs, the operation is
        retried. Returns the size of the resulting storage object.
        '''

        size = fh_size(fh)
        metadata_s = json.dumps(
            {
                'metadata': _wrap_user_meta(metadata or {}),
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

        path = '/upload/storage/v1/b/%s/o' % (urllib.parse.quote(self.bucket_name, safe=''),)
        query_string = {'uploadType': 'multipart'}
        try:
            async with self._pool.get() as conn:
                resp = await self._do_request(
                    conn,
                    'POST',
                    path,
                    query_string=query_string,
                    headers=headers,
                    body=[body_prefix, fh, body_suffix],
                )
                _parse_json_response(resp, await conn.readall())
        except RequestError as exc:
            mapped_exc = _map_request_error(exc, key)
            if mapped_exc:
                raise mapped_exc
            raise

        return size

    async def close(self) -> None:
        await self._pool.aclose()

    def __str__(self) -> str:
        return '<gs.Backend, name=%s, prefix=%s>' % (self.bucket_name, self.prefix)

    async def _ensure_token(self) -> str:
        '''Return a usable bearer token, refreshing proactively if needed.

        A token is reused as long as its deadline (already adjusted by
        `_TOKEN_SAFETY_MARGIN`) is still in the future. Otherwise we take
        `_refresh_lock` and refresh, double-checking that no concurrent
        task has already done so while we waited for the lock.
        '''

        token = self._tokens.get(self.refresh_token)
        if token is not None and trio.current_time() < token.deadline:
            return token.bearer

        async with self._refresh_lock:
            token = self._tokens.get(self.refresh_token)
            if token is None or trio.current_time() >= token.deadline:
                await self._get_access_token()
                token = self._tokens[self.refresh_token]
            return token.bearer

    # Uses a different HTTP connection than its callers, but shares the
    # same retry logic. Errors with this connection may therefore reset the
    # other connection - which is harmless, because there can't be a
    # pending request if we don't have a valid access token.
    async def _get_access_token(self) -> None:
        '''Acquire a fresh access token and cache it with its deadline.

        Must be called with `_refresh_lock` held.
        '''

        log.info('Requesting new access token')

        if self.adc:
            try:
                self.adc[0].refresh(self.adc[1])  # type: ignore[attr-defined]
            except g_auth.exceptions.RefreshError as exc:
                raise AuthenticationError('Failed to refresh credentials: ' + str(exc))
            bearer = str(self.adc[0].token)  # type: ignore[attr-defined]
            lifetime = self._adc_token_lifetime()
        else:
            bearer, lifetime = await self._fetch_oauth_token()

        deadline = trio.current_time() + lifetime - self._TOKEN_SAFETY_MARGIN
        self._tokens[self.refresh_token] = _Token(bearer=bearer, deadline=deadline)
        log.debug('Cached access token, refresh due in %.0fs', deadline - trio.current_time())

    def _adc_token_lifetime(self) -> float:
        '''Return remaining seconds until ADC token expiry, or a fallback.'''

        assert self.adc is not None
        expiry = getattr(self.adc[0], 'expiry', None)
        if expiry is None:
            return self._TOKEN_DEFAULT_LIFETIME
        # google-auth stores expiry as a naive UTC datetime.
        try:
            remaining = (expiry - datetime.now(timezone.utc).replace(tzinfo=None)).total_seconds()
        except TypeError:
            log.warning('Invalid expiry value in ADC credentials: %r', expiry)
            return self._TOKEN_DEFAULT_LIFETIME
        if remaining <= 0:
            log.warning('ADC token already expired according to expiry field: %r', expiry)
            return self._TOKEN_DEFAULT_LIFETIME
        return remaining

    async def _fetch_oauth_token(self) -> tuple[str, float]:
        '''Exchange the refresh token for a bearer token and its lifetime.'''

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

        async with HTTPConnection(
            'accounts.google.com', 443, proxy=self.proxy, ssl_context=self.ssl_context
        ) as conn:
            resp = await conn.send_request(
                'POST', '/o/oauth2/token', headers=headers, body=[body.encode('utf-8')]
            )
            json_resp = _parse_json_response(resp, await conn.readall())

        if 'error' in json_resp:
            raise AuthenticationError(str(json_resp['error']))

        bearer = str(json_resp['access_token'])
        expires_in = json_resp.get('expires_in')
        try:
            lifetime = float(expires_in) if expires_in is not None else self._TOKEN_DEFAULT_LIFETIME  # type: ignore[arg-type]
        except (TypeError, ValueError):
            log.warning('Invalid expires_in value in token response: %r', expires_in)
            lifetime = self._TOKEN_DEFAULT_LIFETIME
        return (bearer, lifetime)

    async def _do_request(
        self,
        conn: HTTPConnection,
        method: str,
        path: str,
        query_string: dict[str, str] | None = None,
        headers: CaseInsensitiveDict | None = None,
        body: BodyT = (),
    ) -> HTTPResponse:
        '''Send a single authorized request on *conn* and return the response.

        Acquires a bearer token (refreshing proactively when close to
        expiry) and sends exactly one request. On 2xx the response body
        is left unread on the connection. A 401 is signalled by raising
        `AccessTokenExpired`. Other non-2xx responses raise `RequestError`.
        '''

        log.debug('started with %s %s, qs=%s', method, path, query_string)

        if headers is None:
            headers = CaseInsensitiveDict()

        headers['host'] = self.hostname
        if query_string:
            s = urllib.parse.urlencode(query_string, doseq=True)
            path += '?%s' % s

        headers['Authorization'] = 'Bearer ' + await self._ensure_token()
        resp = await conn.send_request(method, path, headers=headers, body=body)

        if 200 <= resp.status <= 299:
            return resp
        if resp.status == 401:
            await conn.discard()
            raise AccessTokenExpired()
        raise _parse_error_response(resp, await conn.readall())


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
