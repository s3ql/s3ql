'''
backends/s3c.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import annotations

import binascii
import builtins
import contextlib
import email.message
import hashlib
import hmac
import logging
import re
import ssl
import time
import urllib.parse
from ast import literal_eval
from base64 import b64decode, b64encode
from collections.abc import AsyncIterator
from email.utils import mktime_tz, parsedate_tz
from io import BytesIO
from itertools import count
from typing import NoReturn, cast
from urllib.parse import quote, unquote, urlsplit
from xml.etree.ElementTree import Element

from defusedxml import ElementTree

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

from ..logging import QuietError
from .common import (
    _FACTORY_SENTINEL,
    AuthenticationError,
    AuthorizationError,
    CorruptedObjectError,
    DanglingStorageURLError,
    NoSuchObject,
    checksum_basic_mapping,
    copy_from_http,
    copy_to_http,
    get_proxy,
    get_ssl_context,
    retry,
)
from .common import (
    AsyncBackend as AsyncBackendBase,
)

C_DAY_NAMES = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
C_MONTH_NAMES = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

XML_CONTENT_RE = re.compile(r'^(?:application|text)/xml(?:;|$)', re.IGNORECASE)


log = logging.getLogger(__name__)


class AsyncBackend(AsyncBackendBase):
    """A backend to stored data in some S3 compatible storage service."""

    xml_ns_prefix = '{http://s3.amazonaws.com/doc/2006-03-01/}'
    hdr_prefix = 'x-amz-'
    known_options: set[str] = {
        'no-ssl',
        'ssl-ca-path',
        'tcp-timeout',
        'dumb-copy',
        'disable-expect100',
    }

    ssl_context: ssl.SSLContext | None
    options: dict[str, str | bool]
    bucket_name: str
    prefix: str
    hostname: str
    port: int
    proxy: tuple[str, int] | None
    conn: HTTPConnection
    password: str
    login: str
    _extra_put_headers: CaseInsensitiveDict

    def __init__(self, *, options: BackendOptionsProtocol) -> None:
        '''Initialize backend object - use AsyncBackend.create() instead.'''

        super().__init__(_factory_sentinel=_FACTORY_SENTINEL)

        if 'no-ssl' in options.backend_options:
            self.ssl_context = None
        else:
            ssl_ca_path = options.backend_options.get('ssl-ca-path', None)
            self.ssl_context = get_ssl_context(cast(str | None, ssl_ca_path))

        (host, port, bucket_name, prefix) = self._parse_storage_url(
            options.storage_url, self.ssl_context
        )

        self.options = options.backend_options
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.hostname = host
        self.port = port
        self.proxy = get_proxy(self.ssl_context is not None)
        self.conn = self._get_conn()
        self.password = options.backend_password
        self.login = options.backend_login
        self._extra_put_headers = CaseInsensitiveDict()

    @classmethod
    async def create(cls: type[AsyncBackend], options: BackendOptionsProtocol) -> AsyncBackend:
        '''Create a new S3-compatible backend instance.'''
        return cls(options=options)

    async def reset(self) -> None:
        if self.conn is not None and (self.conn.response_pending() or self.conn._out_remaining):
            log.debug('Resetting state of http connection %d', id(self.conn))
            self.conn.disconnect()

    @staticmethod
    def _parse_storage_url(
        storage_url: str, ssl_context: ssl.SSLContext | None
    ) -> tuple[str, int, str, str]:
        '''Extract information from storage URL

        Return a tuple * (host, port, bucket_name, prefix) * .
        '''

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

        hostname = hit.group(1)
        if hit.group(2):
            port = int(hit.group(2))
        elif ssl_context:
            port = 443
        else:
            port = 80
        bucketname = hit.group(3)
        prefix = hit.group(4) or ''

        return (hostname, port, bucketname, prefix)

    def _get_conn(self) -> HTTPConnection:
        '''Return connection to server'''

        conn = HTTPConnection(
            self.hostname, self.port, proxy=self.proxy, ssl_context=self.ssl_context
        )
        conn.timeout = int(self.options.get('tcp-timeout', 20))
        return conn

    # This method is also used implicitly for the retry handling of
    # `gs.Backend._get_access_token`. When modifying this method, do not forget
    # to check if this makes it unsuitable for use by `_get_access_token` (in
    # that case we will have to implement a custom retry logic there).

    def is_temp_failure(self, exc: BaseException) -> bool:  # IGNORE:W0613
        if is_temp_network_error(exc) or isinstance(exc, ssl.SSLError):
            # We probably can't use the connection anymore, so disconnect (can't use reset, since
            # this would immediately attempt to reconnect, circumventing retry logic)
            self.conn.disconnect()
            return True

        if isinstance(  # noqa: SIM114 # auto-added, needs manual check!
            exc,
            (
                InternalError,
                BadDigestError,
                IncompleteBodyError,
                RequestTimeoutError,
                OperationAbortedError,
                SlowDownError,
                ServiceUnavailableError,
                TemporarilyUnavailableError,
            ),
        ):
            return True

        # In doubt, we retry on 5xx (Server error). However, there are some
        # codes where retry is definitely not desired. For 4xx (client error) we
        # do not retry in general, but for 408 (Request Timeout) RFC 2616
        # specifies that the client may repeat the request without
        # modifications. We also retry on 429 (Too Many Requests).
        elif isinstance(exc, HTTPError) and (
            (500 <= exc.status <= 599 and exc.status not in (501, 505, 508, 510, 511, 523))
            or exc.status in (408, 429)
        ):
            return True

        return False

    async def _dump_response(
        self, resp: HTTPResponse, body: bytes | bytearray | None = None
    ) -> str:
        '''Return string representation of server response

        Only the beginning of the response body is read, so this is
        mostly useful for debugging.
        '''

        if body is None:
            try:
                body = await self.conn.co_read(2048)
                if body:
                    await self.conn.co_discard()
            except UnsupportedResponse:
                log.warning('Unsupported response, trying to retrieve data from raw socket!')
                body = self.conn.read_raw(2048)
                self.conn.close()
        else:
            body = body[:2048]

        return '%d %s\n%s\n\n%s' % (
            resp.status,
            resp.reason,
            '\n'.join('%s: %s' % x for x in resp.headers.items()),
            body.decode('utf-8', errors='backslashreplace'),
        )

    async def _assert_empty_response(self, resp: HTTPResponse) -> None:
        '''Assert that current response body is empty'''

        buf = await self.conn.co_read(2048)
        if not buf:
            return  # expected

        # Log the problem
        await self.conn.co_discard()
        log.error(
            'Unexpected server response. Expected nothing, got:\n%d %s\n%s\n\n%s',
            resp.status,
            resp.reason,
            '\n'.join('%s: %s' % x for x in resp.headers.items()),
            buf,
        )
        raise RuntimeError('Unexpected server response')

    @retry
    async def delete(self, key: str) -> None:
        log.debug('started with %s', key)
        try:
            resp = await self._do_request('DELETE', '/%s%s' % (self.prefix, key))
            await self._assert_empty_response(resp)
        except NoSuchKeyError:
            pass

    async def list(self, prefix: str = '') -> AsyncIterator[str]:
        prefix = self.prefix + prefix
        strip = len(self.prefix)
        page_token: str | None = None
        while True:
            (els, page_token) = await self._list_page(prefix, page_token)
            for el in els:
                yield el[strip:]
            if page_token is None:
                break

    @retry
    async def _list_page(
        self, prefix: str, page_token: str | None = None, batch_size: int = 1000
    ) -> tuple[builtins.list[str], str | None]:
        # We can get at most 1000 keys at a time, so there's no need
        # to bother with streaming.
        query_string: dict[str, str] = {'prefix': prefix, 'max-keys': str(batch_size)}
        if page_token:
            query_string['marker'] = page_token

        resp = await self._do_request('GET', '/', query_string=query_string)

        if not XML_CONTENT_RE.match(resp.headers['Content-Type']):
            raise RuntimeError('unexpected content type: %s' % resp.headers['Content-Type'])

        body = await self.conn.co_readall()
        etree = ElementTree.fromstring(body)
        root_xmlns_uri = _tag_xmlns_uri(etree)
        if root_xmlns_uri is None:
            root_xmlns_prefix = ''
        else:
            # Validate the XML namespace
            root_xmlns_prefix = '{%s}' % (root_xmlns_uri,)
            if root_xmlns_prefix != self.xml_ns_prefix:
                log.error(
                    'Unexpected server reply to list operation:\n%s',
                    await self._dump_response(resp, body=body),
                )
                raise RuntimeError('List response has unknown namespace')

        names: builtins.list[str] = []
        for x in etree.findall(root_xmlns_prefix + 'Contents'):
            key = x.findtext(root_xmlns_prefix + 'Key')
            if key is not None:
                names.append(key)

        is_truncated = etree.find(root_xmlns_prefix + 'IsTruncated')
        if is_truncated is None:
            raise RuntimeError('IsTruncated element not found in response')
        if is_truncated.text == 'false':
            page_token = None
        elif len(names) == 0:
            next_marker = etree.find(root_xmlns_prefix + 'NextMarker')
            if next_marker is None:
                raise RuntimeError('NextMarker element not found in response')
            page_token = next_marker.text
        else:
            page_token = names[-1]

        return (names, page_token)

    @retry
    async def lookup(self, key: str) -> BasicMappingT:
        log.debug('started with %s', key)

        try:
            resp = await self._do_request('HEAD', '/%s%s' % (self.prefix, key))
            await self._assert_empty_response(resp)
        except HTTPError as exc:
            if exc.status == 404:
                raise NoSuchObject(key)
            else:
                raise

        return self._extractmeta(resp, key)

    @retry
    async def get_size(self, key: str) -> int:
        log.debug('started with %s', key)

        try:
            resp = await self._do_request('HEAD', '/%s%s' % (self.prefix, key))
            await self._assert_empty_response(resp)
        except HTTPError as exc:
            if exc.status == 404:
                raise NoSuchObject(key)
            else:
                raise

        try:
            return int(resp.headers['Content-Length'])
        except KeyError:
            raise RuntimeError('HEAD request did not return Content-Length')

    async def readinto_fh(self, key: str, fh: BinaryOutput) -> BasicMappingT:
        '''Transfer data stored under *key* into *fh*, return metadata.

        The data will be inserted at the current offset. If a temporary error (as defined by
        `is_temp_failure`) occurs, the operation is retried.
        '''

        return await self._readinto_fh(key, fh, fh.tell())

    @retry
    async def _readinto_fh(self, key: str, fh: BinaryOutput, off: int) -> BasicMappingT:
        try:
            resp = await self._do_request('GET', '/%s%s' % (self.prefix, key))
        except NoSuchKeyError:
            raise NoSuchObject(key)
        etag = resp.headers['ETag'].strip('"').lower()

        try:
            meta = self._extractmeta(resp, key)
        except (BadDigestError, CorruptedObjectError):
            # If there's less than 64 kb of data, read and throw
            # away. Otherwise re-establish connection.
            if resp.length is not None and resp.length < 64 * 1024:
                await self.conn.co_discard()
            else:
                self.conn.disconnect()
            raise

        md5 = hashlib.md5()
        fh.seek(off)
        await copy_from_http(self.conn, fh, update=md5.update)

        if etag != md5.hexdigest():
            log.warning('MD5 mismatch for %s: %s vs %s', key, etag, md5.hexdigest())
            raise BadDigestError('BadDigest', 'ETag header does not agree with calculated MD5')

        return meta

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
        headers = CaseInsensitiveDict()
        headers.update(self._extra_put_headers)
        self._add_meta_headers(headers, metadata)

        headers['Content-Type'] = 'application/octet-stream'
        fh.seek(off)
        try:
            resp = await self._do_request(
                'PUT',
                '/%s%s' % (self.prefix, key),
                headers=headers,
                body=fh,
                body_len=len_,
            )
        except BadDigestError:
            # Object was corrupted in transit, make sure to delete it
            await self.delete(key)
            raise

        await self._assert_empty_response(resp)
        return len_

    def _add_meta_headers(
        self, headers: CaseInsensitiveDict, metadata: BasicMappingT, chunksize: int = 255
    ) -> None:
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

    async def _do_request(
        self,
        method: str,
        path: str,
        subres: str | None = None,
        query_string: dict[str, str] | None = None,
        headers: CaseInsensitiveDict | None = None,
        body: bytes | bytearray | memoryview | BinaryInput | None = None,
        body_len: int | None = None,
    ) -> HTTPResponse:
        '''Sign and send request, handle redirects, and return response object.

        This is a wrapper around _do_request_inner() that handles Redirect responses.
        '''

        log.debug('started with %s %s?%s, qs=%s', method, path, subres, query_string)

        if headers is None:
            headers = CaseInsensitiveDict()

        if isinstance(body, (bytes, bytearray, memoryview)):
            headers['Content-MD5'] = md5sum_b64(body)

        redirect_count = 0
        this_method = method
        while True:
            resp = await self._do_request_inner(
                this_method,
                path,
                headers=headers,
                subres=subres,
                query_string=query_string,
                body=body,
                body_len=body_len,
            )

            if resp.status < 300 or resp.status > 399:
                break

            # Assume redirect
            redirect_count += 1
            if redirect_count > 10:
                raise RuntimeError('Too many chained redirections')

            # First try location header...
            new_url = resp.headers['Location']
            if new_url:
                # Discard body
                await self.conn.co_discard()

                # Pylint can't infer SplitResult Types
                # pylint: disable=E1103
                o = urlsplit(new_url)
                if o.scheme:
                    if self.ssl_context and o.scheme != 'https':
                        raise RuntimeError('Redirect to non-https URL')
                    elif not self.ssl_context and o.scheme != 'http':
                        raise RuntimeError('Redirect to non-http URL')
                if o.hostname is None:
                    raise RuntimeError('Redirect URL has no hostname')
                new_port = o.port if o.port is not None else self.port
                if o.hostname != self.hostname or new_port != self.port:
                    self.hostname = o.hostname
                    self.port = new_port
                    self.conn.disconnect()
                    self.conn = self._get_conn()
                else:
                    raise RuntimeError('Redirect to different path on same host')

            # ..but endpoint may also be hidden in message body.
            # If we have done a HEAD request, we have to change to GET
            # to actually retrieve the body.
            elif resp.method == 'HEAD':
                log.debug('Switching from HEAD to GET to read redirect body')
                this_method = 'GET'

            # Try to read new URL from request body
            else:
                tree = await self._parse_xml_response(resp)
                new_url = tree.findtext('Endpoint')

                if not new_url:
                    raise get_S3Error(tree.findtext('Code'), tree.findtext('Message'), resp.headers)

                self.hostname = new_url
                self.conn.disconnect()
                self.conn = self._get_conn()

                # Update method
                this_method = method

            log.info('_do_request(): redirected to %s', self.conn.hostname)

            if body and not isinstance(body, (bytes, bytearray, memoryview)):
                body.seek(0)

        # At the end, the request should have gone out with the right
        # method
        if this_method != method:
            raise RuntimeError('Dazed and confused - HEAD fails but GET works?')

        # Success
        if resp.status >= 200 and resp.status <= 299:
            return resp

        # Error
        await self._parse_error_response(resp)

    async def _parse_error_response(self, resp: HTTPResponse) -> NoReturn:
        '''Handle error response from server

        Try to raise most-specific exception.
        '''

        # Note that even though the final server backend may guarantee to always
        # deliver an XML document body with a detailed error message, we may
        # also get errors from intermediate proxies.
        content_type = resp.headers['Content-Type']

        # If method == HEAD, server must not return response body
        # even in case of errors
        if resp.method.upper() == 'HEAD':
            data = await self.conn.co_read(1)
            if data != b'':
                raise RuntimeError('Server sent response body for HEAD request')
            raise HTTPError(resp.status, resp.reason, resp.headers)

        # If not XML, do the best we can
        if content_type is None or resp.length == 0 or not XML_CONTENT_RE.match(content_type):
            await self.conn.co_discard()
            raise HTTPError(resp.status, resp.reason, resp.headers)

        # We don't stream the data into the parser because we want
        # to be able to dump a copy if the parsing fails.
        body = await self.conn.co_readall()
        try:
            tree = ElementTree.parse(BytesIO(body)).getroot()
        except:
            log.error(
                'Unable to parse server response as XML:\n%s', await self._dump_response(resp, body)
            )
            raise

        if tree is None:
            raise RuntimeError('XML document has no root element')
        raise get_S3Error(tree.findtext('Code'), tree.findtext('Message'), resp.headers)

    async def _parse_xml_response(self, resp: HTTPResponse, body: bytes | None = None) -> Element:
        '''Return element tree for XML response'''

        content_type = resp.headers['Content-Type']

        # AWS S3 sometimes "forgets" to send a Content-Type
        # when responding to a multiple delete request.
        # https://forums.aws.amazon.com/thread.jspa?threadID=134372
        if content_type is None:
            log.error('Server did not provide Content-Type, assuming XML')
        elif not XML_CONTENT_RE.match(content_type):
            log.error(
                'Unexpected server reply: expected XML, got:\n%s', await self._dump_response(resp)
            )
            raise RuntimeError('Unexpected server response')

        # We don't stream the data into the parser because we want
        # to be able to dump a copy if the parsing fails.
        if body is None:
            body = await self.conn.co_readall()
        try:
            tree = ElementTree.parse(BytesIO(body)).getroot()
        except:
            log.error(
                'Unable to parse server response as XML:\n%s', await self._dump_response(resp, body)
            )
            raise

        if tree is None:
            raise RuntimeError('XML document has no root element')
        return tree

    def __str__(self) -> str:
        return 's3c://%s/%s/%s' % (self.hostname, self.bucket_name, self.prefix)

    def _authorize_request(
        self,
        method: str,
        path: str,
        headers: CaseInsensitiveDict,
        subres: str | None,
        query_string: dict[str, str] | None,
    ) -> None:
        '''Add authorization information to *headers*'''

        # See http://docs.amazonwebservices.com/AmazonS3/latest/dev/RESTAuthentication.html

        # Date, can't use strftime because it's locale dependent
        now = time.gmtime()
        headers['Date'] = '%s, %02d %s %04d %02d:%02d:%02d GMT' % (
            C_DAY_NAMES[now.tm_wday],
            now.tm_mday,
            C_MONTH_NAMES[now.tm_mon - 1],
            now.tm_year,
            now.tm_hour,
            now.tm_min,
            now.tm_sec,
        )

        auth_strs = [method, '\n']

        for hdr in ('Content-MD5', 'Content-Type', 'Date'):
            if hdr in headers:
                auth_strs.append(headers[hdr])
            auth_strs.append('\n')

        for hdr in sorted(x for x in headers if x.lower().startswith('x-amz-')):
            val = ' '.join(re.split(r'\s*\n\s*', headers[hdr].strip()))
            auth_strs.append('%s:%s\n' % (hdr, val))

        # Always include bucket name in path for signing
        if self.hostname.startswith(self.bucket_name):
            path = '/%s%s' % (self.bucket_name, path)
        sign_path = urllib.parse.quote(path)
        auth_strs.append(sign_path)
        if subres:
            auth_strs.append('?%s' % subres)

        # False positive, hashlib *does* have sha1 member
        # pylint: disable=E1101
        auth_str = ''.join(auth_strs).encode()
        signature = b64encode(
            hmac.new(self.password.encode(), auth_str, hashlib.sha1).digest()
        ).decode()

        headers['Authorization'] = 'AWS %s:%s' % (self.login, signature)

    async def _do_request_inner(
        self,
        method: str,
        path: str,
        headers: CaseInsensitiveDict,
        subres: str | None = None,
        query_string: dict[str, str] | None = None,
        body: bytes | bytearray | memoryview | BinaryInput | None = None,
        body_len: int | None = None,
    ) -> HTTPResponse:
        '''Sign and send request, return response object (including redirects)

        This method does not handle redirects.
        '''

        if not isinstance(headers, CaseInsensitiveDict):
            headers = CaseInsensitiveDict(headers)

        if not self.hostname.startswith(self.bucket_name):
            path = '/%s%s' % (self.bucket_name, path)
        headers['host'] = self.hostname

        self._authorize_request(method, path, headers, subres, query_string)

        path = urllib.parse.quote(path)
        if query_string:
            s = urllib.parse.urlencode(query_string, doseq=True)
            if subres:
                path += '?%s&%s' % (subres, s)
            else:
                path += '?%s' % s
        elif subres:
            path += '?%s' % subres

        use_expect_100c = not self.options.get('disable-expect100', False)
        log.debug('sending %s %s', method, path)
        if body is None or isinstance(body, (bytes, bytearray, memoryview)):
            await self.conn.co_send_request(method, path, body=body, headers=headers)
            return await self.conn.co_read_response()

        assert isinstance(body_len, int)
        await self.conn.co_send_request(
            method,
            path,
            expect100=use_expect_100c,
            headers=headers,
            body=BodyFollowing(body_len),
        )

        if use_expect_100c:
            resp = await self.conn.co_read_response()
            if resp.status != 100:  # Error
                return resp

        if self.ssl_context:
            # No need to check MD5 when using SSL
            md5_update = None
        else:
            md5 = hashlib.md5()
            md5_update = md5.update

        try:
            await copy_to_http(body, self.conn, body_len, update=md5_update)
        except ConnectionClosed:
            # Server closed connection while we were writing body data -
            # but we may still be able to read an error response
            error_resp: HTTPResponse | None = None
            with contextlib.suppress(builtins.BaseException):
                error_resp = await self.conn.co_read_response()
            if error_resp is not None:
                await self._parse_error_response(error_resp)
            else:
                # Re-raise first ConnectionClosed exception
                raise

        resp = await self.conn.co_read_response()

        # On success, check MD5. Not sure if this is returned every time we send a request body, but
        # it seems to work. If not, we have to somehow pass in the information when this is expected
        # (i.e, when storing an object)
        if resp.status >= 200 and resp.status <= 299 and md5_update is not None:
            etag = resp.headers['ETag'].strip('"').lower()

            if etag != md5.hexdigest():
                raise BadDigestError(
                    'BadDigest',
                    f'MD5 mismatch when sending {method} {path} (received: {etag}, sent: {md5.hexdigest()})',  # noqa: E501 # auto-added, needs manual check!
                )

        return resp

    async def close(self) -> None:
        if self.conn is not None:
            self.conn.disconnect()

    def _extractmeta(self, resp: HTTPResponse, obj_key: str) -> BasicMappingT:
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


def _tag_xmlns_uri(elem: Element) -> str | None:
    '''Extract the XML namespace (xmlns) URI from an element'''
    if elem.tag[0] == '{':
        uri = elem.tag[1:].partition("}")[0]
    else:
        uri = None
    return uri


def get_S3Error(
    code: str | None, msg: str | None, headers: email.message.Message | None = None
) -> S3Error:
    '''Instantiate most specific S3Error subclass'''

    # Special case
    # http://code.google.com/p/s3ql/issues/detail?id=369
    if code == 'Timeout':
        code = 'RequestTimeout'

    if code is None:
        return S3Error(code, msg, headers)

    if code.endswith('Error'):
        name = code
    else:
        name = code + 'Error'
    class_ = globals().get(name, S3Error)

    if not issubclass(class_, S3Error):
        return S3Error(code, msg, headers)

    return class_(code, msg, headers)


def md5sum_b64(buf: bytes | bytearray | memoryview) -> str:
    '''Return base64 encoded MD5 sum'''

    return b64encode(hashlib.md5(buf).digest()).decode('ascii')


def _parse_retry_after(header: str) -> float | None:
    '''Parse headers for Retry-After value'''

    hit = re.match(r'^\s*([0-9]+)\s*$', header)
    val: float
    if hit:
        val = int(header)
    else:
        date = parsedate_tz(header)
        if date is None:
            log.warning('Unable to parse retry-after value: %s', header)
            return None
        val = mktime_tz(date) - time.time()

    val_clamp = min(300.0, max(1.0, val))
    if val != val_clamp:
        log.warning('Ignoring retry-after value of %.3f s, using %.3f s instead', val, val_clamp)
        val = val_clamp

    return val


class HTTPError(Exception):
    '''
    Represents an HTTP error returned by S3.
    '''

    status: int
    msg: str
    headers: email.message.Message | None
    retry_after: float | None

    def __init__(self, status: int, msg: str, headers: email.message.Message | None = None) -> None:
        super().__init__()
        self.status = status
        self.msg = msg
        self.headers = headers

        if headers and 'Retry-After' in headers:
            self.retry_after = _parse_retry_after(headers['Retry-After'])
        else:
            self.retry_after = None

    def __str__(self) -> str:
        return '%d %s' % (self.status, self.msg)


class S3Error(Exception):
    '''
    Represents an error returned by S3. For possible codes, see
    http://docs.amazonwebservices.com/AmazonS3/latest/API/ErrorResponses.html
    '''

    code: str | None
    msg: str | None
    retry_after: float | None

    def __init__(
        self,
        code: str | None,
        msg: str | None,
        headers: email.message.Message | None = None,
    ) -> None:
        super().__init__(msg)
        self.code = code
        self.msg = msg

        if headers and 'Retry-After' in headers:
            self.retry_after = _parse_retry_after(headers['Retry-After'])
        else:
            self.retry_after = None

    def __str__(self) -> str:
        return '%s: %s' % (self.code, self.msg)


class NoSuchKeyError(S3Error):
    pass


class AccessDeniedError(S3Error, AuthorizationError):  # type: ignore[misc]
    pass


class BadDigestError(S3Error):
    pass


class IncompleteBodyError(S3Error):
    pass


class InternalError(S3Error):
    pass


class InvalidAccessKeyIdError(S3Error, AuthenticationError):  # type: ignore[misc]
    pass


class InvalidSecurityError(S3Error, AuthenticationError):  # type: ignore[misc]
    pass


class SignatureDoesNotMatchError(S3Error, AuthenticationError):  # type: ignore[misc]
    pass


class OperationAbortedError(S3Error):
    pass


class RequestTimeoutError(S3Error):
    pass


class SlowDownError(S3Error):
    pass


class ServiceUnavailableError(S3Error):
    pass


class TemporarilyUnavailableError(S3Error):
    pass


class RequestTimeTooSkewedError(S3Error):
    pass


class NoSuchBucketError(S3Error, DanglingStorageURLError):
    pass
