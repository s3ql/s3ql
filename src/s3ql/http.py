'''
http.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.

The CaseInsensitiveDict implementation is copyright 2013 Kenneth Reitz and
licensed under the Apache License, Version 2.0
(http://www.apache.org/licenses/LICENSE-2.0)
'''

from __future__ import annotations

import contextlib
import email
import email.policy
import errno
import logging
import os
import socket
import ssl
from collections.abc import AsyncIterator, Callable, Mapping, MutableMapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from http.client import HTTP_PORT, HTTPS_PORT
from io import BytesIO, UnsupportedOperation
from typing import Optional

import h11
import httpcore
import trio
import trio.lowlevel
from httpcore._backends.base import AsyncNetworkStream
from httpcore._backends.trio import TrioBackend

from s3ql import BUFSIZE
from s3ql.types import BinaryInput, BinaryOutput

log = logging.getLogger(__name__)

#: Sequence of ``(hostname, port)`` tuples used to distinguish between permanent
#: and temporary name resolution problems. If at least one of these resolves,
#: an unresolvable target is reported as `HostnameNotResolvable`; otherwise as
#: `DNSUnavailable`.
DNS_TEST_HOSTNAMES = (('www.google.com', 80), ('www.iana.org', 80), ('C.root-servers.org', 53))

_AMBIGUOUS_DNS_ERRNOS = (socket.EAI_NODATA, socket.EAI_NONAME, socket.EAI_AGAIN)

#: Errno codes that indicate a potentially temporary network problem.
_TEMP_NETWORK_ERRNOS = frozenset(
    code
    for code in (
        getattr(errno, name, None)
        for name in (
            'EHOSTDOWN',
            'EHOSTUNREACH',
            'ENETDOWN',
            'ENETRESET',
            'ENETUNREACH',
            'ENOLINK',
            'ENONET',
            'ENOTCONN',
            'ENXIO',
            'EPIPE',
            'EREMCHG',
            'ESHUTDOWN',
            'ETIMEDOUT',
            'EAGAIN',
        )
    )
    if code is not None
)

#: Default per-operation I/O timeout, in seconds. Applied to each individual
#: network operation (`connect_tcp`, TLS handshake, single `read`, single
#: `write`), *not* to a full request/response cycle. Callers that need a
#: deadline on the whole operation must impose it themselves with
#: `trio.move_on_after`.
_DEFAULT_TIMEOUT_S: float = 30

#: How many bytes to pull from the network in one go when assembling h11 events.
_READ_CHUNK = 64 * 1024


#: Type alias for the *body* parameter of `HTTPConnection.send_request`.
#:
#: A request body is always a sequence so that retries (e.g. on redirect)
#: can re-iterate it. Items may be raw `bytes` chunks (sent verbatim) or
#: seekable `BinaryInput` instances (read from offset 0 until EOF).
BodyT = Sequence[bytes | BinaryInput]


def fh_size(fh: BinaryInput) -> int:
    '''Return the size of *fh* in bytes.

    The file position after this call is undefined. Callers that need *fh*
    positioned somewhere must seek explicitly.
    '''

    if isinstance(fh, BytesIO):
        return fh.getbuffer().nbytes

    fileno = getattr(fh, 'fileno', None)
    if fileno is not None:
        try:
            fd = fileno()
        except (OSError, UnsupportedOperation):
            pass
        else:
            return os.fstat(fd).st_size

    fh.seek(0, 2)
    return fh.tell()


@dataclass(slots=True)
class HTTPResponse:
    '''
    Information about an HTTP response.

    Instances of this class are returned by `HTTPConnection.send_request`
    and expose the response status, reason, and headers. Response body
    data has to be read directly from the `HTTPConnection` instance.
    '''

    #: HTTP method of the request this response is associated with.
    method: str

    #: Path of the request this response is associated with.
    path: str

    #: HTTP status code returned by the server.
    status: int

    #: HTTP reason phrase returned by the server.
    reason: str

    #: Response headers, an `email.message.Message` instance.
    headers: email.message.Message

    #: Length of the *transmitted* response body, or `None` if not known.
    #: For responses where RFC 2616 mandates an empty body (HEAD requests,
    #: 1xx, 204, 304) this is zero; the length of the body that *would*
    #: have been sent can be read from the ``Content-Length`` header.
    length: int | None = None


class HostnameNotResolvable(Exception):
    '''Raised if a host name does not resolve to an IP address.

    Raised when a resolution attempt results in a `socket.gaierror` or
    `socket.herror` exception with errno `socket.EAI_NODATA` or
    `socket.EAI_NONAME`, but at least one of the hostnames in
    `DNS_TEST_HOSTNAMES` can be resolved.
    '''

    def __init__(self, hostname: str) -> None:
        self.name = hostname

    def __str__(self) -> str:
        return 'Host %s does not have any ip addresses' % self.name


class DNSUnavailable(Exception):
    '''Raised if the DNS server cannot be reached.

    Raised when a resolution attempt results in a `socket.gaierror` or
    `socket.herror` exception with errno `socket.EAI_AGAIN`, or if none of the
    hostnames in `DNS_TEST_HOSTNAMES` can be resolved.
    '''

    def __init__(self, hostname: str) -> None:
        self.name = hostname

    def __str__(self) -> str:
        return 'Unable to resolve %s, DNS server unavailable.' % self.name


class _GeneralError(Exception):
    msg = 'General HTTP Error'

    def __init__(self, msg: str | None = None) -> None:
        if msg:
            self.msg = msg

    def __str__(self) -> str:
        return self.msg


class StateError(_GeneralError):
    '''Raised when an operation is invalid in the current connection state.'''

    msg = 'Operation invalid in current connection state'


class ExcessBodyData(_GeneralError):
    '''Raised when more data is being written to the request body than announced.'''

    msg = 'Cannot send larger request body than announced'


class InvalidResponse(_GeneralError):
    '''Raised if the server produced a response that is not valid HTTP.'''

    msg = 'Server sent invalid response'


class ConnectionClosed(_GeneralError):
    '''
    Raised when the connection has unexpectedly closed.

    Also raised if the server signals it will close the connection (via a
    ``Connection: close`` header). Such a response can still be read in full,
    but the next attempt to send a request or read a response will raise the
    exception. Call `HTTPConnection.aclose`; the next request will
    automatically reconnect.
    '''

    msg = 'connection closed unexpectedly'


class ConnectionTimedOut(_GeneralError):
    '''Raised when an I/O operation does not complete within the configured timeout.'''

    msg = 'send/recv timeout exceeded'


class HTTPConnection:
    '''
    Encapsulates an HTTP/1.1 connection to a remote server.

    Instances may be used as async context managers; `aclose` is called on exit.
    '''

    def __init__(
        self,
        hostname: str,
        port: Optional[int] = None,
        ssl_context: Optional[ssl.SSLContext] = None,
        proxy: Optional[tuple[str, int]] = None,
    ):
        self.hostname = hostname
        self.port = port if port is not None else (HTTPS_PORT if ssl_context else HTTP_PORT)
        self.ssl_context = ssl_context
        self.proxy = proxy

        self._stream: AsyncNetworkStream | None = None
        self._h11: h11.Connection | None = None
        self._timeout: float = _DEFAULT_TIMEOUT_S

        #: Method of the request whose response has not yet been completely
        #: consumed; `None` otherwise. Used both to populate
        #: `HTTPResponse.method`/`path` and to enforce send/read sequencing.
        self._pending_method: str | None = None
        self._pending_path: str | None = None

        #: True when a response has been received but its body has not yet
        #: been completely consumed.
        self._response_open: bool = False

    @property
    def timeout(self) -> float:
        return self._timeout

    @timeout.setter
    def timeout(self, value: int | float | None) -> None:
        self._timeout = _DEFAULT_TIMEOUT_S if value is None else float(value)

    async def connect(self) -> None:
        '''Open the underlying TCP connection. Usually called automatically.'''

        log.debug('start')
        assert self._stream is None
        backend = TrioBackend()

        try:
            if self.proxy:
                log.debug('connecting to proxy %s', self.proxy)
                self._stream = await backend.connect_tcp(
                    host=self.proxy[0], port=self.proxy[1], timeout=self._timeout
                )
                if self.ssl_context:
                    await self._tunnel()
            else:
                log.debug('connecting to %s:%d', self.hostname, self.port)
                self._stream = await backend.connect_tcp(
                    host=self.hostname, port=self.port, timeout=self._timeout
                )
        except httpcore.ConnectTimeout as exc:
            raise ConnectionTimedOut(str(exc))
        except httpcore.ConnectError as exc:
            self._raise_dns_disambiguated(exc)
            raise

        if self.ssl_context and not self.proxy:
            log.debug('starting TLS for %s', self.hostname)
            assert self._stream is not None
            try:
                self._stream = await self._stream.start_tls(
                    ssl_context=self.ssl_context,
                    server_hostname=self.hostname,
                    timeout=self._timeout,
                )
            except httpcore.ConnectTimeout as exc:
                raise ConnectionTimedOut(str(exc))
            except httpcore.ConnectError as exc:
                _reraise_ssl_or_connection_closed(exc)

        self._h11 = h11.Connection(our_role=h11.CLIENT)
        self._pending_method = None
        self._pending_path = None
        self._response_open = False

        log.debug('done')

    def _raise_dns_disambiguated(self, exc: httpcore.ConnectError) -> None:
        '''Translate a httpcore ConnectError with ambiguous DNS semantics.

        For DNS-related errors that don't distinguish NXDOMAIN from "DNS
        server unreachable", probe `DNS_TEST_HOSTNAMES` and raise either
        `HostnameNotResolvable` or `DNSUnavailable`. For any other
        `ConnectError`, return without raising so the caller can let the
        original exception propagate.
        '''
        cause = exc.__cause__ or (exc.args[0] if exc.args else None)
        if isinstance(cause, (socket.gaierror, socket.herror)) and (
            cause.errno in _AMBIGUOUS_DNS_ERRNOS
        ):
            target = self.proxy[0] if self.proxy else self.hostname
            for hostname, port in DNS_TEST_HOSTNAMES:
                try:
                    socket.getaddrinfo(hostname, port)
                except (socket.gaierror, socket.herror) as probe_exc:
                    if probe_exc.errno in _AMBIGUOUS_DNS_ERRNOS:
                        continue
                    raise
                raise HostnameNotResolvable(target)
            raise DNSUnavailable(target)

    async def _tunnel(self) -> None:
        '''Establish a CONNECT tunnel through an HTTP proxy for HTTPS traffic.'''

        assert self._stream is not None
        log.debug('CONNECT %s:%d', self.hostname, self.port)
        request = ('CONNECT %s:%d HTTP/1.0\r\n\r\n' % (self.hostname, self.port)).encode('latin1')
        await self._raw_write(request)

        # Read until \r\n\r\n; CONNECT responses are short and headers-only.
        buf = bytearray()
        while b'\r\n\r\n' not in buf:
            chunk = await self._raw_read(_READ_CHUNK)
            if not chunk:
                raise ConnectionClosed('proxy closed connection during CONNECT')
            buf.extend(chunk)
            if len(buf) > 64 * 1024:
                raise InvalidResponse('CONNECT response exceeds 64 KiB')

        status_line = bytes(buf).split(b'\r\n', 1)[0].decode('latin1', errors='replace')
        parts = status_line.split(None, 2)
        if len(parts) < 2 or not parts[1].isdigit():
            raise InvalidResponse('CONNECT response invalid: %r' % status_line)
        status = int(parts[1])
        if status != 200:
            await self.aclose()
            raise ConnectionError('Tunnel connection failed: %s' % status_line)

        # Wrap the proxy stream in TLS to talk to the destination server.
        assert self.ssl_context is not None
        try:
            self._stream = await self._stream.start_tls(
                ssl_context=self.ssl_context,
                server_hostname=self.hostname,
                timeout=self._timeout,
            )
        except httpcore.ConnectTimeout as exc:
            raise ConnectionTimedOut(str(exc))
        except httpcore.ConnectError as exc:
            _reraise_ssl_or_connection_closed(exc)

    async def _raw_write(self, data: bytes) -> None:
        assert self._stream is not None
        try:
            await self._stream.write(data, timeout=self._timeout)
        except httpcore.WriteTimeout as exc:
            raise ConnectionTimedOut(str(exc))
        except httpcore.WriteError as exc:
            raise ConnectionClosed(str(exc))

    async def _raw_read(self, size: int) -> bytes:
        assert self._stream is not None
        try:
            return await self._stream.read(size, timeout=self._timeout)
        except httpcore.ReadTimeout as exc:
            raise ConnectionTimedOut(str(exc))
        except httpcore.ReadError as exc:
            raise ConnectionClosed(str(exc))

    async def send_request(
        self,
        method: str,
        path: str,
        headers: Optional[Mapping[str, str]] = None,
        body: BodyT = (),
        body_len: int | None = None,
        expect100: bool | None = None,
    ) -> HTTPResponse:
        '''Send an HTTP request and return the response.

        *body* is a sequence of `bytes` chunks and/or seekable binary
        file objects. An empty sequence (the default) sends no body.
        File objects are seek'ed to position 0 before being read.

        If *body_len* is given, it is used as the request's
        ``Content-Length`` and takes precedence over a size derived from
        the body items. Otherwise the length is computed by summing
        `len()` of `bytes` items and `fh_size()` of file items, so every
        file item must be seekable. Pass *body_len* explicitly when a stream
        is not supported by `fh_size()`.

        *expect100* controls the use of ``Expect: 100-continue``. When
        unset (`None`), it defaults to `True` when the body contains
        at least one stream and `False` otherwise. With
        100-continue enabled the request headers are sent first; if the
        server replies with a non-100 final response the body is
        abandoned and that response is returned.

        Returns an `HTTPResponse` describing the server's final response.
        The response body must be read via `read`, `readall`, or
        `discard` before the next request can be issued.
        '''

        log.debug('start (%s %s)', method, path)

        # A body item that is not `bytes` must be a seekable file-like
        # object. Treat a body that contains at least one such item as
        # "streaming" — that decides the default for `Expect: 100-continue`
        # and whether file positions need to be reset before sending.
        has_fh = any(not isinstance(item, bytes) for item in body)

        if body_len is None:
            content_length = sum(
                len(item) if isinstance(item, bytes) else fh_size(item) for item in body
            )
        else:
            content_length = body_len

        if expect100 is None:
            expect100 = has_fh
        if expect100 and not body:
            raise ValueError('expect100 requires a body')

        if self._stream is None:
            await self.connect()
        assert self._h11 is not None

        if (
            self._h11.our_state is h11.MUST_CLOSE
            or self._h11.their_state is h11.MUST_CLOSE
            or self._h11.our_state is h11.CLOSED
            or self._h11.their_state is h11.CLOSED
        ):
            raise ConnectionClosed('connection was marked must-close')
        if self._response_open or self._pending_method is not None:
            raise StateError('previous response has not been read')
        if self._h11.our_state is not h11.IDLE:
            raise StateError('previous request not finished sending')

        if headers is None:
            req_headers = CaseInsensitiveDict()
        elif isinstance(headers, CaseInsensitiveDict):
            req_headers = headers
        else:
            req_headers = CaseInsensitiveDict(headers)

        req_headers['Content-Length'] = str(content_length)
        if expect100:
            req_headers['Expect'] = '100-continue'

        # Host header per RFC 7230 §5.4.
        host = self.hostname
        if host.find(':') >= 0:
            host = f'[{host}]'
        default_port = HTTPS_PORT if self.ssl_context else HTTP_PORT
        req_headers['Host'] = host if self.port == default_port else f'{host}:{self.port}'

        req_headers.setdefault('Accept-Encoding', 'identity')
        req_headers.setdefault('Connection', 'keep-alive')

        if self.proxy and not self.ssl_context:
            target = 'http://{}{}'.format(req_headers['Host'], path).encode('latin1')
        else:
            target = path.encode('latin1')

        h11_headers = [
            (k.encode('latin1'), str(v).encode('latin1')) for k, v in req_headers.items()
        ]
        try:
            await self._send_event(
                h11.Request(method=method.encode('latin1'), target=target, headers=h11_headers)
            )
        except h11.LocalProtocolError as exc:
            raise StateError(str(exc))

        self._pending_method = method
        self._pending_path = path

        if not body:
            await self._send_event(h11.EndOfMessage())
            return await self._read_response()

        if expect100:
            resp = await self._read_response()
            if resp.status != 100:
                # Server rejected the body before it was sent. Once the
                # caller has consumed the response body, `_finish_cycle`
                # will reset h11 because our_state is stuck in SEND_BODY.
                return resp

        try:
            await self._stream_body(body, content_length)
            await self._send_event(h11.EndOfMessage())
        except ConnectionClosed:
            # Server closed connection while body was being sent. The
            # error response, if any, may already be sitting in the
            # receive buffer; try to read it. Otherwise drop the
            # half-broken connection and re-raise.
            try:
                return await self._read_response()
            except Exception:
                log.debug('no usable response after mid-body close')
                await self.aclose()
                raise

        return await self._read_response()

    async def _stream_body(self, body: BodyT, total: int) -> None:
        '''Stream *body* items as h11 Data events, sending exactly *total* bytes.

        `bytes` items are emitted verbatim. File items are seek'ed to 0
        and read until EOF or until *total* has been sent.
        '''

        remaining = total
        for item in body:
            if remaining <= 0:
                break
            if isinstance(item, bytes):
                if len(item) > remaining:
                    raise ExcessBodyData(
                        'streaming body produced %d extra bytes' % (len(item) - remaining)
                    )
                if item:
                    await self._send_event(h11.Data(data=item))
                remaining -= len(item)
            else:
                remaining = await self._stream_file(item, remaining)
        if remaining > 0:
            raise StateError('streaming body produced %d bytes less than announced' % remaining)

    async def _stream_file(self, fh: BinaryInput, remaining: int) -> int:
        '''Read up to *remaining* bytes from *fh* (from position 0) as h11 Data events.

        Reads happen off-thread for non-`BytesIO` files. Returns the new
        *remaining* counter. The file is read until EOF or until
        *remaining* bytes have been sent, whichever comes first.
        '''

        fh.seek(0)
        sync = isinstance(fh, BytesIO)
        while remaining > 0:
            n = min(BUFSIZE, remaining)
            if sync:
                chunk = fh.read(n)
            else:
                # Wrapping the file object with `trio.wrap_file()` would be cleaner, but adds extra
                # overhead and undefined behavior (will the sync object be closed when the async
                # wrapper is garbage collected?), and just calls trio.to_thread.run_sync()
                # internally anyway.
                chunk = await trio.to_thread.run_sync(fh.read, n)
            if not chunk:
                return remaining
            await self._send_event(h11.Data(data=bytes(chunk)))
            remaining -= len(chunk)
        return remaining

    async def _send_event(self, event: h11.Event) -> None:
        '''Serialise *event* through h11 and write the produced bytes.'''
        assert self._h11 is not None
        try:
            data = self._h11.send(event)
        except h11.LocalProtocolError as exc:
            raise StateError(str(exc))
        if data is not None:
            await self._raw_write(data)

    async def _read_response(self) -> HTTPResponse:
        '''Read the response status line and headers and return an `HTTPResponse`.'''

        log.debug('start')

        assert self._h11 is not None
        assert self._pending_method is not None
        method = self._pending_method
        path = self._pending_path
        assert path is not None

        while True:
            try:
                event = await self._next_event()
            except h11.RemoteProtocolError as exc:
                raise InvalidResponse(str(exc))

            if isinstance(event, h11.InformationalResponse):
                if event.status_code == 100:
                    return HTTPResponse(
                        method=method,
                        path=path,
                        status=100,
                        reason=event.reason.decode('latin1'),
                        headers=email.message.Message(policy=email.policy.compat32),
                        length=0,
                    )
                # Other 1xx (incl. 101 Upgrade) - skip per RFC 7231 §6.2.
                log.debug('skipping 1xx response %d', event.status_code)
                continue

            if isinstance(event, h11.Response):
                return self._make_response(
                    method, path, event.status_code, event.reason.decode('latin1'), event.headers
                )

            raise InvalidResponse('unexpected h11 event during headers: %r' % type(event))

    def _make_response(
        self,
        method: str,
        path: str,
        status: int,
        reason: str,
        h11_headers: object,
    ) -> HTTPResponse:
        '''Build an `HTTPResponse` and prime body-reading state.'''

        assert self._h11 is not None
        # `compat32` (rather than `email.policy.HTTP`) is used here because the HTTP
        # policy enforces `header_max_count == 1` for `Date`/`Expires`/`Last-Modified`
        # and rejects duplicate copies that some real servers (notably misbehaving
        # Swift proxies) do emit. We only use `Message` as a case-insensitive dict.
        msg = email.message.Message(policy=email.policy.compat32)
        for k, v in h11_headers:  # type: ignore[attr-defined]
            msg[k.decode('latin1')] = v.decode('latin1')

        body_length: int | None
        if status in (204, 304) or 100 <= status < 200 or method == 'HEAD':
            body_length = 0
        else:
            body_length_str = msg.get('Content-Length')
            if body_length_str is None:
                body_length = None
            else:
                try:
                    body_length = int(body_length_str)
                except ValueError:
                    raise InvalidResponse('Invalid content-length: %s' % body_length_str)

        # If the body is known to be empty (HEAD, 204, 304, 1xx), drain the
        # synthetic EndOfMessage that h11 emits so the cycle ends cleanly
        # without forcing the caller to call read().
        if body_length == 0:
            with contextlib.suppress(h11.RemoteProtocolError):
                next_event = self._h11.next_event()
                if isinstance(next_event, h11.EndOfMessage) or next_event is h11.PAUSED:
                    self._finish_cycle()
                    return HTTPResponse(
                        method=method,
                        path=path,
                        status=status,
                        reason=reason,
                        headers=msg,
                        length=body_length,
                    )
        self._response_open = True

        return HTTPResponse(
            method=method,
            path=path,
            status=status,
            reason=reason,
            headers=msg,
            length=body_length,
        )

    async def read(self) -> bytes | bytearray:
        '''Read the next chunk of response body data.

        Returns whatever the next ``h11.Data`` event yields (at most
        ``_READ_CHUNK`` bytes), or ``b''`` at end-of-body. The returned
        buffer is owned by the caller and may be modified in place.
        '''
        if self._stream is None:
            raise ConnectionClosed('connection has been closed locally')
        if not self._response_open:
            return b''

        while True:
            try:
                event = await self._next_event()
            except h11.RemoteProtocolError as exc:
                raise InvalidResponse(str(exc))

            if isinstance(event, h11.Data):
                if not event.data:
                    continue
                return bytearray(event.data)

            if isinstance(event, h11.EndOfMessage) or event is h11.PAUSED:
                self._finish_cycle()
                return b''

            raise InvalidResponse('unexpected h11 event during body: %r' % type(event))

    async def read_raw(self) -> bytes:
        '''Read uninterpreted data from the wire after a protocol error.

        Returns whatever bytes h11 had already buffered (its
        ``trailing_data``) prepended to a single socket read of up to
        ``_READ_CHUNK`` bytes, or ``b''`` if the connection has been
        closed and nothing remains. Intended for a single diagnostic
        dump after `InvalidResponse`; after this call the connection's
        framing state is no longer trustworthy, so call `aclose` before
        sending further requests.
        '''

        if self._h11 is None:
            trailing = b''
        else:
            trailing = self._h11.trailing_data[0]

        if self._stream is None:
            return trailing

        return trailing + (await self._raw_read(_READ_CHUNK))

    async def readall(self) -> bytes:
        '''Read and return the complete response body.'''
        if not self._response_open:
            return b''
        parts: list[bytes | bytearray] = []
        while True:
            buf = await self.read()
            if not buf:
                break
            parts.append(buf)
        return b''.join(parts)

    async def discard(self) -> None:
        '''Read and discard the entire response body.'''
        while await self.read():
            pass

    async def readinto_fh(
        self,
        ofh: BinaryOutput,
        update: Callable[[bytes | bytearray], None] | None = None,
    ) -> None:
        '''Copy the remaining response body into *ofh*.

        Writes happen off-thread for non-`BytesIO` sinks. If *update* is
        given, call it with each chunk after it has been written.
        '''
        use_async = not isinstance(ofh, BytesIO)
        while True:
            buf = await self.read()
            if not buf:
                return
            if use_async:
                await trio.to_thread.run_sync(ofh.write, buf)
            else:
                ofh.write(buf)
            if update is not None:
                update(buf)

    async def _next_event(self) -> h11.Event | type[h11.NEED_DATA] | type[h11.PAUSED]:
        '''Pull the next h11 event'''
        assert self._h11 is not None
        eof_seen = False
        while True:
            try:
                event = self._h11.next_event()
            except h11.RemoteProtocolError:
                if eof_seen:
                    # Peer closed before completing the message — surface as
                    # ConnectionClosed rather than h11's protocol error.
                    raise ConnectionClosed('server closed connection mid-response')
                raise
            if event is h11.NEED_DATA:
                if eof_seen:
                    raise ConnectionClosed('server closed connection mid-response')
                data = await self._raw_read(_READ_CHUNK)
                if not data:
                    eof_seen = True
                    self._h11.receive_data(b'')
                else:
                    self._h11.receive_data(data)
                continue
            return event

    def _finish_cycle(self) -> None:
        '''Reset per-cycle state and try to advance h11 to IDLE for keep-alive.'''
        self._response_open = False
        self._pending_method = None
        self._pending_path = None

        assert self._h11 is not None
        if self._h11.our_state is h11.SEND_BODY:
            # 100-continue rejection left our_state stuck in SEND_BODY; the
            # wire is still keep-alive-clean since neither side sent a body.
            self._h11 = h11.Connection(our_role=h11.CLIENT)
            return
        if self._h11.our_state is h11.DONE and self._h11.their_state is h11.DONE:
            self._h11.start_next_cycle()
        elif (
            self._h11.our_state is h11.MUST_CLOSE
            or self._h11.their_state is h11.MUST_CLOSE
            or self._h11.our_state is h11.CLOSED
            or self._h11.their_state is h11.CLOSED
        ):
            # Server signalled Connection: close; let the next send_request
            # surface ConnectionClosed via its state check.
            pass
        else:
            # Reaching this branch means h11 ended a cycle in a state that is
            # neither IDLE-ready, must-close, nor closed. That violates the
            # invariants of `send_request`/`_read_response` (or the server
            # produced something we should have rejected earlier) and is a
            # bug rather than something to paper over.
            raise StateError(
                'unexpected h11 state at end of cycle: our_state=%r, their_state=%r'
                % (self._h11.our_state, self._h11.their_state)
            )

    async def aclose(self) -> None:
        '''Close the underlying TCP connection'''

        log.debug('start')
        stream = self._stream
        if stream is not None:
            with contextlib.suppress(Exception):
                await stream.aclose()
            self._stream = None

    def reset(self) -> None:
        '''Invalidate active TCP connection and reset protocol state.

        Schedules an asynchronous close of any attached stream before
        clearing the reference, so the underlying socket is released
        even when this is called from a synchronous context.
        '''

        log.debug('start')
        if self._stream is not None:
            trio.lowlevel.spawn_system_task(_aclose_stream, self._stream)
            self._stream = None

    @property
    def is_idle(self) -> bool:
        '''True if the connection is connected and in a keep-alive-clean state.'''
        return (
            self._stream is not None
            and self._h11 is not None
            and self._h11.our_state is h11.IDLE
            and self._h11.their_state is h11.IDLE
        )

    async def __aenter__(self) -> HTTPConnection:
        return self

    async def __aexit__(self, exc_type: object, exc_value: object, traceback: object) -> bool:
        await self.aclose()
        return False


async def _aclose_stream(stream: AsyncNetworkStream) -> None:
    with contextlib.suppress(Exception):
        await stream.aclose()


def _reraise_ssl_or_connection_closed(exc: BaseException) -> None:
    '''Walk *exc*'s cause chain; re-raise an SSL error if found, else ConnectionClosed.'''
    cur: BaseException | None = exc
    seen: set[int] = set()
    while cur is not None and id(cur) not in seen:
        if isinstance(cur, ssl.SSLError):
            raise cur
        seen.add(id(cur))
        cur = cur.__cause__ or cur.__context__
    raise ConnectionClosed(str(exc))


def is_temp_network_error(exc: BaseException) -> bool:
    '''Return `True` if *exc* represents a potentially temporary network problem.

    DNS resolution errors (`socket.gaierror`, `socket.herror`) are considered
    permanent because `HTTPConnection` already converts them to
    `HostnameNotResolvable` or `DNSUnavailable`.
    '''

    if isinstance(
        exc,
        (
            socket.timeout,
            ConnectionError,
            TimeoutError,
            InterruptedError,
            ConnectionClosed,
            ConnectionTimedOut,
            ssl.SSLZeroReturnError,
            ssl.SSLEOFError,
            ssl.SSLSyscallError,
            DNSUnavailable,
            httpcore.ConnectError,
            httpcore.ConnectTimeout,
            httpcore.ReadError,
            httpcore.ReadTimeout,
            httpcore.WriteError,
            httpcore.WriteTimeout,
        ),
    ):
        return True

    if isinstance(exc, h11.RemoteProtocolError):
        return True

    return isinstance(exc, OSError) and exc.errno in _TEMP_NETWORK_ERRNOS


class CaseInsensitiveDict(MutableMapping):
    """A case-insensitive `dict`-like object.

    Implements all methods and operations of
    :class:`collections.abc.MutableMapping`.

    All keys are expected to be strings. The structure remembers the case of
    the last key to be set; iteration and `keys`/`items` return case-sensitive
    keys, but lookups and `__contains__` are case-insensitive::

        cid = CaseInsensitiveDict()
        cid['Accept'] = 'application/json'
        cid['aCCEPT'] == 'application/json'  # True
        list(cid) == ['Accept']               # True

    Set operations preserve the case of the last-set key. If two keys differ
    only in case, the last set value wins.
    """

    def __init__(self, data=None, **kwargs):
        self._store: dict[str, tuple[str, str]] = {}
        if data is None:
            data = {}
        self.update(data, **kwargs)

    def __setitem__(self, key, value):
        self._store[key.lower()] = (key, value)

    def __getitem__(self, key):
        return self._store[key.lower()][1]

    def __delitem__(self, key):
        del self._store[key.lower()]

    def __iter__(self):
        return (cased_key for cased_key, _value in self._store.values())

    def __len__(self):
        return len(self._store)

    def __repr__(self):
        return str(dict(self.items()))


class Pool:
    '''Pool of `HTTPConnection` instances bound to one origin.

    All connections share the same hostname, port, TLS configuration, proxy,
    and per-operation timeout. Concurrent borrows are capped by
    *max_connections*; sequential borrows reuse the same connection whenever
    the server's keep-alive state allows.

    Acquire a connection with `get`, which is an async context manager. The
    connection is returned to the pool on normal exit and dropped on
    exception or if its protocol state is no longer keep-alive-clean.
    '''

    def __init__(
        self,
        hostname: str,
        port: Optional[int] = None,
        *,
        ssl_context: Optional[ssl.SSLContext] = None,
        proxy: Optional[tuple[str, int]] = None,
        max_connections: int = 1,
        timeout: float | int | None = None,
    ) -> None:
        self.hostname = hostname
        self.port = port if port is not None else (HTTPS_PORT if ssl_context else HTTP_PORT)
        self.ssl_context = ssl_context
        self.proxy = proxy
        self.max_connections = max_connections
        self.timeout = _DEFAULT_TIMEOUT_S if timeout is None else float(timeout)
        self._idle: list[HTTPConnection] = []
        self._limiter = trio.CapacityLimiter(max_connections)
        self._closed = False

    async def _pop_conn(self) -> HTTPConnection:
        '''Return an idle conn that is keep-alive-clean, or a fresh one.'''
        while self._idle:
            conn = self._idle.pop()
            if conn.is_idle:
                return conn
            await conn.aclose()

        conn = HTTPConnection(
            self.hostname, self.port, ssl_context=self.ssl_context, proxy=self.proxy
        )
        conn.timeout = self.timeout
        return conn

    @asynccontextmanager
    async def get(self) -> AsyncIterator[HTTPConnection]:
        '''Yield a borrowed `HTTPConnection` for the duration of the block.

        On exit (normal or exception) the connection is returned to the pool
        if its h11 state is still IDLE on both sides — i.e. the response body
        has been fully drained and the server has not signalled close.
        Otherwise it is reset. Draining the body before raising is therefore
        sufficient to keep the connection alive even on the error path; a
        request that raises mid-cycle naturally fails the IDLE check.
        '''
        if self._closed:
            raise StateError('Pool is closed')
        async with self._limiter:
            conn = await self._pop_conn()
            try:
                yield conn
            finally:
                if not self._closed and conn.is_idle:
                    self._idle.append(conn)
                else:
                    await conn.aclose()

    async def aclose(self) -> None:
        '''Close all idle connections and refuse further `get` calls.'''
        self._closed = True
        idle = self._idle
        self._idle = []
        for conn in idle:
            await conn.aclose()

    async def __aenter__(self) -> Pool:
        return self

    async def __aexit__(self, exc_type: object, exc_value: object, traceback: object) -> bool:
        await self.aclose()
        return False
