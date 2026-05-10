'''
http.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.

The CaseInsensitiveDict implementation is copyright 2013 Kenneth Reitz and
licensed under the Apache License, Version 2.0
(http://www.apache.org/licenses/LICENSE-2.0)
'''

from __future__ import annotations

import email
import email.policy
import errno
import hashlib
import io
import logging
import os
import socket
import ssl
from base64 import b64encode
from collections.abc import Mapping, MutableMapping
from dataclasses import dataclass
from enum import Enum
from http.client import HTTP_PORT, HTTPS_PORT, NO_CONTENT, NOT_MODIFIED
from typing import Optional

import trio

log = logging.getLogger(__name__)

#: Internal buffer size
BUFFER_SIZE = 64 * 1024

#: Maximal length of HTTP status line. If the server sends a line longer than
#: this value, `InvalidResponse` will be raised.
MAX_LINE_SIZE = BUFFER_SIZE - 1

#: Maximal length of a response header (i.e., for all header
#: lines together). If the server sends a header segment longer than
#: this value, `InvalidResponse` will be raised.
MAX_HEADER_SIZE = BUFFER_SIZE - 1


class Encodings(Enum):
    CHUNKED = 1
    IDENTITY = 2


class _State(Enum):
    #: Sentinel for `HTTPConnection._out_remaining` to indicate that
    #: we're waiting for a 100-continue response from the server.
    WAITING_FOR_100C = 'waiting-for-100c'

    #: Sentinel for `HTTPConnection._in_remaining` to indicate that
    #: the response body cannot be read and that an exception
    #: (stored in the `HTTPConnection._encoding` attribute) should
    #: be raised.
    RESPONSE_BODY_ERROR = 'response-body-error'

    #: Sentinel for `HTTPConnection._in_remaining` to indicate that
    #: we should read until EOF (i.e., no keep-alive).
    READ_UNTIL_EOF = 'read-until-eof'


#: Sequence of ``(hostname, port)`` tuples that are used by distinguish between permanent and
#: temporary name resolution problems.
DNS_TEST_HOSTNAMES = (('www.google.com', 80), ('www.iana.org', 80), ('C.root-servers.org', 53))


@dataclass(slots=True)
class HTTPResponse:
    '''
    Information about an HTTP response.

    Instances of this class are returned by `HTTPConnection.read_response`
    and expose the response status, reason, and headers. Response body data
    has to be read directly from the `HTTPConnection` instance.
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


@dataclass(slots=True, frozen=True)
class BodyFollowing:
    '''
    Sentinel for the *body* parameter of `HTTPConnection.send_request`.

    Passing an instance declares that *length* bytes of body data will be
    provided in separate method calls.
    '''

    #: Number of body bytes that will be sent separately.
    length: int


class _ChunkTooLong(Exception):
    '''
    Raised by `_readstr_until` if the requested end pattern
    cannot be found within the specified byte limit.
    '''

    pass


class _GeneralError(Exception):
    msg = 'General HTTP Error'

    def __init__(self, msg: str | None = None) -> None:
        if msg:
            self.msg = msg

    def __str__(self) -> str:
        return self.msg


class HostnameNotResolvable(Exception):
    '''Raised if a host name does not resolve to an ip address.

    This exception is raised if a resolution attempt results in a `socket.gaierror` or
    `socket.herror` exception with errno :const:`!socket.EAI_NODATA` or
    :const:`!socket.EAI_NONAME`, but at least one of the hostnames in `DNS_TEST_HOSTNAMES`
    can be resolved.
    '''

    def __init__(self, hostname: str) -> None:
        self.name = hostname

    def __str__(self) -> str:
        return 'Host %s does not have any ip addresses' % self.name


class DNSUnavailable(Exception):
    '''Raised if the DNS server cannot be reached.

    This exception is raised if a resolution attempt results in a `socket.gaierror` or
    `socket.herror` exception with errno :const:`socket.EAI_AGAIN`, or if none of the
    hostnames in `DNS_TEST_HOSTNAMES` can be resolved.
    '''

    def __init__(self, hostname: str) -> None:
        self.name = hostname

    def __str__(self) -> str:
        return 'Unable to resolve %s, DNS server unavailable.' % self.name


class _DNSError(Exception):
    '''Internal: DNS lookup failed but cause (NXDOMAIN vs. unavailable) is unclear.

    `create_socket` catches this and converts it to either `HostnameNotResolvable`
    or `DNSUnavailable` once it has probed `DNS_TEST_HOSTNAMES`.
    '''

    def __init__(self, hostname: str) -> None:
        self.name = hostname

    def __str__(self) -> str:
        return 'Unable to resolve host %s' % self.name


class StateError(_GeneralError):
    '''
    Raised when attempting an operation that doesn't make
    sense in the current connection state.
    '''

    msg = 'Operation invalid in current connection state'


class ExcessBodyData(_GeneralError):
    '''
    Raised when trying to send more data to the server than
    announced.
    '''

    msg = 'Cannot send larger request body than announced'


class InvalidResponse(_GeneralError):
    '''
    Raised if the server produced an invalid response (i.e, something
    that is not proper HTTP 1.0 or 1.1).
    '''

    msg = 'Server sent invalid response'


class UnsupportedResponse(_GeneralError):
    '''
    This exception is raised if the server produced a response that is not
    supported. This should not happen for servers that are HTTP 1.1 compatible.

    If an `UnsupportedResponse` exception has been raised, this typically means
    that synchronization with the server will be lost (i.e., we cannot
    determine where the current response ends and the next response starts), so
    the connection needs to be reset by calling the
    :meth:`~HTTPConnection.disconnect` method.
    '''

    msg = 'Server sent unsupported response'


class ConnectionClosed(_GeneralError):
    '''
    Raised if the connection was unexpectedly closed.

    This exception is raised also if the server declared that it will close the
    connection (by sending a ``Connection: close`` header). Such responses can
    still be read completely, but the next attempt to send a request or read a
    response will raise the exception. To re-use the connection after the server
    has closed it, call `HTTPConnection.disconnect`; the next request will
    automatically reconnect.
    '''

    msg = 'connection closed unexpectedly'


class ConnectionTimedOut(_GeneralError):
    '''
    Raised if a regular `HTTPConnection` method (i.e., no coroutine) was
    unable to send or receive data for the timeout specified in the
    `HTTPConnection.timeout` attribute.
    '''

    msg = 'send/recv timeout exceeded'


class _Buffer:
    '''
    This class represents a buffer with a fixed size, but varying
    fill level.
    '''

    __slots__ = ('d', 'b', 'e')

    def __init__(self, size: int) -> None:
        #: Holds the actual data
        self.d: bytearray = bytearray(size)

        #: Position of the first buffered byte that has not yet
        #: been consumed ("*b*eginning")
        self.b: int = 0

        #: Fill-level of the buffer (e is for "end")
        self.e: int = 0

    def __len__(self) -> int:
        '''Return amount of data ready for consumption'''
        return self.e - self.b

    def clear(self) -> None:
        '''Forget all buffered data'''

        self.b = 0
        self.e = 0

    def compact(self) -> None:
        '''Ensure that buffer can be filled up to its maximum size

        If part of the buffer data has been consumed, the unconsumed part is
        copied to the beginning of the buffer to maximize the available space.
        '''

        if self.b == 0:
            return

        log.debug('compacting buffer')
        buf = memoryview(self.d)[self.b : self.e]
        len_ = len(buf)
        self.d = bytearray(len(self.d))
        self.d[:len_] = buf
        self.b = 0
        self.e = len_

    def exhaust(self) -> bytearray:
        '''Return (and consume) all available data'''

        if self.b == 0:
            log.debug('exhausting buffer (truncating)')
            # Return existing buffer after truncating it
            buf = self.d
            self.d = bytearray(len(self.d))
            buf[self.e :] = b''
        else:
            log.debug('exhausting buffer (copying)')
            buf = self.d[self.b : self.e]

        self.b = 0
        self.e = 0

        return buf


class HTTPConnection:
    '''
    This class encapsulates a HTTP connection.

    `HTTPConnection` instances can be used as context managers. The
    `.disconnect` method will be called on exit from the managed block.
    '''

    #: Default value for `_timeout_ms`: 24 hours expressed in milliseconds.
    _DEFAULT_TIMEOUT_MS = 24 * 60 * 60 * 1000

    def __init__(
        self,
        hostname: str,
        port: Optional[int] = None,
        ssl_context: Optional[ssl.SSLContext] = None,
        proxy: Optional[tuple[str, int]] = None,
    ):
        if port is not None:
            self.port = port
        else:
            self.port = HTTPS_PORT if ssl_context else HTTP_PORT

        self.ssl_context = ssl_context
        self.hostname = hostname

        #: Socket object connecting to the server
        self._sock: Optional[socket.socket] = None

        #: Read-buffer
        self._rbuf = _Buffer(BUFFER_SIZE)

        #: a tuple ``(hostname, port)`` of the proxy server to use or `None`.
        self.proxy = proxy

        #: ``(method, path, body_len)`` describing the in-flight request whose
        #: response has not yet been read completely, or `None` if no such
        #: request exists. *body_len* is `None` except while waiting for a
        #: 100-continue response, in which case it is the size of the request
        #: body that still has to be sent.
        self._pending_request: Optional[tuple[str, str, Optional[int]]] = None

        #: This attribute is `None` when a request has been sent completely.  If
        #: request headers have been sent, but request body data is still
        #: pending, it is set to a ``(method, path, body_len)`` tuple. *body_len*
        #: is the number of bytes that that still need to send, or
        #: `_State.WAITING_FOR_100C` if we are waiting for a 100 response from the server.
        self._out_remaining: Optional[tuple[str, str, int | _State]] = None

        #: Number of remaining bytes of the current response body (or current
        #: chunk), `None` if there is no active response or `_State.READ_UNTIL_EOF` if
        #: we have to read until the connection is closed (i.e., we don't know
        #: the content-length and keep-alive is not active).
        self._in_remaining: Optional[int | _State] = None

        #: Transfer encoding of the active response (if any), or an exception
        #: instance if the body cannot be read.
        self._encoding: Optional[Encodings | Exception] = None

        #: If a regular `HTTPConnection` method is unable to send or receive data for more than
        #: this period (in ms), it will raise `ConnectionTimedOut`.
        self._timeout_ms: int = self._DEFAULT_TIMEOUT_MS

        #: Filehandler for tracing
        self.trace_fh: Optional[io.FileIO] = None

    @property
    def timeout(self) -> float:
        return self._timeout_ms / 1000

    @timeout.setter
    def timeout(self, value: int | float | None) -> None:
        if value is None:
            self._timeout_ms = self._DEFAULT_TIMEOUT_MS
        else:
            self._timeout_ms = int(value * 1000)

    async def connect(self) -> None:
        """Connect to the remote server

        This method generally does not need to be called manually.
        """

        log.debug('start')

        if self.proxy:
            log.debug('connecting to %s', self.proxy)
            self._sock = create_socket(self.proxy)
            if self.ssl_context:
                await self._tunnel()
        else:
            log.debug('connecting to %s', (self.hostname, self.port))
            self._sock = create_socket((self.hostname, self.port))

        if self.ssl_context:
            log.debug('establishing ssl layer')

            self._sock = self.ssl_context.wrap_socket(self._sock, server_hostname=self.hostname)

        self._sock.setblocking(False)
        self._rbuf.clear()
        self._out_remaining = None
        self._in_remaining = None
        self._pending_request = None

        if 'S3QL_HTTP_TRACEFILE' in os.environ:
            self.trace_fh = open(  # noqa: SIM115
                os.environ['S3QL_HTTP_TRACEFILE'] % id(self._sock), 'wb+', buffering=0
            )

        log.debug('done')

    async def _tunnel(self) -> None:
        '''Set up CONNECT tunnel to destination server'''

        log.debug('start connecting to %s:%d', self.hostname, self.port)

        await self._send(
            ("CONNECT %s:%d HTTP/1.0\r\n\r\n" % (self.hostname, self.port)).encode('latin1')
        )

        (status, reason) = await self._read_status()
        log.debug('got %03d %s', status, reason)
        await self._read_header()

        if status != 200:
            self.disconnect()
            raise ConnectionError("Tunnel connection failed: %d %s" % (status, reason))

    async def send_request(
        self,
        method: str,
        path: str,
        headers: Optional[Mapping[str, str]] = None,
        body: bytes | bytearray | memoryview | BodyFollowing | None = None,
        expect100: bool = False,
    ) -> None:
        '''Send a new HTTP request to the server

        The message body may be passed in the *body* argument or be sent
        separately. In the former case, *body* must be a :term:`bytes-like
        object`. In the latter case, *body* must be a `BodyFollowing` instance
        specifying the length of the data that will be sent.

        *headers* should be a mapping containing the HTTP headers to be send
        with the request. Multiple header lines with the same key are not
        supported. It is recommended to pass a `CaseInsensitiveDict` instance,
        other mappings will be converted to `CaseInsensitiveDict` automatically.

        If *body* is a provided as a :term:`bytes-like object`, a
        ``Content-MD5`` header is generated automatically unless it has been
        provided in *headers* already.
        '''

        log.debug('start')

        if expect100 and not isinstance(body, BodyFollowing):
            raise ValueError('expect100 only allowed for separate body')

        if self._sock is None:
            await self.connect()

        if self._out_remaining:
            raise StateError('body data has not been sent completely yet')

        if self._pending_request is not None:
            raise StateError('previous response has not been read')

        if headers is None:
            headers = CaseInsensitiveDict()
        elif not isinstance(headers, CaseInsensitiveDict):
            headers = CaseInsensitiveDict(headers)

        pending_body_size: Optional[int] = None
        if body is None:
            headers['Content-Length'] = '0'
        elif isinstance(body, BodyFollowing):
            log.debug('preparing to send %d bytes of body data', body.length)
            if expect100:
                headers['Expect'] = '100-continue'
                # Do not set _out_remaining, we must only send data once we've
                # read the response. Stash the body size on _pending_request so
                # that read_response() can hand it back to _out_remaining
                # after the 100-continue arrives.
                pending_body_size = body.length
                self._out_remaining = (method, path, _State.WAITING_FOR_100C)
            else:
                self._out_remaining = (method, path, body.length)
            headers['Content-Length'] = str(body.length)
            body = None
        elif isinstance(body, (bytes, bytearray, memoryview)):
            headers['Content-Length'] = str(len(body))
            if 'Content-MD5' not in headers:
                log.debug('computing content-md5')
                headers['Content-MD5'] = b64encode(hashlib.md5(body).digest()).decode('ascii')
        else:
            raise TypeError('*body* must be None, bytes-like or BodyFollowing')

        # Generate host header
        host = self.hostname
        if host.find(':') >= 0:
            host = f'[{host}]'
        default_port = HTTPS_PORT if self.ssl_context else HTTP_PORT
        if self.port == default_port:
            headers['Host'] = host
        else:
            headers['Host'] = f'{host}:{self.port}'

        # Assemble request
        headers['Accept-Encoding'] = 'identity'
        if 'Connection' not in headers:
            headers['Connection'] = 'keep-alive'
        if self.proxy and not self.ssl_context:
            gpath = "http://{}{}".format(headers['Host'], path)
        else:
            gpath = path
        request = [f'{method} {gpath} HTTP/1.1'.encode('latin1')]
        for key, val in headers.items():
            request.append(f'{key}: {val}'.encode('latin1'))
        request.append(b'')

        if body is not None:
            request.append(body)
        else:
            request.append(b'')

        buf = b'\r\n'.join(request)

        log.debug('sending %s %s', method, path)
        await self._send(buf)
        if not self._out_remaining or expect100:
            self._pending_request = (method, path, pending_body_size)

    async def _wait_readable(self) -> None:
        assert self._sock is not None
        with trio.move_on_after(self._timeout_ms / 1000) as ctx:
            await trio.lowlevel.wait_readable(self._sock)
        if ctx.cancelled_caught:
            raise ConnectionTimedOut('Timeout in recv()')

    async def _wait_writable(self) -> None:
        assert self._sock is not None
        with trio.move_on_after(self._timeout_ms / 1000) as ctx:
            await trio.lowlevel.wait_writable(self._sock)
        if ctx.cancelled_caught:
            raise ConnectionTimedOut('Timeout in send()')

    async def _send(self, buf: bytes | bytearray | memoryview) -> None:
        '''Send *buf* to server'''

        log.debug('trying to send %d bytes', len(buf))

        if not isinstance(buf, memoryview):
            buf = memoryview(buf)

        while True:
            try:
                if self._sock is None:
                    raise ConnectionClosed('connection has been closed locally')
                len_ = self._sock.send(buf)
                # An SSL socket has the nasty habit of returning zero
                # instead of raising an exception when in non-blocking
                # mode.
                if len_ == 0:
                    raise BlockingIOError()
            except (TimeoutError, ssl.SSLWantWriteError, BlockingIOError):
                log.debug('yielding')
                await self._wait_writable()
                continue
            except (BrokenPipeError, ConnectionResetError, ssl.SSLEOFError):
                raise ConnectionClosed('connection was interrupted')
            except InterruptedError:
                log.debug('interrupted')
                # According to send(2), this means that no data has been sent
                # at all before the interruption, so we just try again.
                continue
            except OSError as exc:
                if exc.errno == errno.EINVAL:
                    # Blackhole routing, according to ip(7)
                    raise ConnectionClosed('ip route goes into black hole')
                else:
                    raise

            log.debug('sent %d bytes', len_)
            if self.trace_fh:
                self.trace_fh.write(buf[:len_])
            buf = buf[len_:]
            if len(buf) == 0:
                log.debug('done')
                return

    async def write(self, buf: bytes | bytearray | memoryview) -> None:
        '''Write request body data

        `ExcessBodyData` will be raised when attempting to send more data than
        required to complete the request body of the active request.
        '''

        log.debug('start (len=%d)', len(buf))

        if not self._out_remaining:
            raise StateError('No active request with pending body data')

        (method, path, remaining) = self._out_remaining
        if remaining is _State.WAITING_FOR_100C:
            raise StateError("can't write when waiting for 100-continue")
        assert isinstance(remaining, int)

        if len(buf) > remaining:
            raise ExcessBodyData(
                'trying to write %d bytes, but only %d bytes pending' % (len(buf), remaining)
            )

        try:
            await self._send(buf)
        except ConnectionClosed:
            # If the server closed the connection, we pretend that all data
            # has been sent, so that we can still read a (buffered) error
            # response.
            self._out_remaining = None
            self._pending_request = (method, path, None)
            raise

        len_ = len(buf)
        if len_ == remaining:
            log.debug('body sent fully')
            self._out_remaining = None
            self._pending_request = (method, path, None)
        else:
            self._out_remaining = (method, path, remaining - len_)

        log.debug('done')

    def response_pending(self) -> bool:
        '''Return `True` if there is an outstanding response

        This includes a response that has been partially read.
        '''

        return self._sock is not None and self._pending_request is not None

    async def read_response(self) -> HTTPResponse:
        '''Read response status line and headers

        Return a `HTTPResponse` instance containing information about response
        status, reason, and headers. The response body data must be retrieved
        separately (e.g. using `.read` or `.readall`).
        '''

        log.debug('start')

        if self._pending_request is None:
            raise StateError('No pending request')

        if self._in_remaining is not None:
            raise StateError('Previous response not read completely')

        (method, path, body_size) = self._pending_request

        # Need to loop to handle any 1xx responses
        while True:
            (status, reason) = await self._read_status()
            log.debug('got %03d %s', status, reason)

            hstring = await self._read_header()
            header = email.message_from_string(hstring, policy=email.policy.HTTP)

            if status < 100 or status > 199:
                break

            # We are waiting for 100-continue
            if body_size is not None and status == 100:
                break

        # Handle (expected) 100-continue
        if status == 100:
            assert self._out_remaining == (method, path, _State.WAITING_FOR_100C)
            assert body_size is not None

            # We're ready to send request body now
            self._out_remaining = (method, path, body_size)
            self._pending_request = None
            self._in_remaining = None

            # Return early, because we don't have to prepare
            # for reading the response body at this time
            return HTTPResponse(method, path, status, reason, header, length=0)

        # Handle non-100 status when waiting for 100-continue
        elif body_size is not None:
            assert self._out_remaining == (method, path, _State.WAITING_FOR_100C)
            # RFC 2616 actually states that the server MAY continue to read the
            # request body after it has sent a final status code
            # (http://tools.ietf.org/html/rfc2616#section-8.2.3). However, that
            # totally defeats the purpose of 100-continue, so we hope that the
            # server behaves sanely and does not attempt to read the body of a
            # request it has already handled.
            self._out_remaining = None

        #
        # Prepare to read body
        #
        body_length = self._setup_read(method, status, header)

        # Don't require calls to read() et al if there is
        # nothing to be read.
        if self._in_remaining is None:
            self._pending_request = None

        log.debug('done (in_remaining=%s)', self._in_remaining)
        return HTTPResponse(method, path, status, reason, header, body_length)

    def _setup_read(self, method: str, status: int, header: email.message.Message) -> int | None:
        '''Prepare for reading response body

        Sets up `._encoding`, `_in_remaining` and returns Content-Length
        (if available).

        See RFC 2616, sec. 4.4 for specific rules.
        '''

        # On error, the exception is stored in _encoding and raised on
        # the next call to read() et al - that way we can still
        # return the http status and headers.

        will_close = header.get('Connection', 'keep-alive').lower() == 'close'

        body_length = header['Content-Length']
        if body_length is not None:
            try:
                body_length = int(body_length)
            except ValueError:
                self._encoding = InvalidResponse('Invalid content-length: %s' % body_length)
                self._in_remaining = _State.RESPONSE_BODY_ERROR
                return None

        if status in (NO_CONTENT, NOT_MODIFIED) or 100 <= status < 200 or method == 'HEAD':
            log.debug('no content by RFC')
            self._in_remaining = None
            self._encoding = None
            return 0

        tc = header.get('Transfer-Encoding', 'identity').lower()
        if tc == 'chunked':
            log.debug('Chunked encoding detected')
            self._encoding = Encodings.CHUNKED
            self._in_remaining = 0
            return None

        elif tc != 'identity':
            log.warning('Server uses invalid response encoding "%s"', tc)
            self._encoding = InvalidResponse('Cannot handle %s encoding' % tc)
            self._in_remaining = _State.RESPONSE_BODY_ERROR
            return None

        log.debug('identity encoding detected')
        self._encoding = Encodings.IDENTITY

        if body_length is not None:
            log.debug('Will read response body of %d bytes', body_length)
            self._in_remaining = body_length or None
            return body_length

        if will_close:
            log.debug('no content-length, will read until EOF')
        else:
            log.warning(
                '%s:%d sent response with missing content-length header. This is not HTTP/1.1 '
                'specification compliant but we will ignore this error. Response headers: %s',
                self.hostname,
                self.port,
                header,
                extra={'rate_limit': 60},
            )
            # correct the connection header for library users (HTTPConnection does not need it)
            del header['Connection']
            header['Connection'] = 'close'
        self._in_remaining = _State.READ_UNTIL_EOF
        return None

    async def _read_status(self) -> tuple[int, str]:
        '''Read response line'''

        log.debug('start')

        # read status
        try:
            line = await self._readstr_until(b'\r\n', MAX_LINE_SIZE)
        except _ChunkTooLong:
            raise InvalidResponse('server send ridiculously long status line')

        parts = line.split(None, 2)
        if len(parts) == 3:
            version, status, reason = parts
        elif len(parts) == 2:
            version, status = parts
            reason = ""
        else:
            # empty version will cause next test to fail.
            version = ""
            status = ""

        if not version.startswith("HTTP/1"):
            raise UnsupportedResponse('%s not supported' % version)

        # The status code is a three-digit number
        try:
            status = int(status)
            if status < 100 or status > 999:
                raise InvalidResponse('%d is not a valid status' % status)
        except ValueError:
            raise InvalidResponse('%s is not a valid status' % status)

        log.debug('done')
        return (status, reason.strip())

    async def _read_header(self) -> str:
        '''Read response header'''

        log.debug('start')

        # Peek into buffer. If the first characters are \r\n, then the header
        # is empty (so our search for \r\n\r\n would fail)
        rbuf = self._rbuf
        if len(rbuf) < 2:
            await self._fill_buffer(2)
        if rbuf.d[rbuf.b : rbuf.b + 2] == b'\r\n':
            log.debug('done (empty header)')
            rbuf.b += 2
            return ''

        try:
            hstring = await self._readstr_until(b'\r\n\r\n', MAX_HEADER_SIZE)
        except _ChunkTooLong:
            raise InvalidResponse('server sent ridiculously long header')

        log.debug('done (%d characters)', len(hstring))
        return hstring

    async def read(self, len_: Optional[int] = None) -> bytes | bytearray:
        '''Read up to *len_* bytes of response body data

        This method may return less than *len_* bytes, but will return ``b''`` only
        if the response body has been read completely. The returned buffer is
        owned by the caller and may be modified in place.

        If *len_* is `None`, this method returns the entire response body.
        '''

        log.debug('start (len=%s)', len_)

        if self._in_remaining is _State.RESPONSE_BODY_ERROR:
            assert isinstance(self._encoding, Exception)
            raise self._encoding
        elif len_ is None:
            return await self.readall()
        elif len_ == 0 or self._in_remaining is None:
            return b''
        elif self._encoding is Encodings.IDENTITY:
            return await self._read_id(len_)
        elif self._encoding is Encodings.CHUNKED:
            return await self._read_chunked(len_=len_)
        else:
            raise AssertionError('unreachable encoding: %r' % self._encoding)

    async def read_raw(self, size: int) -> bytes:
        '''Read up to *size* bytes of uninterpreted data.

        May be called even after `UnsupportedResponse` or `InvalidResponse`
        has been raised. Reads raw bytes from the socket without trying to
        interpret them as part of an HTTP response, which is useful for
        capturing a misbehaving server's response body for diagnostics.
        Returns an empty bytes object once the connection has been closed
        by the peer.

        After this method returns, the connection's framing state is no
        longer trustworthy. The caller must call `disconnect` before
        sending further requests.

        **Don't use this method unless you know exactly what you are
        doing.**
        '''

        if self._sock is None:
            raise ConnectionClosed('connection has been closed locally')

        buf = bytearray()
        rbuf = self._rbuf
        while len(buf) < size:
            if len(rbuf):
                take = min(size - len(buf), len(rbuf))
                buf += rbuf.d[rbuf.b : rbuf.b + take]
                rbuf.b += take
                continue

            rbuf.compact()
            res = self._try_fill_buffer()
            if res is None:
                await self._wait_readable()
            elif res == 0:
                break

        return bytes(buf)

    async def _read_id(self, len_: int) -> bytes | bytearray:
        '''Read up to *len* bytes of response body assuming identity encoding'''

        log.debug('start (len=%d)', len_)
        assert self._in_remaining is not None

        if not self._in_remaining:
            # Body retrieved completely, clean up
            self._in_remaining = None
            self._pending_request = None
            return b''

        rbuf = self._rbuf
        if self._in_remaining is not _State.READ_UNTIL_EOF:
            assert isinstance(self._in_remaining, int)
            len_ = min(len_, self._in_remaining)
        log.debug('updated len_=%d', len_)

        # If buffer is empty, reset so that we start filling from
        # beginning. This check is already done by _try_fill_buffer(), but we
        # have to do it here or we never enter the while loop if the buffer
        # is empty but has no capacity (rbuf.b == rbuf.e == len(rbuf.d))
        if rbuf.b == rbuf.e:
            rbuf.clear()

        # Loop while we could return more data than we have buffered
        # and buffer is not full
        while len(rbuf) < len_ and rbuf.e < len(rbuf.d):
            got_data = self._try_fill_buffer()
            if got_data is None:
                if rbuf:
                    log.debug('nothing more to read')
                    break
                else:
                    log.debug('buffer empty and nothing to read, yielding..')
                await self._wait_readable()
            elif got_data == 0:
                if self._in_remaining is _State.READ_UNTIL_EOF:
                    log.debug('connection closed, %d bytes in buffer', len(rbuf))
                    self._in_remaining = len(rbuf)
                    break
                else:
                    raise ConnectionClosed('server closed connection')

        len_ = min(len_, len(rbuf))
        if self._in_remaining is not _State.READ_UNTIL_EOF:
            assert isinstance(self._in_remaining, int)
            self._in_remaining -= len_

        if len_ < len(rbuf):
            buf = rbuf.d[rbuf.b : rbuf.b + len_]
            rbuf.b += len_
        else:
            buf = rbuf.exhaust()

        # When reading until EOF, it is possible that we read only the EOF. In
        # this case we won't get called again, so we need to clean-up the
        # request. This can not happen when we know the number of remaining
        # bytes, because in this case either the check at the start of the
        # function hits, or we can't read the remaining data and raise.
        if len(buf) == 0:
            assert self._in_remaining == 0
            self._in_remaining = None
            self._pending_request = None

        log.debug('done (%d bytes)', len(buf))
        return buf

    async def _read_chunked(self, len_: int) -> bytes | bytearray:
        '''Read response body assuming chunked encoding'''

        assert isinstance(self._in_remaining, int)

        if self._in_remaining == 0:
            log.debug('starting next chunk')
            try:
                line = await self._readstr_until(b'\r\n', MAX_LINE_SIZE)
            except _ChunkTooLong:
                raise InvalidResponse('could not find next chunk marker')

            i = line.find(";")
            if i >= 0:
                log.debug('stripping chunk extensions: %s', line[i:])
                line = line[:i]  # strip chunk-extensions
            try:
                self._in_remaining = int(line, 16)
            except ValueError:
                raise InvalidResponse('Cannot read chunk size %r' % line[:20])

            log.debug('chunk size is %d', self._in_remaining)
            if self._in_remaining == 0:
                self._in_remaining = None
                self._pending_request = None

        if self._in_remaining is None:
            res = b''
        else:
            res = await self._read_id(len_)

        if not self._in_remaining:
            log.debug('chunk complete')
            await self._read_header()

        log.debug('done')
        return res

    async def _readstr_until(self, substr: bytes | bytearray | memoryview, maxsize: int) -> str:
        '''Read from server until *substr*, and decode to latin1

        If *substr* cannot be found in the next *maxsize* bytes,
        raises `_ChunkTooLong`.
        '''

        if not isinstance(substr, (bytes, bytearray, memoryview)):
            raise TypeError('*substr* must be bytes-like')

        log.debug('reading until %s', substr)

        rbuf = self._rbuf
        sub_len = len(substr)

        # Make sure that substr cannot be split over more than one part
        assert len(rbuf.d) > sub_len

        parts = []
        while True:
            # substr may be split between last part and current buffer
            # This isn't very performant, but it should be pretty rare
            if parts and sub_len > 1:
                buf = b''.join(
                    (parts[-1][-sub_len:], rbuf.d[rbuf.b : min(rbuf.e, rbuf.b + sub_len - 1)])
                )
                idx = buf.find(substr)
                if idx >= 0:
                    idx -= sub_len
                    break

            # log.debug('rbuf is: %s', rbuf.d[rbuf.b:min(rbuf.e, rbuf.b+512)])
            stop = min(rbuf.e, rbuf.b + maxsize)
            idx = rbuf.d.find(substr, rbuf.b, stop)

            if idx >= 0:  # found
                break
            if stop != rbuf.e:
                raise _ChunkTooLong()

            # If buffer is full, store away the part that we need for sure
            if rbuf.e == len(rbuf.d):
                log.debug('buffer is full, storing part')
                buf = rbuf.exhaust()
                parts.append(buf)
                maxsize -= len(buf)

            # Refill buffer
            while True:
                res = self._try_fill_buffer()
                if res is None:
                    log.debug('need more data, yielding')
                    await self._wait_readable()
                elif res == 0:
                    raise ConnectionClosed('server closed connection')
                else:
                    break

        log.debug('found substr at %d', idx)
        idx += len(substr)
        buf = rbuf.d[rbuf.b : idx]
        rbuf.b = idx

        if parts:
            parts.append(buf)
            buf = b''.join(parts)

        try:
            return buf.decode('latin1')
        except UnicodeDecodeError:
            raise InvalidResponse('server response cannot be decoded to latin1')

    def _try_fill_buffer(self) -> Optional[int]:
        '''Try to fill up read buffer

        Returns the number of bytes read into buffer, or `None` if no data was
        available on the socket. Return zero if the TCP connection has been
        properly closed but the socket object still exists. On other problems
        (e.g. if the socket object has been destroyed or the connection
        interrupted), raise `ConnectionClosed`.
        '''

        log.debug('start')
        rbuf = self._rbuf

        # If buffer is empty, reset so that we start filling from beginning
        if rbuf.b == rbuf.e:
            rbuf.clear()

        # There should be free capacity
        assert rbuf.e < len(rbuf.d)

        if self._sock is None:
            raise ConnectionClosed('connection has been closed locally')

        try:
            len_ = self._sock.recv_into(memoryview(rbuf.d)[rbuf.e :])
            if self.trace_fh:
                self.trace_fh.write(rbuf.d[rbuf.e : rbuf.e + len_])
        except (TimeoutError, ssl.SSLWantReadError, BlockingIOError):
            log.debug('done (nothing ready)')
            return None
        except (ConnectionResetError, BrokenPipeError, ssl.SSLEOFError):
            raise ConnectionClosed('connection was interrupted')
        except ssl.SSLError as exc:
            if exc.library == 'SSL' and exc.reason == 'UNEXPECTED_EOF_WHILE_READING':
                raise ConnectionClosed('connection was interrupted')
            else:
                raise

        rbuf.e += len_
        log.debug('done (got %d bytes)', len_)
        return len_

    async def _fill_buffer(self, len_: int) -> None:
        '''Make sure that there are at least *len_* bytes in buffer'''

        rbuf = self._rbuf
        if len_ > len(rbuf.d):
            raise ValueError('Requested more bytes than buffer has capacity')
        while len(rbuf) < len_:
            if len(rbuf.d) - rbuf.b < len_:
                self._rbuf.compact()
            res = self._try_fill_buffer()
            if res is None:
                await self._wait_readable()
            elif res == 0:
                raise ConnectionClosed('server closed connection')

    async def readall(self) -> bytes:
        '''Read and return complete response body'''

        if self._in_remaining is None:
            return b''

        log.debug('start')
        parts = []
        while True:
            buf = await self.read(BUFFER_SIZE)
            log.debug('got %d bytes', len(buf))
            if not buf:
                break
            parts.append(buf)
        buf = b''.join(parts)
        log.debug('done (%d bytes)', len(buf))
        if self.trace_fh:
            self.trace_fh.write(buf)
        return buf

    async def discard(self) -> None:
        '''Read and discard current response body'''

        if self._in_remaining is None:
            return

        log.debug('start')
        while True:
            buf = await self.read(BUFFER_SIZE)
            if not buf:
                break
            log.debug('discarding %d bytes', len(buf))
        log.debug('done')

    def disconnect(self) -> None:
        '''Close HTTP connection.

        Discards any in-flight pending response, so that the next
        `send_request` can reconnect and start fresh. Any partially
        sent request (`_out_remaining`) or partially read response
        (`_in_remaining`) state is left in place: subsequent
        `write`/`read` calls will raise `ConnectionClosed`,
        whereas `send_request` reconnects and resets everything.
        '''

        log.debug('start')
        if self.trace_fh:
            self.trace_fh.close()
        if self._sock:
            self._sock.close()
            self._sock = None
            self._rbuf.clear()
        else:
            log.debug('already closed')
        self._pending_request = None

    def __enter__(self) -> HTTPConnection:
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> bool:
        self.disconnect()
        return False


_AMBIGUOUS_DNS_ERRNOS = (socket.EAI_NODATA, socket.EAI_NONAME, socket.EAI_AGAIN)


def create_socket(address: tuple[str, int]) -> socket.socket:
    '''Create a TCP socket connected to *address*.

    Use `socket.create_connection` to create a connected socket and return
    it. If a DNS related exception is raised, capture it and attempt to
    determine if the DNS server is not reachable, or if the host name could
    not be resolved. Then raise either `HostnameNotResolvable` or
    `DNSUnavailable` as appropriate.

    To distinguish between an unresolvable hostname and a problem with the
    DNS server, attempt to resolve the addresses in `DNS_TEST_HOSTNAMES`. If
    at least one test hostname can be resolved, assume that the DNS server
    is available and that *address* can not be resolved.
    '''

    def connect() -> socket.socket:
        try:
            return socket.create_connection(address)
        except (socket.gaierror, socket.herror) as exc:
            if exc.errno in _AMBIGUOUS_DNS_ERRNOS:
                raise _DNSError(address[0])
            raise

    try:
        return connect()
    except _DNSError:
        pass

    # Some DNS errors don't tell us whether the issue is permanent or temporary
    # (cf. https://stackoverflow.com/questions/24855168/,
    # https://stackoverflow.com/questions/24855669/). Resolve a few "well-known"
    # hosts to disambiguate.
    for hostname, port in DNS_TEST_HOSTNAMES:
        try:
            socket.getaddrinfo(hostname, port)
        except (socket.gaierror, socket.herror) as exc:
            if exc.errno in _AMBIGUOUS_DNS_ERRNOS:
                continue
            raise
        # Reachable; try original address one more time in case DNS was only down briefly.
        break
    else:
        raise DNSUnavailable(address[0])

    try:
        return connect()
    except _DNSError:
        raise HostnameNotResolvable(address[0])


# Errno codes that indicate a potentially temporary network problem. Computed
# at import time because not every code exists on every platform.
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


def is_temp_network_error(exc: BaseException) -> bool:
    '''Return true if *exc* represents a potentially temporary network problem

    DNS resolution errors (`socket.gaierror` or `socket.herror`) are considered
    permanent, because `HTTPConnection` employs a heuristic to convert these
    exceptions to `HostnameNotResolvable` or `DNSUnavailable` instead.
    '''

    if isinstance(
        exc,
        (
            socket.timeout,
            ConnectionError,
            TimeoutError,
            InterruptedError,
            ConnectionClosed,
            ssl.SSLZeroReturnError,
            ssl.SSLEOFError,
            ssl.SSLSyscallError,
            ConnectionTimedOut,
            DNSUnavailable,
        ),
    ):
        return True

    return isinstance(exc, OSError) and exc.errno in _TEMP_NETWORK_ERRNOS


class CaseInsensitiveDict(MutableMapping):
    """A case-insensitive `dict`-like object.

    Implements all methods and operations of
    :class:`collections.abc.MutableMapping` as well as `.copy`.

    All keys are expected to be strings. The structure remembers the case of the
    last key to be set, and :meth:`!iter`, :meth:`!keys` and :meth:`!items` will
    contain case-sensitive keys. However, querying and contains testing is
    case-insensitive::

        cid = CaseInsensitiveDict()
        cid['Accept'] = 'application/json'
        cid['aCCEPT'] == 'application/json' # True
        list(cid) == ['Accept'] # True

    For example, ``headers['content-encoding']`` will return the value of a
    ``'Content-Encoding'`` response header, regardless of how the header name
    was originally stored.

    If the constructor, :meth:`!update`, or equality comparison operations are
    given multiple keys that have equal lower-case representations, the behavior
    is undefined.
    """

    def __init__(self, data: Mapping[str, str] | None = None, **kwargs: str) -> None:
        self._store: dict[str, tuple[str, str]] = dict()
        if data is None:
            data = {}
        self.update(data, **kwargs)

    def __setitem__(self, key: str, value: str) -> None:
        # Use the lowercased key for lookups, but store the actual
        # key alongside the value.
        self._store[key.lower()] = (key, value)

    def __getitem__(self, key: str) -> str:
        return self._store[key.lower()][1]

    def __delitem__(self, key: str) -> None:
        del self._store[key.lower()]

    def __iter__(self):
        return (casedkey for casedkey, _ in self._store.values())

    def __len__(self) -> int:
        return len(self._store)

    def lower_items(self):
        """Like :meth:`!items`, but with all lowercase keys."""
        return ((lowerkey, keyval[1]) for (lowerkey, keyval) in self._store.items())

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Mapping):
            other = CaseInsensitiveDict(other)  # type: ignore[arg-type]
        else:
            return NotImplemented
        # Compare insensitively
        return dict(self.lower_items()) == dict(other.lower_items())

    # Copy is required
    def copy(self) -> CaseInsensitiveDict:
        return CaseInsensitiveDict(self._store.values())  # type: ignore[arg-type]

    def __repr__(self) -> str:
        return '%s(%r)' % (self.__class__.__name__, dict(self.items()))
