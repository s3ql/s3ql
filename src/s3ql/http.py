'''
dugong.py - Python HTTP Client Module

Copyright © 2014 Nikolaus Rath <Nikolaus.org>

This module may be distributed under the terms of the Python Software Foundation
License Version 2.

The CaseInsensitiveDict implementation is copyright 2013 Kenneth Reitz and
licensed under the Apache License, Version 2.0
(http://www.apache.org/licenses/LICENSE-2.0)
'''

import socket
import logging
import errno
import ssl
import os
import hashlib
from inspect import getdoc
import textwrap
from base64 import b64encode
from collections import deque
from collections.abc import MutableMapping, Mapping
import email
import email.policy
from http.client import (HTTPS_PORT, HTTP_PORT, NO_CONTENT, NOT_MODIFIED)
import select
try:
    from select import POLLIN, POLLOUT
    _USE_POLL = True
except ImportError:
    POLLIN, POLLOUT = 1, 2
    _USE_POLL = False

import sys

try:
    import asyncio
except ImportError:
    asyncio = None

# Enums are only available in 3.4 and newer
try:
    from enum import Enum
except ImportError:
    Enum = object

__version__ = '3.8.2'

log = logging.getLogger(__name__)

#: Internal buffer size
BUFFER_SIZE = 64*1024

#: Maximal length of HTTP status line. If the server sends a line longer than
#: this value, `InvalidResponse` will be raised.
MAX_LINE_SIZE = BUFFER_SIZE-1

#: Maximal length of a response header (i.e., for all header
#: lines together). If the server sends a header segment longer than
#: this value, `InvalidResponse` will be raised.
MAX_HEADER_SIZE = BUFFER_SIZE-1

class Symbol:
    '''
    A symbol instance represents a specific state. Its value is
    not relevant, as it should only ever be assigned to or compared
    with other variables.
    '''
    __slots__ = [ 'name' ]

    def __init__(self, name):
        self.name = name

    def __str__(self):
        return self.name

    def __repr__(self):
        return '<%s>' % (self.name,)

class Encodings(Enum):
    CHUNKED = 1
    IDENTITY = 2

#: Sentinel for `HTTPConnection._out_remaining` to indicate that
#: we're waiting for a 100-continue response from the server
WAITING_FOR_100c = Symbol('WAITING_FOR_100c')

#: Sentinel for `HTTPConnection._in_remaining` to indicate that
#: the response body cannot be read and that an exception
#: (stored in the `HTTPConnection._encoding` attribute) should
#: be raised.
RESPONSE_BODY_ERROR = Symbol('RESPONSE_BODY_ERROR')

#: Sentinel for `HTTPConnection._in_remaining` to indicate that
#: we should read until EOF (i.e., no keep-alive)
READ_UNTIL_EOF = Symbol('READ_UNTIL_EOF')

#: Sequence of ``(hostname, port)`` tuples that are used by
#: dugong to distinguish between permanent and temporary name
#: resolution problems.
DNS_TEST_HOSTNAMES=(('www.google.com', 80),
                    ('www.iana.org', 80),
                    ('C.root-servers.org', 53))

class PollNeeded(tuple):
    '''
    This class encapsulates the requirements for a IO operation to continue.
    `PollNeeded` instances are typically yielded by coroutines.
    '''

    __slots__ = ()

    def __new__(self, fd, mask):
        return tuple.__new__(self, (fd, mask))

    @property
    def fd(self):
        '''File descriptor that the IO operation depends on'''

        return self[0]

    @property
    def mask(self):
        '''Event mask specifiying the type of required IO

        This attribute defines what type of IO the provider of the `PollNeeded`
        instance needs to perform on *fd*. It is expected that, when *fd* is
        ready for IO of the specified type, operation will continue without
        blocking.

        The type of IO is specified as a :ref:`poll <poll-objects>`
        compatible event mask, i.e. a bitwise combination of `!select.POLLIN`
        and `!select.POLLOUT`.
        '''

        return self[1]

    def poll(self, timeout=None):
        '''Wait until fd is ready for requested IO

        This is a convenince function that uses `~select.poll` to wait until
        `.fd` is ready for the requested type of IO. If poll is unavailable
        (e.g. on Windows) it uses `~select.select` instead.

        If *timeout* is specified, return `False` if the timeout is exceeded
        without the file descriptor becoming ready.
        '''

        if _USE_POLL:
            poll = select.poll()
            poll.register(self.fd, self.mask)

            log.debug('calling poll')
            if timeout:
                return bool(poll.poll(timeout*1000)) # convert to ms
            else:
                return bool(poll.poll())
        else:
            read_fds = (self.fd,) if self.mask & POLLIN else ()
            write_fds = (self.fd,) if self.mask & POLLOUT else ()

            log.debug('calling select')
            if timeout:
                return any(select.select(read_fds, write_fds, (), timeout))
            else:
                return any(select.select(read_fds, write_fds, ()))


class HTTPResponse:
    '''
    This class encapsulates information about HTTP response.  Instances of this
    class are returned by the `HTTPConnection.read_response` method and have
    access to response status, reason, and headers.  Response body data
    has to be read directly from the `HTTPConnection` instance.
    '''

    def __init__(self, method, path, status, reason, headers,
                 length=None):

        #: HTTP Method of the request this was response is associated with
        self.method = method

        #: Path of the request this was response is associated with
        self.path = path

        #: HTTP status code returned by the server
        self.status = status

        #: HTTP reason phrase returned by the server
        self.reason = reason

        #: HTTP Response headers, a `email.message.Message` instance
        self.headers = headers

        #: Length of the response body or `None` if not known. This attribute
        #: contains the actual length of the *transmitted* response. That means
        #: that for responses where RFC 2616 mandates that no request body
        #: be sent (e.g. in response to HEAD requests or for 1xx response
        #: codes) this value is zero. In these cases, the length of the body that
        #: *would* have been send can be extracted from the ``Content-Length``
        #: response header.
        self.length = length


class BodyFollowing:
    '''
    Sentinel class for the *body* parameter of the
    `~HTTPConnection.send_request` method. Passing an instance of this class
    declares that body data is going to be provided in separate method calls.

    If no length is specified in the constructor, the body data will be send
    using chunked encoding.
    '''

    __slots__ = 'length'

    def __init__(self, length=None):
        #: the length of the body data that is going to be send, or `None`
        #: to use chunked encoding.
        self.length = length


class _ChunkTooLong(Exception):
    '''
    Raised by `_co_readstr_until` if the requested end pattern
    cannot be found within the specified byte limit.
    '''

    pass


class _GeneralError(Exception):
    msg = 'General HTTP Error'

    def __init__(self, msg=None):
        if msg:
            self.msg = msg

    def __str__(self):
        return self.msg

class HostnameNotResolvable(Exception):
    '''Raised if a host name does not resolve to an ip address.

    Dugong raises this exception if a resolution attempt results in a `socket.gaierror` or
    `socket.herror` exception with errno :const:`!socket.EAI_NODATA` or
    :const:`!socket.EAI_NONAME`, but at least one of the hostnames in `DNS_TEST_HOSTNAMES`
    can be resolved.
    '''

    def __init__(self, hostname):
        self.name = hostname

    def __str__(self):
        return 'Host %s does not have any ip addresses' % self.name


class DNSUnavailable(Exception):
    '''Raised if the DNS server cannot be reached.

    Dugong raises this exception if a resolution attempt results in a `socket.gaierror` or
    `socket.herror` exception with errno :const:`socket.EAI_AGAIN`, or if none of the
    hostnames in `DNS_TEST_HOSTNAMES` can be resolved.
    '''

    def __init__(self, hostname):
        self.name = hostname

    def __str__(self):
        return 'Unable to resolve %s, DNS server unavailable.' % self.name


class HostnameNotResolvableOrDNSUnavailable(Exception):
    '''Raised if a host name does not resolve or DNS server cannot be reached.'''

    def __init__(self, hostname):
        self.name = hostname

    def __str__(self):
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
    that synchronization with the server will be lost (i.e., dugong cannot
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
    has closed the connection, call `HTTPConnection.reset` before further
    requests are send.

    This behavior is intentional, because the caller may have already issued
    other requests (i.e., used pipelining). By raising an exception, the caller
    is notified that any pending requests have been lost and need to be resend.
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

    def __init__(self, size):

        #: Holds the actual data
        self.d = bytearray(size)

        #: Position of the first buffered byte that has not yet
        #: been consumed ("*b*eginning")
        self.b = 0

        #: Fill-level of the buffer ("*e*nd")
        self.e = 0

    def __len__(self):
        '''Return amount of data ready for consumption'''
        return self.e - self.b

    def clear(self):
        '''Forget all buffered data'''

        self.b = 0
        self.e = 0

    def compact(self):
        '''Ensure that buffer can be filled up to its maximum size

        If part of the buffer data has been consumed, the unconsumed part is
        copied to the beginning of the buffer to maximize the available space.
        '''

        if self.b == 0:
            return

        log.debug('compacting buffer')
        buf = memoryview(self.d)[self.b:self.e]
        len_ = len(buf)
        self.d = bytearray(len(self.d))
        self.d[:len_] = buf
        self.b = 0
        self.e = len_

    def exhaust(self):
        '''Return (and consume) all available data'''

        if self.b == 0:
            log.debug('exhausting buffer (truncating)')
            # Return existing buffer after truncating it
            buf = self.d
            self.d = bytearray(len(self.d))
            buf[self.e:] = b''
        else:
            log.debug('exhausting buffer (copying)')
            buf = self.d[self.b:self.e]

        self.b = 0
        self.e = 0

        return buf


class HTTPConnection:
    '''
    This class encapsulates a HTTP connection.

    Methods whose name begin with ``co_`` return coroutines. Instead of
    blocking, a coroutines will yield a `PollNeeded` instance that encapsulates
    information about the IO operation that would block. The coroutine should be
    resumed once the operation can be performed without blocking.

    `HTTPConnection` instances can be used as context managers. The
    `.disconnect` method will be called on exit from the managed block.
    '''

    def __init__(self, hostname, port=None, ssl_context=None, proxy=None):

        if port is None:
            if ssl_context is None:
                self.port = HTTP_PORT
            else:
                self.port = HTTPS_PORT
        else:
            self.port = port

        self.ssl_context = ssl_context
        self.hostname = hostname

        #: Socket object connecting to the server
        self._sock = None

        #: Read-buffer
        self._rbuf = _Buffer(BUFFER_SIZE)

        #: a tuple ``(hostname, port)`` of the proxy server to use or `None`.
        self.proxy = proxy

        #: a deque of ``(method, path, body_len)`` tuples corresponding to
        #: requests whose response has not yet been read completely. Requests
        #: with Expect: 100-continue will be added twice to this queue, once
        #: after the request header has been sent, and once after the request
        #: body data has been sent. *body_len* is `None`, or the size of the
        #: **request** body that still has to be sent when using 100-continue.
        self._pending_requests = deque()

        #: This attribute is `None` when a request has been sent completely.  If
        #: request headers have been sent, but request body data is still
        #: pending, it is set to a ``(method, path, body_len)`` tuple. *body_len*
        #: is the number of bytes that that still need to send, or
        #: `WAITING_FOR_100c` if we are waiting for a 100 response from the server.
        self._out_remaining = None

        #: Number of remaining bytes of the current response body (or current
        #: chunk), `None` if there is no active response or `READ_UNTIL_EOF` if
        #: we have to read until the connection is closed (i.e., we don't know
        #: the content-length and keep-alive is not active).
        self._in_remaining = None

        #: Transfer encoding of the active response (if any).
        self._encoding = None

        #: If a regular `HTTPConnection` method is unable to send or receive
        #: data for more than this period (in seconds), it will raise
        #: `ConnectionTimedOut`. Coroutines are not affected by this
        #: attribute.
        self.timeout = None

        #: Filehandler for tracing
        self.trace_fh = None

    # Implement bare-bones `io.BaseIO` interface, so that instances
    # can be wrapped in `io.TextIOWrapper` if desired.
    def writable(self):
        return True
    def readable(self):
        return True
    def seekable(self):
        return False

    # One could argue that the stream should be considered closed if
    # there is no active response. However, this breaks TextIOWrapper
    # (which fails if the stream becomes closed even after b'' has
    # been read), so we just declare to be always open.
    closed = False

    def connect(self):
        """Connect to the remote server

        This method generally does not need to be called manually.
        """

        log.debug('start')

        if self.proxy:
            log.debug('connecting to %s', self.proxy)
            self._sock = create_socket(self.proxy)
            if self.ssl_context:
                eval_coroutine(self._co_tunnel(), self.timeout)
        else:
            log.debug('connecting to %s', (self.hostname, self.port))
            self._sock = create_socket((self.hostname, self.port))

        if self.ssl_context:
            log.debug('establishing ssl layer')
            if (sys.version_info >= (3, 5) or
                (sys.version_info >= (3, 4) and ssl.HAS_SNI)):
                # Automatic hostname verification was added in 3.4, but only
                # if SNI is available. In 3.5 the hostname can be checked even
                # if there is no SNI support.
                server_hostname = self.hostname
            else:
                server_hostname = None
            self._sock = self.ssl_context.wrap_socket(self._sock, server_hostname=server_hostname)

            if server_hostname is None:
                # Manually check hostname for Python < 3.4, or if we have
                # 3.4 without SNI.
                try:
                    ssl.match_hostname(self._sock.getpeercert(), self.hostname)
                except:
                    self.close()
                    raise

        self._sock.setblocking(False)
        self._rbuf.clear()
        self._out_remaining = None
        self._in_remaining = None
        self._pending_requests = deque()

        if 'DUGONG_TRACEFILE' in os.environ:
            self.trace_fh = open(os.environ['DUGONG_TRACEFILE'] % id(self._sock),
                                 'wb+', buffering=0)

        log.debug('done')

    def _co_tunnel(self):
        '''Set up CONNECT tunnel to destination server'''

        log.debug('start connecting to %s:%d', self.hostname, self.port)

        yield from self._co_send(("CONNECT %s:%d HTTP/1.0\r\n\r\n"
                                  % (self.hostname, self.port)).encode('latin1'))

        (status, reason) = yield from self._co_read_status()
        log.debug('got %03d %s', status, reason)
        yield from self._co_read_header()

        if status != 200:
            self.disconnect()
            raise ConnectionError("Tunnel connection failed: %d %s" % (status, reason))

    def get_ssl_peercert(self, binary_form=False):
        '''Get peer SSL certificate

        If plain HTTP is used, return `None`. Otherwise, the call is delegated
        to the underlying SSL sockets `~ssl.SSLSocket.getpeercert` method.
        '''

        if not self.ssl_context:
            return None
        else:
            if not self._sock:
                self.connect()
            return self._sock.getpeercert()

    def get_ssl_cipher(self):
        '''Get active SSL cipher

        If plain HTTP is used, return `None`. Otherwise, the call is delegated
        to the underlying SSL sockets `~ssl.SSLSocket.cipher` method.
        '''

        if not self.ssl_context:
            return None
        else:
            if not self._sock:
                self.connect()
            return self._sock.cipher()

    def send_request(self, method, path, headers=None, body=None, expect100=False):
        '''placeholder, will be replaced dynamically'''
        eval_coroutine(self.co_send_request(method, path, headers=headers,
                                            body=body, expect100=expect100),
                       self.timeout)

    def co_send_request(self, method, path, headers=None, body=None, expect100=False):
        '''Send a new HTTP request to the server

        The message body may be passed in the *body* argument or be sent
        separately. In the former case, *body* must be a :term:`bytes-like
        object`. In the latter case, *body* must be an a `BodyFollowing`
        instance specifying the length of the data that will be sent. If no
        length is specified, the data will be send using chunked encoding.

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
            self.connect()

        if self._out_remaining:
            raise StateError('body data has not been sent completely yet')

        if headers is None:
            headers = CaseInsensitiveDict()
        elif not isinstance(headers, CaseInsensitiveDict):
            headers = CaseInsensitiveDict(headers)

        pending_body_size = None
        if body is None:
            headers['Content-Length'] = '0'
        elif isinstance(body, BodyFollowing):
            if body.length is None:
                raise ValueError('Chunked encoding not yet supported.')
            log.debug('preparing to send %d bytes of body data', body.length)
            if expect100:
                headers['Expect'] = '100-continue'
                # Do not set _out_remaining, we must only send data once we've
                # read the response. Instead, save body size in
                # _pending_requests so that it can be restored by
                # read_response().
                pending_body_size = body.length
                self._out_remaining = (method, path, WAITING_FOR_100c)
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
            host = '[{}]'.format(host)
        default_port = HTTPS_PORT if self.ssl_context else HTTP_PORT
        if self.port == default_port:
            headers['Host'] = host
        else:
            headers['Host'] = '{}:{}'.format(host, self.port)

        # Assemble request
        headers['Accept-Encoding'] = 'identity'
        if 'Connection' not in headers:
            headers['Connection'] = 'keep-alive'
        if self.proxy and not self.ssl_context:
            gpath = "http://{}{}".format(headers['Host'], path)
        else:
            gpath = path
        request = [ '{} {} HTTP/1.1'.format(method, gpath).encode('latin1') ]
        for key, val in headers.items():
            request.append('{}: {}'.format(key, val).encode('latin1'))
        request.append(b'')

        if body is not None:
            request.append(body)
        else:
            request.append(b'')

        buf = b'\r\n'.join(request)

        log.debug('sending %s %s', method, path)
        yield from self._co_send(buf)
        if not self._out_remaining or expect100:
            self._pending_requests.append((method, path, pending_body_size))

    def _co_send(self, buf):
        '''Send *buf* to server'''

        log.debug('trying to send %d bytes', len(buf))

        if not isinstance(buf, memoryview):
            buf = memoryview(buf)

        while True:
            try:
                if self._sock is None:
                    raise ConnectionClosed('connection has been closed locally')
                if self.trace_fh:
                    self.trace_fh.write(buf)
                len_ = self._sock.send(buf)
                # An SSL socket has the nasty habit of returning zero
                # instead of raising an exception when in non-blocking
                # mode.
                if len_ == 0:
                    raise BlockingIOError()
            except (socket.timeout, ssl.SSLWantWriteError, BlockingIOError):
                log.debug('yielding')
                yield PollNeeded(self._sock.fileno(), POLLOUT)
                continue
            except (BrokenPipeError, ConnectionResetError):
                raise ConnectionClosed('connection was interrupted')
            except OSError as exc:
                if exc.errno == errno.EINVAL:
                    # Blackhole routing, according to ip(7)
                    raise ConnectionClosed('ip route goes into black hole')
                else:
                    raise
            except InterruptedError:
                log.debug('interrupted')
                # According to send(2), this means that no data has been sent
                # at all before the interruption, so we just try again.
                continue

            log.debug('sent %d bytes', len_)
            buf = buf[len_:]
            if len(buf) == 0:
                log.debug('done')
                return

    def write(self, buf):
        '''placeholder, will be replaced dynamically'''
        eval_coroutine(self.co_write(buf), self.timeout)

    def co_write(self, buf):
        '''Write request body data

        `ExcessBodyData` will be raised when attempting to send more data than
        required to complete the request body of the active request.
        '''

        log.debug('start (len=%d)', len(buf))

        if not self._out_remaining:
            raise StateError('No active request with pending body data')

        (method, path, remaining) = self._out_remaining
        if remaining is WAITING_FOR_100c:
            raise StateError("can't write when waiting for 100-continue")

        if len(buf) > remaining:
            raise ExcessBodyData('trying to write %d bytes, but only %d bytes pending'
                                    % (len(buf), remaining))

        try:
            yield from self._co_send(buf)
        except ConnectionClosed:
            # If the server closed the connection, we pretend that all data
            # has been sent, so that we can still read a (buffered) error
            # response.
            self._out_remaining = None
            self._pending_requests.append((method, path, None))
            raise

        len_ = len(buf)
        if len_ == remaining:
            log.debug('body sent fully')
            self._out_remaining = None
            self._pending_requests.append((method, path, None))
        else:
            self._out_remaining = (method, path, remaining - len_)

        log.debug('done')

    def response_pending(self):
        '''Return `True` if there are still outstanding responses

        This includes responses that have been partially read.
        '''

        return self._sock is not None and len(self._pending_requests) > 0

    def read_response(self):
        '''placeholder, will be replaced dynamically'''
        return eval_coroutine(self.co_read_response(), self.timeout)

    def co_read_response(self):
        '''Read response status line and headers

        Return a `HTTPResponse` instance containing information about response
        status, reason, and headers. The response body data must be retrieved
        separately (e.g. using `.read` or `.readall`).
        '''

        log.debug('start')

        if len(self._pending_requests) == 0:
            raise StateError('No pending requests')

        if self._in_remaining is not None:
            raise StateError('Previous response not read completely')

        (method, path, body_size) = self._pending_requests[0]

        # Need to loop to handle any 1xx responses
        while True:
            (status, reason) = yield from self._co_read_status()
            log.debug('got %03d %s', status, reason)

            hstring = yield from self._co_read_header()
            header = email.message_from_string(hstring, policy=email.policy.HTTP)

            if status < 100 or status > 199:
                break

            # We are waiting for 100-continue
            if body_size is not None and status == 100:
                break

        # Handle (expected) 100-continue
        if status == 100:
            assert self._out_remaining == (method, path, WAITING_FOR_100c)

            # We're ready to sent request body now
            self._out_remaining = self._pending_requests.popleft()
            self._in_remaining = None

            # Return early, because we don't have to prepare
            # for reading the response body at this time
            return HTTPResponse(method, path, status, reason, header, length=0)

        # Handle non-100 status when waiting for 100-continue
        elif body_size is not None:
            assert self._out_remaining == (method, path, WAITING_FOR_100c)
            # RFC 2616 actually states that the server MAY continue to read
            # the request body after it has sent a final status code
            # (http://tools.ietf.org/html/rfc2616#section-8.2.3). However,
            # that totally defeats the purpose of 100-continue, so we hope
            # that the server behaves sanely and does not attempt to read
            # the body of a request it has already handled. (As a side note,
            # this ambuigity in the RFC also totally breaks HTTP pipelining,
            # as we can never be sure if the server is going to expect the
            # request or some request body data).
            self._out_remaining = None

        #
        # Prepare to read body
        #
        body_length = self._setup_read(method, status, header)

        # Don't require calls to co_read() et al if there is
        # nothing to be read.
        if self._in_remaining is None:
            self._pending_requests.popleft()

        log.debug('done (in_remaining=%s)', self._in_remaining)
        return HTTPResponse(method, path, status, reason, header, body_length)

    def _setup_read(self, method, status, header):
        '''Prepare for reading response body

        Sets up `._encoding`, `_in_remaining` and returns Content-Length
        (if available).

        See RFC 2616, sec. 4.4 for specific rules.
        '''

        # On error, the exception is stored in _encoding and raised on
        # the next call to co_read() et al - that way we can still
        # return the http status and headers.

        will_close = header.get('Connection', 'keep-alive').lower() == 'close'

        body_length = header['Content-Length']
        if body_length is not None:
            try:
                body_length = int(body_length)
            except ValueError:
                self._encoding = InvalidResponse('Invalid content-length: %s'
                                                 % body_length)
                self._in_remaining = RESPONSE_BODY_ERROR
                return None

        if (status == NO_CONTENT or status == NOT_MODIFIED or
            100 <= status < 200 or method == 'HEAD'):
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
            self._in_remaining = RESPONSE_BODY_ERROR
            return None

        log.debug('identity encoding detected')
        self._encoding = Encodings.IDENTITY

        if body_length is not None:
            log.debug('Will read response body of %d bytes', body_length)
            self._in_remaining = body_length or None
            return body_length

        if will_close:
            log.debug('no content-length, will read until EOF')
            self._in_remaining = READ_UNTIL_EOF
            return None

        log.debug('no content length and no chunked encoding, will raise on read')
        self._encoding = UnsupportedResponse('No content-length and no chunked encoding')
        self._in_remaining = RESPONSE_BODY_ERROR
        return None

    def _co_read_status(self):
        '''Read response line'''

        log.debug('start')

        # read status
        try:
            line = yield from self._co_readstr_until(b'\r\n', MAX_LINE_SIZE)
        except _ChunkTooLong:
            raise InvalidResponse('server send ridicously long status line')

        try:
            version, status, reason = line.split(None, 2)
        except ValueError:
            try:
                version, status = line.split(None, 1)
                reason = ""
            except ValueError:
                # empty version will cause next test to fail.
                version = ""

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

    def _co_read_header(self):
        '''Read response header'''

        log.debug('start')

        # Peek into buffer. If the first characters are \r\n, then the header
        # is empty (so our search for \r\n\r\n would fail)
        rbuf = self._rbuf
        if len(rbuf) < 2:
            yield from self._co_fill_buffer(2)
        if rbuf.d[rbuf.b:rbuf.b+2] == b'\r\n':
            log.debug('done (empty header)')
            rbuf.b += 2
            return ''

        try:
            hstring = yield from self._co_readstr_until(b'\r\n\r\n', MAX_HEADER_SIZE)
        except _ChunkTooLong:
            raise InvalidResponse('server sent ridicously long header')

        log.debug('done (%d characters)', len(hstring))
        return hstring

    def read(self, len_=None):
        '''placeholder, will be replaced dynamically'''
        if len_ is None:
            return self.readall()
        buf = eval_coroutine(self.co_read(len_), self.timeout)

        # Some modules like TextIOWrapper unfortunately rely on read()
        # to return bytes, and do not accept bytearrays or memoryviews.
        # cf. http://bugs.python.org/issue21057
        if sys.version_info < (3,5,0) and not isinstance(buf, bytes):
            buf = bytes(buf)
        return buf

    def co_read(self, len_=None):
        '''Read up to *len_* bytes of response body data

        This method may return less than *len_* bytes, but will return ``b''`` only
        if the response body has been read completely.

        If *len_* is `None`, this method returns the entire response body.
        '''

        log.debug('start (len=%d)', len_)

        if self._in_remaining is RESPONSE_BODY_ERROR:
            raise self._encoding
        elif len_ is None:
            return (yield from self.co_readall())
        elif len_ == 0 or self._in_remaining is None:
            return b''
        elif self._encoding is Encodings.IDENTITY:
            return (yield from self._co_read_id(len_))
        elif self._encoding is Encodings.CHUNKED:
            return (yield from self._co_read_chunked(len_=len_))
        else:
            raise RuntimeError('ooops, this should not be possible')

    def read_raw(self, size):
        '''Read *size* bytes of uninterpreted data

        This method may be used even after `UnsupportedResponse` or
        `InvalidResponse` has been raised. It reads raw data from the socket
        without attempting to interpret it. This is probably only useful for
        debugging purposes to take a look at the raw data received from the
        server. This method blocks if no data is available, and returns ``b''``
        if the connection has been closed.

        Calling this method will break the internal state and switch the socket
        to blocking operation. The connection has to be closed and reestablished
        afterwards.

        **Don't use this method unless you know exactly what you are doing**.
        '''

        if self._sock is None:
            raise ConnectionClosed('connection has been closed locally')

        self._sock.setblocking(True)

        buf = bytearray()
        rbuf = self._rbuf
        while len(buf) < size:
            len_ = min(size - len(buf), len(rbuf))
            if len_ < len(rbuf):
                buf += rbuf.d[rbuf.b:rbuf.b+len_]
                rbuf.b += len_
            elif len_ == 0:
                buf2 = self._sock.recv(size - len(buf))
                if not buf2:
                    break
                if self.trace_fh:
                    self.trace_fh.write(buf2)
                buf += buf2
            else:
                buf += rbuf.exhaust()

        return buf

    def readinto(self, buf):
        '''placeholder, will be replaced dynamically'''
        return eval_coroutine(self.co_readinto(buf), self.timeout)

    def co_readinto(self, buf):
        '''Read response body data into *buf*

        Return the number of bytes written or zero if the response body has been
        read completely.

        *buf* must implement the memoryview protocol.
        '''

        log.debug('start (buflen=%d)', len(buf))

        if self._in_remaining is RESPONSE_BODY_ERROR:
            raise self._encoding
        elif len(buf) == 0 or self._in_remaining is None:
            return 0
        elif self._encoding is Encodings.IDENTITY:
            return (yield from self._co_readinto_id(buf))
        elif self._encoding is Encodings.CHUNKED:
            return (yield from self._co_read_chunked(buf=buf))
        else:
            raise RuntimeError('ooops, this should not be possible')

    def _co_read_id(self, len_):
        '''Read up to *len* bytes of response body assuming identity encoding'''

        log.debug('start (len=%d)', len_)
        assert self._in_remaining is not None

        if not self._in_remaining:
            # Body retrieved completely, clean up
            self._in_remaining = None
            self._pending_requests.popleft()
            return b''

        rbuf = self._rbuf
        if self._in_remaining is not READ_UNTIL_EOF:
            len_ = min(len_, self._in_remaining)
        log.debug('updated len_=%d', len_)

        # If buffer is empty, reset so that we start filling from
        # beginning. This check is already done by _try_fill_buffer(), but we
        # have to do it here or we never enter the while loop if the buffer
        # is empty but has no capacity (rbuf.b == rbuf.e == len(rbuf.d))
        if rbuf.b == rbuf.e:
            rbuf.b = 0
            rbuf.e = 0

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
                    yield PollNeeded(self._sock.fileno(), POLLIN)
            elif got_data == 0:
                if self._in_remaining is READ_UNTIL_EOF:
                    log.debug('connection closed, %d bytes in buffer', len(rbuf))
                    self._in_remaining = len(rbuf)
                    break
                else:
                    raise ConnectionClosed('server closed connection')

        len_ = min(len_, len(rbuf))
        if self._in_remaining is not READ_UNTIL_EOF:
            self._in_remaining -= len_

        if len_ < len(rbuf):
            buf = rbuf.d[rbuf.b:rbuf.b+len_]
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
            self._pending_requests.popleft()

        log.debug('done (%d bytes)', len(buf))
        return buf

    def _co_readinto_id(self, buf):
        '''Read response body into *buf* assuming identity encoding'''

        log.debug('start (buflen=%d)', len(buf))

        assert self._in_remaining is not None
        if not self._in_remaining:
            # Body retrieved completely, clean up
            self._in_remaining = None
            self._pending_requests.popleft()
            return 0

        rbuf = self._rbuf
        if not isinstance(buf, memoryview):
            buf = memoryview(buf)
        if self._in_remaining is READ_UNTIL_EOF:
            len_ = len(buf)
        else:
            len_ = min(len(buf), self._in_remaining)
        log.debug('set len_=%d', len_)

        # First use read buffer contents
        pos = min(len(rbuf), len_)
        if pos:
            log.debug('using buffered data')
            buf[:pos] = rbuf.d[rbuf.b:rbuf.b+pos]
            rbuf.b += pos
            if self._in_remaining is not READ_UNTIL_EOF:
                self._in_remaining -= pos

            # If we've read enough, return immediately
            if pos == len_:
                log.debug('done (buffer filled completely)')
                return pos

            # Otherwise, prepare to read more from socket
            log.debug('got %d bytes from buffer', pos)
            assert not len(rbuf)

        while True:
            log.debug('trying to read from socket')
            if self._sock is None:
                raise ConnectionClosed('connection has been closed locally')
            try:
                read = self._sock.recv_into(buf[pos:len_])
                if self.trace_fh:
                    self.trace_fh.write(buf[pos:pos+read])
            except (ConnectionResetError, BrokenPipeError):
                raise ConnectionClosed('connection was interrupted')
            except (socket.timeout, ssl.SSLWantReadError, BlockingIOError):
                if pos:
                    log.debug('done, no additional data available')
                    return pos
                else:
                    log.debug('no data yet and nothing to read, yielding..')
                    yield PollNeeded(self._sock.fileno(), POLLIN)
                    continue

            if not read:
                if self._in_remaining is READ_UNTIL_EOF:
                    log.debug('reached EOF')
                    self._in_remaining = 0
                    return pos
                else:
                    raise ConnectionClosed('server closed connection')
            log.debug('got %d bytes from socket', read)

            if self._in_remaining is not READ_UNTIL_EOF:
                self._in_remaining -= read
            pos += read
            if pos == len_:
                log.debug('done (buffer filled completely)')
                return pos

    def _co_read_chunked(self, len_=None, buf=None):
        '''Read response body assuming chunked encoding

        If *len_* is not `None`, reads up to *len_* bytes of data and returns
        a `bytes-like object`. If *buf* is not `None`, reads data into *buf*.
        '''

        log.debug('start (%s mode)', 'readinto' if buf else 'read')
        assert (len_ is None) != (buf is None)
        assert bool(len_) or bool(buf)
        assert isinstance(self._in_remaining, int)

        if self._in_remaining == 0:
            log.debug('starting next chunk')
            try:
                line = yield from self._co_readstr_until(b'\r\n', MAX_LINE_SIZE)
            except _ChunkTooLong:
                raise InvalidResponse('could not find next chunk marker')

            i = line.find(";")
            if i >= 0:
                log.debug('stripping chunk extensions: %s', line[i:])
                line = line[:i] # strip chunk-extensions
            try:
                self._in_remaining = int(line, 16)
            except ValueError:
                raise InvalidResponse('Cannot read chunk size %r' % line[:20])

            log.debug('chunk size is %d', self._in_remaining)
            if self._in_remaining == 0:
                self._in_remaining = None
                self._pending_requests.popleft()

        if self._in_remaining is None:
            res = 0 if buf else b''
        elif buf:
            res = yield from self._co_readinto_id(buf)
        else:
            res = yield from self._co_read_id(len_)

        if not self._in_remaining:
            log.debug('chunk complete')
            yield from self._co_read_header()

        log.debug('done')
        return res

    def _co_readstr_until(self, substr, maxsize):
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
                buf = _join((parts[-1][-sub_len:],
                            rbuf.d[rbuf.b:min(rbuf.e, rbuf.b+sub_len-1)]))
                idx = buf.find(substr)
                if idx >= 0:
                    idx -= sub_len
                    break

            #log.debug('rbuf is: %s', rbuf.d[rbuf.b:min(rbuf.e, rbuf.b+512)])
            stop = min(rbuf.e, rbuf.b + maxsize)
            idx = rbuf.d.find(substr, rbuf.b, stop)

            if idx >= 0: # found
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
                    yield PollNeeded(self._sock.fileno(), POLLIN)
                elif res == 0:
                    raise ConnectionClosed('server closed connection')
                else:
                    break

        log.debug('found substr at %d', idx)
        idx += len(substr)
        buf = rbuf.d[rbuf.b:idx]
        rbuf.b = idx

        if parts:
            parts.append(buf)
            buf = _join(parts)

        try:
            return buf.decode('latin1')
        except UnicodeDecodeError:
            raise InvalidResponse('server response cannot be decoded to latin1')

    def _try_fill_buffer(self):
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
            rbuf.b = 0
            rbuf.e = 0

        # There should be free capacity
        assert rbuf.e < len(rbuf.d)

        if self._sock is None:
            raise ConnectionClosed('connection has been closed locally')

        try:
            len_ = self._sock.recv_into(memoryview(rbuf.d)[rbuf.e:])
            if self.trace_fh:
                self.trace_fh.write(rbuf.d[rbuf.e:rbuf.e+len_])
        except (socket.timeout, ssl.SSLWantReadError, BlockingIOError):
            log.debug('done (nothing ready)')
            return None
        except (ConnectionResetError, BrokenPipeError):
            raise ConnectionClosed('connection was interrupted')

        rbuf.e += len_
        log.debug('done (got %d bytes)', len_)
        return len_

    def _co_fill_buffer(self, len_):
        '''Make sure that there are at least *len_* bytes in buffer'''

        rbuf = self._rbuf
        if len_ > len(rbuf.d):
            raise ValueError('Requested more bytes than buffer has capacity')
        while len(rbuf) < len_:
            if len(rbuf.d) - rbuf.b < len_:
                self._rbuf.compact()
            res = self._try_fill_buffer()
            if res is None:
                yield PollNeeded(self._sock.fileno(), POLLIN)
            elif res == 0:
                raise ConnectionClosed('server closed connection')

    def readall(self):
        '''placeholder, will be replaced dynamically'''
        return eval_coroutine(self.co_readall(), self.timeout)

    def co_readall(self):
        '''Read and return complete response body'''

        if self._in_remaining is None:
            return b''

        log.debug('start')
        parts = []
        while True:
            buf = yield from self.co_read(BUFFER_SIZE)
            log.debug('got %d bytes', len(buf))
            if not buf:
                break
            parts.append(buf)
        buf = _join(parts)
        log.debug('done (%d bytes)', len(buf))
        if self.trace_fh:
            self.trace_fh.write(buf)
        return buf

    def discard(self):
        '''placeholder, will be replaced dynamically'''
        return eval_coroutine(self.co_discard(), self.timeout)

    def co_discard(self):
        '''Read and discard current response body'''

        if self._in_remaining is None:
            return

        log.debug('start')
        buf = memoryview(bytearray(BUFFER_SIZE))
        while True:
            len_ = yield from self.co_readinto(buf)
            if not len_:
                break
            log.debug('discarding %d bytes', len_)
        log.debug('done')

    def reset(self):
        '''Reset HTTP connection

        This method resets the status of the HTTP connection after an exception
        has occured. Any cached data and pending responses are discarded.
        '''
        self.disconnect()

    def disconnect(self):
        '''Close HTTP connection'''

        log.debug('start')
        if self.trace_fh:
            self.trace_fh.close()
        if self._sock:
            try:
                self._sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                # When called to reset after connection problems, socket
                # may have shut down already.
                pass
            self._sock.close()
            self._sock = None
            self._rbuf.clear()
        else:
            log.debug('already closed')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.disconnect()
        return False

def _extend_HTTPConnection_docstrings():

    co_suffix = '\n\n' + textwrap.fill(
        'This method returns a coroutine. `.%s` is a regular method '
        'implementing the same functionality.', width=78)
    reg_suffix = '\n\n' + textwrap.fill(
        'This method may block. `.co_%s` provides a coroutine '
        'implementing the same functionality without blocking.', width=78)

    for name in ('read', 'read_response', 'readall', 'readinto', 'send_request',
                 'write', 'discard'):
        fn = getattr(HTTPConnection, name)
        cofn = getattr(HTTPConnection, 'co_' + name)

        fn.__doc__ = getdoc(cofn) + reg_suffix % name
        cofn.__doc__ = getdoc(cofn) + co_suffix % name

_extend_HTTPConnection_docstrings()

if sys.version_info < (3,4,0):
    def _join(parts):
        '''Join a sequence of byte-like objects

        This method is necessary because `bytes.join` does not work with
        memoryviews prior to Python 3.4.
        '''

        size = 0
        for part in parts:
            size += len(part)

        buf = bytearray(size)
        i = 0
        for part in parts:
            len_ = len(part)
            buf[i:i+len_] = part
            i += len_

        return buf
else:
    def _join(parts):
        return b''.join(parts)

def eval_coroutine(crt, timeout=None):
    '''Evaluate *crt* (polling as needed) and return its result

    If *timeout* seconds pass without being able to send or receive
    anything, raises `ConnectionTimedOut`.
    '''

    try:
        while True:
            log.debug('polling')
            if not next(crt).poll(timeout=timeout):
                raise ConnectionTimedOut()
    except StopIteration as exc:
        return exc.value

def create_socket(address):
    '''Create socket connected to *address**

    Use `socket.create_connection` to create a connected socket and return
    it. If a DNS related exception is raised, capture it and attempt to
    determine if the dns server is not reachable, or if the host name could
    not be resolved. Then raise either `NoSuchAddress` or
    `NameResolutionError` as appropriate.

    To distinguish between an unresolvable hostname and a problem with the
    DNS server, attempt to resolve the addresses in `DNS_TEST_HOSTNAMES`. If
    at least one test hostname can be resolved, assume that the DNS server
    is available and that *address* can not be resolved.
    '''

    def try_connect(fn, arg):
        try:
            return fn()
        except (socket.gaierror, socket.herror) as exc:
            if exc.errno == socket.EAI_AGAIN:
                raise DNSUnavailable(arg)
            elif exc.errno in (socket.EAI_NODATA, socket.EAI_NONAME):
                raise HostnameNotResolvableOrDNSUnavailable(arg)
            else:
                raise

    try:
        return try_connect(lambda: socket.create_connection(address), address[0])
    except HostnameNotResolvableOrDNSUnavailable:
        pass

    # Unfortunately, some exceptions do not help us to distinguish between
    # permanent and temporary problems (cf. https://stackoverflow.com/questions/24855168/,
    # https://stackoverflow.com/questions/24855669/). Therefore, we try to resolve
    # "well-known" hosts to resolve the ambiguity.

    for (hostname, port) in DNS_TEST_HOSTNAMES:
        try:
            try_connect(lambda: socket.getaddrinfo(hostname, port), address[0])
        except HostnameNotResolvableOrDNSUnavailable:
            pass
        else:
            # Reachable, now try to resolve original address again (maybe dns was only
            # down briefly)
            break
    else:
        # No host was reachable
        raise DNSUnavailable(address[0])

    # Try to connect to original host again
    try:
        return try_connect(lambda: socket.create_connection(address), address[0])
    except HostnameNotResolvableOrDNSUnavailable:
        raise HostnameNotResolvable(address[0])


def is_temp_network_error(exc):
    '''Return true if *exc* represents a potentially temporary network problem

    DNS resolution errors (`socket.gaierror` or `socket.herror`) are considered
    permanent, because `HTTPConnection` employs a heuristic to convert these
    exceptions to `HostnameNotResolvable` or `DNSUnavailable` instead.
    '''

    if isinstance(exc, (socket.timeout, ConnectionError, TimeoutError, InterruptedError,
                        ConnectionClosed, ssl.SSLZeroReturnError, ssl.SSLEOFError,
                        ssl.SSLSyscallError, ConnectionTimedOut, DNSUnavailable)):
        return True

    elif isinstance(exc, OSError):
        # We have to be careful when retrieving errno codes, because
        # not all of them may exist on every platform.
        for errcode in ('EHOSTDOWN', 'EHOSTUNREACH', 'ENETDOWN',
                        'ENETRESET', 'ENETUNREACH', 'ENOLINK',
                        'ENONET', 'ENOTCONN', 'ENXIO', 'EPIPE',
                        'EREMCHG', 'ESHUTDOWN', 'ETIMEDOUT', 'EAGAIN'):
            try:
                if getattr(errno, errcode) == exc.errno:
                    return True
            except AttributeError:
                pass

    return False


class CaseInsensitiveDict(MutableMapping):
    """A case-insensitive `dict`-like object.

    Implements all methods and operations of
    :class:`collections.abc.MutableMapping` as well as `.copy`.

    All keys are expected to be strings. The structure remembers the case of the
    last key to be set, and :meth:`!iter`, :meth:`!keys` and :meth:`!items` will
    contain case-sensitive keys. However, querying and contains testing is case
    insensitive::

        cid = CaseInsensitiveDict()
        cid['Accept'] = 'application/json'
        cid['aCCEPT'] == 'application/json' # True
        list(cid) == ['Accept'] # True

    For example, ``headers['content-encoding']`` will return the value of a
    ``'Content-Encoding'`` response header, regardless of how the header name
    was originally stored.

    If the constructor, :meth:`!update`, or equality comparison operations are
    given multiple keys that have equal lower-case representions, the behavior
    is undefined.
    """

    def __init__(self, data=None, **kwargs):
        self._store = dict()
        if data is None:
            data = {}
        self.update(data, **kwargs)

    def __setitem__(self, key, value):
        # Use the lowercased key for lookups, but store the actual
        # key alongside the value.
        self._store[key.lower()] = (key, value)

    def __getitem__(self, key):
        return self._store[key.lower()][1]

    def __delitem__(self, key):
        del self._store[key.lower()]

    def __iter__(self):
        return (casedkey for casedkey, mappedvalue in self._store.values())

    def __len__(self):
        return len(self._store)

    def lower_items(self):
        """Like :meth:`!items`, but with all lowercase keys."""
        return (
            (lowerkey, keyval[1])
            for (lowerkey, keyval)
            in self._store.items()
        )

    def __eq__(self, other):
        if isinstance(other, Mapping):
            other = CaseInsensitiveDict(other)
        else:
            return NotImplemented
        # Compare insensitively
        return dict(self.lower_items()) == dict(other.lower_items())

    # Copy is required
    def copy(self):
         return CaseInsensitiveDict(self._store.values())

    def __repr__(self):
        return '%s(%r)' % (self.__class__.__name__, dict(self.items()))


if asyncio:
    class AioFuture(asyncio.Future):
        '''
        This class wraps a coroutine that yields `PollNeeded` instances
        into an `asyncio` compatible `~asyncio.Future`.

        This is done by registering a callback with the event loop that resumes
        the coroutine when the requested IO is available.
        '''

        #: Set of fds that that any `_Future` instance currently has
        #: read callbacks registered for (class attribute)
        _read_fds = dict()

        #: Set of fds that that any `_Future` instance currently has
        #: write callbacks registered for (class attribute)
        _write_fds = dict()

        def __init__(self, crt, loop=None):
            super().__init__(loop=loop)
            self._crt = crt

            #: The currently pending io request (that we have registered
            #: callbacks for).
            self._io_req = None

            self._loop.call_soon(self._resume_crt)

        def _resume_crt(self, exc=None):
            '''Resume coroutine

            If coroutine has completed, mark self as done. Otherwise, reschedule
            call when requested io is available. If *exc* is specified, raise
            *exc* in coroutine.
            '''

            log.debug('start')
            try:
                if exc is not None:
                    io_req = self._crt.throw(exc)
                else:
                    io_req = next(self._crt)
            except Exception as exc:
                if isinstance(exc, StopIteration):
                    log.debug('coroutine completed')
                    self.set_result(exc.value)
                else:
                    log.debug('coroutine raised exception')
                    self.set_exception(exc)
                io_req = self._io_req
                if io_req:
                    # This is a bit fragile.. what if there is more than one
                    # reader or writer? However, in practice this should not be
                    # the case: they would read or write unpredictable parts of
                    # the input/output.
                    if io_req.mask & POLLIN:
                        self._loop.remove_reader(io_req.fd)
                        del self._read_fds[io_req.fd]
                    if io_req.mask & POLLOUT:
                        self._loop.remove_writer(io_req.fd)
                        del self._write_fds[io_req.fd]
                    self._io_req = None
                return

            if not isinstance(io_req, PollNeeded):
                self._loop.call_soon(self._resume_crt,
                                     TypeError('Coroutine passed to asyncio_future did not yield '
                                               'PollNeeded instance!'))
                return

            if io_req.mask & POLLIN:
                reader = self._read_fds.get(io_req.fd, None)
                if reader is None:
                    log.debug('got poll needed, registering reader')
                    self._loop.add_reader(io_req.fd, self._resume_crt)
                    self._read_fds[io_req.fd] = self
                elif reader is self:
                    log.debug('got poll needed, reusing read callback')
                else:
                    self._loop.call_soon(self._resume_crt,
                                         RuntimeError('There is already a read callback for this socket'))
                    return

            if io_req.mask & POLLOUT:
                writer = self._write_fds.get(io_req.fd, None)
                if writer is None:
                    log.debug('got poll needed, registering writer')
                    self._loop.add_writer(io_req.fd, self._resume_crt)
                    self._write_fds[io_req.fd] = self
                elif writer is self:
                    log.debug('got poll needed, reusing write callback')
                else:
                    self._loop.call_soon(self._resume_crt,
                                         RuntimeError('There is already a write callback for this socket'))
                    return

            self._io_req = io_req
