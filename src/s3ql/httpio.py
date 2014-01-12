'''
httpio.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

import socket
import logging
import errno
import ssl
from collections import deque
import email.parser
from http.client import (LineTooLong, HTTPS_PORT, HTTP_PORT, NO_CONTENT, NOT_MODIFIED)
from .common import BUFSIZE
from select import select

log = logging.getLogger(__name__)

# maximal line length when calling readline().
_MAXLINE = 65536

CHUNKED_ENCODING = 'chunked_encoding'
IDENTITY_ENCODING = 'identity_encoding'

# Marker object for request body size when we're waiting
# for a 100-continue response from the server
WAITING_FOR_100c = object()

class _GeneralError(Exception):
    msg = 'General HTTP Error'

    def __init__(self, msg=None):
        if msg:
            self.msg = msg

    def __str__(self):
        return self.msg


class StateError(_GeneralError):
    '''
    Raised when attempting an operation that doesn't make
    sense in the current connection state.
    '''

    msg = 'Operation invalid in current connection state'


class ExcessBodyData(_GeneralError):
    msg = 'Cannot send larger request body than announced'


class InvalidResponse(_GeneralError):
    '''
    Raised if the server produced an invalid response.
    '''

    msg = 'Server sent invalid response'


class UnsupportedResponse(_GeneralError):
    '''
    Raised if the server produced a response that we do not
    support (e.g. with undefined length).
    '''

    msg = 'Server sent unsupported response'


class ConnectionClosed(_GeneralError):
    '''
    Raised if the server unexpectedly closed the connection.
    '''

    msg = 'connection closed unexpectedly'


class HTTPConnection:
    '''
    This class encapsulates a HTTP connection.

    In contrast to the standard library's http.client module, this class

     - allows you to send multiple requests right after each other without
       having to read the responses first.

     - supports waiting for 100-continue before sending the request body.

     - raises an exception instead of silently delivering partial data if the
       connection is closed before all data has been received.

     - raises one specific exception (ConnectionClosed) if the connection has
       been closed (while http.client connection may raise any of
       BrokenPipeError, BadStatusLineError, ConnectionAbortedError,
       ConnectionResetError, or simply return '' on read)

    These features come for a price:

     - It is recommended to use this class only for idempotent HTTP
       methods. This is because if a connection is terminated earlier than
       expected (e.g. because of the server sending an unsupported reply) but
       responses for multiple requests are pending, the client cannot determine
       which requests have been processed.

     - Only HTTP 1.1 connections are supported

     - Responses and requests *must* specify a Content-Length header when
       not using chunked encoding.

    If a server response doesn't fulfill the last two requirements, an
    `UnsupportedResponse` exception is raised. Typically, this means that
    synchronization with the server will be lost, so the connection needs to be
    reset by calling the `close` method.

    All request and response headers are represented as strings, but must be
    representable in latin1. Request and response body must be bytes.


    Avoiding Deadlocks
    ------------------

    The `HTTPConnection` class allows you to send an unlimited number of
    requests to the server before reading any of the responses. However, at some
    point the transmit and receive buffers on both the ends of the connection
    will fill up, and no more requests can be send before at least some of the
    responses are read, and attempts to send more data to the server will
    block. If the thread that attempts to send data is is also responsible for
    reading the responses, this will result in a deadlock.

    There are several ways to avoid this:
    
    - Do not send a new request before the last response has been read. This is
      the easiest solution, but it means that no HTTP pipelining can be used.

    - Use different threads for sending requests and receiving responses. This
      may or may not be easy to do in your application.

    - Use the *via_cofun* parameter of `send_request` to send requests, and a
      combination of `write` with *partial=True* and `select` to send request
      body data.

      
    Coroutine based API
    -------------------
    
    The last point warrants slightly more explanation. When called with
    ``via_cofun=True``, the `send_request` method does not send the request
    itself, but prepares and returns a cofunction (in form of a Python
    generator) that performs the actual transmission.  A cofunction is entered
    and resumed by passing it to the built-in `next` function. Execution of the
    cofunction is completed (and all data has been sent) when the `next` call
    raises `StopIteration`. This means that a cofunction can be conviently used
    as iterator in a for loop: whenever the cofunction suspends, the loop body
    will be executed, and then the coroutine resumed.

    The confunction based API is suitable to avoid deadlocks, because the
    cofunctions returned by this class will suspend sending request data as soon
    as (even partial) response data has been received from the server. To avoid
    congestion of the transmit buffers (and eventual deadlock), the caller is
    expected to read the available response data (using one of the ``read*``
    methods) before resuming the cofunction.
    
    In code, this looks as follows::

        documents = [ '/file_{}.html'.format(x) for x in range(10) ]
        conn = HTTPConnection('www.server.com')

        def read_response():
            nonlocal out_fh
            if conn.get_current_response(): 
                # Active response, so we are reading body
                out_fh.write(conn.read(8192))
            else:
                # No active response
                (method, url, status, reason, header) = conn.read_response()
                assert status == 200
                out_fh = open(url[1:], 'w')

        # Try to send all the requests...
        for doc in documents:
            cofun = conn.send_request('GET', doc, via_cofun=True)
            for _ in cofun: # value of _ is irrelevant and undefined
                # ..but interrupt is partial response data is available
                read_response()

        # All requests send, now read rest of responses
        while conn.response_pending():
            read_response()

            
    If request body data needs to be transmitted as well, a bit more work is
    needed. In principle, the `write` method could return a cofunction as
    well. However, this typically does not make sense as `write` itself is
    already called repeatedly until all data has been written. Instead, `write`
    therefore accepts *partial=True* argument, which causes it to write only as
    much data as currently fits into the transmit buffer (the actual number of
    bytes written is returned). As long as a prior `select` indicates that the
    connection is ready for writing, calls to `write` (with ``partial=True``)
    are then guaranteed not to block.

    For example, a number of large files could be uploaded using pipelining with
    the following code::

        from select import select
        from http.client import NO_CONTENT
        
        files = [ 'file_{}.tgz'.format(x) for x range(10) ]
        conn = HTTPConnection('www.server.com')

        def read_response():
            (method, url, status, reason, header) = conn.read_response()
            assert status == NO_CONTENT
            assert conn.read(42) == b''

        for name in files:
            cofun = conn.send_request('PUT', '/' + name, via_cofun=True,
                                      body=os.path.getsize(name))
            for _ in cofun:
                read_response()

            with open(name, 'rb') as fh:
                buf = b''
                while True:
                    (writeable, readable, _) = select((conn,), (conn,), ())
                    if readable:
                        read_response()

                    if not writeable:
                        continue

                    if not buf:
                        buf = fh.read(8192)
                        if not buf:
                            break

                    len_ = conn.write(buf, partial=True)
                    buf = buf[len_:]

        # All requests send, now read rest of responses
        while conn.response_pending():
            read_response()

            
    Since sending a file-like object as the request body is a rather common use
    case, there actually is a convenience `co_sendfile` method that provides a
    cofunction for this special case. Using `co_sendfile`, the outer loop in the
    above code can be written as::

        for name in files:
            cofun = conn.send_request('PUT', '/' + name, via_cofun=True,
                                      body=os.path.getsize(name))
            for _ in cofun:
                read_response()

            with open(name, 'rb') as fh:
                cofun = conn.co_sendfile(fh)
                for _ in cofun:
                    read_response()


    The use of coroutines and `select` allows pipelining with low latency and
    high throughput. However, it should be noted that even when using the
    techniques described above `HTTPConnection` instances do not provide a fully
    non-blocking API. Both the `read` and `read_response` methods may still
    block if insufficient data is available in the receive buffer. This is
    because `read_response` always reads the entire response header, and `read`
    always retrieves chunk headers completely. It is expected that this will not
    significantly impact throughput, as response headers are typically short
    enough to be transmitted in a single TCP packet, and the likelihood of a
    chunk header being split among two packets is very small.

    It is also important that `select` should only be used with `HTTPConnection`
    instances to avoid congestion when pipelining multiple requests, i.e. to
    interrupt transmitting request data when response data is available. In
    particular, a `select` call MUST NOT must not be used to wait for incoming
    data. This can lead to a deadlock, since server responses are buffered
    internally by the `HTTPConnection` instance and may thus be available for
    retrieval even if `select` reports that no data is available for reading
    from the socket.

    100-Continue Support
    --------------------

    When having to transfer large amounts of request bodies to the server, you
    typically do not want to sent all the data over the network just to find out
    that the server rejected the request because of e.g. insufficient
    permissions. To avoid this situation, HTTP 1.1 specifies the "100-continue"
    mechanism. When using 100-continue, the client transmits an additional
    "Expect: 100-continue" request header, and then waits for the server to
    reply with status "100 Continue" before sending the request body data. If
    the server instead responds with an error, the client can avoid pointless
    transmission of the request body.

    To use this mechanism with `httpio`, simply pass the *expect100* parameter
    to `send_request` and call `read_response` twice: once before sending body
    data, and a second time to read the final response::

        conn = HTTPConnection(hostname)
        conn.send_request('PUT', '/huge_file', body=os.path.getsize(filename),
                          expect100=True)
                          
        (method, url, status, reason, header) = conn.read_response()
        if status != 100:
            raise RuntimeError('Server said: %s' % reason)

        with open(filename, 'rb') as fh:
            for _ in conn.co_sendfile(fh):
                pass
                
        (method, url, status, reason, header) = conn.read_response()
        assert status in (200, 204)

        
    Attributes
    ----------

     :proxy:
          a tuple ``(hostname, port)`` of the proxy server to use or `None`.
     :_pending_requests:
          a deque of ``(method, url, body_len)`` tuples corresponding to
          requests whose response has not yet been read completely. Requests
          with Expect: 100-continue will be added twice to this queue, once
          after the request header has been sent, and once after the request
          body data has been sent. *body_len* is None, or the size of the
          **request** body that still has to be sent when using 100-continue.
     :_out_remaining:
          This attribute is None when a request has been sent completely.  If
          request headers have been sent, but request body data is still
          pending, it is set to a ``(method, url, body_len)`` tuple. *body_len*
          is the number of bytes that that still need to send, or
          WAITING_FOR_100c if we are waiting for a 100 response from the server.
     :_in_remaining:
          Number of remaining bytes of the current response body (or current
          chunk), or None if the response header has not yet been read.
     :_encoding:
          Transfer encoding of the active response (if any).
     :_coroutine_active:
          True if there is an active coroutine (there can be only one, since
          outgoing data from the different coroutines could get interleaved)
    '''

    def __init__(self, hostname, port=None, ssl_context=None, proxy=None):

        if port is None:
            if ssl_context is None:
                self.port = HTTP_PORT
            else:
                self.port = HTTPS_PORT
        else:
            self.port = port

        self.proxy = proxy
        self.ssl_context = ssl_context
        self.hostname = hostname
        self._sock_fh = None
        self._sock = None
        self._pending_requests = deque()
        self._out_remaining = None
        self._in_remaining = None
        self._encoding = None
        self._coroutine_active = False


    def _send(self, buf, partial=False):
        '''Send data over socket

        If partial is True, may send only part of the data.
        Return number of bytes sent.
        '''

        while True:
            try:
                if partial:
                    len_ = self._sock.send(buf)
                else:
                    self._sock.sendall(buf)
                    len_ = len(buf)
                break
            except BrokenPipeError:
                raise ConnectionClosed('found closed when trying to write') from None
            except OSError as exc:
                if exc.errno == errno.EINVAL:
                    # Blackhole routing, according to ip(7)
                    raise ConnectionClosed('ip route goes into black hole') from None
                else:
                    raise
            except InterruptedError:
                # According to send(2), this means that no data has been sent
                # at all before the interruption, so we just try again.
                pass

        return len_

    
    def _tunnel(self):
    
        self._send(("CONNECT %s:%d HTTP/1.0\r\n\r\n"
                    % (self.hostname, self.port)).encode('latin1'))

        self._sock_fh = self._sock.makefile(mode='rb', buffering=BUFSIZE)
        (status, reason) = self._read_status()
        self._read_header()

        if status != 200:
            self.close()
            raise OSError("Tunnel connection failed: %d %s" % (status, reason))
        
        self._sock_fh.detach()
        
    def connect(self):
        """Connect to the host and port specified in __init__

        This method generally does not need to be called manually.
        """

        if self.proxy:
            self._sock = socket.create_connection(self.proxy)
            self._tunnel()
        else:
            self._sock = socket.create_connection((self.hostname, self.port))

        if self.ssl_context:
            server_hostname = self.hostname if ssl.HAS_SNI else None
            self._sock = self.ssl_context.wrap_socket(self._sock, server_hostname=server_hostname)

            try:
                ssl.match_hostname(self._sock.getpeercert(), self.hostname)
            except:
                self.close()
                raise

        # Python <= 3.3's BufferedReader never reads more data than then
        # buffer size on read1(), so we make sure this is reasonably large.
        self._sock_fh = self._sock.makefile(mode='rb', buffering=BUFSIZE)
        self._out_remaining = None
        self._in_remaining = None
        self._pending_requests = deque()
  

    def close(self):
        '''Close HTTP connection'''

        if self._sock_fh:
            self._sock_fh.close()
            self._sock_fh = None

        if self._sock:
            try:
                self._sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                # When called to reset after connection problems, socket
                # may have shut down already.
                pass
            self._sock.close()
            self._sock = None


    def write(self, buf, partial=False):
        '''Write request body data

        `ExcessBodyData` will be raised when attempting to send more data than
        required to complete the request body of the active request.

        If *partial* is True, this method may write less than *buf*. The actual
        number of bytes written is returned.
        '''

        if not self._out_remaining:
            raise StateError('No active request with pending body data')

        (method, url, remaining) = self._out_remaining
        if remaining is WAITING_FOR_100c:
            raise StateError("can't write when waiting for 100-continue")
            
        if len(buf) > remaining:
            raise ExcessBodyData('trying to write %d bytes, but only %d bytes pending'
                                    % (len(buf), remaining))

        log.debug('trying to write %d bytes', len(buf))
        len_ = self._send(buf, partial)
        log.debug('wrote %d bytes', len_)
        if len_ == remaining:
            log.debug('body sent fully')
            self._out_remaining = None
            self._pending_requests.append((method, url, None))
        else:
            self._out_remaining = (method, url, remaining - len_)
        return len_

        
    def co_sendfile(self, fh):
        '''Return coroutine to send request body data from *fh*

        *fh* needs to have *readinto* method. The method will read and transfer
        only as much data as necessary to complete the request body of the
        active request.
        
        This method does not send any data but returns a coroutine in form of a
        generator.  The data must then be sent by repeatedly calling `next` on
        the generator until `StopIteration` is raised. A `next` call will return
        normally only if not all data has been send *and* response data is
        available in the receive buffer.
        '''

        if not hasattr(fh, 'readinto'):
            raise TypeError('*fh* needs to have a *readinto* method')

        if not self._out_remaining:
            raise StateError('No active request with pending body data')

        sock_tup = (self._sock,)

        if self._coroutine_active:
            raise RuntimeError('Cannot have multiple coroutines sending simultaneously')

        buf = bytearray(min(BUFSIZE, self._out_remaining[2]))
        sbuf = buf[:0]
        self._coroutine_active = True
        try:
            while True:
                log.debug('running select')
                (readable, writeable, _) = select(sock_tup, sock_tup, ())
                
                if readable:
                    log.debug('socket is readable, yielding')
                    yield

                if not writeable:
                    continue

                if len(sbuf) == 0:
                    log.debug('reading data from fh...')
                    len_ = fh.readinto(buf[:min(BUFSIZE, self._out_remaining[2])])
                    if not len_:
                        break
                    sbuf = buf[:len_]

                log.debug('sending data...')
                len_ = self.write(sbuf, partial=True)
                sbuf = sbuf[len_:]
                if not self._out_remaining:
                    break

        finally:
            self._coroutine_active = False


    def send_request(self, method, url, headers=None, body=None,
                     via_cofun=False, expect100=False):
        '''Send a new HTTP request to the server

        The message body may be passed in the *body* argument or be sent
        separately using the *send_data* method. In the later case, *body* must
        be an integer specifying the size of the data that will be sent.

        If *via_cofun* is True, this method does not actually send any data
        but returns a coroutine in form of a generator.  The request data must
        then be sent by repeatedly calling `next` on the generator until
        `StopIteration` is raised. A `next` call will return normally only if
        not all data has been send *and* response data is available in the
        receive buffer.
        '''

        if expect100 and not isinstance(body, int):
            raise ValueError('expect100 only allowed for separate body')
        
        if self._sock is None:
            log.debug('connection seems closed, reconnecting.')
            self.connect()

        if self._out_remaining:
            raise StateError('body data has not been sent completely yet')

        if headers is None:
            headers = dict()

        pending_body_size = None
        if body is None:
            headers['Content-Length'] = '0'
        elif isinstance(body, int):
            log.debug('preparing to send %d bytes of body data', body)
            if expect100:
                headers['Expect'] = '100-continue'
                # Do not set _out_remaining, we must only send data once we've
                # read the response. Instead, save body size in
                # _pending_requests so that it can be restored by
                # read_response().
                pending_body_size = body
                self._out_remaining = (method, url, WAITING_FOR_100c)
            else:
                self._out_remaining = (method, url, body)
            headers['Content-Length'] = str(body)
            body = None
        elif isinstance(body, (bytes, bytearray, memoryview)):
            headers['Content-Length'] = str(len(body))
        else:
            raise TypeError('*body* must be None, int or bytes-like')

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
        headers['Connection'] = 'keep-alive'
        request = [ '{} {} HTTP/1.1'.format(method, url).encode('latin1') ]
        for key, val in headers.items():
            request.append('{}: {}'.format(key, val).encode('latin1'))
        request.append(b'')

        if body is not None:
            request.append(body)
        else:
            request.append(b'')

        buf = b'\r\n'.join(request)

        if via_cofun:
            def success():
                log.debug('request for %s %s transmitted completely', method, url)
                if not self._out_remaining or expect100:
                    self._pending_requests.append((method, url, pending_body_size))
            return self._co_send_data(buf, completion_hook=success)
        else:
            log.debug('sending %s request for %s', method, url)
            self._send(buf)
            if not self._out_remaining or expect100:
                self._pending_requests.append((method, url, pending_body_size))


    def _co_send_data(self, buf, completion_hook=None):
        '''Return generator for sending *buf*

        This method is ignorant of the current connection state and
        for internal use only.

        *completion_hook* is called once all data has been sent.
        '''

        sock_tup = (self._sock,)

        if self._coroutine_active:
            raise RuntimeError('Cannot have multiple coroutines sending simultaneously')

        self._coroutine_active = True
        try:
            while True:
                log.debug('running select')
                (readable, writeable, _) = select(sock_tup, sock_tup, ())

                if readable:
                    log.debug('socket is readable, yielding')
                    yield

                if not writeable:
                    continue

                log.debug('sending data...')
                sent = self._send(buf, partial=True)
                if sent == len(buf):
                    break
                buf = buf[sent:]

            if completion_hook is not None:
                completion_hook()
        finally:
            self._coroutine_active = False


    def fileno(self):
        '''Return file no of underlying socket

        This allows HTTPConnection instances to be used in a `select`
        call. Due to internal buffering, data may be available for
        *reading* (but not writing) even if `select` indicates that
        the socket is not currently readable.
        '''

        return self._sock.fileno()

    
    def response_pending(self):
        '''Return True if there are still outstanding responses

        This includes responses that have been partially read.
        '''

        return len(self._pending_requests) > 0


    def get_current_response(self):
        '''Get method and URL of active response 

        Return None if there is no active response.
        '''

        if self._in_remaining is None:
            return None
        else:
            return self._pending_requests[0][:2]
    
    def read_response(self):
        '''Read response status line and headers

        Return a tuple (method, url, code, message, headers).

        Even for a response with empty body, the `read` method must be called
        once before the next response can be processed.
        '''
        
        if len(self._pending_requests) == 0:
            raise StateError('No pending requests')

        if self._in_remaining is not None:
            raise StateError('Previous response not read completely')

        (method, url, body_size) = self._pending_requests[0]

        # Need to loop to handle any 1xx responses
        while True:
            (status, reason) = self._read_status()
            log.debug('got %03d %s', status, reason)
            header = self._read_header()

            if status < 100 or status > 199:
                break

            # We are waiting for 100-continue
            if body_size is not None and status == 100:
                break
            
        # Handle (expected) 100-continue
        if status == 100:
            assert self._out_remaining == (method, url, WAITING_FOR_100c)

            # We're reading to sent request body now
            self._out_remaining = self._pending_requests.popleft()
            self._in_remaining = None

            # Return early, because we don't have to prepare
            # for reading the response body at this time
            return (method, url, status, reason, header)

        # Handle non-100 status when waiting for 100-continue
        elif body_size is not None:
            assert self._out_remaining == (method, url, WAITING_FOR_100c)
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
        
        tc = header['Transfer-Encoding']
        if tc:
            tc = tc.lower()
        if tc and tc == 'chunked':
            log.debug('Chunked encoding detected')
            self._encoding = CHUNKED_ENCODING
            self._in_remaining = 0
        elif tc and tc != 'identity':
            # Server must not sent anything other than identity or chunked,
            # so we raise InvalidResponse rather than UnsupportedResponse
            raise InvalidResponse('Cannot handle %s encoding' % tc)
        else:
            log.debug('identity encoding detected')
            self._encoding = IDENTITY_ENCODING

        # does the body have a fixed length? (of zero)
        if (status == NO_CONTENT or status == NOT_MODIFIED or
            100 <= status < 200 or method == 'HEAD'):
            log.debug('no content by RFC')
            self._in_remaining = 0
            # for these cases, there isn't even a zero chunk we could read
            self._encoding = IDENTITY_ENCODING

        # Chunked doesn't require content-length
        elif self._encoding is CHUNKED_ENCODING:
            pass

        # Otherwise we require a content-length. We defer raising
        # the exception to read(), so that we can still return
        # the headers and status.
        elif 'Content-Length' not in header:
            log.debug('no content length and no chuckend encoding, will raise on read')
            self._encoding = UnsupportedResponse('No content-length and no chunked encoding')
            self._in_remaining = 0
            
        else:
            self._in_remaining = int(header['Content-Length'])

        log.debug('setting up for %d byte body', self._in_remaining)
                
        return (method, url, status, reason, header)

    def _read_status(self):
        '''Read response line'''
        
        log.debug('reading response status line')

        # read status
        line = self._sock_fh.readline(_MAXLINE + 1)
        if len(line) > _MAXLINE:
            raise LineTooLong("status line too long")

        if not line:
            # Presumably, the server closed the connection before
            # sending a valid response.
            raise ConnectionClosed()

        line = line.decode('latin1')
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
            raise InvalidResponse('%s is not a valid status' % status) from None

        return (status, reason.strip())

    
    def _read_header(self):
        '''Read response header'''
        
        log.debug('reading response header')

        # In the long term we should just use email.parser.BytesParser with the
        # email.policy.HTTPPolicy. However, as of Python 3.3 the existence of
        # the later is only provisional, and the documentation isn't quite clear
        # about what encoding will be used when using the BytesParser.
        headers = []
        while True:
            line = self._sock_fh.readline(_MAXLINE + 1)
            if len(line) > _MAXLINE:
                raise LineTooLong("header line too long")
            log.debug('got %r', line)
            headers.append(line)
            if line in (b'\r\n', b'\n', b''):
                break
        hstring = b''.join(headers).decode('iso-8859-1')
        msg = email.parser.Parser().parsestr(hstring)

        return msg


    def readall(self):
        '''Read complete response body'''

        parts = []
        while True:
            buf = self.read(BUFSIZE)
            if not buf:
                break
            parts.append(buf)

        return b''.join(parts)

    def discard(self):
        '''Read and discard current response body'''

        while True:
            buf = self.read(BUFSIZE)
            if not buf:
                break
    
    def read(self, len_):
        '''Read len_ bytes of response body data
        
        This method may return less than *len_* bytes, but will return b'' only
        if the response body has been read completely. Further attempts to read
        more data after b'' has been returned will result in `StateError` being
        raised.
        '''
        
        if self._in_remaining is None:
            raise StateError('No active response with body')

        if not isinstance(len_, (int)):
            raise TypeError('*len_* must be int')

        if len_ == 0:
            return b''
        
        if self._encoding is IDENTITY_ENCODING:
            return self._read_id(len_)
        elif self._encoding is CHUNKED_ENCODING:
            return self._read_chunked(len_)
        elif isinstance(self._encoding, Exception):
            raise self._encoding
        else:
            raise RuntimeError('ooops, this should not be possible')

        
    def _read_id(self, len_):
        '''Read *len_* bytes from response body assuming identity encoding'''

        if self._in_remaining == 0:
            self._in_remaining = None
            self._pending_requests.popleft()
            return b''

        if len_ > self._in_remaining:
            len_ = self._in_remaining
        log.debug('trying to read %d bytes', len_)
        buf = self._sock_fh.read1(len_)
        self._in_remaining -= len(buf)

        if not buf:
            raise ConnectionClosed('connection closed with %d bytes outstanding'
                                   % self._in_remaining)
        return buf


    def _read_chunked(self, len_):
        '''Read *len_* bytes from response body assuming chunked encoding'''
        
        if self._in_remaining == 0:
            log.debug('starting next chunk')
            self._in_remaining = self._read_next_chunk_size()
            if self._in_remaining == 0:
                self._read_and_discard_trailer()
                self._in_remaining = None
                self._pending_requests.popleft()
                return b''

        buf = self._read_id(len_)
        
        if self._in_remaining == 0:
            log.debug('chunk completed')
            # toss the CRLF at the end of the chunk
            if len(self._sock_fh.read(2)) != 2:
                raise ConnectionClosed('connection closed with 2 bytes outstanding')

        return buf


    def _read_next_chunk_size(self):
        log.debug('reading next chunk size')

        # Read the next chunk size from the file
        line = self._sock_fh.readline(_MAXLINE + 1)
        if not line:
            raise ConnectionClosed('connection closed before final chunk')
        if len(line) > _MAXLINE:
            raise LineTooLong("chunk size")
        i = line.find(b";")
        if i >= 0:
            line = line[:i] # strip chunk-extensions
        try:
            return int(line, 16)
        except ValueError:
            raise InvalidResponse('Cannot read chunk size %r' % line) from None


    def _read_and_discard_trailer(self):
        log.debug('discarding chunk trailer')

        # read and discard trailer up to the CRLF terminator
        ### note: we shouldn't have any trailers!
        while True:
            line = self._sock_fh.readline(_MAXLINE + 1)
            if len(line) > _MAXLINE:
                raise LineTooLong("trailer line")
            if not line:
                # a vanishingly small number of sites EOF without
                # sending the trailer
                break
            if line in (b'\r\n', b'\n', b''):
                break


def is_temp_network_error(exc):
    '''Return true if *exc* represents a potentially temporary network problem'''

    if isinstance(exc, (socket.timeout, ConnectionError, TimeoutError, InterruptedError,
                        ConnectionClosed, ssl.SSLZeroReturnError, ssl.SSLEOFError,
                        ssl.SSLSyscallError)):
        return True

    # Formally this is a permanent error. However, it may also indicate
    # that there is currently no network connection to the DNS server
    elif (isinstance(exc, (socket.gaierror, socket.herror))
          and exc.errno in (socket.EAI_AGAIN, socket.EAI_NONAME)):
        return True

    return False
