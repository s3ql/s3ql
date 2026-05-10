#!/usr/bin/env python3
'''
t0_http.py - this file is part of S3QL.

Copyright © 2014 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

if __name__ == '__main__':
    import sys

    import pytest

    sys.exit(pytest.main([__file__] + sys.argv[1:]))


import contextlib
import hashlib
import html
import http.client
import io
import os
import re
import socket
import socketserver
import ssl
import threading
import time
from base64 import b64encode
from http.server import BaseHTTPRequestHandler

import pytest

import s3ql.http
from s3ql.http import (
    CaseInsensitiveDict,
    ConnectionClosed,
    DNSUnavailable,
    ExcessBodyData,
    HostnameNotResolvable,
    HTTPConnection,
    InvalidResponse,
    StateError,
)

# We want to test with a real certificate
SSL_TEST_HOST = 'www.google.com'

TEST_DIR = os.path.dirname(__file__)


def _client_ssl_context():
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    ctx.verify_mode = ssl.CERT_REQUIRED
    return ctx


def check_for_internet_access():
    ssl_context = _client_ssl_context()
    ssl_context.set_default_verify_paths()
    try:
        conn = http.client.HTTPSConnection(SSL_TEST_HOST, context=ssl_context)
        conn.request('GET', '/')
        resp = conn.getresponse()
        assert resp.status in (200, 301, 302)
        return True
    except:  # noqa: E722
        return False
    finally:
        conn.close()


no_internet_access = not check_for_internet_access()


class HTTPServer(socketserver.TCPServer):
    def get_request(self):
        (sock, addr) = super().get_request()
        if self.ssl_context:
            sock = self.ssl_context.wrap_socket(sock, server_side=True)
        return (sock, addr)


class HTTPServerThread(threading.Thread):
    def __init__(self, use_ssl=False):
        super().__init__()
        self.host = 'localhost'
        self.httpd = HTTPServer((self.host, 0), MockRequestHandler)
        self.port = self.httpd.socket.getsockname()[1]
        self.use_ssl = use_ssl
        self.timeout = 1

        if use_ssl:
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            ssl_context.minimum_version = ssl.TLSVersion.TLSv1_2
            ssl_context.verify_mode = ssl.CERT_NONE
            ssl_context.load_cert_chain(
                os.path.join(TEST_DIR, 'server.crt'), os.path.join(TEST_DIR, 'server.key')
            )
            self.httpd.ssl_context = ssl_context
        else:
            self.httpd.ssl_context = None

    def run(self):
        self.httpd.serve_forever()

    def shutdown(self):
        self.httpd.shutdown()
        self.httpd.server_close()


# Run tests with both SSL and plain HTTP, unless the test is annotated with
# a `no_ssl` marker.
def pytest_generate_tests(metafunc):
    if 'http_server' not in metafunc.fixturenames:
        return

    if metafunc.definition.get_closest_marker('no_ssl'):
        params = ('plain',)
    else:
        params = ('plain', 'ssl')

    metafunc.parametrize("http_server", params, indirect=True, scope='module')


@pytest.fixture()
def http_server(request):
    httpd = HTTPServerThread(use_ssl=(request.param == 'ssl'))
    httpd.start()
    request.addfinalizer(httpd.shutdown)
    return httpd


@pytest.fixture()
async def conn(http_server):
    if http_server.use_ssl:
        ssl_context = _client_ssl_context()
        ssl_context.load_verify_locations(cafile=os.path.join(TEST_DIR, 'ca.crt'))
    else:
        ssl_context = None
    conn = HTTPConnection(http_server.host, port=http_server.port, ssl_context=ssl_context)
    conn.timeout = 0.5
    try:
        yield conn
    finally:
        await conn.aclose()


@pytest.fixture(scope='module')
def random_fh(request):
    fh = open('/dev/urandom', 'rb')  # noqa: SIM115
    request.addfinalizer(fh.close)
    return fh


@pytest.mark.skipif(no_internet_access, reason='no internet access available')
async def test_connect_ssl():
    ssl_context = _client_ssl_context()
    ssl_context.set_default_verify_paths()

    conn = HTTPConnection(SSL_TEST_HOST, ssl_context=ssl_context)
    resp = await conn.send_request('GET', '/')
    assert resp.status in (200, 301, 302)
    assert resp.path == '/'
    await conn.discard()
    await conn.aclose()


@pytest.mark.skipif(no_internet_access, reason='no internet access available')
async def test_invalid_ssl():
    # Don't load certificates
    context = _client_ssl_context()

    conn = HTTPConnection(SSL_TEST_HOST, ssl_context=context)
    with pytest.raises(ssl.SSLError):
        await conn.send_request('GET', '/')
    await conn.aclose()


@pytest.mark.skipif(no_internet_access, reason='no internet access available')
async def test_dns_one(monkeypatch):
    monkeypatch.setattr(s3ql.http, 'DNS_TEST_HOSTNAMES', ((SSL_TEST_HOST, 443),))
    with pytest.raises(HostnameNotResolvable):
        conn = HTTPConnection('foobar.invalid')
        await conn.connect()


async def test_dns_two(monkeypatch):
    monkeypatch.setattr(s3ql.http, 'DNS_TEST_HOSTNAMES', (('grumpf.invalid', 80),))
    with pytest.raises(DNSUnavailable):
        conn = HTTPConnection('foobar.invalid')
        await conn.connect()


@pytest.mark.parametrize('test_port', (None, 8080))
@pytest.mark.no_ssl
async def test_http_proxy(http_server, monkeypatch, test_port):
    test_host = 'www.foobarz.invalid'
    test_path = '/someurl?barf'

    get_path = None

    def do_GET(self):
        nonlocal get_path
        get_path = self.path
        self.send_response(200)
        self.send_header("Content-Type", 'application/octet-stream')
        self.send_header("Content-Length", '0')
        self.end_headers()

    monkeypatch.setattr(MockRequestHandler, 'do_GET', do_GET)

    conn = HTTPConnection(test_host, test_port, proxy=(http_server.host, http_server.port))
    try:
        resp = await conn.send_request('GET', test_path)
        assert resp.status == 200
        await conn.discard()
    finally:
        await conn.aclose()

    if test_port is None:
        exp_path = 'http://%s%s' % (test_host, test_path)
    else:
        exp_path = 'http://%s:%d%s' % (test_host, test_port, test_path)

    assert get_path == exp_path


@pytest.mark.parametrize('test_port', (None, 8080))
@pytest.mark.no_ssl
async def test_connect_proxy(http_server, monkeypatch, test_port):
    test_path = '/someurl?barf'

    # Upstream TLS server; the proxy will pipe bytes through to this server
    # after a successful CONNECT.
    upstream = HTTPServerThread(use_ssl=True)
    upstream.start()
    try:
        get_path = None

        def do_GET(self):
            nonlocal get_path
            get_path = self.path
            self.send_response(200)
            self.send_header("Content-Type", 'application/octet-stream')
            self.send_header("Content-Length", '0')
            self.end_headers()

        monkeypatch.setattr(MockRequestHandler, 'do_GET', do_GET)

        connect_path = None

        def do_CONNECT(self):
            nonlocal connect_path
            connect_path = self.path
            self.send_response(200)
            self.end_headers()
            self.wfile.flush()

            # The CONNECT line declares the client's intended host:port; we
            # always relay through to our local upstream regardless of that
            # value, so the connect_path assertion can verify what the
            # client sent without needing real DNS.
            with socket.create_connection((upstream.host, upstream.port)) as upstream_sock:
                _relay_bidirectional(self.connection, upstream_sock)
            self.close_connection = True

        monkeypatch.setattr(MockRequestHandler, 'do_CONNECT', do_CONNECT, raising=False)

        ssl_context = _client_ssl_context()
        ssl_context.load_verify_locations(cafile=os.path.join(TEST_DIR, 'ca.crt'))
        # The test certificate is issued for ``upstream.host`` (localhost),
        # so the client must use that hostname to satisfy hostname verification.
        client_host = upstream.host

        conn = HTTPConnection(
            client_host,
            test_port,
            proxy=(http_server.host, http_server.port),
            ssl_context=ssl_context,
        )
        try:
            resp = await conn.send_request('GET', test_path)
            assert resp.status == 200
            await conn.discard()
        finally:
            await conn.aclose()

        if test_port is None:
            test_port = 443
        exp_path = '%s:%d' % (client_host, test_port)
        assert connect_path == exp_path
        assert get_path == test_path
    finally:
        upstream.shutdown()


def _relay_bidirectional(sock_a, sock_b):
    '''Copy bytes between two sockets until either side closes the connection.

    Used by the test proxy to splice the client and an upstream server after
    a CONNECT handshake. Runs one direction in a helper thread; the other
    direction runs on the caller's thread.
    '''

    def pump(src, dst):
        with contextlib.suppress(OSError):
            while True:
                data = src.recv(4096)
                if not data:
                    break
                dst.sendall(data)
        with contextlib.suppress(OSError):
            dst.shutdown(socket.SHUT_WR)

    helper = threading.Thread(target=pump, args=(sock_b, sock_a), daemon=True)
    helper.start()
    pump(sock_a, sock_b)
    helper.join(timeout=5)


def get_chunked_GET_handler(path, chunks, delay=None):
    '''Return GET handler for *path* sending *chunks* of data'''

    def do_GET(self):
        if self.path != path:
            self.send_error(500, 'Assertion failure: %s != %s' % (self.path, path))
            return
        self.send_response(200)
        self.send_header("Content-Type", 'application/octet-stream')
        self.send_header("Transfer-Encoding", 'chunked')
        self.end_headers()
        for i, chunk_size in enumerate(chunks):
            if i % 3 == 0 and delay:
                time.sleep(delay * 1e-3)
            self.wfile.write(('%x\r\n' % chunk_size).encode('us-ascii'))
            if i % 3 == 1 and delay:
                self.wfile.write(DUMMY_DATA[: chunk_size // 2])
                time.sleep(delay * 1e-3)
                self.wfile.write(DUMMY_DATA[chunk_size // 2 : chunk_size])
            else:
                self.wfile.write(DUMMY_DATA[:chunk_size])
            if i % 3 == 2 and delay:
                time.sleep(delay * 1e-3)
            self.wfile.write(b'\r\n')
            self.wfile.flush()
        self.wfile.write(b'0\r\n\r\n')

    return do_GET


async def test_send_before_read(conn):
    # Sending a second request before reading the first response is not
    # allowed (a body must be drained before the next request).
    resp = await conn.send_request('GET', '/send_120_bytes')
    assert resp.length == 120
    with pytest.raises(StateError):
        await conn.send_request('GET', '/send_120_bytes')


async def test_discard(conn, monkeypatch):
    data_len = 512
    path = '/send_%d_bytes' % data_len
    resp = await conn.send_request('GET', path)
    assert resp.status == 200
    assert resp.path == path
    assert resp.length == data_len
    await conn.discard()
    # Connection should be reusable now.
    resp = await conn.send_request('GET', path)
    assert resp.status == 200
    await conn.discard()


async def test_read_identity(conn):
    resp = await conn.send_request('GET', '/send_512_bytes')
    assert resp.status == 200
    assert resp.path == '/send_512_bytes'
    assert resp.length == 512
    assert await conn.readall() == DUMMY_DATA[:512]


async def test_conn_close_with_close_header(conn, monkeypatch):
    data_size = 500

    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", 'application/octet-stream')
        self.send_header("Connection", 'close')
        self.end_headers()
        self.wfile.write(DUMMY_DATA[:data_size])
        self.rfile.close()
        self.wfile.close()

    monkeypatch.setattr(MockRequestHandler, 'do_GET', do_GET)

    resp = await conn.send_request('GET', '/whatever')
    assert resp.status == 200
    assert await conn.readall() == DUMMY_DATA[:data_size]

    with pytest.raises(ConnectionClosed):
        await conn.send_request('GET', '/whatever')


async def test_read_raw(conn, monkeypatch):
    path = '/ooops'

    def do_GET(self):
        assert self.path == path
        self.send_response(200)
        self.send_header("Content-Type", 'application/octet-stream')
        self.send_header("Content-Length", 'bogus')
        self.end_headers()
        self.wfile.write(b'body data')
        self.wfile.close()

    monkeypatch.setattr(MockRequestHandler, 'do_GET', do_GET)
    # h11 surfaces malformed Content-Length as a parse error during
    # send_request; read_raw() then drains whatever h11 had buffered
    # from the wire so callers can capture the diagnostic bytes.
    with pytest.raises(InvalidResponse):
        await conn.send_request('GET', path)
    raw = await conn.read_raw()
    assert b'body data' in raw


async def test_abort_read(conn, monkeypatch):
    path = '/foo/wurfl'
    chunks = [300, 317, 283]
    monkeypatch.setattr(MockRequestHandler, 'do_GET', get_chunked_GET_handler(path, chunks))
    resp = await conn.send_request('GET', path)
    assert resp.status == 200
    await conn.read()
    await conn.aclose()
    with pytest.raises(ConnectionClosed):
        await conn.read()


async def test_excess_streaming_body(conn):
    # Streaming bodies that produce more bytes than announced are rejected.
    with pytest.raises(ExcessBodyData):
        await conn.send_request(
            'PUT',
            '/allgood',
            body=[DUMMY_DATA[:60]],
            body_len=42,
        )


async def test_put(conn):
    data = DUMMY_DATA
    # Without Content-MD5, the mock server replies "Ok, but no MD5".
    resp = await conn.send_request('PUT', '/allgood', body=[data])
    await conn.discard()
    assert resp.status == 204
    assert resp.length == 0
    assert resp.reason == 'Ok, but no MD5'

    headers = CaseInsensitiveDict()
    headers['Content-MD5'] = b64encode(hashlib.md5(data).digest()).decode('ascii')
    resp = await conn.send_request('PUT', '/allgood', body=[data], headers=headers)
    await conn.discard()
    assert resp.status == 204
    assert resp.reason == 'MD5 matched'

    headers['Content-MD5'] = 'nUzaJEag3tOdobQVU/39GA=='
    resp = await conn.send_request('PUT', '/allgood', body=[data], headers=headers)
    await conn.discard()
    assert resp.status == 400
    assert resp.reason.startswith('MD5 mismatch')


async def test_put_streaming(conn):
    data = DUMMY_DATA
    # Streaming body without 100-continue.
    resp = await conn.send_request(
        'PUT',
        '/allgood',
        body=[io.BytesIO(data)],
        expect100=False,
    )
    await conn.discard()
    assert resp.status == 204
    assert resp.length == 0
    assert resp.reason == 'Ok, but no MD5'

    headers = CaseInsensitiveDict()
    headers['Content-MD5'] = b64encode(hashlib.md5(data).digest()).decode('ascii')
    resp = await conn.send_request(
        'PUT',
        '/allgood',
        body=[io.BytesIO(data)],
        headers=headers,
        expect100=False,
    )
    await conn.discard()
    assert resp.status == 204
    assert resp.reason == 'MD5 matched'

    headers['Content-MD5'] = 'nUzaJEag3tOdobQVU/39GA=='
    resp = await conn.send_request(
        'PUT',
        '/allgood',
        body=[io.BytesIO(data)],
        headers=headers,
        expect100=False,
    )
    await conn.discard()
    assert resp.status == 400
    assert resp.reason.startswith('MD5 mismatch')


async def test_100cont(conn, monkeypatch):
    path = '/check_this_out'

    def handle_expect_100(self):  # pyright: ignore[reportRedeclaration]
        if self.path != path:
            self.send_error(500, 'Assertion error, %s != %s' % (self.path, path))
        else:
            self.send_error(403)

    monkeypatch.setattr(MockRequestHandler, 'handle_expect_100', handle_expect_100)

    # Server rejects with 403 before the body is sent; the body file
    # object is never read from.
    body_fh = io.BytesIO(DUMMY_DATA[:256])

    resp = await conn.send_request(
        'PUT',
        path,
        body=[body_fh],
        expect100=True,
    )
    assert resp.status == 403
    assert body_fh.tell() == 0
    await conn.discard()

    def handle_expect_100(self):
        if self.path != path:
            self.send_error(500, 'Assertion error, %s != %s' % (self.path, path))
            return

        self.send_response_only(100)
        self.end_headers()
        return True

    monkeypatch.setattr(MockRequestHandler, 'handle_expect_100', handle_expect_100)
    resp = await conn.send_request(
        'PUT',
        path,
        body=[DUMMY_DATA[:256]],
        expect100=True,
    )
    assert resp.status == 204
    assert resp.length == 0


async def test_aborted_streaming_body(conn, monkeypatch, random_fh):
    BUFSIZE = 64 * 1024

    # monkeypatch request handler
    def do_PUT(self):
        # Read half the data, then generate error and close connection.
        self.rfile.read(BUFSIZE)
        self.send_error(code=401, message='Please stop!')
        self.close_connection = True

    monkeypatch.setattr(MockRequestHandler, 'do_PUT', do_PUT)

    body = [random_fh.read(BUFSIZE) for _ in range(50)]

    # In the best case the buffered 401 still reaches us; in the worst
    # case the broken stream prevents that and we see ConnectionClosed.
    try:
        resp = await conn.send_request(
            'PUT',
            '/big_object',
            body=body,
            expect100=True,
        )
    except ConnectionClosed:
        return
    assert resp.status == 401
    assert resp.reason == 'Please stop!'


async def test_empty_response(conn):
    resp = await conn.send_request('HEAD', '/send_512_bytes')
    assert resp.status == 200
    assert resp.path == '/send_512_bytes'
    assert resp.length == 0

    # Check that we can go to the next response without reading anything.
    resp = await conn.send_request('GET', '/send_512_bytes')
    assert resp.status == 200
    assert resp.path == '/send_512_bytes'
    assert resp.length == 512
    assert await conn.readall() == DUMMY_DATA[:512]


DUMMY_DATA = ','.join(str(x) for x in range(10000)).encode()


class MockRequestHandler(BaseHTTPRequestHandler):
    server_version = "MockHTTP"
    protocol_version = 'HTTP/1.1'

    def log_message(self, format, *args):
        pass

    def handle(self):
        # Ignore exceptions resulting from the client closing
        # the connection.
        try:
            return super().handle()
        except ValueError as exc:
            if exc.args == ('I/O operation on closed file.',):
                pass
            else:
                raise
        except (ssl.SSLEOFError, ssl.SSLZeroReturnError):
            pass
        # Linux generates BrokenPipeError, FreeBSD uses ConnectionResetError
        except (BrokenPipeError, ConnectionResetError):
            pass

    def do_GET(self):
        len_ = int(self.headers['Content-Length'])
        if len_:
            self.rfile.read(len_)

        hit = re.match(r'^/send_([0-9]+)_bytes', self.path)
        if hit:
            len_ = int(hit.group(1))
            self.do_HEAD()
            self.wfile.write(DUMMY_DATA[:len_])
            return

        self.send_error(500)

    def do_PUT(self):
        encoding = self.headers['Content-Encoding']
        if encoding and encoding != 'identity':
            self.send_error(415)
            return

        len_ = int(self.headers['Content-Length'])
        data = self.rfile.read(len_)
        if 'Content-MD5' in self.headers:
            md5 = b64encode(hashlib.md5(data).digest()).decode('ascii')
            if md5 != self.headers['Content-MD5']:
                self.send_error(400, 'MD5 mismatch: %s vs %s' % (md5, self.headers['Content-MD5']))
                return

            self.send_response(204, 'MD5 matched')
        else:
            self.send_response(204, 'Ok, but no MD5')

        self.send_header('Content-Length', '0')
        self.end_headers()

    def do_HEAD(self):
        hit = re.match(r'^/send_([0-9]+)_bytes', self.path)
        if hit:
            len_ = int(hit.group(1))
            self.send_response(200)
            self.send_header("Content-Type", 'application/octet-stream')
            self.send_header("Content-Length", str(len_))
            self.end_headers()
            return

        # No idea
        self.send_error(500)

    def send_error(self, code, message=None):
        # Overwritten to not close connection on errors and provide
        # content-length
        try:
            shortmsg, longmsg = self.responses[code]
        except KeyError:
            shortmsg, longmsg = '???', '???'
        if message is None:
            message = shortmsg
        explain = longmsg
        self.log_error("code %d, message %s", code, message)
        # HTML encode to prevent Cross Site Scripting attacks (see bug #1100201)
        content = (
            self.error_message_format
            % {'code': code, 'message': html.escape(message, quote=False), 'explain': explain}
        ).encode('utf-8', 'replace')
        self.send_response(code, message)
        self.send_header("Content-Type", self.error_content_type)
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        if self.command != 'HEAD' and code >= 200 and code not in (204, 304):
            self.wfile.write(content)
