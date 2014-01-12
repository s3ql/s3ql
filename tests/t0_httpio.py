'''
t0_httpclient.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (c) Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from s3ql.httpio import HTTPConnection
from s3ql import httpio
from s3ql.common import BUFSIZE
from http.server import BaseHTTPRequestHandler, _quote_html
from io import BytesIO
import http.client
import pytest
import time
import ssl
import re
import os
import threading
import socketserver

# We want to test with a real certificate
SSL_TEST_HOST = 'www.google.com'

@pytest.fixture
def ssl_context(path=None):
    context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
    context.options |= ssl.OP_NO_SSLv2
    context.verify_mode = ssl.CERT_REQUIRED

    if path is None:
        context.set_default_verify_paths()
    elif os.path.isfile(path):
        context.load_verify_locations(cafile=path)
    else:
        context.load_verify_locations(capath=path)

    return context


class MockHTTPServer(threading.Thread):
    def __init__(self):
        super().__init__()
        self.host = 'localhost'
        self.httpd = socketserver.TCPServer((self.host, 0),
                                            MockRequestHandler)
        self.port = self.httpd.socket.getsockname()[1]

    def run(self):
        self.httpd.serve_forever()

    def shutdown(self):
        self.httpd.shutdown()
        self.httpd.server_close()


@pytest.fixture(scope='module')
def http_server(request):
    httpd = MockHTTPServer()
    httpd.start()
    request.addfinalizer(httpd.shutdown)
    return httpd

@pytest.fixture
def conn(request, http_server):
    conn = HTTPConnection(http_server.host, port=http_server.port)
    request.addfinalizer(conn.close)
    return conn

def check_http_connection(ssl_context):
    '''Skip test if we can't connect to ssl test server'''

    try:
        conn = http.client.HTTPSConnection(SSL_TEST_HOST, context=ssl_context)
        conn.request('GET', '/')
        resp = conn.getresponse()
        assert resp.status == 200
    except:
        pytest.skip('%s not reachable but required for testing' % SSL_TEST_HOST)
    finally:
        conn.close()


def readall(conn):
    '''Read from *conn* until EOF, return number of bytes read'''
    
    read = 0
    while True:
        buf = conn.read(BUFSIZE)
        read += len(buf)
        if not buf:
            return read
        
def test_connect_ssl(ssl_context):
    check_http_connection(ssl_context)

    conn = HTTPConnection(SSL_TEST_HOST, ssl_context=ssl_context)
    conn.send_request('GET', '/')
    (method, url, status, reason, header) = conn.read_response()
    assert status == 200
    assert url == '/'
    readall(conn)
    conn.close()

def test_invalid_ssl(ssl_context):
    check_http_connection(ssl_context)

    # Don't load certificates
    context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
    context.options |= ssl.OP_NO_SSLv2
    context.verify_mode = ssl.CERT_REQUIRED
    conn = HTTPConnection(SSL_TEST_HOST, ssl_context=context)
    with pytest.raises(ssl.SSLError):
        conn.send_request('GET', '/')
    conn.close()
    

def test_get_pipeline(conn):
    
    def read_response():
        if conn.get_current_response():
            conn.read(BUFSIZE)
        else:
            (method, url, status, reason, header) = conn.read_response()

    interrupted = False
    sleeptime = 0.01
    for doc in ('/send_%d_120-byte_chunks' % x for x in range(30)):
        cofun = conn.send_request('GET', doc, via_cofun=True)
        for _ in cofun:
            read_response()
            interrupted = True

        # We want to be interrupted at least once, so wait a little bit
        # before sending the next request
        time.sleep(sleeptime)
        
        if not interrupted:
            sleeptime *= 2
        
    while conn.response_pending():
        read_response()

    assert interrupted


def test_read_identity(conn):
    conn.send_request('GET', '/send_10_bytes')
    (method, url, status, reason, header) = conn.read_response()
    assert status == 200
    assert url == '/send_10_bytes'
    assert readall(conn) == 10

def test_read_chunked(conn):
    conn.send_request('GET', '/send_3_15-byte_chunks')
    (method, url, status, reason, header) = conn.read_response()
    assert status == 200
    assert readall(conn) == 3*15
    
def test_double_read(conn):
    conn.send_request('GET', '/send_10_bytes')
    (method, url, status, reason, header) = conn.read_response()
    assert status == 200
    assert url == '/send_10_bytes'
    with pytest.raises(httpio.StateError):
        (method, url, status, reason, header) = conn.read_response()
    

def writeall(conn, buf):
    while buf:
        off = conn.write(buf)
        buf = buf[off:]

def test_put(conn):
    conn.send_request('PUT', '/allgood', body=b'a nice body string')
    (method, url, status, reason, header) = conn.read_response()
    assert status == 204

def test_body_separate(conn):
    conn.send_request('PUT', '/allgood', body=50)
    writeall(conn, DUMMY_DATA[:50])    
    (method, url, status, reason, header) = conn.read_response()
    assert status == 204
    
def test_write_toomuch(conn):
    conn.send_request('PUT', '/allgood', body=42)
    with pytest.raises(httpio.ExcessBodyData):
        writeall(conn, DUMMY_DATA[:43])
    
def test_write_toolittle(conn):
    conn.send_request('PUT', '/allgood', body=42)
    writeall(conn, DUMMY_DATA[:24])
    with pytest.raises(httpio.StateError):
        conn.send_request('GET', '/send_5_bytes')
        
def test_write_toolittle2(conn):
    conn.send_request('PUT', '/allgood', body=42)
    writeall(conn, DUMMY_DATA[:24])
    with pytest.raises(httpio.StateError):
        conn.read_response()

def test_write_toolittle3(conn):
    conn.send_request('GET', '/send_10_bytes')
    conn.send_request('PUT', '/allgood', body=42)
    writeall(conn, DUMMY_DATA[:24])
    (method, url, status, reason, header) = conn.read_response()
    assert status == 200
    assert url == '/send_10_bytes'
    assert readall(conn) == 10
    with pytest.raises(httpio.StateError):
        conn.read_response()

def test_co_sendfile(conn):
    fh = BytesIO(DUMMY_DATA)
    conn.send_request('PUT', '/allgood', body=len(DUMMY_DATA)//2)
    for _ in conn.co_sendfile(fh):
        pass
    (method, url, status, reason, header) = conn.read_response()
    assert status == 204
    
def test_co_sendfile2(conn):
    fh = BytesIO(DUMMY_DATA)
    conn.send_request('PUT', '/allgood', body=2*len(DUMMY_DATA)-2)
    for _ in conn.co_sendfile(fh):
        pass
    with pytest.raises(httpio.StateError):
        conn.read_response()
    fh.seek(0)
    for _ in conn.co_sendfile(fh):
        pass
    (method, url, status, reason, header) = conn.read_response()
    assert status == 204

def test_100cont(conn):
    conn.send_request('PUT', '/fail_with_403', body=256, expect100=True)
    (method, url, status, reason, header) = conn.read_response()
    assert status == 403
    readall(conn)
    
    conn.send_request('PUT', '/all_good', body=256, expect100=True)
    (method, url, status, reason, header) = conn.read_response()
    assert status == 100
    writeall(conn, DUMMY_DATA[:256])
    (method, url, status, reason, header) = conn.read_response()
    assert status == 204
    
def test_100cont_2(conn):
    conn.send_request('PUT', '/fail_with_403', body=256, expect100=True)

    with pytest.raises(httpio.StateError):
        conn.send_request('PUT', '/fail_with_403', body=256, expect100=True)

def test_100cont_3(conn):
    conn.send_request('PUT', '/fail_with_403', body=256, expect100=True)

    with pytest.raises(httpio.StateError):
        conn.write(b'barf!')

        
def test_tunnel(http_server):
    conn = HTTPConnection('remote_server', proxy=(http_server.host, http_server.port))
    
    conn.send_request('GET', '/send_10_bytes')
    (method, url, status, reason, header) = conn.read_response()
    assert status == 200
    assert url == '/send_10_bytes'
    assert readall(conn) == 10
    conn.close()


def test_request_via_cofun(conn):
    cofun = conn.send_request('GET', '/send_10_bytes', via_cofun=True)
    with pytest.raises(httpio.StateError):
        conn.read_response()

    for _ in cofun:
        pass
    
    (method, url, status, reason, header) = conn.read_response()
    assert status == 200
    assert readall(conn) == 10

def test_read_toomuch(conn):
    conn.send_request('GET', '/send_10_bytes')
    conn.send_request('GET', '/send_8_bytes')
    (method, url, status, reason, header) = conn.read_response()
    assert status == 200
    assert url == '/send_10_bytes'
    assert readall(conn) == 10
    with pytest.raises(httpio.StateError):
        conn.read(8)

        
def test_read_toolittle(conn):
    conn.send_request('GET', '/send_10_bytes')
    (method, url, status, reason, header) = conn.read_response()
    assert status == 200
    assert url == '/send_10_bytes'
    conn.read(8)
    with pytest.raises(httpio.StateError):
        (method, url, status, reason, header) = conn.read_response()

        
def test_current_response(conn):
    assert conn.get_current_response() is None
    conn.send_request('GET', '/send_10_bytes')
    (method, url, status, reason, header) = conn.read_response()
    assert conn.get_current_response() == (method, url)
    conn.read(5)
    assert conn.get_current_response() == (method, url)
    readall(conn)
    assert conn.get_current_response() is None
    

def test_head(conn):
    conn.send_request('HEAD', '/send_10_bytes')
    (method, url, status, reason, header) = conn.read_response()
    assert status == 200
    assert readall(conn) == 0
    
    conn.send_request('HEAD', '/fail_with_317')
    (method, url, status, reason, header) = conn.read_response()
    assert status == 317
    assert readall(conn) == 0


with open(__file__, 'rb') as fh:
    DUMMY_DATA = fh.read()
    
class MockRequestHandler(BaseHTTPRequestHandler):

    server_version = "MockHTTP"
    protocol_version = 'HTTP/1.1'
    
    def handle_expect_100(self):
        if self.handle_errors():
            return
        else:
            self.send_response_only(100)
            self.end_headers()
            return True
        
    def do_GET(self):
        if self.handle_errors():
            return
        
        hit = re.match(r'^/send_([0-9]+)_bytes', self.path)
        if hit:
            len_ = int(hit.group(1))
            self.send_response(200)
            self.send_header("Content-Type", 'application/octet-stream')
            self.send_header("Content-Length", str(len_))
            self.end_headers()
            self.send_dummy_data(len_)
            return

        hit = re.match(r'^/send_([0-9]+)_([0-9]+)-byte_chunks', self.path)
        if hit:
            count = int(hit.group(1))
            len_ = int(hit.group(2))
            self.send_response(200)
            self.send_header("Content-Type", 'application/octet-stream')
            self.send_header("Transfer-Encoding", 'chunked')
            self.end_headers()
            for i in range(count):
                self.wfile.write(('%x\r\n' % len_).encode('us-ascii')) 
                self.send_dummy_data(len_)
                self.wfile.write(b'\r\n')
            self.wfile.write(b'0\r\n\r\n')
            return
        
        self.send_error(500)
        
    def send_dummy_data(self, len_):
        while len_ > 0:
            self.wfile.write(DUMMY_DATA[:len_])
            len_ -= len(DUMMY_DATA)

    def handle_errors(self):
        hit = re.match(r'^/fail_with_([0-9]+)', self.path)
        if hit:
            self.send_error(int(hit.group(1)))
            return True

        if self.command == 'PUT':
            encoding = self.headers['Content-Encoding']
            if encoding and encoding != 'identity':
                self.send_error(415)
                return True

        return False

            
    def do_PUT(self):
        if self.handle_errors():
            return

        len_ = int(self.headers['Content-Length'])
        self.rfile.read(len_)

        self.send_response(204)
        self.end_headers()
        

    def do_CONNECT(self):
        # Just pretend we're the remote server too
        self.send_response(200)
        self.end_headers()
        self.close_connection = 0
        
    def do_HEAD(self):
        if self.handle_errors():
            return

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
        # using _quote_html to prevent Cross Site Scripting attacks (see bug #1100201)
        content = (self.error_message_format % {'code': code, 'message': _quote_html(message),
                                               'explain': explain}).encode('utf-8', 'replace')
        self.send_response(code, message)
        self.send_header("Content-Type", self.error_content_type)
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        if self.command != 'HEAD' and code >= 200 and code not in (204, 304):
            self.wfile.write(content)
