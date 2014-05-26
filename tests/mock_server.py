#!/usr/bin/env python3
'''
mock_server.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from http.server import BaseHTTPRequestHandler
from s3ql import backends
import re
import socketserver
import logging
import hashlib
import urllib.parse
from xml.sax.saxutils import escape as xml_escape

log = logging.getLogger(__name__)

ERROR_RESPONSE_TEMPLATE = '''\
<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>%(code)s</Code>
  <Message>%(message)s</Message>
  <Resource>%(resource)s</Resource>
  <RequestId>%(request_id)s</RequestId>
</Error>
'''

class StorageServer(socketserver.ThreadingTCPServer):

    def __init__(self, request_handler, server_address):
        super().__init__(server_address, request_handler)
        self.data = dict()
        self.metadata = dict()
        self.hostname = self.server_address[0]
        self.port = self.server_address[1]

class ParsedURL:
    __slots__ = [ 'bucket', 'key', 'params', 'fragment' ]

def parse_url(path):
    p = ParsedURL()
    q = urllib.parse.urlsplit(path)

    path = urllib.parse.unquote(q.path)

    assert path[0] == '/'
    (p.bucket, p.key) = path[1:].split('/', maxsplit=1)

    p.params = urllib.parse.parse_qs(q.query)
    p.fragment = q.fragment

    return p

class S3RequestHandler(BaseHTTPRequestHandler):
    '''A request handler implementing a subset of the AWS S3 Interface

    Bucket names are ignored, all keys share the same global
    namespace.
    '''

    server_version = "MockHTTP"
    protocol_version = 'HTTP/1.1'
    meta_header_re = re.compile(r'X-AMZ-Meta-([a-z0-9_.-]+)$',
                                re.IGNORECASE)
    xml_ns_prefix = '{http://s3.amazonaws.com/doc/2006-03-01/}'

    def log_message(self, format, *args):
        log.debug(format, *args)

    def handle(self):
        # Ignore exceptions resulting from the client closing
        # the connection.
        try:
            return super().handle()
        except ValueError as exc:
            if exc.args ==  ('I/O operation on closed file.',):
                pass
            else:
                raise
        except BrokenPipeError:
            pass

    def do_DELETE(self):
        q = parse_url(self.path)
        try:
            del self.server.data[q.key]
            del self.server.metadata[q.key]
        except KeyError:
            self.send_error(404, code='NoSuchKey', resource=q.key)
            return
        else:
            self.send_response(204)
            self.end_headers()

    def _check_encoding(self):
        encoding = self.headers['Content-Encoding']
        if 'Content-Length' not in self.headers:
            self.send_error(400, message='Missing Content-Length',
                            code='MissingContentLength')
            return
        elif encoding and encoding != 'identity':
            self.send_error(501, message='Unsupport encoding',
                            code='NotImplemented')
            return

        return int(self.headers['Content-Length'])

    def do_PUT(self):
        len_ = self._check_encoding()

        q = parse_url(self.path)
        meta = dict()
        for (name, value) in self.headers.items():
            hit = self.meta_header_re.search(name)
            if hit:
                meta[hit.group(1)] = value

        src = self.headers.get('x-amz-copy-source')
        if src and len_:
            self.send_error(400, message='Upload and copy are mutually exclusive',
                            code='UnexpectedContent')
            return
        elif src:
            hit = re.match('^/([a-z0-9._-]+)/(.+)$', src)
            if not hit:
                self.send_error(400, message='Cannot parse copy-source',
                                code='InvalidArgument')
                return

            src = hit.group(2)
            try:
                data = self.server.data[src]
                self.server.metadata[q.key] = self.server.metadata[src]
                self.server.data[q.key] = data
            except KeyError:
                self.send_error(404, code='NoSuchKey', resource=src)
        else:
            data = self.rfile.read(len_)
            self.server.metadata[q.key] = meta
            self.server.data[q.key] = data

        md5 = hashlib.md5()
        md5.update(data)
        self.send_response(201)
        self.send_header('ETag', '"%s"' % md5.hexdigest())
        self.send_header('Content-Length', '0')
        self.end_headers()

    def handle_expect_100(self):
        if self.command == 'PUT':
            self._check_encoding()

        self.send_response_only(100)
        self.end_headers()
        return True

    def do_GET(self):
        q = parse_url(self.path)

        if not q.key:
            return self.do_list(q)

        try:
            data = self.server.data[q.key]
            meta = self.server.metadata[q.key]
        except KeyError:
            self.send_error(404, code='NoSuchKey', resource=q.key)
            return

        self.send_response(200)
        self.send_header("Content-Type", 'application/octet-stream')
        self.send_header("Content-Length", str(len(data)))
        for (name, value) in meta.items():
            self.send_header('X-AMZ-Meta-%s' % name, value)
        md5 = hashlib.md5()
        md5.update(data)
        self.send_header('ETag', '"%s"' % md5.hexdigest())
        self.end_headers()
        self.wfile.write(data)

    def do_list(self, q):
        marker = q.params['marker'][0] if 'marker' in q.params else None
        max_keys = int(q.params['max_keys'][0]) if 'max_keys' in q.params else 1000
        prefix = q.params['prefix'][0] if 'prefix' in q.params else ''

        resp = ['<?xml version="1.0" encoding="UTF-8"?>',
                '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">',
                '<MaxKeys>%d</MaxKeys>' % max_keys,
                '<IsTruncated>false</IsTruncated>' ]

        count = 0
        for key in self.server.data:
            if not key.startswith(prefix):
                continue
            if marker and key <= marker:
                continue
            resp.append('<Contents><Key>%s</Key></Contents>' % xml_escape(key))
            count += 1
            if count == max_keys:
                resp[3] = '<IsTruncated>true</IsTruncated>'
                break

        resp.append('</ListBucketResult>')
        body = '\n'.join(resp).encode()

        self.send_response(200)
        self.send_header("Content-Type", 'text/xml')
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_HEAD(self):
        q = parse_url(self.path)
        try:
            meta = self.server.metadata[q.key]
            data = self.server.data[q.key]
        except KeyError:
            self.send_error(404, code='NoSuchKey', resource=q.key)
            return

        self.send_response(200)
        self.send_header("Content-Type", 'application/octet-stream')
        self.send_header("Content-Length", str(len(data)))
        for (name, value) in meta.items():
            self.send_header('X-AMZ-Meta-%s' % name, value)
        self.end_headers()

    def send_error(self, status, message=None, code='', resource=''):

        if not message:
            try:
                (_, message) = self.responses[status]
            except KeyError:
                message = 'Unknown'

        self.log_error("code %d, message %s", status, message)
        content = (ERROR_RESPONSE_TEMPLATE %
                   {'code': code,
                    'message': xml_escape(message),
                    'request_id': 42,
                    'resource': xml_escape(resource)}).encode('utf-8', 'replace')
        self.send_response(status, message)
        self.send_header("Content-Type", 'text/xml; charset="utf-8"')
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        if self.command != 'HEAD' and status >= 200 and status not in (204, 304):
            self.wfile.write(content)


#: A list of the available mock request handlers with
#: corresponding storage urls
handler_list = [ (S3RequestHandler, 's3c://%(host)s:%(port)d/s3ql_test') ]
