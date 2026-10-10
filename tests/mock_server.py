#!/usr/bin/env python3
# pyright: ignore[]
'''
mock_server.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

import hashlib
import json
import logging
import os
import re
import socketserver
import ssl
import urllib.parse
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler
from xml.sax.saxutils import escape as xml_escape

log = logging.getLogger(__name__)

TEST_DIR = os.path.dirname(os.path.abspath(__file__))

#: Certificate (valid for localhost and 127.0.0.1) and key used by TLS mock servers
SERVER_CERT = os.path.join(TEST_DIR, 'server.crt')
SERVER_KEY = os.path.join(TEST_DIR, 'server.key')

#: CA certificate used to sign `SERVER_CERT`
CA_CERT = os.path.join(TEST_DIR, 'ca.crt')

ERROR_RESPONSE_TEMPLATE = '''\
<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>%(code)s</Code>
  <Message>%(message)s</Message>
  <Resource>%(resource)s</Resource>
  <RequestId>%(request_id)s</RequestId>
</Error>
'''

COPY_RESPONSE_TEMPLATE = '''\
<?xml version="1.0" encoding="UTF-8"?>
<CopyObjectResult xmlns="%(ns)s">
   <LastModified>2008-02-20T22:13:01</LastModified>
   <ETag>&quot;%(etag)s&quot;</ETag>
</CopyObjectResult>
'''

DELETE_RESULT_TEMPLATE = '''\
<?xml version="1.0" encoding="UTF-8"?>
<DeleteResult xmlns="%(ns)s">
%(items)s</DeleteResult>
'''


class StorageServer(socketserver.TCPServer):
    def __init__(self, request_handler, server_address, use_tls: bool = False):
        '''If *use_tls* is true, serve TLS using the certificate in `SERVER_CERT`.'''

        super().__init__(server_address, request_handler)
        self.data = dict()
        self.metadata = dict()
        self.hostname = self.server_address[0]
        self.port = self.server_address[1]

        self.ssl_context: ssl.SSLContext | None
        if use_tls:
            self.ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            self.ssl_context.minimum_version = ssl.TLSVersion.TLSv1_2
            self.ssl_context.load_cert_chain(SERVER_CERT, SERVER_KEY)
        else:
            self.ssl_context = None

    def get_request(self):
        (sock, addr) = super().get_request()
        if self.ssl_context:
            sock = self.ssl_context.wrap_socket(sock, server_side=True)
        return (sock, addr)


class ParsedURL:
    __slots__ = ['bucket', 'key', 'params', 'fragment']


class MockRequestHandler(BaseHTTPRequestHandler):
    '''Base class for mock storage request handlers'''

    server_version = "MockHTTP"
    protocol_version = 'HTTP/1.1'

    def send_error(self, status, message=None, code='', resource='', extra_headers=None):
        '''Send an error response with HTTP status *status*

        *message* is a human-readable description, *code* and *resource* are the error code and
        the affected resource in the protocol's terms, and *extra_headers* is a dict of
        additional response headers.
        '''

        raise NotImplementedError()

    def log_message(self, format, *args):
        log.debug(format, *args)

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
        except (BrokenPipeError, ConnectionResetError):
            pass

    def _check_encoding(self) -> int | None:
        encoding = self.headers['Content-Encoding']
        if 'Content-Length' not in self.headers:
            self.send_error(400, message='Missing Content-Length', code='MissingContentLength')
            return None
        elif encoding and encoding != 'identity':
            self.send_error(501, message='Unsupported encoding', code='NotImplemented')
            return None

        return int(self.headers['Content-Length'])

    def send_data(self, data):
        '''Write *data* to the response body

        This is a separate method so that tests can intercept the response body.
        '''

        self.wfile.write(data)


class S3CRequestHandler(MockRequestHandler):
    '''A request handler implementing a subset of the AWS S3 Interface

    Bucket names are ignored, all keys share the same global
    namespace.
    '''

    meta_header_re = re.compile(r'X-AMZ-Meta-([a-z0-9_.-]+)$', re.IGNORECASE)
    hdr_prefix = 'X-AMZ-'
    xml_ns = 'http://s3.amazonaws.com/doc/2006-03-01/'

    def parse_url(self, path):
        p = ParsedURL()
        q = urllib.parse.urlsplit(path)

        path = urllib.parse.unquote(q.path)

        assert path[0] == '/'
        (p.bucket, p.key) = path[1:].split('/', maxsplit=1)

        p.params = urllib.parse.parse_qs(q.query)
        p.fragment = q.fragment

        return p

    def do_DELETE(self):
        q = self.parse_url(self.path)
        try:
            del self.server.data[q.key]
            del self.server.metadata[q.key]
        except KeyError:
            self.send_error(404, code='NoSuchKey', resource=q.key)
            return
        else:
            self.send_response(204)
            self.end_headers()

    def _get_meta(self):
        meta = dict()
        for name, value in self.headers.items():
            hit = self.meta_header_re.search(name)
            if hit:
                meta[hit.group(1)] = value
        return meta

    def do_PUT(self):
        len_ = self._check_encoding()
        if len_ is None:
            return
        q = self.parse_url(self.path)
        meta = self._get_meta()

        src = self.headers.get(self.hdr_prefix + 'copy-source')
        if src and len_:
            self.send_error(
                400, message='Upload and copy are mutually exclusive', code='UnexpectedContent'
            )
            return
        elif src:
            src = urllib.parse.unquote(src)
            hit = re.match('^/([a-z0-9._-]+)/(.+)$', src)
            if not hit:
                self.send_error(400, message='Cannot parse copy-source', code='InvalidArgument')
                return

            metadata_directive = self.headers.get(self.hdr_prefix + 'metadata-directive', 'COPY')
            if metadata_directive not in ('COPY', 'REPLACE'):
                self.send_error(400, message='Invalid metadata directive', code='InvalidArgument')
                return
            src = hit.group(2)
            try:
                data = self.server.data[src]
                self.server.data[q.key] = data
                if metadata_directive == 'COPY':
                    self.server.metadata[q.key] = self.server.metadata[src]
                else:
                    self.server.metadata[q.key] = meta
            except KeyError:
                self.send_error(404, code='NoSuchKey', resource=src)
                return
        else:
            data = self.rfile.read(len_)
            self.server.metadata[q.key] = meta
            self.server.data[q.key] = data

        md5 = hashlib.md5()
        md5.update(data)

        if src:
            content = (
                COPY_RESPONSE_TEMPLATE % {'etag': md5.hexdigest(), 'ns': self.xml_ns}
            ).encode('utf-8')
            self.send_response(200)
            self.send_header('ETag', '"%s"' % md5.hexdigest())
            self.send_header('Content-Length', str(len(content)))
            self.send_header("Content-Type", 'text/xml')
            self.end_headers()
            self.wfile.write(content)
        else:
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
        q = self.parse_url(self.path)
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
        for name, value in meta.items():
            self.send_header(self.hdr_prefix + 'Meta-%s' % name, value)
        md5 = hashlib.md5()
        md5.update(data)
        self.send_header('ETag', '"%s"' % md5.hexdigest())
        self.end_headers()
        self.send_data(data)

    def do_list(self, q):
        marker = q.params['marker'][0] if 'marker' in q.params else None
        max_keys = int(q.params['max_keys'][0]) if 'max_keys' in q.params else 1000
        prefix = q.params['prefix'][0] if 'prefix' in q.params else ''

        resp = [
            '<?xml version="1.0" encoding="UTF-8"?>',
            '<ListBucketResult xmlns="%s">' % self.xml_ns,
            '<MaxKeys>%d</MaxKeys>' % max_keys,
            '<IsTruncated>false</IsTruncated>',
        ]

        count = 0
        for key in sorted(self.server.data):
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
        q = self.parse_url(self.path)
        try:
            meta = self.server.metadata[q.key]
            data = self.server.data[q.key]
        except KeyError:
            self.send_error(404, code='NoSuchKey', resource=q.key)
            return

        self.send_response(200)
        self.send_header("Content-Type", 'application/octet-stream')
        self.send_header("Content-Length", str(len(data)))
        for name, value in meta.items():
            self.send_header(self.hdr_prefix + 'Meta-%s' % name, value)
        self.end_headers()

    def send_error(self, status, message=None, code='', resource='', extra_headers=None):
        if not message:
            try:
                (_, message) = self.responses[status]
            except KeyError:
                message = 'Unknown'

        self.log_error("code %d, message %s", status, message)
        content = (
            ERROR_RESPONSE_TEMPLATE
            % {
                'code': code,
                'message': xml_escape(message),
                'request_id': 42,
                'resource': xml_escape(resource),
            }
        ).encode('utf-8', 'replace')
        self.send_response(status, message)
        self.send_header("Content-Type", 'text/xml; charset="utf-8"')
        self.send_header("Content-Length", str(len(content)))
        if extra_headers:
            for name, value in extra_headers.items():
                self.send_header(name, value)
        self.end_headers()
        if self.command != 'HEAD' and status >= 200 and status not in (204, 304):
            self.wfile.write(content)


class S3C4RequestHandler(S3CRequestHandler):
    '''Request Handler for s3c4 backend

    Extends S3CRequestHandler with POST /?delete (batch delete), validating
    Content-MD5 as AWS S3 now requires.
    '''

    def do_POST(self):
        raw = urllib.parse.urlsplit(self.path)
        if raw.query != 'delete':
            self.send_error(400, message='Unsupported POST operation', code='InvalidRequest')
            return

        # AWS S3 requires Content-MD5 for POST /?delete
        if 'Content-MD5' not in self.headers:
            self.send_error(
                400,
                message='Missing required header for this request: Content-MD5 OR x-amz-checksum-*',
                code='InvalidRequest',
            )
            return

        len_ = self._check_encoding()
        if len_ is None:
            return
        body = self.rfile.read(len_)

        try:
            root = ET.fromstring(body.decode('utf-8'))
        except ET.ParseError as e:
            self.send_error(400, message='XML parse error: %s' % e, code='MalformedXML')
            return

        items = []
        for obj in root.findall('Object'):
            key_el = obj.find('Key')
            if key_el is None or not key_el.text:
                continue
            key = key_el.text
            self.server.data.pop(key, None)
            self.server.metadata.pop(key, None)
            items.append('  <Deleted><Key>%s</Key></Deleted>\n' % xml_escape(key))

        content = (DELETE_RESULT_TEMPLATE % {'ns': self.xml_ns, 'items': ''.join(items)}).encode(
            'utf-8'
        )
        self.send_response(200)
        self.send_header('Content-Type', 'text/xml; charset=utf-8')
        self.send_header('Content-Length', str(len(content)))
        self.end_headers()
        self.wfile.write(content)


class BasicSwiftRequestHandler(S3CRequestHandler):
    '''A request handler implementing a subset of the OpenStack Swift Interface

    Container and AUTH_* prefix are ignored, all keys share the same global
    namespace.

    To keep it simple, this handler is both storage server and authentication
    server in one.
    '''

    meta_header_re = re.compile(r'X-Object-Meta-([a-z0-9_.-]+)$', re.IGNORECASE)
    hdr_prefix = 'X-Object-'

    SWIFT_INFO = {
        "swift": {
            "max_meta_count": 90,
            "max_meta_value_length": 256,
            "container_listing_limit": 10000,
            "extra_header_count": 0,
            "max_meta_overall_size": 4096,
            "version": "2.0.0",  # < 2.8
            "max_meta_name_length": 128,
            "max_header_size": 16384,
        }
    }

    def parse_url(self, path):
        p = ParsedURL()
        q = urllib.parse.urlsplit(path)

        path = urllib.parse.unquote(q.path)

        assert path[0:4] == '/v1/'
        (_, p.bucket, p.key) = path[4:].split('/', maxsplit=2)

        p.params = urllib.parse.parse_qs(q.query, True)
        p.fragment = q.fragment

        return p

    def do_PUT(self):
        len_ = self._check_encoding()
        if len_ is None:
            return
        q = self.parse_url(self.path)
        meta = self._get_meta()

        src = self.headers.get('x-copy-from')
        if src and len_:
            self.send_error(
                400, message='Upload and copy are mutually exclusive', code='UnexpectedContent'
            )
            return
        elif src:
            src = urllib.parse.unquote(src)
            hit = re.match('^/([a-z0-9._-]+)/(.+)$', src)
            if not hit:
                self.send_error(400, message='Cannot parse x-copy-from', code='InvalidArgument')
                return

            src = hit.group(2)
            try:
                data = self.server.data[src]
                self.server.data[q.key] = data
                if 'x-fresh-metadata' in self.headers:
                    self.server.metadata[q.key] = meta
                else:
                    self.server.metadata[q.key] = self.server.metadata[src].copy()
                    self.server.metadata[q.key].update(meta)
            except KeyError:
                self.send_error(404, code='NoSuchKey', resource=src)
                return
        else:
            data = self.rfile.read(len_)
            self.server.metadata[q.key] = meta
            self.server.data[q.key] = data

        md5 = hashlib.md5()
        md5.update(data)

        if src:
            self.send_response(202)
            self.send_header('X-Copied-From', self.headers['x-copy-from'])
            self.send_header('Content-Length', '0')
            self.end_headers()
        else:
            self.send_response(201)
            self.send_header('ETag', '"%s"' % md5.hexdigest())
            self.send_header('Content-Length', '0')
            self.end_headers()

    def do_POST(self):
        q = self.parse_url(self.path)
        meta = self._get_meta()

        if q.key not in self.server.metadata:
            self.send_error(404, code='NoSuchKey', resource=q.key)
            return

        self.server.metadata[q.key] = meta

        self.send_response(204)
        self.send_header('Content-Length', '0')
        self.end_headers()

    def do_GET(self):
        if self.path in ('/v1.0', '/auth/v1.0'):
            self.send_response(200)
            self.send_header(
                'X-Storage-Url',
                'http://%s:%d/v1/AUTH_xyz' % (self.server.hostname, self.server.port),
            )
            self.send_header('X-Auth-Token', 'static')
            # Some real Swift deployments emit a duplicate Date header (typically
            # via a misbehaving proxy). The client must tolerate this; reproduce
            # it here so the mock-swift tests exercise the same code path.
            self.send_header('Date', 'Thu, 01 Jan 1970 00:00:00 GMT')
            self.send_header('Content-Length', '0')
            self.end_headers()
        elif self.path == '/info':
            content = json.dumps(self.SWIFT_INFO).encode('utf-8')
            self.send_response(200)
            self.send_header('Content-Length', str(len(content)))
            self.send_header("Content-Type", 'application/json; charset="utf-8"')
            self.end_headers()
            self.wfile.write(content)
        else:
            return super().do_GET()

    def do_list(self, q):
        marker = q.params['marker'][0] if 'marker' in q.params else None
        max_keys = int(q.params['limit'][0]) if 'limit' in q.params else 10000
        prefix = q.params['prefix'][0] if 'prefix' in q.params else ''

        resp = []

        count = 0
        for key in sorted(self.server.data):
            if not key.startswith(prefix):
                continue
            if marker and key <= marker:
                continue
            resp.append({'name': key})
            count += 1
            if count == max_keys:
                break

        body = json.dumps(resp).encode('utf-8')

        self.send_response(200)
        self.send_header("Content-Type", 'application/json; charset="utf-8"')
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class CopySwiftRequestHandler(BasicSwiftRequestHandler):
    '''OpenStack Swift handler that emulates Copy middleware.'''

    SWIFT_INFO = {
        "swift": {
            "max_meta_count": 90,
            "max_meta_value_length": 256,
            "container_listing_limit": 10000,
            "extra_header_count": 0,
            "max_meta_overall_size": 4096,
            "version": "2.9.0",  # >= 2.8
            "max_meta_name_length": 128,
            "max_header_size": 16384,
        }
    }

    def do_COPY(self):
        src = self.parse_url(self.path)
        meta = self._get_meta()

        try:
            dst = self.headers['destination']
            assert dst[0] == '/'
            (_, dst) = dst[1:].split('/', maxsplit=1)
        except KeyError:
            self.send_error(400, message='No Destination provided', code='InvalidArgument')
            return

        if src.key not in self.server.metadata:
            self.send_error(404, code='NoSuchKey', resource=src)
            return

        if 'x-fresh-metadata' in self.headers:
            self.server.metadata[dst] = meta
        else:
            self.server.metadata[dst] = self.server.metadata[src.key].copy()
            self.server.metadata[dst].update(meta)

        self.server.data[dst] = self.server.data[src.key]

        self.send_response(202)
        self.send_header('X-Copied-From', '%s/%s' % (src.bucket, src.key))
        self.send_header('Content-Length', '0')
        self.end_headers()


class BulkDeleteSwiftRequestHandler(BasicSwiftRequestHandler):
    '''OpenStack Swift handler that emulates bulk middleware (the delete part).'''

    MAX_DELETES = 8  # test deletes 16 objects, so needs two requests
    SWIFT_INFO = {
        "bulk_delete": {"max_failed_deletes": MAX_DELETES, "max_deletes_per_request": MAX_DELETES},
        "swift": {
            "max_meta_count": 90,
            "max_meta_value_length": 256,
            "container_listing_limit": 10000,
            "extra_header_count": 0,
            "max_meta_overall_size": 4096,
            "version": "2.0.0",  # < 2.8
            "max_meta_name_length": 128,
            "max_header_size": 16384,
        },
    }

    def do_POST(self):
        q = self.parse_url(self.path)
        if 'bulk-delete' not in q.params:
            return super().do_POST()

        response = {
            'Response Status': '200 OK',
            'Response Body': '',
            'Number Deleted': 0,
            'Number Not Found': 0,
            'Errors': [],
        }

        def send_response(status_int):
            content = json.dumps(response).encode('utf-8')
            self.send_response(status_int)
            self.send_header('Content-Length', str(len(content)))
            self.send_header("Content-Type", 'application/json; charset="utf-8"')
            self.end_headers()
            self.wfile.write(content)

        def error(reason):
            response['Response Status'] = '502 Internal Server Error'
            response['Response Body'] = reason
            send_response(502)

        def inline_error(http_status, body):
            '''bail out when processing begun. Always HTTP 200 Ok.'''
            response['Response Status'] = http_status
            response['Response Body'] = body
            send_response(200)

        len_ = self._check_encoding()
        if len_ is None:
            return
        lines = self.rfile.read(len_).decode('utf-8').split("\n")
        for index, to_delete in enumerate(lines):
            if index >= self.MAX_DELETES:
                return inline_error(
                    '413 Request entity too large',
                    'Maximum Bulk Deletes: %d per request' % self.MAX_DELETES,
                )
            to_delete = urllib.parse.unquote(to_delete.strip())
            assert to_delete[0] == '/'
            to_delete = to_delete[1:].split('/', maxsplit=1)
            if len(to_delete) < 2:
                return error("deleting containers is not supported")
            to_delete = to_delete[1]
            try:
                del self.server.data[to_delete]
                del self.server.metadata[to_delete]
            except KeyError:
                response['Number Not Found'] += 1
            else:
                response['Number Deleted'] += 1

        if not (response['Number Deleted'] or response['Number Not Found']):
            return inline_error('400 Bad Request', 'Invalid bulk delete.')
        send_response(200)


class GSRequestHandler(MockRequestHandler):
    '''A request handler implementing a subset of the Google Cloud Storage JSON API

    Bucket names are ignored; all keys share the same global namespace. Access tokens are not
    checked.
    '''

    # The `bucket` and `key` groups are still URL-quoted.
    path_re = re.compile(
        r'^(?:/upload)?/storage/v1/b/(?P<bucket>[^/]+)(?P<collection>/o(?:/(?P<key>.+))?)?$'
    )

    def parse_url(self, path: str) -> ParsedURL | None:
        '''Return `ParsedURL` for *path*, or `None` if it is not a storage API path

        The `key` attribute is `None` for bucket requests and `''` for requests to the object
        collection (listing and upload).
        '''

        q = urllib.parse.urlsplit(path)
        hit = self.path_re.match(q.path)
        if not hit:
            return None

        p = ParsedURL()
        p.bucket = urllib.parse.unquote(hit['bucket'])
        if hit['collection'] is None:
            p.key = None
        else:
            p.key = urllib.parse.unquote(hit['key'] or '')
        p.params = urllib.parse.parse_qs(q.query)
        p.fragment = q.fragment
        return p

    def send_json(self, obj: object, status: int = 200):
        # *obj* is anything that `json.dumps` can serialize.
        content = json.dumps(obj).encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', 'application/json; charset=UTF-8')
        self.send_header('Content-Length', str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def do_GET(self):
        q = self.parse_url(self.path)
        if q is None:
            self.send_error(404)
        elif q.key is None:
            self.send_json({'kind': 'storage#bucket', 'name': q.bucket})
        elif not q.key:
            self.do_list(q)
        elif q.key not in self.server.data:
            self.send_error(404, message='No such object: %s' % q.key)
        elif q.params.get('alt') == ['media']:
            data = self.server.data[q.key]
            self.send_response(200)
            self.send_header('Content-Type', 'application/octet-stream')
            self.send_header('Content-Length', str(len(data)))
            self.end_headers()
            self.send_data(data)
        else:
            resource = {
                'kind': 'storage#object',
                'name': q.key,
                'size': str(len(self.server.data[q.key])),
            }
            if meta := self.server.metadata[q.key]:
                resource['metadata'] = meta
            self.send_json(resource)

    def do_list(self, q: ParsedURL):
        prefix = q.params.get('prefix', [''])[0]
        max_results = int(q.params.get('maxResults', ['1000'])[0])
        page_token = q.params.get('pageToken', [None])[0]

        keys = [
            key
            for key in sorted(self.server.data)
            if key.startswith(prefix) and (page_token is None or key > page_token)
        ]

        resp = {'kind': 'storage#objects'}
        if keys:
            resp['items'] = [{'kind': 'storage#object', 'name': key} for key in keys[:max_results]]
        if len(keys) > max_results:
            resp['nextPageToken'] = keys[max_results - 1]
        self.send_json(resp)

    def do_DELETE(self):
        q = self.parse_url(self.path)
        if q is None or not q.key:
            self.send_error(404)
            return
        try:
            del self.server.data[q.key]
            del self.server.metadata[q.key]
        except KeyError:
            self.send_error(404, message='No such object: %s' % q.key)
            return
        self.send_response(204)
        self.send_header('Content-Length', '0')
        self.end_headers()

    def do_POST(self):
        '''Handle a multipart upload

        The request body consists of a JSON part with the object resource (name and metadata),
        followed by a part with the object data.
        '''

        q = self.parse_url(self.path)
        len_ = self._check_encoding()
        if len_ is None:
            return
        body = self.rfile.read(len_)

        if q is None or q.key != '' or q.params.get('uploadType') != ['multipart']:
            self.send_error(400, message='Unsupported POST request')
            return

        hit = re.match(r'multipart/related;\s*boundary=(.+)$', self.headers['Content-Type'])
        if not hit:
            self.send_error(400, message='Expected multipart/related body')
            return
        delimiter = b'--' + hit.group(1).encode()

        # The data part comes last and may contain arbitrary bytes, so locate the parts
        # by position instead of splitting at every delimiter.
        suffix = b'\n' + delimiter + b'--\n'
        try:
            (_, json_part, data_part) = body.removesuffix(suffix).split(delimiter + b'\n', 2)
            (_, json_body) = json_part.split(b'\n\n', 1)
            (_, data) = data_part.split(b'\n\n', 1)
            resource = json.loads(json_body)
        except ValueError:
            self.send_error(400, message='Malformed multipart body')
            return

        key = resource['name']
        self.server.data[key] = data
        self.server.metadata[key] = resource.get('metadata', {})
        self.send_json({'kind': 'storage#object', 'name': key, 'size': str(len(data))})

    def send_error(
        self,
        status: int,
        message: str | None = None,
        code: str = '',
        resource: str = '',
        extra_headers: dict[str, str] | None = None,
    ):
        # The JSON API error format has no fields for *code* and *resource*, so they are ignored.
        try:
            (reason, _) = self.responses[status]
        except KeyError:
            reason = 'Unknown'
        if not message:
            message = reason

        self.log_error("code %d, message %s", status, message)
        content = json.dumps({'error': {'code': status, 'message': message}}).encode('utf-8')
        # *message* may contain an object name, which need not be encodable as latin-1, so it
        # only goes into the body.
        self.send_response(status, reason)
        self.send_header('Content-Type', 'application/json; charset=UTF-8')
        self.send_header('Content-Length', str(len(content)))
        if extra_headers:
            for name, value in extra_headers.items():
                self.send_header(name, value)
        self.end_headers()
        if self.command != 'HEAD' and status >= 200 and status not in (204, 304):
            self.wfile.write(content)


@dataclass(frozen=True)
class MockBackendSpec:
    '''How to run a mock server and connect a backend to it'''

    handler: type[MockRequestHandler]

    #: Storage URL template, interpolated with the server's `host` and `port`.
    storage_url: str

    # Mock servers speak plain HTTP unless configured otherwise.
    backend_options: dict[str, str | bool] = field(default_factory=lambda: {'no-ssl': True})
    login: str = 'joe'
    password: str = 'swordfish'
    use_tls: bool = False


mock_backends = [
    MockBackendSpec(S3CRequestHandler, 's3c://%(host)s:%(port)d/s3ql_test'),
    MockBackendSpec(S3C4RequestHandler, 's3c4://%(host)s:%(port)d/s3ql_test'),
    # Special syntax only for testing against mock server
    MockBackendSpec(BasicSwiftRequestHandler, 'swift://%(host)s:%(port)d/s3ql_test'),
    MockBackendSpec(CopySwiftRequestHandler, 'swift://%(host)s:%(port)d/s3ql_test'),
    MockBackendSpec(BulkDeleteSwiftRequestHandler, 'swift://%(host)s:%(port)d/s3ql_test'),
    # The Google Storage backend always uses TLS and needs OAuth2 login. The `!unittest!`
    # storage URL prefix allows it to connect to a host other than Google's.
    MockBackendSpec(
        GSRequestHandler,
        'gs://!unittest!%(host)s:%(port)d/s3ql_test',
        backend_options={'ssl-ca-path': CA_CERT},
        login='oauth2',
        password='refresh-token',
        use_tls=True,
    ),
]
