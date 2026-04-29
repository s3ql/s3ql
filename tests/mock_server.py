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
import re
import socketserver
import urllib.parse
from http.server import BaseHTTPRequestHandler
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

COPY_RESPONSE_TEMPLATE = '''\
<?xml version="1.0" encoding="UTF-8"?>
<CopyObjectResult xmlns="%(ns)s">
   <LastModified>2008-02-20T22:13:01</LastModified>
   <ETag>&quot;%(etag)s&quot;</ETag>
</CopyObjectResult>
'''


class StorageServer(socketserver.ThreadingTCPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, request_handler, server_address):
        super().__init__(server_address, request_handler)
        self.data = dict()
        self.metadata = dict()
        self.hostname = self.server_address[0]
        self.port = self.server_address[1]


class ParsedURL:
    __slots__ = ['bucket', 'key', 'params', 'fragment']


class S3CRequestHandler(BaseHTTPRequestHandler):
    '''A request handler implementing a subset of the AWS S3 Interface

    Bucket names are ignored, all keys share the same global
    namespace.
    '''

    server_version = "MockHTTP"
    protocol_version = 'HTTP/1.1'
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

    def _check_encoding(self) -> int | None:
        encoding = self.headers['Content-Encoding']
        if 'Content-Length' not in self.headers:
            self.send_error(400, message='Missing Content-Length', code='MissingContentLength')
            return None
        elif encoding and encoding != 'identity':
            self.send_error(501, message='Unsupported encoding', code='NotImplemented')
            return None

        return int(self.headers['Content-Length'])

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

    def send_data(self, data):
        self.wfile.write(data)

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

    Currently identical to S3CRequestHandler since mock request handlers
    do not check request signatures.
    '''

    pass


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



class B2RequestHandler(BaseHTTPRequestHandler):
    '''A request handler implementing a subset of the Backblaze B2 API.

    Provides just enough of the B2 HTTP API for the backend tests to run
    against a local mock server without requiring real B2 credentials.
    '''

    server_version = 'MockHTTP'
    protocol_version = 'HTTP/1.1'
    _api_prefix = '/b2api/v2/'
    _bucket_id = 'test_bucket_id_001'

    def log_message(self, format, *args):
        log.debug(format, *args)

    def handle(self):
        try:
            return super().handle()
        except ValueError as exc:
            if exc.args == ('I/O operation on closed file.',):
                pass
            else:
                raise
        except (BrokenPipeError, ConnectionResetError):
            pass

    # -- initialisation of B2-specific server state --

    def _init_b2_state(self):
        if not hasattr(self.server, 'b2_file_counter'):
            self.server.b2_file_counter = 0
            # {file_id: {'fileName': str, 'data': bytes, 'headers': dict}}
            self.server.b2_files = {}
            # {filename: [file_id_newest, ..., file_id_oldest]}
            self.server.b2_versions_by_name = {}

    def _new_file_id(self):
        self.server.b2_file_counter += 1
        return 'b2_file_%08d' % self.server.b2_file_counter

    # -- response helpers --

    def _send_json(self, status, payload):
        data = json.dumps(payload).encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _send_json_error(self, status, code, message):
        self._send_json(status, {'status': status, 'code': code, 'message': message})

    def _read_json(self):
        length = int(self.headers.get('Content-Length', 0))
        return json.loads(self.rfile.read(length).decode('utf-8'))

    # -- request dispatchers --

    def do_GET(self):
        self._init_b2_state()
        path = urllib.parse.urlsplit(self.path).path
        if path == self._api_prefix + 'b2_authorize_account':
            self._handle_authorize()
        elif path.startswith('/file/'):
            self._handle_download(path)
        else:
            self._send_json_error(404, 'not_found', 'Not found: ' + self.path)

    def do_HEAD(self):
        self._init_b2_state()
        path = urllib.parse.urlsplit(self.path).path
        if path.startswith('/file/'):
            self._handle_head(path)
        else:
            self.send_response(404)
            self.send_header('Content-Length', '0')
            self.end_headers()

    def do_POST(self):
        self._init_b2_state()
        path = urllib.parse.urlsplit(self.path).path
        if path.startswith(self._api_prefix):
            action = path[len(self._api_prefix):]
            data = self._read_json()
            dispatch = {
                'b2_list_buckets': self._handle_list_buckets,
                'b2_list_file_versions': self._handle_list_file_versions,
                'b2_list_file_names': self._handle_list_file_names,
                'b2_get_upload_url': self._handle_get_upload_url,
                'b2_delete_file_version': self._handle_delete_file_version,
            }
            handler = dispatch.get(action)
            if handler:
                handler(data)
            else:
                self._send_json_error(404, 'not_found', 'Unknown action: ' + action)
        elif path == '/upload':
            self._handle_upload()
        else:
            self._send_json_error(404, 'not_found', 'Not found: ' + self.path)

    # -- B2 API endpoint handlers --

    def _handle_authorize(self):
        host = self.headers.get('Host', 'localhost')
        self._send_json(200, {
            'accountId': 'test_account',
            'apiUrl': 'http://' + host,
            'downloadUrl': 'http://' + host,
            'authorizationToken': 'test_auth_token',
            'allowed': {
                'capabilities': [
                    'listBuckets',
                    'listFiles',
                    'readFiles',
                    'writeFiles',
                    'deleteFiles',
                ],
                'bucketId': None,
                'bucketName': None,
            },
        })

    def _handle_list_buckets(self, data):
        # Echo back whichever bucket name the client asked for so that the
        # bucket-ID lookup always succeeds regardless of the storage URL used.
        bucket_name = data.get('bucketName', 's3qltest')
        self._send_json(200, {
            'buckets': [
                {
                    'bucketId': self._bucket_id,
                    'bucketName': bucket_name,
                    'bucketType': 'allPrivate',
                }
            ]
        })

    def _handle_list_file_versions(self, data):
        start_filename = data.get('startFileName', '')
        start_file_id = data.get('startFileId', None)
        max_count = min(data.get('maxFileCount', 1000), 1000)

        # Build sorted list of all versions (alphabetical by name, newest-first
        # within the same name - matching the real B2 API ordering).
        all_versions = []
        for filename in sorted(self.server.b2_versions_by_name):
            if filename >= start_filename:
                for file_id in self.server.b2_versions_by_name[filename]:
                    all_versions.append({'fileName': filename, 'fileId': file_id})

        # Apply startFileId pagination (skip up to and including that entry).
        if start_file_id:
            for i, v in enumerate(all_versions):
                if v['fileId'] == start_file_id:
                    all_versions = all_versions[i + 1:]
                    break

        page = all_versions[:max_count]
        next_filename = next_file_id = None
        if len(all_versions) > max_count:
            next_filename = all_versions[max_count]['fileName']
            next_file_id = all_versions[max_count]['fileId']

        self._send_json(200, {
            'files': page,
            'nextFileName': next_filename,
            'nextFileId': next_file_id,
        })

    def _handle_list_file_names(self, data):
        prefix = data.get('prefix', '')
        start_filename = data.get('startFileName', prefix)
        max_count = min(data.get('maxFileCount', 1000), 1000)

        files = []
        for filename in sorted(self.server.b2_versions_by_name):
            if filename.startswith(prefix) and filename >= start_filename:
                latest_id = self.server.b2_versions_by_name[filename][0]
                files.append({'fileName': filename, 'fileId': latest_id})

        page = files[:max_count]
        next_filename = None
        if len(files) > max_count:
            next_filename = files[max_count]['fileName']

        self._send_json(200, {'files': page, 'nextFileName': next_filename})

    def _handle_get_upload_url(self, data):
        host = self.headers.get('Host', 'localhost')
        self._send_json(200, {
            'bucketId': self._bucket_id,
            'uploadUrl': 'http://' + host + '/upload',
            'authorizationToken': 'upload_auth_token',
        })

    def _handle_upload(self):
        content_length = int(self.headers.get('Content-Length', 0))
        raw = self.rfile.read(content_length)

        # The B2 backend appends the 40-byte SHA1 hex-digest after the actual
        # file data (indicated by the 'hex_digits_at_end' sentinel value in the
        # X-Bz-Content-Sha1 header).
        sha1_hex = raw[-40:].decode('ascii')
        actual_data = raw[:-40]

        import hashlib as _hashlib
        computed_sha1 = _hashlib.sha1(actual_data).hexdigest()
        if computed_sha1 != sha1_hex:
            self._send_json_error(400, 'bad_digest', 'SHA1 mismatch')
            return

        # URL-decode the file name (B2 URL-encodes it in the header).
        file_name_encoded = self.headers.get('X-Bz-File-Name', '')
        file_name = urllib.parse.unquote(file_name_encoded)

        # Collect all B2 info headers to be returned on download/HEAD.
        stored_headers = {'X-Bz-Content-Sha1': sha1_hex}
        for k, v in self.headers.items():
            if k.lower().startswith('x-bz-info-'):
                stored_headers[k] = v

        file_id = self._new_file_id()
        self.server.b2_files[file_id] = {
            'fileName': file_name,
            'data': actual_data,
            'headers': stored_headers,
        }
        if file_name not in self.server.b2_versions_by_name:
            self.server.b2_versions_by_name[file_name] = []
        # Prepend so index 0 is always the newest version.
        self.server.b2_versions_by_name[file_name].insert(0, file_id)

        self._send_json(200, {
            'fileId': file_id,
            'fileName': file_name,
            'accountId': 'test_account',
            'bucketId': self._bucket_id,
            'contentLength': len(actual_data),
            'contentSha1': sha1_hex,
        })

    def _handle_delete_file_version(self, data):
        file_id = data.get('fileId')
        file_name = data.get('fileName')

        if file_id not in self.server.b2_files:
            self._send_json_error(400, 'file_not_present', 'File not found')
            return

        del self.server.b2_files[file_id]
        versions = self.server.b2_versions_by_name.get(file_name, [])
        if file_id in versions:
            versions.remove(file_id)
        if not versions:
            self.server.b2_versions_by_name.pop(file_name, None)

        self._send_json(200, {'fileId': file_id, 'fileName': file_name})

    # -- file download helpers --

    def _lookup_file(self, path):
        '''Return (file_name, file_info) for *path*, or (None, None) if missing.'''
        # path has the form /file/{bucket}/{url-encoded-filename}
        parts = path.split('/', 3)
        if len(parts) < 4:
            return None, None
        file_name = urllib.parse.unquote(parts[3])
        versions = self.server.b2_versions_by_name.get(file_name)
        if not versions:
            return None, None
        return file_name, self.server.b2_files[versions[0]]

    def _send_file_headers(self, file_name, file_info, include_body=True):
        data = file_info['data']
        file_name_encoded = urllib.parse.quote(file_name, safe='/')
        self.send_response(200)
        self.send_header('Content-Length', str(len(data)))
        self.send_header('X-Bz-File-Name', file_name_encoded)
        for k, v in file_info['headers'].items():
            self.send_header(k, v)
        self.end_headers()
        if include_body:
            self.wfile.write(data)

    def _handle_download(self, path):
        file_name, file_info = self._lookup_file(path)
        if file_info is None:
            self._send_json_error(404, 'not_found', 'File not found')
            return
        self._send_file_headers(file_name, file_info, include_body=True)

    def _handle_head(self, path):
        file_name, file_info = self._lookup_file(path)
        if file_info is None:
            self.send_response(404)
            self.send_header('Content-Length', '0')
            self.end_headers()
            return
        self._send_file_headers(file_name, file_info, include_body=False)


#: A list of the available mock request handlers with
#: corresponding storage urls
handler_list = [
    (S3CRequestHandler, 's3c://%(host)s:%(port)d/s3ql_test'),
    (S3C4RequestHandler, 's3c4://%(host)s:%(port)d/s3ql_test'),
    # Special syntax only for testing against mock server
    (BasicSwiftRequestHandler, 'swift://%(host)s:%(port)d/s3ql_test'),
    (CopySwiftRequestHandler, 'swift://%(host)s:%(port)d/s3ql_test'),
    (BulkDeleteSwiftRequestHandler, 'swift://%(host)s:%(port)d/s3ql_test'),
    # B2 mock: auth-host:port is embedded in the URL so the backend redirects
    # its b2_authorize_account call to the local mock server.
    (B2RequestHandler, 'b2://%(host)s:%(port)d@s3qltest/'),
]
