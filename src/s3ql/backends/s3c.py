'''
backends/s3c.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

from ..logging import logging, QuietError # Ensure use of custom logger class
from .. import BUFSIZE
from .common import (AbstractBackend, NoSuchObject, retry, AuthorizationError,
                     AuthenticationError, DanglingStorageURLError, get_proxy,
                     get_ssl_context, CorruptedObjectError, checksum_basic_mapping)
from ..inherit_docstrings import (copy_ancestor_docstring, prepend_ancestor_docstring,
                                  ABCDocstMeta)
from io import BytesIO
from shutil import copyfileobj
from dugong import (HTTPConnection, is_temp_network_error, BodyFollowing, CaseInsensitiveDict,
                    UnsupportedResponse, ConnectionClosed)
from base64 import b64encode, b64decode
from email.utils import parsedate_tz, mktime_tz
from ast import literal_eval
from urllib.parse import urlsplit, quote, unquote
import defusedxml.cElementTree as ElementTree
from itertools import count
import hashlib
import os
import binascii
import hmac
import re
import tempfile
import time
import ssl
import urllib.parse

C_DAY_NAMES = [ 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun' ]
C_MONTH_NAMES = [ 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec' ]

XML_CONTENT_RE = re.compile(r'^(?:application|text)/xml(?:;|$)', re.IGNORECASE)

# Used only by adm.py
UPGRADE_MODE=False

log = logging.getLogger(__name__)

class Backend(AbstractBackend, metaclass=ABCDocstMeta):
    """A backend to stored data in some S3 compatible storage service.

    The backend guarantees only immediate get after create consistency.
    """

    xml_ns_prefix = '{http://s3.amazonaws.com/doc/2006-03-01/}'
    hdr_prefix = 'x-amz-'
    known_options = {'no-ssl', 'ssl-ca-path', 'tcp-timeout',
                     'dumb-copy', 'disable-expect100'}

    def __init__(self, options):
        '''Initialize backend object

        *ssl_context* may be a `ssl.SSLContext` instance or *None*.
        '''

        super().__init__()

        if 'no-ssl' in options.backend_options:
            self.ssl_context = None
        else:
            self.ssl_context = get_ssl_context(
                options.backend_options.get('ssl-ca-path', None))

        (host, port, bucket_name, prefix) = self._parse_storage_url(
            options.storage_url, self.ssl_context)

        self.options = options.backend_options
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.hostname = host
        self.port = port
        self.proxy = get_proxy(self.ssl_context is not None)
        self.conn = self._get_conn()
        self.password = options.backend_password
        self.login = options.backend_login

    @property
    @copy_ancestor_docstring
    def has_native_rename(self):
        return False

    # NOTE: ! This function is also used by the swift backend !
    @copy_ancestor_docstring
    def reset(self):
        if (self.conn is not None and
            (self.conn.response_pending() or self.conn._out_remaining)):
            log.debug('Resetting state of http connection %d', id(self.conn))
            self.conn.disconnect()

    @staticmethod
    def _parse_storage_url(storage_url, ssl_context):
        '''Extract information from storage URL

        Return a tuple * (host, port, bucket_name, prefix) * .
        '''

        hit = re.match(r'^[a-zA-Z0-9]+://' # Backend
                       r'([^/:]+)' # Hostname
                       r'(?::([0-9]+))?' # Port
                       r'/([^/]+)' # Bucketname
                       r'(?:/(.*))?$', # Prefix
                       storage_url)
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

    def _get_conn(self):
        '''Return connection to server'''

        conn =  HTTPConnection(self.hostname, self.port, proxy=self.proxy,
                               ssl_context=self.ssl_context)
        conn.timeout = int(self.options.get('tcp-timeout', 20))
        return conn

    # This method is also used implicitly for the retry handling of
    # `gs.Backend._get_access_token`. When modifying this method, do not forget
    # to check if this makes it unsuitable for use by `_get_access_token` (in
    # that case we will have to implement a custom retry logic there).
    @copy_ancestor_docstring
    def is_temp_failure(self, exc): #IGNORE:W0613
        if is_temp_network_error(exc) or isinstance(exc, ssl.SSLError):
            # We probably can't use the connection anymore, so use this
            # opportunity to reset it
            self.conn.reset()

        if isinstance(exc, (InternalError, BadDigestError, IncompleteBodyError,
                            RequestTimeoutError, OperationAbortedError,
                            SlowDownError, ServiceUnavailableError)):
            return True

        elif is_temp_network_error(exc):
            return True

        # In doubt, we retry on 5xx (Server error). However, there are some
        # codes where retry is definitely not desired. For 4xx (client error) we
        # do not retry in general, but for 408 (Request Timeout) RFC 2616
        # specifies that the client may repeat the request without
        # modifications.
        elif (isinstance(exc, HTTPError) and
              ((500 <= exc.status <= 599
                and exc.status not in (501,505,508,510,511,523))
               or exc.status == 408)):
            return True

        # Consider all SSL errors as temporary. There are a lot of bug
        # reports from people where various SSL errors cause a crash
        # but are actually just temporary. On the other hand, we have
        # no information if this ever revealed a problem where retrying
        # was not the right choice.
        elif isinstance(exc, ssl.SSLError):
            return True

        return False

    # NOTE: ! This function is also used by the swift backend. !
    def _dump_response(self, resp, body=None):
        '''Return string representation of server response

        Only the beginning of the response body is read, so this is
        mostly useful for debugging.
        '''

        if body is None:
            try:
                body = self.conn.read(2048)
                if body:
                    self.conn.discard()
            except UnsupportedResponse:
                log.warning('Unsupported response, trying to retrieve data from raw socket!')
                body = self.conn.read_raw(2048)
                self.conn.close()
        else:
            body = body[:2048]

        return '%d %s\n%s\n\n%s' % (resp.status, resp.reason,
                                    '\n'.join('%s: %s' % x for x in resp.headers.items()),
                                    body.decode('utf-8', errors='backslashreplace'))

    # NOTE: ! This function is also used by the swift backend. !
    def _assert_empty_response(self, resp):
        '''Assert that current response body is empty'''

        buf = self.conn.read(2048)
        if not buf:
            return # expected

        # Log the problem
        self.conn.discard()
        log.error('Unexpected server response. Expected nothing, got:\n'
                  '%d %s\n%s\n\n%s', resp.status, resp.reason,
                  '\n'.join('%s: %s' % x for x in resp.headers.items()),
                  buf)
        raise RuntimeError('Unexpected server response')

    @retry
    @copy_ancestor_docstring
    def delete(self, key, force=False, is_retry=False):
        log.debug('started with %s', key)
        try:
            resp = self._do_request('DELETE', '/%s%s' % (self.prefix, key))
            self._assert_empty_response(resp)
        except NoSuchKeyError:
            # Server may have deleted the object even though we did not
            # receive the response.
            if force or is_retry:
                pass
            else:
                raise NoSuchObject(key)

    @copy_ancestor_docstring
    def list(self, prefix=''):
        prefix = self.prefix + prefix
        strip = len(self.prefix)
        page_token = None
        while True:
            (els, page_token) = self._list_page(prefix, page_token)
            for el in els:
                yield el[strip:]
            if page_token is None:
                break

    @retry
    def _list_page(self, prefix, page_token=None, batch_size=1000):

        # We can get at most 1000 keys at a time, so there's no need
        # to bother with streaming.
        query_string = { 'prefix': prefix, 'max-keys': str(batch_size) }
        if page_token:
            query_string['marker'] = page_token

        resp = self._do_request('GET', '/', query_string=query_string)

        if not XML_CONTENT_RE.match(resp.headers['Content-Type']):
            raise RuntimeError('unexpected content type: %s' %
                               resp.headers['Content-Type'])

        body = self.conn.readall()
        etree = ElementTree.fromstring(body)
        root_xmlns_uri = _tag_xmlns_uri(etree)
        if root_xmlns_uri is None:
            root_xmlns_prefix = ''
        else:
            # Validate the XML namespace
            root_xmlns_prefix = '{%s}' % (root_xmlns_uri, )
            if root_xmlns_prefix != self.xml_ns_prefix:
                log.error('Unexpected server reply to list operation:\n%s',
                          self._dump_response(resp, body=body))
                raise RuntimeError('List response has unknown namespace')

        names = [ x.findtext(root_xmlns_prefix + 'Key')
                  for x in etree.findall(root_xmlns_prefix + 'Contents') ]

        is_truncated = etree.find(root_xmlns_prefix + 'IsTruncated')
        if is_truncated.text == 'false':
            page_token = None
        elif len(names) == 0:
            next_marker = etree.find(root_xmlns_prefix + 'NextMarker')
            page_token = next_marker.text
        else:
            page_token = names[-1]

        return (names, page_token)


    @retry
    @copy_ancestor_docstring
    def lookup(self, key):
        log.debug('started with %s', key)

        try:
            resp = self._do_request('HEAD', '/%s%s' % (self.prefix, key))
            self._assert_empty_response(resp)
        except HTTPError as exc:
            if exc.status == 404:
                raise NoSuchObject(key)
            else:
                raise

        return self._extractmeta(resp, key)

    @retry
    @copy_ancestor_docstring
    def get_size(self, key):
        log.debug('started with %s', key)

        try:
            resp = self._do_request('HEAD', '/%s%s' % (self.prefix, key))
            self._assert_empty_response(resp)
        except HTTPError as exc:
            if exc.status == 404:
                raise NoSuchObject(key)
            else:
                raise

        try:
            return int(resp.headers['Content-Length'])
        except KeyError:
            raise RuntimeError('HEAD request did not return Content-Length')


    @retry
    @copy_ancestor_docstring
    def open_read(self, key):
        try:
            resp = self._do_request('GET', '/%s%s' % (self.prefix, key))
        except NoSuchKeyError:
            raise NoSuchObject(key)

        try:
            meta = self._extractmeta(resp, key)
        except (BadDigestError, CorruptedObjectError):
            # If there's less than 64 kb of data, read and throw
            # away. Otherwise re-establish connection.
            if resp.length is not None and resp.length < 64*1024:
                self.conn.discard()
            else:
                self.conn.disconnect()
            raise

        return ObjectR(key, resp, self, meta)

    @prepend_ancestor_docstring
    def open_write(self, key, metadata=None, is_compressed=False, extra_headers=None):
        """
        The returned object will buffer all data and only start the upload
        when its `close` method is called.
        """

        log.debug('started with %s', key)

        headers = CaseInsensitiveDict()
        if extra_headers is not None:
            headers.update(extra_headers)
        if metadata is None:
            metadata = dict()
        self._add_meta_headers(headers, metadata)

        return ObjectW(key, self, headers)

    # NOTE: ! This function is also used by the swift backend. !
    def _add_meta_headers(self, headers, metadata, chunksize=255):

        hdr_count = 0
        length = 0
        for key in metadata.keys():
            if not isinstance(key, str):
                raise ValueError('dict keys must be str, not %s' % type(key))
            val = metadata[key]
            if (not isinstance(val, (str, bytes, int, float, complex, bool))
                and val is not None):
                raise ValueError('value for key %s (%s) is not elementary' % (key, val))

            if isinstance(val, (bytes, bytearray)):
                val = b64encode(val)

            buf = ('%s: %s,' % (repr(key), repr(val)))
            buf = quote(buf, safe='!@#$^*()=+/?-_\'"><\\| `.,;:~')
            if len(buf) < chunksize:
                headers['%smeta-%03d' % (self.hdr_prefix, hdr_count)] = buf
                hdr_count += 1
                length += 4 + len(buf)
            else:
                i = 0
                while i*chunksize < len(buf):
                    k = '%smeta-%03d' % (self.hdr_prefix, hdr_count)
                    v = buf[i*chunksize:(i+1)*chunksize]
                    headers[k] = v
                    i += 1
                    hdr_count += 1
                    length += 4 + len(buf)

            if length > 2048:
                raise ValueError('Metadata too large')

        assert hdr_count <= 999
        md5 = b64encode(checksum_basic_mapping(metadata)).decode('ascii')
        headers[self.hdr_prefix + 'meta-format'] = 'raw2'
        headers[self.hdr_prefix + 'meta-md5'] = md5

    @retry
    @copy_ancestor_docstring
    def copy(self, src, dest, metadata=None, extra_headers=None):
        log.debug('started with %s, %s', src, dest)

        headers = CaseInsensitiveDict()
        if extra_headers is not None:
            headers.update(extra_headers)
        headers[self.hdr_prefix + 'copy-source'] = \
            urllib.parse.quote('/%s/%s%s' % (self.bucket_name, self.prefix, src))

        if metadata is None:
            headers[self.hdr_prefix + 'metadata-directive'] = 'COPY'
        else:
            headers[self.hdr_prefix + 'metadata-directive'] = 'REPLACE'
            self._add_meta_headers(headers, metadata)

        try:
            resp = self._do_request('PUT', '/%s%s' % (self.prefix, dest), headers=headers)
        except NoSuchKeyError:
            raise NoSuchObject(src)

        # When copying, S3 may return error despite a 200 OK status
        # http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectCOPY.html
        # https://doc.s3.amazonaws.com/proposals/copy.html
        if self.options.get('dumb-copy', False):
            self.conn.discard()
            return
        body = self.conn.readall()
        root = self._parse_xml_response(resp, body)

        # Some S3 implemenentations do not have a namespace on
        # CopyObjectResult.
        if root.tag in [self.xml_ns_prefix + 'CopyObjectResult', 'CopyObjectResult']:
            return
        elif root.tag in [self.xml_ns_prefix + 'Error', 'Error']:
            raise get_S3Error(root.findtext('Code'), root.findtext('Message'),
                              resp.headers)
        else:
            log.error('Unexpected server reply to copy operation:\n%s',
                      self._dump_response(resp, body))
            raise RuntimeError('Copy response has %s as root tag' % root.tag)

    @copy_ancestor_docstring
    def update_meta(self, key, metadata):
        self.copy(key, key, metadata)

    def _do_request(self, method, path, subres=None, query_string=None,
                    headers=None, body=None):
        '''Send request, read and return response object'''

        log.debug('started with %s %s?%s, qs=%s', method, path, subres, query_string)

        if headers is None:
            headers = CaseInsensitiveDict()

        if isinstance(body, (bytes, bytearray, memoryview)):
            headers['Content-MD5'] = md5sum_b64(body)

        redirect_count = 0
        this_method = method
        while True:
            resp = self._send_request(this_method, path, headers=headers, subres=subres,
                                      query_string=query_string, body=body)

            if (resp.status < 300 or resp.status > 399):
                break

            # Assume redirect
            redirect_count += 1
            if redirect_count > 10:
                raise RuntimeError('Too many chained redirections')

            # First try location header...
            new_url = resp.headers['Location']
            if new_url:
                # Discard body
                self.conn.discard()

                # Pylint can't infer SplitResult Types
                #pylint: disable=E1103
                o = urlsplit(new_url)
                if o.scheme:
                    if self.ssl_context and o.scheme != 'https':
                        raise RuntimeError('Redirect to non-https URL')
                    elif not self.ssl_context and o.scheme != 'http':
                        raise RuntimeError('Redirect to non-http URL')
                if o.hostname != self.hostname or o.port != self.port:
                    self.hostname = o.hostname
                    self.port = o.port
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
                tree = self._parse_xml_response(resp)
                new_url = tree.findtext('Endpoint')

                if not new_url:
                    raise get_S3Error(tree.findtext('Code'), tree.findtext('Message'),
                                      resp.headers)

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
        self._parse_error_response(resp)

    def _parse_error_response(self, resp):
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
            assert self.conn.read(1) == b''
            raise HTTPError(resp.status, resp.reason, resp.headers)

        # If not XML, do the best we can
        if not XML_CONTENT_RE.match(content_type) or resp.length == 0:
            self.conn.discard()
            raise HTTPError(resp.status, resp.reason, resp.headers)

        # We don't stream the data into the parser because we want
        # to be able to dump a copy if the parsing fails.
        body = self.conn.readall()
        try:
            tree = ElementTree.parse(BytesIO(body)).getroot()
        except:
            log.error('Unable to parse server response as XML:\n%s',
                      self._dump_response(resp, body))
            raise

        raise get_S3Error(tree.findtext('Code'), tree.findtext('Message'), resp.headers)

    def _parse_xml_response(self, resp, body=None):
        '''Return element tree for XML response'''

        content_type = resp.headers['Content-Type']

        # AWS S3 sometimes "forgets" to send a Content-Type
        # when responding to a multiple delete request.
        # https://forums.aws.amazon.com/thread.jspa?threadID=134372
        if content_type is None:
            log.error('Server did not provide Content-Type, assuming XML')
        elif not XML_CONTENT_RE.match(content_type):
            log.error('Unexpected server reply: expected XML, got:\n%s',
                      self._dump_response(resp))
            raise RuntimeError('Unexpected server response')

        # We don't stream the data into the parser because we want
        # to be able to dump a copy if the parsing fails.
        if body is None:
            body = self.conn.readall()
        try:
            tree = ElementTree.parse(BytesIO(body)).getroot()
        except:
            log.error('Unable to parse server response as XML:\n%s',
                      self._dump_response(resp, body))
            raise

        return tree

    def __str__(self):
        return 's3c://%s/%s/%s' % (self.hostname, self.bucket_name, self.prefix)

    def _authorize_request(self, method, path, headers, subres, query_string):
        '''Add authorization information to *headers*'''

        # See http://docs.amazonwebservices.com/AmazonS3/latest/dev/RESTAuthentication.html

        # Date, can't use strftime because it's locale dependent
        now = time.gmtime()
        headers['Date'] = ('%s, %02d %s %04d %02d:%02d:%02d GMT'
                           % (C_DAY_NAMES[now.tm_wday],
                              now.tm_mday,
                              C_MONTH_NAMES[now.tm_mon - 1],
                              now.tm_year, now.tm_hour,
                              now.tm_min, now.tm_sec))

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
        #pylint: disable=E1101
        auth_str = ''.join(auth_strs).encode()
        signature = b64encode(hmac.new(self.password.encode(), auth_str,
                                       hashlib.sha1).digest()).decode()

        headers['Authorization'] = 'AWS %s:%s' % (self.login, signature)

    def _send_request(self, method, path, headers, subres=None, query_string=None, body=None):
        '''Add authentication and send request

        Returns the response object.
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

        # We can probably remove the assertions at some point and
        # call self.conn.read_response() directly
        def read_response():
            resp = self.conn.read_response()
            assert resp.method == method
            assert resp.path == path
            return resp

        use_expect_100c = not self.options.get('disable-expect100', False)
        log.debug('sending %s %s', method, path)
        if body is None or isinstance(body, (bytes, bytearray, memoryview)):
            self.conn.send_request(method, path, body=body, headers=headers)
        else:
            body_len = os.fstat(body.fileno()).st_size
            self.conn.send_request(method, path, expect100=use_expect_100c,
                                   headers=headers, body=BodyFollowing(body_len))

            if use_expect_100c:
                resp = read_response()
                if resp.status != 100: # Error
                    return resp

            try:
                copyfileobj(body, self.conn, BUFSIZE)
            except ConnectionClosed:
                # Server closed connection while we were writing body data -
                # but we may still be able to read an error response
                try:
                    resp = read_response()
                except ConnectionClosed: # No server response available
                    pass
                else:
                    if resp.status >= 400: # Got error response
                        return resp
                    log.warning('Server broke connection during upload, but signaled '
                                '%d %s', resp.status, resp.reason)

                # Re-raise first ConnectionClosed exception
                raise

        return read_response()

    @copy_ancestor_docstring
    def close(self):
        self.conn.disconnect()

    # NOTE: ! This function is also used by the swift backend !
    def _extractmeta(self, resp, obj_key):
        '''Extract metadata from HTTP response object'''

        format_ = resp.headers.get('%smeta-format' % self.hdr_prefix, 'raw')
        if format_ != 'raw2': # Current
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
        for (k,v) in meta.items():
            if not isinstance(v, bytes):
                continue
            try:
                meta[k] = b64decode(v)
            except binascii.Error:
                # This should trigger a MD5 mismatch below
                meta[k] = None

        # TODO: Remove on next file system revision bump
        if UPGRADE_MODE:
            md5 = resp.headers.get('%smeta-md5' % self.hdr_prefix, None)
            if md5 == b64encode(checksum_basic_mapping(meta)).decode('ascii'):
                meta['needs_reupload'] = False
            elif md5 == b64encode(UPGRADE_MODE(meta)).decode('ascii'):
                meta['needs_reupload'] = True
            else:
                raise BadDigestError('BadDigest', 'Meta MD5 for %s does not match' % obj_key)
            return meta

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


def _tag_xmlns_uri(elem):
    '''Extract the XML namespace (xmlns) URI from an element'''
    if elem.tag[0] == '{':
        uri, ignore, tag = elem.tag[1:].partition("}")
    else:
        uri = None
    return uri


class ObjectR(object):
    '''An S3 object open for reading'''

    # NOTE: This class is used as a base class for the swift backend,
    # so changes here should be checked for their effects on other
    # backends.

    def __init__(self, key, resp, backend, metadata=None):
        self.key = key
        self.resp = resp
        self.closed = False
        self.md5_checked = False
        self.backend = backend
        self.metadata = metadata

        # False positive, hashlib *does* have md5 member
        #pylint: disable=E1101
        self.md5 = hashlib.md5()

    def read(self, size=None):
        '''Read up to *size* bytes of object data

        For integrity checking to work, this method has to be called until
        it returns an empty string, indicating that all data has been read
        (and verified).
        '''

        if size == 0:
            return b''

        # This may raise an exception, in which case we probably can't
        # re-use the connection. However, we rely on the caller
        # to still close the file-like object, so that we can do
        # cleanup in close().
        buf = self.backend.conn.read(size)
        self.md5.update(buf)

        # Check MD5 on EOF
        # (size == None implies EOF)
        if (not buf or size is None) and not self.md5_checked:
            etag = self.resp.headers['ETag'].strip('"')
            self.md5_checked = True
            if etag != self.md5.hexdigest():
                log.warning('MD5 mismatch for %s: %s vs %s',
                            self.key, etag, self.md5.hexdigest())
                raise BadDigestError('BadDigest',
                                     'ETag header does not agree with calculated MD5')
        return buf

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()
        return False

    def close(self, checksum_warning=True):
        '''Close object

        If *checksum_warning* is true, this will generate a warning message if
        the object has not been fully read (because in that case the MD5
        checksum cannot be checked).
        '''

        if self.closed:
            return
        self.closed = True

        # If we have not read all the data, close the entire
        # connection (otherwise we loose synchronization)
        if not self.md5_checked:
            if checksum_warning:
                log.warning("Object closed prematurely, can't check MD5, and have to "
                            "reset connection")
            self.backend.conn.disconnect()


class ObjectW(object):
    '''An S3 object open for writing

    All data is first cached in memory, upload only starts when
    the close() method is called.
    '''

    # NOTE: This class is used as a base class for the swift backend,
    # so changes here should be checked for their effects on other
    # backends.

    def __init__(self, key, backend, headers):
        self.key = key
        self.backend = backend
        self.headers = headers
        self.closed = False
        self.obj_size = 0

        # According to http://docs.python.org/3/library/functions.html#open
        # the buffer size is typically ~8 kB. We process data in much
        # larger chunks, so buffering would only hurt performance.
        self.fh = tempfile.TemporaryFile(buffering=0)

        # False positive, hashlib *does* have md5 member
        #pylint: disable=E1101
        self.md5 = hashlib.md5()

    def write(self, buf):
        '''Write object data'''

        self.fh.write(buf)
        self.md5.update(buf)
        self.obj_size += len(buf)

    def is_temp_failure(self, exc):
        return self.backend.is_temp_failure(exc)

    @retry
    def close(self):
        '''Close object and upload data'''

        # Access to protected member ok
        #pylint: disable=W0212

        log.debug('started with %s', self.key)

        if self.closed:
            # still call fh.close, may have generated an error before
            self.fh.close()
            return

        self.fh.seek(0)
        self.headers['Content-Type'] = 'application/octet-stream'
        resp = self.backend._do_request('PUT', '/%s%s' % (self.backend.prefix, self.key),
                                        headers=self.headers, body=self.fh)
        etag = resp.headers['ETag'].strip('"')
        self.backend._assert_empty_response(resp)

        if etag != self.md5.hexdigest():
            # delete may fail, but we don't want to loose the BadDigest exception
            try:
                self.backend.delete(self.key)
            finally:
                raise BadDigestError('BadDigest', 'MD5 mismatch for %s (received: %s, sent: %s)' %
                                     (self.key, etag, self.md5.hexdigest()))

        self.closed = True
        self.fh.close()

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()
        return False

    def get_obj_size(self):
        if not self.closed:
            raise RuntimeError('Object must be closed first.')
        return self.obj_size


def get_S3Error(code, msg, headers=None):
    '''Instantiate most specific S3Error subclass'''

    # Special case
    # http://code.google.com/p/s3ql/issues/detail?id=369
    if code == 'Timeout':
        code = 'RequestTimeout'

    if code.endswith('Error'):
        name = code
    else:
        name = code + 'Error'
    class_ = globals().get(name, S3Error)

    if not issubclass(class_, S3Error):
        return S3Error(code, msg, headers)

    return class_(code, msg, headers)

def md5sum_b64(buf):
    '''Return base64 encoded MD5 sum'''

    return b64encode(hashlib.md5(buf).digest()).decode('ascii')

def _parse_retry_after(header):
    '''Parse headers for Retry-After value'''

    hit = re.match(r'^\s*([0-9]+)\s*$', header)
    if hit:
        val = int(header)
    else:
        date = parsedate_tz(header)
        if date is None:
            log.warning('Unable to parse retry-after value: %s', header)
            return None
        val = mktime_tz(*date) - time.time()

    val_clamp = min(300, max(1, val))
    if val != val_clamp:
        log.warning('Ignoring retry-after value of %.3f s, using %.3f s instead', val, val_clamp)
        val = val_clamp

    return val


class HTTPError(Exception):
    '''
    Represents an HTTP error returned by S3.
    '''

    def __init__(self, status, msg, headers=None):
        super().__init__()
        self.status = status
        self.msg = msg
        self.headers = headers

        if headers and 'Retry-After' in headers:
            self.retry_after = _parse_retry_after(headers['Retry-After'])
        else:
            self.retry_after = None

    def __str__(self):
        return '%d %s' % (self.status, self.msg)


class S3Error(Exception):
    '''
    Represents an error returned by S3. For possible codes, see
    http://docs.amazonwebservices.com/AmazonS3/latest/API/ErrorResponses.html
    '''

    def __init__(self, code, msg, headers=None):
        super().__init__(msg)
        self.code = code
        self.msg = msg

        if headers and 'Retry-After' in headers:
            self.retry_after = _parse_retry_after(headers['Retry-After'])
        else:
            self.retry_after = None

    def __str__(self):
        return '%s: %s' % (self.code, self.msg)

class NoSuchKeyError(S3Error): pass
class AccessDeniedError(S3Error, AuthorizationError): pass
class BadDigestError(S3Error): pass
class IncompleteBodyError(S3Error): pass
class InternalError(S3Error): pass
class InvalidAccessKeyIdError(S3Error, AuthenticationError): pass
class InvalidSecurityError(S3Error, AuthenticationError): pass
class SignatureDoesNotMatchError(S3Error, AuthenticationError): pass
class OperationAbortedError(S3Error): pass
class RequestTimeoutError(S3Error): pass
class SlowDownError(S3Error): pass
class ServiceUnavailableError(S3Error): pass
class RequestTimeTooSkewedError(S3Error): pass
class NoSuchBucketError(S3Error, DanglingStorageURLError): pass
