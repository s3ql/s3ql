'''
backends/s3c.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from ..logging import logging # Ensure use of custom logger class
from ..common import QuietError, PICKLE_PROTOCOL, ChecksumError, BUFSIZE
from .common import (AbstractBackend, NoSuchObject, retry, AuthorizationError,
    AuthenticationError, DanglingStorageURLError, retry_generator)
from ..inherit_docstrings import (copy_ancestor_docstring, prepend_ancestor_docstring,
                                  ABCDocstMeta)
from io import BytesIO
from shutil import copyfileobj
from dugong import (HTTPConnection, is_temp_network_error, BodyFollowing, CaseInsensitiveDict,
                    UnsupportedResponse)
from base64 import b64encode, b64decode
from email.utils import parsedate_tz, mktime_tz
from urllib.parse import urlsplit
import defusedxml.cElementTree as ElementTree
import hashlib
import os
import hmac
import re
import tempfile
import time
import urllib.parse
import pickle

C_DAY_NAMES = [ 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun' ]
C_MONTH_NAMES = [ 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec' ]

XML_CONTENT_RE = re.compile(r'^(?:application|text)/xml(?:;|$)', re.IGNORECASE)

log = logging.getLogger(__name__)

class Backend(AbstractBackend, metaclass=ABCDocstMeta):
    """A backend to stored data in some S3 compatible storage service.

    This class uses standard HTTP connections to connect to GS.

    The backend guarantees only immediate get after create consistency.

    If the *expect_100c* class variable is True, the 'Expect: 100-continue'
    header is used to check for error codes before uploading payload data.
    """

    use_expect_100c = True
    xml_ns_prefix = '{http://s3.amazonaws.com/doc/2006-03-01/}'

    def __init__(self, storage_url, login, password, ssl_context=None,
                 proxy=None):
        '''Initialize backend object

        *ssl_context* may be a `ssl.SSLContext` instance or *None*.
        '''

        super().__init__()

        (host, port, bucket_name, prefix) = self._parse_storage_url(storage_url, ssl_context)

        self.bucket_name = bucket_name
        self.prefix = prefix
        self.hostname = host
        self.port = port
        self.ssl_context = ssl_context
        self.proxy = proxy
        self.conn = self._get_conn()

        self.password = password
        self.login = login
        self.hdr_prefix = 'x-amz-'

    @copy_ancestor_docstring
    def reset(self):
        if self.conn.response_pending() or self.conn._out_remaining:
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

        return HTTPConnection(self.hostname, self.port, proxy=self.proxy,
                              ssl_context=self.ssl_context)

    @copy_ancestor_docstring
    def is_temp_failure(self, exc): #IGNORE:W0613
        if isinstance(exc, (InternalError, BadDigestError, IncompleteBodyError,
                            RequestTimeoutError, OperationAbortedError, SlowDownError,
                            RequestTimeTooSkewedError)):
            return True

        elif is_temp_network_error(exc):
            return True

        # In doubt, we retry on 5xx. However, there are some codes
        # where retry is definitely not desired.
        elif (isinstance(exc, HTTPError)
              and exc.status >= 500 and exc.status <= 599
              and exc.status not in (501,505,508,510,511,523)):
            return True

        return False

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
    def delete(self, key, force=False):
        log.debug('delete(%s)', key)
        try:
            resp = self._do_request('DELETE', '/%s%s' % (self.prefix, key))
            self._assert_empty_response(resp)
        except NoSuchKeyError:
            if force:
                pass
            else:
                raise NoSuchObject(key)

    @retry_generator
    @copy_ancestor_docstring
    def list(self, prefix='', start_after=''):
        log.debug('list(%s, %s): start', prefix, start_after)

        keys_remaining = True
        marker = start_after
        prefix = self.prefix + prefix
        ns_p = self.xml_ns_prefix

        while keys_remaining:
            log.debug('list(%s): requesting with marker=%s', prefix, marker)

            keys_remaining = None
            resp = self._do_request('GET', '/', query_string={ 'prefix': prefix,
                                                              'marker': marker,
                                                              'max-keys': 1000 })

            if not XML_CONTENT_RE.match(resp.headers['Content-Type']):
                raise RuntimeError('unexpected content type: %s' %
                                   resp.headers['Content-Type'])

            itree = iter(ElementTree.iterparse(self.conn, events=("start", "end")))
            (event, root) = next(itree)

            try:
                for (event, el) in itree:
                    if event != 'end':
                        continue

                    if el.tag == ns_p + 'IsTruncated':
                        keys_remaining = (el.text == 'true')

                    elif el.tag == ns_p + 'Contents':
                        marker = el.findtext(ns_p + 'Key')
                        yield marker[len(self.prefix):]
                        root.clear()

            except GeneratorExit:
                # Need to read rest of response
                self.conn.discard()
                break

            if keys_remaining is None:
                raise RuntimeError('Could not parse body')

    @retry
    @copy_ancestor_docstring
    def lookup(self, key):
        log.debug('lookup(%s)', key)

        try:
            resp = self._do_request('HEAD', '/%s%s' % (self.prefix, key))
            self._assert_empty_response(resp)
        except HTTPError as exc:
            if exc.status == 404:
                raise NoSuchObject(key)
            else:
                raise

        return self._extractmeta(resp)

    @retry
    @copy_ancestor_docstring
    def get_size(self, key):
        log.debug('get_size(%s)', key)

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

        return ObjectR(key, resp, self, self._extractmeta(resp))

    @prepend_ancestor_docstring
    def open_write(self, key, metadata=None, is_compressed=False):
        """
        The returned object will buffer all data and only start the upload
        when its `close` method is called.
        """

        log.debug('open_write(%s): start', key)

        headers = CaseInsensitiveDict()
        self._add_meta_headers(headers, metadata)

        return ObjectW(key, self, headers)

    def _add_meta_headers(self, headers, metadata):

        # We don't store the metadata keys directly, because HTTP headers
        # are case insensitive (so the server may change capitalization)
        # and we may run into length restrictions.
        meta_buf = b64encode(pickle.dumps(metadata, PICKLE_PROTOCOL)).decode('us-ascii')

        chunksize = 255
        i = 0
        headers[self.hdr_prefix + 'meta-format'] = 'pickle'
        while i*chunksize < len(meta_buf):
            headers[self.hdr_prefix +
                    'meta-data-%02d' % i] = meta_buf[i*chunksize:(i+1)*chunksize]
            i += 1

    @retry
    @copy_ancestor_docstring
    def copy(self, src, dest, metadata=None):
        log.debug('copy(%s, %s): start', src, dest)

        headers = CaseInsensitiveDict()
        headers[self.hdr_prefix + 'copy-source'] = \
            '/%s/%s%s' % (self.bucket_name, self.prefix, src)

        if metadata is None:
            headers[self.hdr_prefix + 'metadata-directive'] = 'COPY'
        else:
            headers[self.hdr_prefix + 'metadata-directive'] = 'REPLACE'
            self._add_meta_headers(headers, metadata)

        try:
            self._do_request('PUT', '/%s%s' % (self.prefix, dest), headers=headers)
            self.conn.discard()
        except NoSuchKeyError:
            raise NoSuchObject(src)

    def _do_request(self, method, path, subres=None, query_string=None,
                    headers=None, body=None):
        '''Send request, read and return response object'''

        log.debug('_do_request(): start with parameters (%r, %r, %r, %r, %r, %r)',
                  method, path, subres, query_string, headers, body)

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
        if not XML_CONTENT_RE.match(content_type):
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

    def _parse_xml_response(self, resp):
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
        body = self.conn.readall()
        try:
            tree = ElementTree.parse(BytesIO(body)).getroot()
        except:
            log.error('Unable to parse server response as XML:\n%s',
                      self._dump_response(resp, body))
            raise

        return tree

    @prepend_ancestor_docstring
    def clear(self):
        """
        This method may not be able to see (and therefore also not delete)
        recently uploaded objects.
        """

        # We have to cache keys, because otherwise we can't use the
        # http connection to delete keys.
        for (no, s3key) in enumerate(list(self)):
            if no != 0 and no % 1000 == 0:
                log.info('clear(): deleted %d objects so far..', no)

            log.debug('clear(): deleting key %s', s3key)

            # Ignore missing objects when clearing bucket
            self.delete(s3key, True)

    def __str__(self):
        return 's3c://%s/%s/%s' % (self.hostname, self.bucket_name, self.prefix)

    def _authorize_request(self, method, path, headers, subres):
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
        sign_path = urllib.parse.quote('/%s%s' % (self.bucket_name, path))
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

        self._authorize_request(method, path, headers, subres)

        # Construct full path
        if not self.hostname.startswith(self.bucket_name):
            path = '/%s%s' % (self.bucket_name, path)
        path = urllib.parse.quote(path)
        if query_string:
            s = urllib.parse.urlencode(query_string, doseq=True)
            if subres:
                path += '?%s&%s' % (subres, s)
            else:
                path += '?%s' % s
        elif subres:
            path += '?%s' % subres

        try:
            log.debug('_send_request(): %s %s', method, path)
            if body is None or isinstance(body, (bytes, bytearray, memoryview)):
                self.conn.send_request(method, path, body=body, headers=headers)
            else:
                body_len = os.fstat(body.fileno()).st_size
                self.conn.send_request(method, path, expect100=self.use_expect_100c,
                                       headers=headers, body=BodyFollowing(body_len))

                if self.use_expect_100c:
                    resp = self.conn.read_response()
                    assert resp.method == method
                    assert resp.path == path
                    if resp.status != 100:
                        # Error
                        return resp

                copyfileobj(body, self.conn, BUFSIZE)

            resp = self.conn.read_response()
            assert resp.method == method
            assert resp.path == path
            return resp

        except Exception as exc:
            if is_temp_network_error(exc):
                # We probably can't use the connection anymore
                self.conn.disconnect()
            raise

    @copy_ancestor_docstring
    def close(self):
        self.conn.disconnect()

    def _extractmeta(self, resp):
        '''Extract metadata from HTTP response object'''

        meta = dict()
        pattern = re.compile(r'^%smeta-(.+)$' % re.escape(self.hdr_prefix),
                             re.IGNORECASE)
        format_ = 'raw'
        for (name, val) in resp.headers.items():
            # HTTP headers are case-insensitive and pre 2.x S3QL versions metadata
            # names verbatim (and thus loose capitalization info), so we force lower
            # case (since we know that this is the original capitalization).
            name = name.lower()

            hit = pattern.search(name)
            if not hit:
                continue

            if hit.group(1) == 'format':
                format_ = val
            else:
                meta[hit.group(1)] = val

        if format_ == 'pickle':
            buf = ''.join(meta[x] for x in sorted(meta)
                          if x.startswith('data-'))
            try:
                return pickle.loads(b64decode(buf))
            except pickle.UnpicklingError as exc:
                if (isinstance(exc.args[0], str)
                    and exc.args[0].startswith('invalid load key')):
                    raise ChecksumError('Invalid metadata')
                raise
        else:
            return meta

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

        log.debug('ObjectW(%s).close(): start', self.key)

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

    if val > 300 or val < 0:
        log.warning('Ignoring retry-after value of %.3f s, using 1 s instead', val)
        val = 1

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
class RequestTimeTooSkewedError(S3Error): pass
class NoSuchBucketError(S3Error, DanglingStorageURLError): pass
