'''
backends/s3c.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from ..logging import logging # Ensure use of custom logger class
from ..common import BUFSIZE, QuietError, PICKLE_PROTOCOL, ChecksumError, md5sum
from .common import (AbstractBackend, NoSuchObject, retry, AuthorizationError, http_connection, 
    AuthenticationError, DanglingStorageURLError, is_temp_network_error, retry_generator)
from ..inherit_docstrings import (copy_ancestor_docstring, prepend_ancestor_docstring,
                                  ABCDocstMeta)
from base64 import b64encode, b64decode
from email.utils import parsedate_tz, mktime_tz
from urllib.parse import urlsplit
from xml.etree import ElementTree
from http.client import _MAXLINE, CONTINUE, LineTooLong
import hashlib
import hmac
import http.client
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

    def __init__(self, storage_url, login, password, ssl_context):
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
        self.conn = self._get_conn()

        self.password = password
        self.login = login
        self.xml_ns_prefix = '{http://s3.amazonaws.com/doc/2006-03-01/}'

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
            raise QuietError('Invalid storage URL')

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
        
        return http_connection(self.hostname, self.port, self.ssl_context)

    @copy_ancestor_docstring
    def is_temp_failure(self, exc): #IGNORE:W0613
        if isinstance(exc, (InternalError, BadDigestError, IncompleteBodyError, 
                            RequestTimeoutError, OperationAbortedError, SlowDownError, 
                            RequestTimeTooSkewedError)):
            return True

        elif is_temp_network_error(exc):
            return True
        
        elif isinstance(exc, HTTPError) and exc.status >= 500 and exc.status <= 599:
            return True
        
        return False

    @retry
    @copy_ancestor_docstring
    def delete(self, key, force=False):
        log.debug('delete(%s)', key)
        try:
            resp = self._do_request('DELETE', '/%s%s' % (self.prefix, key))
            assert resp.length == 0
        except NoSuchKeyError:
            if force:
                pass
            else:
                raise NoSuchObject(key) from None

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

            if not XML_CONTENT_RE.match(resp.getheader('Content-Type')):
                raise RuntimeError('unexpected content type: %s' % resp.getheader('Content-Type'))

            itree = iter(ElementTree.iterparse(resp, events=("start", "end")))
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
                while True:
                    buf = resp.read(BUFSIZE)
                    if buf == b'':
                        break
                break

            if keys_remaining is None:
                raise RuntimeError('Could not parse body')

    @retry
    @copy_ancestor_docstring
    def lookup(self, key):
        log.debug('lookup(%s)', key)

        try:
            resp = self._do_request('HEAD', '/%s%s' % (self.prefix, key))
            assert resp.length == 0
        except HTTPError as exc:
            if exc.status == 404:
                raise NoSuchObject(key) from None
            else:
                raise

        return extractmeta(resp)

    @retry
    @copy_ancestor_docstring
    def get_size(self, key):
        log.debug('get_size(%s)', key)

        try:
            resp = self._do_request('HEAD', '/%s%s' % (self.prefix, key))
            assert resp.length == 0
        except HTTPError as exc:
            if exc.status == 404:
                raise NoSuchObject(key) from None
            else:
                raise

        for (name, val) in resp.getheaders():
            if name.lower() == 'content-length':
                return int(val)
        raise RuntimeError('HEAD request did not return Content-Length')


    @retry
    @copy_ancestor_docstring
    def open_read(self, key):
        try:
            resp = self._do_request('GET', '/%s%s' % (self.prefix, key))
        except NoSuchKeyError:
            raise NoSuchObject(key) from None

        return ObjectR(key, resp, self, extractmeta(resp))

    @prepend_ancestor_docstring
    def open_write(self, key, metadata=None, is_compressed=False):
        """
        The returned object will buffer all data and only start the upload
        when its `close` method is called.
        """

        log.debug('open_write(%s): start', key)

        # We don't store the metadata keys directly, because HTTP headers
        # are case insensitive (so the server may change capitalization)
        # and we may run into length restrictions.
        meta_buf = b64encode(pickle.dumps(metadata, PICKLE_PROTOCOL)).decode('us-ascii')

        chunksize = 255
        i = 0
        headers = dict()
        headers['x-amz-meta-format'] = 'pickle'
        while i*chunksize < len(meta_buf):
            headers['x-amz-meta-data-%02d' % i] = meta_buf[i*chunksize:(i+1)*chunksize]
            i += 1

        return ObjectW(key, self, headers)


    @retry
    @copy_ancestor_docstring
    def copy(self, src, dest):
        log.debug('copy(%s, %s): start', src, dest)

        try:
            resp = self._do_request('PUT', '/%s%s' % (self.prefix, dest),
                                    headers={ 'x-amz-copy-source': '/%s/%s%s' % (self.bucket_name,
                                                                                self.prefix, src)})
            # Discard response body
            resp.read()
        except NoSuchKeyError:
            raise NoSuchObject(src) from None

    def _do_request(self, method, path, subres=None, query_string=None,
                    headers=None, body=None):
        '''Send request, read and return response object'''

        log.debug('_do_request(): start with parameters (%r, %r, %r, %r, %r, %r)',
                  method, path, subres, query_string, headers, body)

        if headers is None:
            headers = dict()

        headers['connection'] = 'keep-alive'

        if not body:
            headers['content-length'] = '0'
        elif isinstance(body, (str, bytes, bytearray, memoryview)):
            if isinstance(body, str):
                body = body.encode('ascii')
            headers['content-length'] = '%d' % (len(body))
            headers['content-md5'] = b64encode(md5sum(body)).decode('ascii')

        redirect_count = 0
        while True:
                
            resp = self._send_request(method, path, headers, subres, query_string, body)
            log.debug('_do_request(): request-id: %s', resp.getheader('x-amz-request-id'))

            if (resp.status < 300 or resp.status > 399):
                break

            # Assume redirect
            new_url = resp.getheader('Location')
            if new_url is None:
                break
            log.info('_do_request(): redirected to %s', new_url)
                        
            redirect_count += 1
            if redirect_count > 10:
                raise RuntimeError('Too many chained redirections')
    
            # Pylint can't infer SplitResult Types
            #pylint: disable=E1103
            o = urlsplit(new_url)
            if o.scheme:
                if isinstance(self.conn, http.client.HTTPConnection) and o.scheme != 'http':
                    raise RuntimeError('Redirect to non-http URL')
                elif isinstance(self.conn, http.client.HTTPSConnection) and o.scheme != 'https':
                    raise RuntimeError('Redirect to non-https URL')
            if o.hostname != self.hostname or o.port != self.port:
                self.hostname = o.hostname
                self.port = o.port
                self.conn = self._get_conn()
            else:
                raise RuntimeError('Redirect to different path on same host')
            
            if body and not isinstance(body, (bytes, bytearray, memoryview)):
                body.seek(0)

            # Read and discard body
            log.debug('Response body: %s', resp.read())

        # We need to call read() at least once for httplib to consider this
        # request finished, even if there is no response body.
        if resp.length == 0:
            resp.read()

        # Success 
        if resp.status >= 200 and resp.status <= 299:
            return resp

        # If method == HEAD, server must not return response body
        # even in case of errors
        if method.upper() == 'HEAD':
            raise HTTPError(resp.status, resp.reason)

        content_type = resp.getheader('Content-Type')
        if not content_type or not XML_CONTENT_RE.match(content_type):
            raise HTTPError(resp.status, resp.reason, resp.getheaders(), resp.read())
 
        # Error
        tree = ElementTree.parse(resp).getroot()
        raise get_S3Error(tree.findtext('Code'), tree.findtext('Message'))


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

    def _send_request(self, method, path, headers, subres=None, query_string=None, body=None):
        '''Add authentication and send request
        
        Returns the response object.
        '''

        # See http://docs.amazonwebservices.com/AmazonS3/latest/dev/RESTAuthentication.html

        # Lowercase headers
        headers = { x.lower(): y for (x,y) in headers.items() }

        # Date, can't use strftime because it's locale dependent
        now = time.gmtime()
        headers['date'] = ('%s, %02d %s %04d %02d:%02d:%02d GMT'
                           % (C_DAY_NAMES[now.tm_wday],
                              now.tm_mday,
                              C_MONTH_NAMES[now.tm_mon - 1],
                              now.tm_year, now.tm_hour,
                              now.tm_min, now.tm_sec))

        auth_strs = [method, '\n']

        for hdr in ('content-md5', 'content-type', 'date'):
            if hdr in headers:
                auth_strs.append(headers[hdr])
            auth_strs.append('\n')

        for hdr in sorted(x for x in headers if x.startswith('x-amz-')):
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

        headers['authorization'] = 'AWS %s:%s' % (self.login, signature)
        
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
            if body is None or not self.use_expect_100c or isinstance(body, bytes):
                # Easy case, small or no payload
                log.debug('_send_request(): processing request for %s', path)
                self.conn.request(method, path, body, headers)
                return self.conn.getresponse()

            # Potentially big message body, so we use 100-continue
            log.debug('_send_request(): sending request for %s', path)
            self.conn.putrequest(method, path)
            headers['expect'] = '100-continue'
            for (hdr, value) in headers.items():
                self.conn.putheader(hdr, value)
            self.conn.endheaders(None)

            log.debug('_send_request(): Waiting for 100-cont..')

            # Sneak in our own response class as instance variable,
            # so that it knows about the body that still needs to
            # be sent...
            resp = HTTPResponse(self.conn.sock, body)
            native_response_class = self.conn.response_class
            try:
                self.conn.response_class = resp
                assert self.conn.getresponse() is resp
            finally:
                self.conn.response_class = native_response_class
            return resp
            
        except Exception as exc:
            if is_temp_network_error(exc):
                # We probably can't use the connection anymore
                self.conn.close()
            raise

    @copy_ancestor_docstring
    def close(self):
        self.conn.close()
        
class HTTPResponse(http.client.HTTPResponse):
    '''
    This class provides a HTTP Response object that supports waiting for
    "100 Continue" and then sending the request body.

    The http.client.HTTPConnection module is almost impossible to extend,
    because the __response and __state variables are mangled. Implementing
    support for "100-Continue" doesn't fit into the existing state
    transitions.  Therefore, it has been implemented in a custom
    HTTPResponse class instead.

    Even though HTTPConnection allows to define a custom HTTPResponse class, it
    doesn't provide any means to pass extra information to the response
    constructor. Therefore, we instantiate the response manually and save the
    instance in the `response_class` attribute of the connection instance. We
    turn the class variable holding the response class into an instance variable
    holding the response instance. Only after the response instance has been
    created, we call the connection's `getresponse` method. Since this method
    doesn't know about the changed semantics of `response_class`, HTTPResponse
    instances have to fake an instantiation by just returning itself when called.
    '''

    def __init__(self, sock, body):
        self.sock = sock
        self.body = body
        self.__cached_status = None
        
    def __call__(self, sock, debuglevel=0, method=None, url=None):
        '''Fake object instantiation'''

        assert self.sock is sock

        super().__init__(sock, debuglevel, method=method, url=url)

        return self

    def _read_status(self):
        if self.__cached_status is None:
            self.__cached_status = super()._read_status()

        return self.__cached_status

    def begin(self):
        log.debug('Waiting for 100-continue...')

        (version, status, reason) = self._read_status()
        if status != CONTINUE:
            # Oops, error. Let regular code take over
            return super().begin()

        # skip the header from the 100 response
        while True:
            skip = self.fp.readline(_MAXLINE + 1)
            if len(skip) > _MAXLINE:
                raise LineTooLong("header line")
            skip = skip.strip()
            if not skip:
                break
            log.debug('Got 100 continue header: %s', skip)

        # Send body
        if not hasattr(self.body, 'read'):
            self.sock.sendall(self.body)
        else:
            while True:
                buf = self.body.read(BUFSIZE)
                if not buf:
                    break
                self.sock.sendall(buf)

        # *Now* read the actual response
        self.__cached_status = None
        return super().begin()

                
class ObjectR(object):
    '''An S3 object open for reading'''

    # NOTE: This class is used as a base class for the swift backend,
    # so changes here should be checked for their effects on other
    # backends.
    
    def __init__(self, key, resp, backend, metadata=None):
        self.key = key
        self.resp = resp
        self.md5_checked = False
        self.backend = backend
        self.metadata = metadata

        # False positive, hashlib *does* have md5 member
        #pylint: disable=E1101        
        self.md5 = hashlib.md5()

    def read(self, size=None):
        '''Read object data
        
        For integrity checking to work, this method has to be called until
        it returns an empty string, indicating that all data has been read
        (and verified).
        '''

        # chunked encoding handled by httplib
        buf = self.resp.read(size)

        # Check MD5 on EOF
        if not buf and not self.md5_checked:
            etag = self.resp.getheader('ETag').strip('"')
            self.md5_checked = True
            
            if not self.resp.isclosed():
                # http://bugs.python.org/issue15633, but should be fixed in 
                # Python 3.3 and newer
                log.error('ObjectR.read(): response not closed after end of data, '
                          'please report on http://code.google.com/p/s3ql/issues/')
                log.error('Method: %s, chunked: %s, read length: %s '
                          'response length: %s, chunk_left: %s, status: %d '
                          'reason "%s", version: %s, will_close: %s',
                          self.resp._method, self.resp.chunked, size, self.resp.length,
                          self.resp.chunk_left, self.resp.status, self.resp.reason,
                          self.resp.version, self.resp.will_close)             
                self.resp.close() 
            
            if etag != self.md5.hexdigest():
                log.warning('ObjectR(%s).close(): MD5 mismatch: %s vs %s', self.key, etag,
                         self.md5.hexdigest())
                raise BadDigestError('BadDigest', 'ETag header does not agree with calculated MD5')
            
            return buf

        self.md5.update(buf)
        return buf

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def close(self):
        '''Close object'''

        pass

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

        self.fh.seek(0)
        self.headers['Content-Length'] = self.obj_size
        self.headers['Content-Type'] = 'application/octet-stream'
        try:
            resp = self.backend._do_request('PUT', '/%s%s' % (self.backend.prefix, self.key),
                                            headers=self.headers, body=self.fh)
            etag = resp.getheader('ETag').strip('"')
            assert resp.length == 0

            if etag != self.md5.hexdigest():
                raise BadDigestError('BadDigest', 'MD5 mismatch for %s (received: %s, sent: %s)' %
                                     (self.key, etag, self.md5.hexdigest))
        except:
            self.backend.delete(self.key)
            raise

        self.fh.close()
        self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()
        return False

    def get_obj_size(self):
        if not self.closed:
            raise RuntimeError('Object must be closed first.')
        return self.obj_size


def get_S3Error(code, msg):
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
        return S3Error(code, msg)
    
    return class_(code, msg)

def extractmeta(resp):
    '''Extract metadata from HTTP response object'''

    meta = dict()
    format_ = 'raw'
    for (name, val) in resp.getheaders():
        # HTTP headers are case-insensitive and pre 2.x S3QL versions metadata
        # names verbatim (and thus loose capitalization info), so we force lower
        # case (since we know that this is the original capitalization).
        name = name.lower()

        hit = re.match(r'^x-amz-meta-(.+)$', name, re.IGNORECASE)
        if not hit:
            continue

        if hit.group(1).lower() == 'format':
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
                raise ChecksumError('Invalid metadata') from None
            raise
    else:
        return meta


class HTTPError(Exception):
    '''
    Represents an HTTP error returned by S3.
    '''

    def __init__(self, status, msg, headers=None, body=None):
        super().__init__()
        self.status = status
        self.msg = msg
        self.headers = headers
        self.body = body
        self.retry_after = None
        
        if self.headers is not None:
            self._set_retry_after()
        
    def _set_retry_after(self):
        '''Parse headers for Retry-After value'''
        
        val = None
        for (k, v) in self.headers:
            if k.lower() == 'retry-after':
                hit = re.match(r'^\s*([0-9]+)\s*$', v)
                if hit:
                    val = int(v)
                else:
                    date = parsedate_tz(v)
                    if date is None:
                        log.warning('Unable to parse header: %s: %s', k, v)
                        continue
                    val = mktime_tz(*date) - time.time()
                    
        if val is not None:
            if val > 300 or val < 0:
                log.warning('Ignoring invalid retry-after value of %.3f', val)
            else:
                self.retry_after = val
            
    def __str__(self):
        return '%d %s' % (self.status, self.msg)

class S3Error(Exception):
    '''
    Represents an error returned by S3. For possible codes, see
    http://docs.amazonwebservices.com/AmazonS3/latest/API/ErrorResponses.html
    '''

    def __init__(self, code, msg):
        super().__init__(msg)
        self.code = code
        self.msg = msg

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
