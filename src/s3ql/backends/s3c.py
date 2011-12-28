'''
backends/s3c.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import division, print_function, absolute_import
from ..common import BUFSIZE, QuietError
from .common import AbstractBucket, NoSuchObject, retry, AuthorizationError
from .common import NoSuchBucket as NoSuchBucket_common
from base64 import b64encode
import errno
import hashlib
import hmac
import httplib
import logging
import re
import tempfile
import time
import urllib
import xml.etree.cElementTree as ElementTree


C_DAY_NAMES = [ 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun' ]
C_MONTH_NAMES = [ 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec' ]

XML_CONTENT_RE = re.compile('^application/xml(?:;\s+|$)', re.IGNORECASE)

log = logging.getLogger("backends.s3c")

class Bucket(AbstractBucket):
    """A bucket stored in some S3 compatible storage service.
    
    This class uses standard HTTP connections to connect to GS.
    
    The bucket guarantees only immediate get after create consistency.
    """

    def __init__(self, storage_url, login, password):
        super(Bucket, self).__init__()

        (host, port, bucket_name, prefix) = self._parse_storage_url(storage_url)

        self.bucket_name = bucket_name
        self.prefix = prefix
        self.hostname = host
        self.port = port
        self.conn = self._get_conn()

        self.password = password
        self.login = login
        self.namespace = 'http://s3.amazonaws.com/doc/2006-03-01/'

    @staticmethod
    def _parse_storage_url(storage_url):
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
        port = int(hit.group(2) or '80')
        bucketname = hit.group(3)
        prefix = hit.group(4) or ''

        return (hostname, port, bucketname, prefix)

    def _get_conn(self):
        '''Return connection to server'''

        return httplib.HTTPConnection(self.hostname, self.port)

    def is_temp_failure(self, exc): #IGNORE:W0613
        '''Return true if exc indicates a temporary error
    
        Return true if the given exception is used by this bucket's backend
        to indicate a temporary problem. Most instance methods automatically
        retry the request in this case, so the caller does not need to
        worry about temporary failures.
        
        However, in same cases (e.g. when reading or writing an object), the
        request cannot automatically be retried. In these case this method can
        be used to check for temporary problems and so that the request can
        be manually restarted if applicable.
        '''

        if isinstance(exc, (InternalError, BadDigest, IncompleteBody, RequestTimeout,
                            OperationAborted, SlowDown, RequestTimeTooSkewed,
                            httplib.IncompleteRead)):
            return True

        # Server closed connection
        elif (isinstance(exc, httplib.BadStatusLine)
              and (not exc.line or exc.line == "''")):
            return True

        elif (isinstance(exc, IOError) and
              exc.errno in (errno.EPIPE, errno.ECONNRESET, errno.ETIMEDOUT,
                            errno.EINTR)):
            return True

        return False

    @retry
    def delete(self, key, force=False):
        '''Delete the specified object'''

        log.debug('delete(%s)', key)
        try:
            resp = self._do_request('DELETE', '/%s%s' % (self.prefix, key))
            assert resp.length == 0
        except NoSuchKey:
            if force:
                pass
            else:
                raise NoSuchObject(key)

    def list(self, prefix=''):
        '''List keys in bucket

        Returns an iterator over all keys in the bucket.
        '''

        log.debug('list(%s): start', prefix)

        marker = ''
        waited = 0
        interval = 1 / 50
        iterator = self._list(prefix, marker)
        while True:
            try:
                marker = iterator.next()
                waited = 0
            except StopIteration:
                break
            except Exception as exc:
                if not self.is_temp_failure(exc):
                    raise
                if waited > 60 * 60:
                    log.error('list(): Timeout exceeded, re-raising %r exception', exc)
                    raise

                log.info('Encountered %r exception, retrying call to s3.Bucket.list()', exc)
                time.sleep(interval)
                waited += interval
                if interval < 20 * 60:
                    interval *= 2
                iterator = self._list(prefix, marker)

            else:
                yield marker

    def _list(self, prefix='', start=''):
        '''List keys in bucket, starting with *start*

        Returns an iterator over all keys in the bucket.
        '''

        keys_remaining = True
        marker = start
        prefix = self.prefix + prefix

        while keys_remaining:
            log.debug('list(%s): requesting with marker=%s', prefix, marker)

            keys_remaining = None
            resp = self._do_request('GET', '/', query_string={ 'prefix': prefix,
                                                              'marker': marker,
                                                              'max-keys': 1000 })

            if not XML_CONTENT_RE.match(resp.getheader('Content-Type')):
                raise RuntimeError('unexpected content type: %s' % resp.getheader('Content-Type'))

            itree = iter(ElementTree.iterparse(resp, events=("start", "end")))
            (event, root) = itree.next()

            namespace = re.sub(r'^\{(.+)\}.+$', r'\1', root.tag)
            if namespace != self.namespace:
                raise RuntimeError('Unsupported namespace: %s' % namespace)

            try:
                for (event, el) in itree:
                    if event != 'end':
                        continue

                    if el.tag == '{%s}IsTruncated' % self.namespace:
                        keys_remaining = (el.text == 'true')

                    elif el.tag == '{%s}Contents' % self.namespace:
                        marker = el.findtext('{%s}Key' % self.namespace)
                        yield marker[len(self.prefix):]
                        root.clear()

            except GeneratorExit:
                # Need to read rest of response
                while True:
                    buf = resp.read(BUFSIZE)
                    if buf == '':
                        break
                break

            if keys_remaining is None:
                raise RuntimeError('Could not parse body')

    @retry
    def lookup(self, key):
        """Return metadata for given key"""

        log.debug('lookup(%s)', key)

        try:
            resp = self._do_request('HEAD', '/%s%s' % (self.prefix, key))
            assert resp.length == 0
        except HTTPError as exc:
            if exc.status == 404:
                raise NoSuchObject(key)
            else:
                raise

        return extractmeta(resp)

    @retry
    def get_size(self, key):
        '''Return size of object stored under *key*'''

        log.debug('get_size(%s)', key)

        try:
            resp = self._do_request('HEAD', '/%s%s' % (self.prefix, key))
            assert resp.length == 0
        except HTTPError as exc:
            if exc.status == 404:
                raise NoSuchObject(key)
            else:
                raise

        for (name, val) in resp.getheaders():
            if name.lower() == 'content-length':
                return int(val)
        raise RuntimeError('HEAD request did not return Content-Length')


    @retry
    def open_read(self, key):
        ''''Open object for reading

        Return a tuple of a file-like object. Bucket contents can be read from
        the file-like object, metadata is stored in its *metadata* attribute and
        can be modified by the caller at will. The object must be closed explicitly.
        '''

        try:
            resp = self._do_request('GET', '/%s%s' % (self.prefix, key))
        except NoSuchKey:
            raise NoSuchObject(key)

        return ObjectR(key, resp, self, extractmeta(resp))

    def open_write(self, key, metadata=None, is_compressed=False):
        """Open object for writing

        `metadata` can be a dict of additional attributes to store with the
        object. Returns a file-like object. The object must be closed
        explicitly. After closing, the *get_obj_size* may be used to retrieve
        the size of the stored object (which may differ from the size of the
        written data).

        The *is_compressed* parameter indicates that the caller is going
        to write compressed data, and may be used to avoid recompression
        by the bucket.   
                
        Since Amazon S3 does not support chunked uploads, the entire data will
        be buffered in memory before upload.
        """

        log.debug('open_write(%s): start', key)

        headers = dict()
        if metadata:
            for (hdr, val) in metadata.iteritems():
                headers['x-amz-meta-%s' % hdr] = val

        return ObjectW(key, self, headers)


    @retry
    def copy(self, src, dest):
        """Copy data stored under key `src` to key `dest`
        
        If `dest` already exists, it will be overwritten. The copying is done on
        the remote side.
        """

        log.debug('copy(%s, %s): start', src, dest)

        try:
            resp = self._do_request('PUT', '/%s%s' % (self.prefix, dest),
                                    headers={ 'x-amz-copy-source': '/%s/%s%s' % (self.bucket_name,
                                                                                self.prefix, src)})
            # Discard response body
            resp.read()
        except NoSuchKey:
            raise NoSuchObject(src)

    def _do_request(self, method, url, subres=None, query_string=None,
                    headers=None, body=None):
        '''Send request, read and return response object'''

        log.debug('_do_request(): start with parameters (%r, %r, %r, %r, %r, %r)',
                  method, url, subres, query_string, headers, body)

        if headers is None:
            headers = dict()

        headers['connection'] = 'keep-alive'

        if not body:
            headers['content-length'] = '0'

        # When signing, always include bucket name in URL
        self._add_auth(method, '/%s%s' % (self.bucket_name, url), headers, subres)

        # Construct full URL
        if not self.hostname.startswith(self.bucket_name):
            url = '/%s%s' % (self.bucket_name, url)
        if query_string:
            s = urllib.urlencode(query_string, doseq=True)
            if subres:
                url += '?%s&%s' % (subres, s)
            else:
                url += '?%s' % s
        elif subres:
            url += '?%s' % subres

        redirect_count = 0
        while True:
            try:
                log.debug('_do_request(): sending request')
                self.conn.request(method, url, body, headers)

                log.debug('_do_request(): Reading response')
                resp = self.conn.getresponse()
            except:
                # We probably can't use the connection anymore
                self.conn.close()
                raise

            log.debug('_do_request(): request-id: %s', resp.getheader('x-amz-request-id'))

            if (resp.status < 300 or resp.status > 399):
                break

            redirect_count += 1
            if redirect_count > 10:
                raise RuntimeError('Too many chained redirections')
            url = resp.getheader('Location')

            log.info('_do_request(): redirected to %s', url)

            if body and not isinstance(body, bytes):
                body.seek(0)

            # Read and discard body
            resp.read()

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
            raise RuntimeError('\n'.join(['Unexpected S3 reply:',
                                          '%d %s' % (resp.status, resp.reason) ]
                                         + [ '%s: %s' % x for x in resp.getheaders() ])
                               + '\n' + resp.read())
        # Error
        tree = ElementTree.parse(resp).getroot()
        raise get_S3Error(tree.findtext('Code'), tree.findtext('Message'))


    def clear(self):
        """Delete all objects in bucket
        
        Note that this method may not be able to see (and therefore also not
        delete) recently uploaded objects if `is_list_create_consistent` is
        False.
        """

        # We have to cache keys, because otherwise we can't use the
        # http connection to delete keys.
        for (no, s3key) in enumerate(list(self)):
            if no != 0 and no % 1000 == 0:
                log.info('clear(): deleted %d objects so far..', no)

            log.debug('clear(): deleting key %s', s3key)

            # Ignore missing objects when clearing bucket
            self.delete(s3key, True)

    def is_get_consistent(self):
        '''If True, objects retrievals are guaranteed to be up-to-date
        
        If this method returns True, then creating, deleting, or overwriting an
        object is guaranteed to be immediately reflected in subsequent object
        retrieval attempts.
        '''
        return False

    def is_list_create_consistent(self):
        '''If True, new objects are guaranteed to show up in object listings
        
        If this method returns True, creation of objects will immediately be
        reflected when retrieving the list of available objects.
        '''
        return False

    def __str__(self):
        return 's3c://%s/%s/%s' % (self.hostname, self.bucket_name, self.prefix)

    def _add_auth(self, method, url, headers, subres=None):
        '''Add authentication to *headers*
        
        Note that * headers * is modified in -place. *subres * must be a
        string or * None * .
        '''

        # See http://docs.amazonwebservices.com/AmazonS3/latest/dev/RESTAuthentication.html

        # Lowercase headers
        keys = list(headers.iterkeys())
        for key in keys:
            key_l = key.lower()
            if key_l == key:
                continue
            headers[key_l] = headers[key]
            del headers[key]

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

        url = urllib.quote(url)
        auth_strs.append(url)
        if subres:
            auth_strs.append('?%s' % subres)

        # False positive, hashlib *does* have sha1 member
        #pylint: disable=E1101
        signature = b64encode(hmac.new(self.password, ''.join(auth_strs), hashlib.sha1).digest())

        headers['authorization'] = 'AWS %s:%s' % (self.login, signature)

class ObjectR(object):
    '''An S3 object open for reading'''

    def __init__(self, key, resp, bucket, metadata=None):
        self.key = key
        self.resp = resp
        self.md5_checked = False
        self.bucket = bucket
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
            if etag != self.md5.hexdigest():
                log.warn('ObjectR(%s).close(): MD5 mismatch: %s vs %s', self.key, etag,
                         self.md5.hexdigest())
                raise BadDigest('BadDigest', 'ETag header does not agree with calculated MD5')
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

    def __init__(self, key, bucket, headers):
        self.key = key
        self.bucket = bucket
        self.headers = headers
        self.closed = False
        self.obj_size = 0
        self.fh = tempfile.TemporaryFile(bufsize=0) # no Python buffering

        # False positive, hashlib *does* have md5 member
        #pylint: disable=E1101        
        self.md5 = hashlib.md5()

    def write(self, buf):
        '''Write object data'''

        self.fh.write(buf)
        self.md5.update(buf)
        self.obj_size += len(buf)

    def is_temp_failure(self, exc):
        return self.bucket.is_temp_failure(exc)

    @retry
    def close(self):
        '''Close object and upload data'''

        # Access to protected member ok
        #pylint: disable=W0212

        log.debug('ObjectW(%s).close(): start', self.key)

        self.closed = True
        self.headers['Content-Length'] = self.obj_size

        self.fh.seek(0)
        resp = self.bucket._do_request('PUT', '/%s%s' % (self.bucket.prefix, self.key),
                                       headers=self.headers, body=self.fh)
        etag = resp.getheader('ETag').strip('"')
        assert resp.length == 0

        if etag != self.md5.hexdigest():
            log.warn('ObjectW(%s).close(): MD5 mismatch (%s vs %s)', self.key, etag,
                     self.md5.hexdigest)
            raise BadDigest('BadDigest', 'Received ETag does not agree with our calculations.')

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

    return globals().get(code, S3Error)(code, msg)

def extractmeta(resp):
    '''Extract metadata from HTTP response object'''

    meta = dict()
    for (name, val) in resp.getheaders():
        hit = re.match(r'^x-amz-meta-(.+)$', name)
        if not hit:
            continue
        meta[hit.group(1)] = val

    return meta

class HTTPError(Exception):
    '''
    Represents an HTTP error returned by S3 in response to a 
    HEAD request.
    '''

    def __init__(self, status, msg):
        super(HTTPError, self).__init__()
        self.status = status
        self.msg = msg

    def __str__(self):
        return '%d %s' % (self.status, self.msg)

class S3Error(Exception):
    '''
    Represents an error returned by S3. For possible codes, see
    http://docs.amazonwebservices.com/AmazonS3/latest/API/ErrorResponses.html
    '''

    def __init__(self, code, msg):
        super(S3Error, self).__init__(msg)
        self.code = code
        self.msg = msg

    def __str__(self):
        return '%s: %s' % (self.code, self.msg)

class NoSuchKey(S3Error): pass
class AccessDenied(S3Error, AuthorizationError): pass
class BadDigest(S3Error): pass
class IncompleteBody(S3Error): pass
class InternalError(S3Error): pass
class InvalidAccessKeyId(S3Error, AuthorizationError): pass
class InvalidSecurity(S3Error, AuthorizationError): pass
class OperationAborted(S3Error): pass
class RequestTimeout(S3Error): pass
class SlowDown(S3Error): pass
class RequestTimeTooSkewed(S3Error): pass
class NoSuchBucket(S3Error, NoSuchBucket_common): pass

