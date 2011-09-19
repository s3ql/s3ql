'''
s3.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import division, print_function, absolute_import

from .common import AbstractBucket, NoSuchObject
from s3ql.common import retry
import logging
import httplib
import re
import time
from base64 import b64encode
import hmac
import hashlib
import tempfile
import urllib
import xml.etree.cElementTree as ElementTree
import os
import errno


log = logging.getLogger("backend.s3")

C_DAY_NAMES = [ 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun' ]
C_MONTH_NAMES = [ 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec' ]
    
XML_CONTENT_RE = re.compile('^application/xml(?:;\s+|$)', re.IGNORECASE)

class Bucket(AbstractBucket):
    """A bucket stored in Amazon S3
    
    This class uses standard HTTP connections to connect to S3.
    
    The bucket guarantees get after create consistency, i.e. a newly created
    object will be immediately retrievable. Additional consistency guarantees
    may or may not be available and can be queried for with instance methods.    
    """

    def __init__(self, bucket_name, aws_key_id, aws_key):
        super(Bucket, self).__init__()
        
        try:
            idx = bucket_name.index('/')
        except ValueError:
            idx = len(bucket_name)

        self.bucket_name = bucket_name[:idx]
        self.prefix = bucket_name[idx+1:]
        self.aws_key = aws_key
        self.aws_key_id = aws_key_id
        self.namespace = 'http://s3.amazonaws.com/doc/2006-03-01/'
        
        self._init()
        
    def _init(self):
        '''Additional initialization code
        
        Called by constructor and provided solely for easier subclassing.
        '''
        # Attributes defined outside init
        #pylint: disable=W0201
        
        self.conn = self._get_conn()
        self.region = self._get_region()
        
        if self.region not in ('EU', 'us-west-1', 'ap-southeast-1'):
            log.warn('Warning: bucket provides insufficient consistency guarantees!')
        
    def _get_conn(self):
        '''Return connection to server'''
        
        return httplib.HTTPConnection('%s.s3.amazonaws.com' % self.bucket_name)          
      
    @staticmethod    
    def is_temp_failure(exc):
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
        elif isinstance(exc, httplib.BadStatusLine) and exc.line == "''":
            return True
        
        elif isinstance(exc, IOError) and exc.errno == errno.EPIPE:
            return True
        
        return False

    _retry_on = is_temp_failure
        
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
        interval = 1/50
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
                if waited > 60*60:                
                    log.error('list(): Timeout exceeded, re-raising %r exception', exc)
                    raise
                                        
                log.debug('list(): trying again after %r exception:', exc)
                time.sleep(interval)
                waited += interval
                if interval < 20*60:
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
                        yield marker
                        root.clear()
                        
            except GeneratorExit:
                # Need to read rest of response
                while True:
                    buf = resp.read(8192)
                    if buf == '':
                        break   
                break               
            
            if keys_remaining is None:
                raise RuntimeError('Could not parse body')

    @retry
    def _get_region(self):
        ''''Return bucket region'''
        
        log.debug('_get_region()')
        resp = self._do_request('GET', '/', subres='location')
        
        region = ElementTree.parse(resp).getroot().text
        
        if not region:
            region = 'us-standard'
            
        if region not in ('EU', 'us-west-1', 'ap-southeast-1', 
                          'ap-northeast-1', 'us-standard'):
            raise RuntimeError('Unknown bucket region: %s' % region)
        
        return region
        
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
    
    def open_write(self, key, metadata=None):
        """Open object for writing

        `metadata` can be a dict of additional attributes to store with the
        object. Returns a file-like object that must be closed when all data has
        been written.
        
        Since Amazon S3 does not support chunked uploads, the entire data will
        be buffered in memory before upload.
        """
        
        log.debug('open_write(%s): start', key)
        
        headers = dict()
        if metadata:
            for (hdr, val) in metadata.iteritems():
                headers['x-amz-meta-%s' % hdr] = val
            
        return ObjectW(key, self, headers)

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

        return self.region in ('EU', 'us-west-1', 'ap-southeast-1')
        
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
                    headers=None, body=None ):
        '''Send request, read and return response object'''
        
        log.debug('_do_request(): start with parameters (%r, %r, %r, %r, %r, %r)', 
                  method, url, subres, query_string, headers, body)
        
        if headers is None:
            headers = dict()
            
        headers['connection'] = 'keep-alive'
        
        if not body:
            headers['content-length'] = '0'
            
        full_url = self._add_auth(method, url, headers, subres, query_string)
        
        redirect_count = 0
        while True:
            log.debug('_do_request(): sending request')
            self.conn.request(method, full_url, body, headers)
              
            log.debug('_do_request(): Reading response')
            try:
                resp = self.conn.getresponse()
            except httplib.BadStatusLine as exc:
                if exc.line == "''":
                    # Server closed connection, reconnect
                    self.conn.close()
                raise
                        
            log.debug('_do_request(): request-id: %s',  resp.getheader('x-amz-request-id'))
            
            if (resp.status < 300 or resp.status > 399):
                break
            
            redirect_count += 1
            if redirect_count > 10:
                raise RuntimeError('Too many chained redirections')
            full_url = resp.getheader('Location')
            
            log.debug('_do_request(): redirecting to %s', full_url)
            
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

        for (no, s3key) in enumerate(self):
            if no != 0 and no % 1000 == 0:
                log.info('clear(): deleted %d objects so far..', no)

            log.debug('clear(): deleting key %s', s3key)

            # Ignore missing objects when clearing bucket
            self.delete(s3key, True)

    def __str__(self):
        return 's3://%s/%s' % (self.bucket_name, self.prefix)

    def _add_auth(self, method, url, headers, subres=None, query_string=None):
        '''Add authentication to *headers*
        
        Note that *headers* is modified in-place. As a convenience,
        this method returns the encoded URL with query string and
        sub resource appended.
        
        *query_string* must be a dict or *None*. *subres* must be a
        string or *None*.
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
            auth_strs.append('%s:%s\n' % (hdr,val))
    
        auth_strs.append('/' + self.bucket_name)
        url = urllib.quote(url)
        auth_strs.append(url)
        if subres:
            auth_strs.append('?%s' % subres)
        
        # False positive, hashlib *does* have sha1 member
        #pylint: disable=E1101
        signature = b64encode(hmac.new(self.aws_key, ''.join(auth_strs), hashlib.sha1).digest())
         
        headers['authorization'] = 'AWS %s:%s' % (self.aws_key_id, signature)
    
        full_url = url
        if query_string:
            s = urllib.urlencode(query_string, doseq=True)
            if subres:
                full_url += '?%s&%s' % (subres, s)
            else:
                full_url += '?%s' % s
        elif subres:
            full_url += '?%s' % subres
                
        return full_url

            
class ObjectR(object):
    '''An S3 object open for reading'''
    
    def __init__(self, key, resp, bucket, metadata=None):
        self.key = key
        self.resp = resp
        self.closed = False
        self.bucket = bucket
        self.metadata = metadata
        
        # False positive, hashlib *does* have md5 member
        #pylint: disable=E1101        
        self.md5 = hashlib.md5()
            
    def read(self, size=None):
        '''Read object data'''
        
        # chunked encoding handled by httplib
        buf = self.resp.read(size)
        self.md5.update(buf)
        return buf
       
    def __enter__(self):
        return self
    
    def __exit__(self, *a):
        self.close()
        return False
        
    def _retry_on(self, exc):
        return self.bucket._retry_on(exc) #IGNORE:W0212
    
    @retry
    def close(self):
        '''Close object'''

        # Access to protected member ok
        #pylint: disable=W0212
        
        log.debug('ObjectR(%s).close(): start', self.key)        
        self.closed = True

        while True:
            buf = self.read(8192)
            if buf == '':
                break

        etag = self.resp.getheader('ETag').strip('"')
        
        if etag != self.md5.hexdigest():
            log.warn('ObjectR(%s).close(): MD5 mismatch: %s vs %s', self.key, etag, self.md5.hexdigest())
            raise BadDigest('BadDigest', 'Received ETag does not agree with our calculations.')
        
    def __del__(self):
        if not self.closed:
            try:
                self.close()
            except:
                pass
            raise RuntimeError('ObjectR %s has been destroyed without calling close()!' % self.key)
    
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
        self.fh = tempfile.TemporaryFile(bufsize=0) # no Python buffering
        
        # False positive, hashlib *does* have md5 member
        #pylint: disable=E1101        
        self.md5 = hashlib.md5()
        
    def write(self, buf):
        '''Write object data'''
        
        self.fh.write(buf)
        self.md5.update(buf)
       
    def _retry_on(self, exc):
        return self.bucket._retry_on(exc) #IGNORE:W0212
           
    @retry
    def close(self):
        '''Close object and upload data'''

        # Access to protected member ok
        #pylint: disable=W0212
        
        log.debug('ObjectW(%s).close(): start', self.key)
        
        self.closed = True
        self.headers['Content-Length'] = os.fstat(self.fh.fileno()).st_size
        
        self.fh.seek(0)
        resp = self.bucket._do_request('PUT', '/%s%s' % (self.bucket.prefix, self.key), 
                                       headers=self.headers, body=self.fh)
        etag = resp.getheader('ETag').strip('"')
        assert resp.length == 0
        
        if etag != self.md5.hexdigest():
            log.warn('ObjectW(%s).close(): MD5 mismatch (%s vs %s)', self.key, etag, 
                     self.md5.hexdigest)
            raise BadDigest('BadDigest', 'Received ETag does not agree with our calculations.')
        
    def __del__(self):
        if not self.closed:
            try:
                self.close()
            except:
                pass
            raise RuntimeError('ObjectW %s has been destroyed without calling close()!' % self.key)
          
    def __enter__(self):
        return self
    
    def __exit__(self, *a):
        self.close()
        return False
    
          
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
        super(S3Error, self).__init__()
        self.code = code
        self.msg = msg
        
    def __str__(self):
        return '%s: %s' % (self.code, self.msg)
    
class NoSuchKey(S3Error): pass
class AccessDenied(S3Error): pass
class BadDigest(S3Error): pass
class IncompleteBody(S3Error): pass
class InternalError(S3Error): pass
class InvalidAccessKeyId(S3Error): pass
class InvalidSecurity(S3Error): pass
class OperationAborted(S3Error): pass
class RequestTimeout(S3Error): pass
class SlowDown(S3Error): pass
class RequestTimeTooSkewed(S3Error): pass
