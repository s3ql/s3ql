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

NAMESPACE = 'http://s3.amazonaws.com/doc/2006-03-01/'

    
class Bucket(AbstractBucket):
    """A bucket stored in Amazon S3
    
    This class uses standard HTTP connections to connect to S3.
    
    This class assumes that if you are requesting any action on an object other
    than checking for existence, you know that the object exists in the bucket.
    This means that if `read_after_create_consistent()` is false,
    `is_temp_failure` will return true for `NoSuchObject` exceptions and all
    methods of this class (except for `contains`) will retry their operation at
    increasing intervals.

    If you want to perform an operation on an object and are not sure that it
    exists, it is a good idea to first check existence using `contains`.
    However, even that is not completely safe, because if
    `list_after_delete_consistent` is false, then a DELETE operation may still
    be propagating through AWS.
    
    You have been warned.    
    """

    def __init__(self, bucket_name, aws_key_id, aws_key):
        super(Bucket, self).__init__()
        
        idx = bucket_name.index('/')
        self.bucket_name = bucket_name[:idx]
        self.prefix = bucket_name[idx:]
        self.aws_key = aws_key
        self.aws_key_id = aws_key_id
        
        self.conn = self._get_conn()
        self.region = self._get_region()
            
    def _get_conn(self):
        '''Return connection to server'''
        
        return httplib.HTTPConnection('%s.s3.amazonaws.com' % self.bucket_name)          
          
    def is_temp_failure(self, exc):
        '''Return true if exc indicates a temporary error
    
        Return true if the given exception is used by this bucket's backend
        to indicate a temporary problem. Most instance methods automatically
        retry the request in this case, so the caller does not need to
        worry about temporary failures.
        
        However, in same cases (e.g. when reading or writing an object), the
        request cannot automatically be retried. In these case this method can
        be used to check for temporary problems and so that the request can
        be manually restarted if applicable.
        
        **Warning**: If `read_after_create_consistent` is false, `NoSuchObject` is
        considered a temporary failure.
        '''          
    
        if isinstance(exc, (InternalError, BadDigest, IncompleteBody, RequestTimeout,
                            OperationAborted, SlowDown, RequestTimeTooSkewed,
                            httplib.IncompleteRead, httplib.BadStatusLine)):
            return True
        
        elif isinstance(exc, IOError) and exc.errno == errno.EPIPE:
            return True
        
        elif isinstance(exc, NoSuchObject) and not self.read_after_create_consistent():
            return True
        
        return False

    _retry_on = is_temp_failure
        
    @retry
    def delete(self, key, force=False):
        '''Delete the specified object'''
        
        # TODO: Take into account answers to 
        # https://forums.aws.amazon.com/message.jspa?messageID=272309#272309
        log.debug('delete(%s)', key)
        try:
            self._do_request('DELETE', '/%s%s' % (self.prefix, key))
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
            
            if resp.getheader('Content-Type').lower() != 'application/xml':
                raise RuntimeError('unexpected content type: %s' % resp.getheader('Content-Type'))
            
            itree = iter(ElementTree.iterparse(resp, events=("start", "end")))
            (event, root) = itree.next()

            namespace = re.sub(r'^\{(.+)\}.+$', r'\1', root.tag)
            if namespace != NAMESPACE:
                raise RuntimeError('Unsupported namespace: %s' % namespace)
             
            try:
                for (event, el) in itree:
                    if event != 'end':
                        continue
                    
                    if el.tag == '{%s}IsTruncated' % NAMESPACE:
                        keys_remaining = (el.text == 'true')
                    
                    elif el.tag == '{%s}Contents' % NAMESPACE:
                        marker = el.findtext('{%s}Key' % NAMESPACE)
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
            region = 'us-classic'
            
        if region not in ('EU', 'us-west-1', 'ap-southeast-1', 
                          'ap-northeast-1', 'us-classic'):
            raise RuntimeError('Unknown bucket region: %s' % region)
        
        return region
        
    @retry
    def lookup(self, key):
        """Return metadata for given key"""
        
        log.debug('lookup(%s)', key)
        
        try:
            resp = self._do_request('HEAD', '/%s%s' % (self.prefix, key))
        except NoSuchKey:
            raise NoSuchObject(key)

        return extractmeta(resp)
    
    @retry
    def open_read(self, key):
        ''''Open object for reading

        Return a tuple of a file-like object. Bucket contents can be read from
        the file-like object, metadata is available in its *metadata* attribute.
        The object must be closed explicitly.
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
            
    def read_after_create_consistent(self):
        '''Does this backend provide read-after-create consistency?'''
        
        return self.region in ('EU', 'us-west-1', 'ap-southeast-1', 'ap-northeast-1')
    
    def read_after_write_consistent(self):
        '''Does this backend provide read-after-write consistency?'''
        
        return False
        
    def read_after_delete_consistent(self):
        '''Does this backend provide read-after-delete consistency?'''
        
        return False

    def list_after_delete_consistent(self):
        '''Does this backend provide list-after-delete consistency?'''
        
        return False
        
    def list_after_create_consistent(self):
        '''Does this backend provide list-after-create consistency?'''
        
        return self.region in ('EU', 'us-west-1', 'ap-southeast-1')

    @retry
    def contains(self, key):
        '''Check if `key` is in bucket'''
        
        log.debug('contains(%s)', key)
        
        try:
            self._do_request('HEAD', '/%s%s' % (self.prefix, key))
        except NoSuchKey:
            return False
        else:
            return True
        
    @retry
    def copy(self, src, dest):
        """Copy data stored under key `src` to key `dest`
        
        If `dest` already exists, it will be overwritten. The copying is done on
        the remote side.
        """
        
        log.debug('copy(%s, %s): start', src, dest)
        
        try:
            self._do_request('PUT', '/%s%s' % (self.prefix, dest),
                             headers={ 'x-amz-copy-source': '/%s%s%s' % (self.bucket_name,
                                                                         self.prefix, src)})
        except NoSuchKey:
            raise NoSuchObject(src)
    
    def _do_request(self, method, url, subres=None, query_string=None,
                    headers=None, body=None ):
        '''Send request, read and return response object'''
        
        log.debug('_do_request(): start with parameters (%r, %r, %r, %r, %r, %r)', 
                  method, url, subres, query_string, headers, body)
        
        if headers is None:
            headers = dict()
            
        full_url = self._add_auth('DELETE', '/%s%s' % (self.prefix, url),
                                  headers, subres, query_string)
        
        redirect_count = 0
        while True:
            log.debug('_do_request(): sending request')
            self.conn.request(method, full_url, body, headers)
              
            log.debug('_do_request(): Reading response')
            resp = self.conn.getresponse()
        
            log.debug('_do_request(): request-id: %s',  resp.getheader('x-amz-request-id'))
            
            if (resp.status < 300 and resp.status > 399):
                break
            
            redirect_count += 1
            if redirect_count > 10:
                raise RuntimeError('Too many chained redirections')
            full_url = resp.getheader('Location')
            
            log.debug('_do_request(): redirecting to %s', full_url)
            
            if not isinstance(body, bytes):
                body.seek(0)

        if resp.status == httplib.OK:
            return resp
        
        if resp.getheader('Content-Type').lower() != 'application/xml':
            raise RuntimeError('unexpected content type %s for status %d'
                               % (resp.getheader('Content-Type'), resp.status)) 
        
        # Error
        tree = ElementTree.parse(resp).getroot()
        raise get_S3Error(tree.findtext('Code'), tree.findtext('Message'))
        
                                                  
    def clear(self):
        """Delete all objects in bucket
        
        Note that this method may not be able to see (and therefore also not
        delete) recently uploaded objects if `list_after_create_consistent` is
        false.
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
        headers = dict((x.lower(), y) for (x,y) in headers.iteritems())

        # Date
        now = time.gmtime()
        # Can't use strftime because it's locale dependent
        headers['date'] = ('%s, %02d %s %04d %02d:%02d:%02d GMT' 
                           % (C_DAY_NAMES[now.tm_wday],
                              now.tm_mday,
                              C_MONTH_NAMES[now.tm_mon - 1],
                              now.tm_year, now.tm_hour, 
                              now.tm_min, now.tm_sec))

        headers['connection'] = 'keep-alive'
            
        auth_strs = [method, '\n']
        
        for hdr in ('content-md5', 'content-type', 'date'):
            if hdr in headers:
                auth_strs.append(headers[hdr])
            auth_strs.append('\n')
    
        for hdr in sorted(x for x in headers if x.startswith('x-amz-')):
            val = ' '.join(re.split(r'\s*\n\s*', headers[hdr].strip()))
            auth_strs.append('%s:%s\n' % (hdr,val))
    
        auth_strs.append('/' + self.bucket_name)
        auth_strs.append(url)
        if subres:
            auth_strs.append('?%s' % subres)
        
        # False positive, hashlib *does* have sha1 member
        #pylint: disable=E1101
        signature = b64encode(hmac.new(self.aws_key, ''.join(auth_strs), hashlib.sha1).digest())
         
        headers['Authorization'] = 'AWS %s:%s' % (self.aws_key_id, signature)
    
        full_url = urllib.quote(url)
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
        
        resp = self.bucket._do_request('PUT', '/%s%s' % (self.bucket.prefix, self.key), 
                                       headers=self.headers, body=self.fh)
        etag = resp.getheader('ETag').strip('"')
        
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
    
    return getattr(globals(), code, S3Error)(code, msg)
    

def extractmeta(resp):
    '''Extract metadata from HTTP response object'''
    
    meta = dict()
    for (name, val) in resp.getheaders():
        hit = re.match(r'^x-amz-meta-(.+)$', name)
        if not hit:
            continue
        meta[hit.match(1)] = val
        
    return meta
                    
              
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
        return self.msg
    
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
