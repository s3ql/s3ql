'''
swift.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from ..logging import logging # Ensure use of custom logger class
from . import s3c
from ..common import QuietError, BUFSIZE
from .common import (AbstractBackend, NoSuchObject, retry, AuthorizationError, http_connection, 
    DanglingStorageURLError, is_temp_network_error)
from .s3c import HTTPError, BadDigestError
from urllib.parse import urlsplit
import json
import re
import io
import time
import urllib.parse

log = logging.getLogger(__name__)

class Backend(AbstractBackend):
    """A backend to store data in OpenStack Swift
    
    The backend guarantees get after create consistency, i.e. a newly created
    object will be immediately retrievable. 
    """

    def __init__(self, storage_url, login, password, ssl_context):
        # Unused argument
        #pylint: disable=W0613
        
        super().__init__()

        (host, port, container_name, prefix) = self._parse_storage_url(storage_url, ssl_context)
            
        self.hostname = host
        self.port = port
        self.container_name = container_name
        self.prefix = prefix
        self.password = password
        self.login = login
        self.auth_token = None
        self.auth_prefix = None
        self.conn = None
        self.ssl_context = ssl_context
        
        self._container_exists()
    
    @retry
    def _container_exists(self):
        '''Make sure that the container exists'''
        
        try:
            resp = self._do_request('GET', '/', query_string={'limit': 1 })
        except HTTPError as exc:
            if exc.status == 404:
                raise DanglingStorageURLError(self.container_name) from None
            raise
        resp.read()   
                    
    @staticmethod
    def _parse_storage_url(storage_url, ssl_context):
        '''Extract information from storage URL
        
        Return a tuple *(host, port, container_name, prefix)* .
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
        containername = hit.group(3)
        prefix = hit.group(4) or ''
        
        return (hostname, port, containername, prefix)

    def is_temp_failure(self, exc): #IGNORE:W0613
        '''Return true if exc indicates a temporary error
    
        Return true if the given exception indicates a temporary problem. Most instance methods
        automatically retry the request in this case, so the caller does not need to worry about
        temporary failures.
        
        However, in same cases (e.g. when reading or writing an object), the request cannot
        automatically be retried. In these case this method can be used to check for temporary
        problems and so that the request can be manually restarted if applicable.
        '''

        if isinstance(exc, AuthenticationExpired):
            return True

        elif isinstance(exc, HTTPError) and exc.status >= 500 and exc.status <= 599:
            return True

        elif is_temp_network_error(exc):
            return True
                
        return False
        
    def _get_conn(self):
        '''Obtain connection to server and authentication token'''

        log.debug('_get_conn(): start')
        
        conn = http_connection(self.hostname, self.port, self.ssl_context)
        headers={ 'X-Auth-User': self.login,
                  'X-Auth-Key': self.password }
        
        for auth_path in ('/v1.0', '/auth/v1.0'):
            log.debug('_get_conn(): GET %s', auth_path)
            conn.request('GET', auth_path, None, headers)
            resp = conn.getresponse()
            
            if resp.status == 412:
                log.debug('_refresh_auth(): auth to %s failed, trying next path', auth_path)
                resp.read()
                continue
            
            elif resp.status == 401:
                raise AuthorizationError(resp.read())
            
            elif resp.status > 299 or resp.status < 200:
                raise HTTPError(resp.status, resp.reason, resp.getheaders(), resp.read())
                
            # Pylint can't infer SplitResult Types
            #pylint: disable=E1103                
            self.auth_token = resp.getheader('X-Auth-Token')
            o = urlsplit(resp.getheader('X-Storage-Url'))
            self.auth_prefix = urllib.parse.unquote(o.path)
            conn.close()

            return http_connection(o.hostname, o.port, self.ssl_context)
        
        raise RuntimeError('No valid authentication path found')
    
    def _do_request(self, method, path, subres=None, query_string=None,
                    headers=None, body=None):
        '''Send request, read and return response object
        
        This method modifies the *headers* dictionary.
        '''

        log.debug('_do_request(): start with parameters (%r, %r, %r, %r, %r, %r)',
                  method, path, subres, query_string, headers, body)

        if headers is None:
            headers = dict()

        if not body:
            headers['content-length'] = '0'

        if self.conn is None:
            log.debug('_do_request(): no active connection, calling _get_conn()')
            self.conn =  self._get_conn()
                        
        # Construct full path
        path = urllib.parse.quote('%s/%s%s' % (self.auth_prefix, self.container_name, path))
        if query_string:
            s = urllib.parse.urlencode(query_string, doseq=True)
            if subres:
                path += '?%s&%s' % (subres, s)
            else:
                path += '?%s' % s
        elif subres:
            path += '?%s' % subres

        headers['connection'] = 'keep-alive'
        headers['X-Auth-Token'] = self.auth_token
    
        try:
            log.debug('_do_request(): %s %s', method, path)
            self.conn.request(method, path, body, headers)

            log.debug('_do_request(): Reading response..')
            resp = self.conn.getresponse()
        except:
            # We probably can't use the connection anymore
            self.conn.close()
            raise
    
        # We need to call read() at least once for httplib to consider this
        # request finished, even if there is no response body.
        if resp.length == 0:
            resp.read()

        # Success 
        if resp.status >= 200 and resp.status <= 299:
            return resp

        # Expired auth token
        if resp.status == 401:
            log.info('OpenStack auth token seems to have expired, requesting new one.')
            self.conn = None
            raise AuthenticationExpired(resp.reason)
        
        # If method == HEAD, server must not return response body
        # even in case of errors
        if method.upper() == 'HEAD':
            raise HTTPError(resp.status, resp.reason)
        else:
            raise HTTPError(resp.status, resp.reason, resp.getheaders(), resp.read())
     
    @retry 
    def lookup(self, key):
        """Return metadata for given key.

        If the key does not exist, `NoSuchObject` is raised.
        """
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
    def get_size(self, key):
        '''Return size of object stored under *key*'''

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
    def open_read(self, key):
        ''''Open object for reading

        Return a tuple of a file-like object. Backend contents can be read from
        the file-like object, metadata is stored in its *metadata* attribute and
        can be modified by the caller at will. The object must be closed explicitly.
        '''

        try:
            resp = self._do_request('GET', '/%s%s' % (self.prefix, key))
        except HTTPError as exc:
            if exc.status == 404:
                raise NoSuchObject(key) from None
            raise

        return ObjectR(key, resp, self, extractmeta(resp))

    def open_write(self, key, metadata=None, is_compressed=False):
        """Open object for writing

        `metadata` can be a dict of additional attributes to store with the object. Returns a file-
        like object. The object must be closed explicitly. After closing, the *get_obj_size* may be
        used to retrieve the size of the stored object (which may differ from the size of the
        written data).
        
        The *is_compressed* parameter indicates that the caller is going to write compressed data,
        and may be used to avoid recompression by the backend.
        """
        
        log.debug('open_write(%s): start', key)

        headers = dict()
        if metadata:
            for (hdr, val) in metadata.items():
                headers['X-Object-Meta-%s' % hdr] = val

        return ObjectW(key, self, headers)

    def clear(self):
        """Delete all objects in backend"""
        
        # We have to cache keys, because otherwise we can't use the
        # http connection to delete keys.
        for (no, s3key) in enumerate(list(self)):
            if no != 0 and no % 1000 == 0:
                log.info('clear(): deleted %d objects so far..', no)

            log.debug('clear(): deleting key %s', s3key)

            # Ignore missing objects when clearing backend
            self.delete(s3key, True)

    @retry
    def delete(self, key, force=False):
        """Delete object stored under `key`

        ``backend.delete(key)`` can also be written as ``del backend[key]``.
        If `force` is true, do not return an error if the key does not exist.
        """

        log.debug('delete(%s)', key)
        try:
            resp = self._do_request('DELETE', '/%s%s' % (self.prefix, key))
            assert resp.length == 0
        except HTTPError as exc:
            if exc.status == 404 and not force:
                raise NoSuchObject(key) from None
            elif exc.status != 404:
                raise

    @retry
    def copy(self, src, dest):
        """Copy data stored under key `src` to key `dest`
        
        If `dest` already exists, it will be overwritten. The copying
        is done on the remote side. 
        """

        log.debug('copy(%s, %s): start', src, dest)

        try:
            resp = self._do_request('PUT', '/%s%s' % (self.prefix, dest),
                                    headers={ 'X-Copy-From': '/%s/%s%s' % (self.container_name,
                                                                           self.prefix, src)})
            # Discard response body
            resp.read()
        except HTTPError as exc:
            if exc.status == 404:
                raise NoSuchObject(src) from None
            raise

    def list(self, prefix=''):
        '''List keys in backend

        Returns an iterator over all keys in the backend. This method
        handles temporary errors.
        '''

        log.debug('list(%s): start', prefix)

        marker = ''
        waited = 0
        interval = 1 / 50
        iterator = self._list(prefix, marker)
        while True:
            try:
                marker = next(iterator)
                waited = 0
            except StopIteration:
                break
            except Exception as exc:
                if not self.is_temp_failure(exc):
                    raise
                if waited > 60 * 60:
                    log.error('list(): Timeout exceeded, re-raising %s exception', 
                              type(exc).__name__)
                    raise

                log.info('Encountered %s exception (%s), retrying call to swift.Backend.list()',
                          type(exc).__name__, exc)
                
                if hasattr(exc, 'retry_after') and exc.retry_after:
                    interval = exc.retry_after
                                    
                time.sleep(interval)
                waited += interval
                interval = min(5*60, 2*interval)
                iterator = self._list(prefix, marker)

            else:
                yield marker

    def _list(self, prefix='', start='', batch_size=5000):
        '''List keys in backend, starting with *start*

        Returns an iterator over all keys in the backend. This method
        does not retry on errors.
        '''

        keys_remaining = True
        marker = start
        prefix = self.prefix + prefix
        
        while keys_remaining:
            log.debug('list(%s): requesting with marker=%s', prefix, marker)

            try:
                resp = self._do_request('GET', '/', query_string={'prefix': prefix,
                                                                  'format': 'json',
                                                                  'marker': marker,
                                                                  'limit': batch_size })
            except HTTPError as exc:
                if exc.status == 404:
                    raise DanglingStorageURLError(self.container_name) from None
                raise
            
            if resp.status == 204:
                return
            
            assert resp.getheader('content-type') == 'application/json; charset=utf-8'
            
            strip = len(self.prefix)
            count = 0
            try:
                text_resp = io.TextIOWrapper(resp, encoding='utf-8')
                for dataset in json.load(text_resp):
                    count += 1
                    marker = dataset['name']
                    yield marker[strip:]
                
            except GeneratorExit:
                # Need to read rest of response
                while True:
                    buf = resp.read(BUFSIZE)
                    if buf == b'':
                        break
                break
            
            keys_remaining = count == batch_size 

            
class ObjectW(s3c.ObjectW):
    '''A SWIFT object open for writing
    
    All data is first cached in memory, upload only starts when
    the close() method is called.
    '''

    @retry
    def close(self):
        '''Close object and upload data'''

        # Access to protected member ok
        #pylint: disable=W0212

        log.debug('ObjectW(%s).close(): start', self.key)

        self.closed = True
        self.headers['Content-Length'] = self.obj_size
        self.headers['Content-Type'] = 'application/octet-stream'

        self.fh.seek(0)
        resp = self.backend._do_request('PUT', '/%s%s' % (self.backend.prefix, self.key),
                                       headers=self.headers, body=self.fh)
        etag = resp.getheader('ETag').strip('"')
        resp.read()

        if etag != self.md5.hexdigest():
            log.warning('ObjectW(%s).close(): MD5 mismatch (%s vs %s)', self.key, etag,
                     self.md5.hexdigest)
            try:
                self.backend.delete(self.key)
            except:
                log.exception('Objectw(%s).close(): unable to delete corrupted object!',
                              self.key)            
            raise BadDigestError('BadDigest', 'Received ETag does not agree with our calculations.')

    
class ObjectR(s3c.ObjectR):
    '''A SWIFT object opened for reading'''

    pass

    
def extractmeta(resp):
    '''Extract metadata from HTTP response object'''

    meta = dict()
    for (name, val) in resp.getheaders():
        hit = re.match(r'^X-Object-Meta-(.+)$', name, re.IGNORECASE)
        if not hit:
            continue
        meta[hit.group(1)] = val

    return meta


class AuthenticationExpired(Exception):
    '''Raised if the provided Authentication Token has expired'''

    def __init__(self, msg):
        super().__init__()
        self.msg = msg

    def __str__(self):
        return 'Auth token expired. Server said: %s' % self.msg
    
        
