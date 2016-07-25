import re
import random
import time
import threading
import json
import tempfile
import hashlib
from shutil import copyfileobj
from dugong import (HTTPConnection, BodyFollowing, CaseInsensitiveDict,
                    is_temp_network_error)
import urllib.parse

from . import s3c
from .s3c import HTTPError, ObjectR
from .common import (AbstractBackend, get_ssl_context, get_proxy, NoSuchObject,
                     retry, retry_generator)
from .. import BUFSIZE
from ..common import (freeze_basic_mapping, thaw_basic_mapping,
                      get_backend_cachedir)
from ..logging import logging
from ..inherit_docstrings import copy_ancestor_docstring, ABCDocstMeta
from ..database import Connection, NoSuchRowError

ACD_REFRESH_URL = 'https://api.amazon.com/auth/o2/token'
ACD_GET_ENDPOINTS_URL = 'https://drive.amazonaws.com/drive/v1/account/endpoint'
APP_ID = 'test-A3EVMPGFH2F42D'
CLIENT_ID = 'amzn1.application-oa2-client.1e723be5211c4426bea2743683b77fa0'
CLIENT_SECRET = 'd36b62955cb6422e4506e8829ce4e84efb8fc38f79e92b79cf8673fbfa1abe50'

# placeholder in cache for nonexisting files
NO_FILE = 0

filter_escape_re = re.compile("([+\\-&|!(){}[\\]^'\"~*?:\\\\ ])")
like_escape_re = re.compile("([%_^])")
log = logging.getLogger(__name__)

class Backend(AbstractBackend, metaclass=ABCDocstMeta):
    """A backend to store data in Amazon Cloud Drive."""

    known_options = {'ssl-ca-path', 'timeout'}

    clear = s3c.Backend.clear

    _static_data = dict()
    _static_lock = threading.Lock()

    def __init__(self, storage_url, login, password, options, cachedir):
        '''Initialize ACD backend.

        Password is the refresh token, login is ignored.
        '''
        # Unused argument
        #pylint: disable=W0613

        super().__init__()

        self.ssl_context = get_ssl_context(options.get('ssl-ca-path', None))
        self.timeout = int(options.get('timeout', 20))
        self.proxy = get_proxy(True)
        self.metadata_conn = None
        self.content_conn = None

        self.refresh_token = password
        if not password in self._static_data:
            self._static_data[password] = d = {}
            self._get_access_token()
            self._get_endpoints()
            self._open()

            path = storage_url[len('acd://'):]
            if path[0] == '/':
                d['parent_node'] = -1 # prevent KeyError
                d['parent_node'] = self._get_path(path)
            else:
                d['parent_node'] = path
                log.debug('parent_node = %r' % d['parent_node'])

            self._open_db(storage_url, cachedir)
        else:
            self._open()


    def _simple_req(self, url, method, headers=None, body=None):
        '''Do a single request to a https server, expecting status 200 and a
        JSON response in body in UTF-8 encoding.
        '''

        url = urllib.parse.urlsplit(url)
        assert url.scheme == 'https'

        with HTTPConnection(url.hostname, url.port,
                            ssl_context=self.ssl_context) as conn:
            conn.timeout = self.timeout

            conn.send_request(method, url.path, headers=headers, body=body)
            return read_json(conn)

    def _get_static(self):
        return self._static_data[self.refresh_token]

    @retry
    def _get_access_token(self):
        '''Get a new access token using the stored refresh_token'''
        log.debug('refreshing token')

        headers = {'Content-Type': 'application/x-www-form-urlencoded; charset=utf-8'}
        body = urllib.parse.urlencode({
            'grant_type': 'refresh_token',
            'refresh_token': self.refresh_token,
            'client_id': CLIENT_ID,
            'client_secret': CLIENT_SECRET}).encode('utf-8')
        json = self._simple_req(ACD_REFRESH_URL, 'POST', headers=headers, body=body)

        if json.get('token_type', None) != 'bearer' or \
           json.get('refresh_token', None) != self.refresh_token or \
           not 'access_token' in json or \
           not isinstance(json.get('expires_in', None), int):
            raise ACDError('Invalid response while refreshing token')

        d = self._get_static()
        d['access_token'] = 'Bearer ' + json['access_token']
        d['access_token_expires_at'] = time.time() + json['expires_in'] - 20
        log.debug('new token %r expires at %r' %
                  (d['access_token'], d['access_token_expires_at']))

    def _maybe_refresh_token(self):
        '''Refresh auth token if expired'''
        with self._static_lock:
            if time.time() > self._get_static()['access_token_expires_at']:
                self._get_access_token()

    @retry
    def _get_endpoints(self):
        log.debug('getting acd endpoints')

        d = self._get_static()
        json = self._simple_req(ACD_GET_ENDPOINTS_URL, 'GET',
                                headers={'Authorization': d['access_token']})
        if not 'metadataUrl' in json or not 'contentUrl' in json:
            raise ACDError('Invalid response while getting endpoints')

        # "should be cached for three to five days" according to acd docs
        d['endpoints_expire_at'] = time.time() + 5*24*60*60

        for x in ['metadata', 'content']:
            raw_url = json['%sUrl' % x]
            log.debug('%s url: %r' % (x, raw_url))
            url = urllib.parse.urlsplit(raw_url)
            if not url.scheme == 'https' or url.query != '' or url.fragment != '':
                raise ACDError('Invalid url returned by server: %r' % raw_url)

            d['%s_url' % x] = url

    def _maybe_get_endpoints(self):
        '''Get endpoints if cached data expired'''
        with self._static_lock:
            if time.time() > self._get_static()['endpoints_expire_at']:
                self._get_endpoints()

    def _open(self):
        self._maybe_get_endpoints()
        for x in ['metadata', 'content']:
            url = self._get_static()['%s_url' % x]
            conn = HTTPConnection(url.hostname, url.port,
                                  ssl_context=self.ssl_context)
            conn.timeout = self.timeout

            setattr(self, '%s_conn' % x, conn)
            setattr(self, '%s_prefix' % x, url.path)

        self.conn = self.content_conn # compat with s3c ObjectR

    def _open_db(self, storage_url, cachedir):
        cachedir = get_backend_cachedir(storage_url, cachedir)
        db = Connection(cachedir+'-acd.db')
        db.execute('DROP TABLE IF EXISTS node_cache')
        db.execute('CREATE TABLE node_cache(key PRIMARY KEY, acd_id, metadata, size)')
        self._get_static()['db'] = db

        parent = self._get_static()['parent_node']
        json = self._get_nodes(parent=parent)

        # according to docs nextToken is returned even on last page, so iterate
        # until we receive no more items
        # except in reality not always...
        n = 0
        while True:
            for node in json['data']:
                self._db_add(node)

            if not 'nextToken' in json:
                break
            json = self._get_nodes(parent=parent, start=json['nextToken'])

    def _db_get(self, key, field):
        try:
            return self._get_static()['db'].get_val(
                'SELECT %s FROM node_cache WHERE key = ?' % field,
                (key,))
        except NoSuchRowError:
            raise NoSuchObject(key)

    def _db_add(self, node):
        md = node['properties'].get(APP_ID, None)
        if md != None: md = md.get('metadata')

        self._get_static()['db'].execute(
            'INSERT OR REPLACE INTO node_cache(key, acd_id, metadata, size) VALUES(?,?,?,?)',
            (node['name'], node['id'], md, node['contentProperties']['size']))

    def _db_remove(self, key):
        self._get_static()['db'].execute(
            'DELETE FROM node_cache WHERE key = ?', (key,))


    def _send_request(self, conn, method, path, headers=None, query=None,
                      body=None, expect100=False):
        self._maybe_refresh_token()

        if not isinstance(headers, CaseInsensitiveDict):
            headers = CaseInsensitiveDict(headers)
        headers['Authorization'] = self._get_static()['access_token']

        if query:
            path = '%s?%s' % (path, urllib.parse.urlencode(query))

        log.debug('%r %r %r' % (method, path, body))
        conn.send_request(method, path, headers=headers,
                          body=body, expect100=expect100)

        resp = conn.read_response()
        if resp.status == 401: # access token expired
            self._get_static()['access_token_expires_at'] = 0
        return resp

    def _get_json_response(self, conn, *k, **kw):
        resp = self._send_request(conn, *k, **kw)
        return read_json(conn, resp)

    def _metadata_request(self, method, path, query=None, body=None):
        npath = self.metadata_prefix + path
        return self._get_json_response(self.metadata_conn, method, npath,
                                       query=query, body=body)

    # these functions only used during initialization
    @retry
    def _get_nodes(self, filters=None, parent=None, start=None):
        log.debug('getting node %s/%s' % (parent, filters))
        if parent:
            path = 'nodes/%s/children' % urllib.parse.quote(parent)
        else:
            path = 'nodes'

        query = {}
        if filters:
            query['filters'] = filters
        if start:
            query['startToken'] = start
        return self._metadata_request('GET', path, query=query)

    def _get_single_node(self, filters, parent=None):
        json = self._get_nodes(filters, parent)
        if json.get('count', None) != 1:
            raise NoSuchObject(filters)
        return json['data'][0]

    def _filter_escape(self, name):
        return filter_escape_re.sub(r'\\\1', name)

    def _get_path(self, path):
        parent = self._get_single_node('kind:FOLDER AND isRoot:true')['id']
        components = path.split('/')[1:] # it starts with /
        for c in components:
            parent = self._get_single_node('name:%s' % self._filter_escape(c),
                                           parent=parent)['id']
        return parent

    # public methods
    @property
    @copy_ancestor_docstring
    def has_native_rename(self):
        return True

    @copy_ancestor_docstring
    def reset(self):
        self.metadata_conn.disconnect()
        self.content_conn.disconnect()

    @copy_ancestor_docstring
    def is_temp_failure(self, exc):
        self.close()

        if is_temp_network_error(exc):
            return True
        elif isinstance(exc, HTTPError):
            return exc.status in (401,429) or exc.status >= 500
        return False

    @retry
    @copy_ancestor_docstring
    def lookup(self, key):
        md = self._db_get(key, 'metadata')
        if md != None: md = thaw_basic_mapping(md.encode('utf-8'))
        return md

    @retry
    @copy_ancestor_docstring
    def get_size(self, key):
        return self._db_get('size')

    @retry
    @copy_ancestor_docstring
    def open_read(self, key):
        id = self._db_get(key, 'acd_id')

        pth = '%snodes/%s/content' % (
            self.content_prefix, urllib.parse.quote(id))
        resp = self._send_request(self.content_conn, 'GET', pth)
        return ObjectR(key, resp, self, self.lookup(key))

    @retry
    @copy_ancestor_docstring
    def open_write(self, key, metadata=None, is_compressed=False):
        try:
            id = self._db_get(key, 'acd_id')
            # overwrite - we need to update metadata separately
            if metadata:
                self.update_meta(key, metadata)

            acd_metadata = None
            method = 'PUT'
            path = '%snodes/%s/content' % (
                self.content_prefix, urllib.parse.quote(id))
        except NoSuchObject:
            # upload
            method = 'POST'
            path = '%snodes?suppress=deduplication' % self.content_prefix
            acd_metadata = {
                'name': key, 'kind': 'FILE',
                'parents': [self._get_static()['parent_node']] }
            if metadata:
                acd_metadata['properties'] = {
                    APP_ID: {
                        'metadata': freeze_basic_mapping(metadata).decode('utf-8') } }

        return ObjectW(key, self, method, path, acd_metadata)

    # At the moment it only moves to trash, apparently it's not possible to
    # permanently delete files from the api...
    @retry
    @copy_ancestor_docstring
    def delete(self, key, force=False):
        try:
            id = self._db_get(key, 'acd_id')
        except NoSuchObject:
            if force: return
            raise

        self._metadata_request('PUT', 'trash/%s' % urllib.parse.quote(id))
        self._db_remove(key)

    @retry_generator
    @copy_ancestor_docstring
    def list(self, prefix=''):
        db = self._get_static()['db']
        if prefix == '':
            cur = db.query('SELECT key FROM node_cache')
        else:
            cur = db.query('SELECT key FROM node_cache WHERE key LIKE ? ESCAPE "^"',
                           (like_escape_re.sub(r'^\1', prefix)+'%',))

        with cur:
            for row in cur:
                yield row[0]

    @retry
    @copy_ancestor_docstring
    def copy(self, src, dst, metadata=None):
        # It's not possible to do remote copy in acd api, download and reupload...
        log.warning('client side copying from %r to %r' % (src, dst))
        if not metadata:
            metadata = self.lookup(src)

        with self.open_read(src) as srcf:
            with self.open_write(dst, metadata) as dstf:
                buf = srcf.read(BUFSIZE)
                while len(buf) > 0:
                    dstf.write(buf)
                    buf = srcf.read(BUFSIZE)

    @copy_ancestor_docstring
    def update_meta(self, key, metadata):
        self.rename(key, key, metadata)

    @retry
    @copy_ancestor_docstring
    def rename(self, src, dest, metadata=None):
        # can't overwrite with rename...
        if src != dest:
            self.delete(dest, force=True)

        id = self._db_get(src, 'acd_id')
        body = {'name': dest}
        if metadata:
            body['properties'] = {}
            body['properties'][APP_ID] = {}
            body['properties'][APP_ID]['metadata'] = freeze_basic_mapping(metadata).decode('utf-8')

        node = self._metadata_request(
            'PATCH', 'nodes/%s' % urllib.parse.quote(id),
            body=json.dumps(body).encode('utf-8'))

        self._db_remove(src)
        self._db_add(node)

    @retry
    @copy_ancestor_docstring
    def close(self):
        self.metadata_conn.disconnect()
        self.content_conn.disconnect()

class ObjectW(object):
    def __init__(self, key, backend, method, path, metadata):
        self.key = key
        self.backend = backend
        self.method = method
        self.path = path
        self.metadata = metadata
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
        headers = CaseInsensitiveDict()
        boundary = hex(random.randint(0,2**128))
        headers['Content-Type'] = 'multipart/form-data; boundary=%s' % boundary
        if self.metadata:
            pre = '--%s\r\nContent-Disposition: form-data; name="metadata"\r\n\r\n%s\r\n' % (
                boundary, json.dumps(self.metadata))
        else:
            pre = ''
        pre += '--%s\r\nContent-Disposition: form-data; name="content"; filename="foo"\r\nContent-Type: application/octet-stream\r\n\r\n' % boundary
        post = '\r\n--%s--\r\n' % boundary

        conn = self.backend.content_conn
        resp = self.backend._send_request(
            conn, self.method, self.path, headers=headers,
            body=BodyFollowing(len(pre)+len(post)+self.obj_size),
            expect100=True)
        if resp.status != 100:
            raise HTTPError(resp.status, resp.reason, resp.headers)

        conn.write(pre.encode('utf-8'))
        copyfileobj(self.fh, conn, BUFSIZE)
        conn.write(post.encode('utf-8'))
        # x = pre.encode('utf-8') + self.fh.read() + post.encode('utf-8')
        # conn.write(x)

        node = read_json(conn)
        md5 = node['contentProperties']['md5']

        if md5 != self.md5.hexdigest():
            # delete may fail, but we don't want to loose the BadDigest exception
            try:
                self.backend.delete(self.key)
            finally:
                raise BadDigestError('BadDigest', 'MD5 mismatch for %s (received: %s, sent: %s)' %
                                     (self.key, md5, self.md5.hexdigest()))

        self.backend._db_add(node)

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

def read_json(conn, resp = None):
    resp = resp or conn.read_response()
    if resp.status != 200 and resp.status != 201:
        log.debug(conn.readall().decode('utf-8'))
        raise HTTPError(resp.status, resp.reason, resp.headers)

    body = conn.readall().decode('utf-8')
    resp_json = json.loads(body)
    if not isinstance(resp_json, dict):
        raise ACDError('Invalid server response')
    return resp_json

class ACDError(Exception): pass
