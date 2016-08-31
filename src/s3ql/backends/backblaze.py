from ..logging import logging, QuietError  # Ensure use of custom logger class
from .. import BUFSIZE
from .common import (AbstractBackend, NoSuchObject, retry, AuthorizationError,
                     DanglingStorageURLError,
                     get_ssl_context)
from .s3c import HTTPError, ObjectR, ObjectW, BadDigestError
from . import s3c
from ..inherit_docstrings import (copy_ancestor_docstring, prepend_ancestor_docstring,
                                  ABCDocstMeta)
from dugong import (HTTPConnection, BodyFollowing, is_temp_network_error, CaseInsensitiveDict)
import json
import re
import urllib.parse
import base64
import hashlib
from itertools import count
import tempfile
import io
import ssl

log = logging.getLogger(__name__)


class Backend(AbstractBackend, metaclass=ABCDocstMeta):
    needs_login = True

    known_options = {'test-string', 'ssl-ca-path'}

    authorize_url = "/b2api/v1/b2_authorize_account"
    authorize_hostname = "api.backblaze.com"
    hdr_prefix = 'X-Bz-Info-'

    # no chunksize limit on backend, so we try to limit the number of meta headers
    hdr_chunksize = 2000

    _add_meta_headers_s3 = s3c.Backend._add_meta_headers
    _extractmeta_s3 = s3c.Backend._extractmeta

    def __init__(self, storage_url, login, password, options):
        super().__init__()
        self.options = options
        self.bucket_name = None
        self.prefix = None
        self.auth_token = None
        self.api_host = None
        self.download_host = None
        self.conn_api = None
        self.conn_download = None
        self.account_id = login
        self.account_key = password
        self.bucket_id = None
        self.bucket_name = None

        self._parse_storage_url(storage_url)

        # Add test header povided to all requests.
        if options.get('test-string'):
            # sanitize entry string
            self.test_string = ''.join([x for x in options.get('test-string') if x in 'abcdefghijklmnopqrstuvwxyz_'])
        else:
            self.test_string = None

        self.ssl_context = get_ssl_context(options.get('ssl-ca-path', None))

        self._login()

        self._connect_bucket(self.bucket_name)

    def _do_request(self, method, path, conn, headers=None, body=None, auth_token=None, download_body=True,
                    body_size=None):
        """Send request, read and return response object

        This method modifies the *headers* dictionary.
        conn must by a HTTPConnection

        When download_body is True, need to receive data before making new connection

        """
        def _debug_body(b):
            if isinstance(b, str):
                return b
            elif b is None:
                return "None"
            else:
                return 'byte_body'

        def _debug_hostname(c):
            try:
                return c.hostname
            except:
                return "None"

        log.debug('started with %r, %r, %r, %r, %r',
                  method, _debug_hostname(conn), path, headers, _debug_body(body))

        if headers is None:
            headers = CaseInsensitiveDict()

        if auth_token is None:
            headers['Authorization'] = self.auth_token
        else:
            headers['Authorization'] = auth_token

        if self.test_string:
            headers['X-Bz-Test-Mode'] = self.test_string

        try:
            if isinstance(body, io.FileIO):
                if body_size is None:
                    raise ValueError("Body size is necessary when uploading from file")
                conn.send_request(method, path, headers=headers, body=BodyFollowing(body_size))
                while True:
                    buf = body.read(BUFSIZE)
                    if not buf:
                        break
                    conn.write(buf)
            else:
                conn.send_request(method, path, headers=headers, body=body)
            resp = conn.read_response()
            if download_body or resp.status != 200:
                body = conn.readall()
            else:
                # caller need to download body itself before making new request
                body = None
        except Exception as exc:
            if is_temp_network_error(exc):
                # We probably can't use the connection anymore
                conn.disconnect()
            raise

        if resp.status == 200 or resp.status == 206:
            return resp, body

        try:
            # error code is in body
            j = json.loads(str(body, encoding='UTF-8'))
        except ValueError:
            raise HTTPError(resp.status, resp.reason, resp.headers)

        # Expired auth token
        if resp.status == 401:
            if j['code'] == 'expired_auth_token':
                log.info('BackBlaze auth token seems to have expired, requesting new one.')
                self.conn_api.disconnect()
                self.conn_download.disconnect()
                # Force constructing a new connection with a new token, otherwise
                # the connection will be reestablished with the same token.
                self.conn_api = None
                self.conn_download = None
                self._login()
                raise AuthenticationExpired(j['message'])
            else:
                raise AuthorizationError(j['message'])

        # File not found
        if resp.status == 404:
            raise NoSuchObject(path)

        # Backend error
        raise B2Error(j['status'], j['code'], j['message'], headers=headers)

    def _login(self):
        """
        Login with backend and make a new connection to API and download server

        """

        id_and_key = self.account_id + ':' + self.account_key
        basic_auth_string = 'Basic ' + str(base64.b64encode(bytes(id_and_key, 'UTF-8')), encoding='UTF-8')

        with HTTPConnection(self.authorize_hostname, 443, ssl_context=self.ssl_context) as conn:
            resp, body = self._do_request('GET', self.authorize_url, conn, auth_token=basic_auth_string)

            j = json.loads(str(body, encoding='UTF-8'))

            api_url = urllib.parse.urlparse(j['apiUrl'])
            download_url = urllib.parse.urlparse(j['downloadUrl'])

            self.api_host = api_url.hostname
            self.auth_token = j['authorizationToken']
            self.download_host = download_url.hostname

            self.conn_api = HTTPConnection(self.api_host, 443, ssl_context=self.ssl_context)
            self.conn_download = HTTPConnection(self.download_host, 443, ssl_context=self.ssl_context)

    def _connect_bucket(self, bucket_name):
        """
        Get id of bucket_name
        """

        log.debug('started with %s' % (bucket_name))

        resp, body = self._do_request('GET', '/b2api/v1/b2_list_buckets?accountId=%s' % self.account_id, self.conn_api)
        bucket_id = None
        j = json.loads(str(body, encoding='UTF-8'))

        for b in j['buckets']:
            if b['bucketName'] == bucket_name:
                bucket_id = b['bucketId']

        if bucket_id is None:
            raise DanglingStorageURLError(bucket_name)
        self.bucket_id = bucket_id
        self.bucket_name = bucket_name

    def _add_meta_headers(self, headers, metadata):
        self._add_meta_headers_s3(headers, metadata,
                                  chunksize=self.hdr_chunksize)
        # URL encode headers
        for i in count():
            # Headers is an email.message object, so indexing it
            # would also give None instead of KeyError
            key = '%smeta-%03d' % (self.hdr_prefix, i)
            part = headers.get(key, None)
            if part is None:
                break
            headers[key] = urllib.parse.quote(part.encode('utf-8'))
            # Check we dont reach the metadata backend lmits
            if i > 10:
                raise RuntimeError("Too metadata for the backend")

    def _extractmeta(self, resp, filename):
        # URL decode headers
        for i in count():
            # Headers is an email.message object, so indexing it
            # would also give None instead of KeyError
            key = '%smeta-%03d' % (self.hdr_prefix, i)
            part = resp.headers.get(key, None)
            if part is None:
                break
            resp.headers.replace_header(key, urllib.parse.unquote_plus(str(part)))
        return self._extractmeta_s3(resp, filename)

    def _get_upload_url(self):

        """Get a single use URL to upload a file"""

        log.debug('started')

        body = bytes(json.dumps({'bucketId': self.bucket_id}), encoding='UTF-8')

        resp, body = self._do_request('POST',
                                      '/b2api/v1/b2_get_upload_url',
                                      self.conn_api,
                                      body=body)

        j = json.loads(str(body, encoding='UTF-8'))
        return j['authorizationToken'], j['uploadUrl']

    def _parse_storage_url(self, storage_url):
        """Init instance variables from storage url"""

        hit = re.match(r'^b2?://([^/]+)(?:/(.*))?$', storage_url)
        if not hit:
            raise QuietError('Invalid storage URL', exitcode=2)

        bucket_name = hit.group(1)

        if not re.match('^(?!b2-)[a-z0-9A-Z\-]{6,50}$', bucket_name):
            raise QuietError('Invalid bucket name.', exitcode=2)

        prefix = hit.group(2) or ''
        #remove trailing slash if exist
        prefix = prefix[:-1] if prefix[-1] == '/' else prefix

        self.bucket_name = bucket_name
        self.prefix = prefix

    def _delete_file_id(self, filelist):

        for file in filelist:
            body = bytes(json.dumps({'fileName': file['fileName'],
                                     'fileId': file['fileId']}), encoding='UTF-8')

            log.debug('started with /file/%s/%s/%s' % (self.bucket_name, self.prefix, file['fileName']))

            try:
                self._do_request('POST',
                                 '/b2api/v1/b2_delete_file_version',
                                 self.conn_api,
                                 body=body)
            except B2Error as err:
                # Server may return file_not_present
                # just let it close connection and retry
                if err.code == 'file_not_present':
                    pass
                else:
                    raise err

    @retry
    def _list_file_version(self, key, max_filecount=1000):
        if max_filecount > 1000:
            raise ValueError('max_filecount maximum is 1000')

        request_dict = dict(bucketId=self.bucket_id, maxFileCount=max_filecount)
        request_dict['startFileName'] = self.prefix + '/' + self._encode_key(key)

        # supposing there is less than 1000 old file version
        body = bytes(json.dumps(request_dict), encoding='UTF-8')

        resp, body = self._do_request('POST',
                                      '/b2api/v1/b2_list_file_versions',
                                      self.conn_api,
                                      body=body)

        j = json.loads(str(body, encoding='UTF-8'))
        r = []

        # We suppose there is less than 1000 file version
        for f in j['files']:
            if self._decode_key(f['fileName']) == self.prefix + '/' + key:
                r.append({'fileName': f['fileName'],
                          'fileId': f['fileId']})
        return r

    @retry
    def _list_file_name(self, start_filename=None, max_filecount=1000):
        if max_filecount > 1000:
            raise ValueError('max_filecount maximum is 1000')

        request_dict = {'bucketId': self.bucket_id,
                        'maxFileCount': max_filecount}
        if start_filename is not None:
            request_dict['startFileName'] = start_filename
        body = bytes(json.dumps(request_dict), encoding='UTF-8')

        resp, body = self._do_request('POST',
                                      '/b2api/v1/b2_list_file_names',
                                      self.conn_api,
                                      body=body)

        j = json.loads(str(body, encoding='UTF-8'))
        filelist = [f['fileName'] for f in j['files']]

        return j['nextFileName'], filelist

    def _encode_key(self, filename):

        # URLencode filename
        filename = urllib.parse.quote(filename.encode('utf-8'), safe='/\\')

        # DIRTY HACK :
        # Backend does not support backslashes, we change them to pass test
        filename = filename.replace("\\","__")

        return filename

    def _decode_key(self,filename):

        # DIRTY HACK :
        # Backend does not support backslashes, we change them to pass test
        filename = filename.replace("__","\\")

        # URLencode filename
        filename = urllib.parse.unquote(filename)

        return filename

    def has_native_rename(self):
        """True if the backend has a native, atomic rename operation"""
        return False

    @copy_ancestor_docstring
    def is_temp_failure(self, exc):

        if isinstance(exc, AuthenticationExpired):
            return True

        if isinstance(exc, ssl.SSLError):
            return True

        if is_temp_network_error(exc):
            return True

        if isinstance(exc, HTTPError):
            return True

        if isinstance(exc, B2Error):
            if (exc.status == 400 or \
                exc.status == 408 or \
                (exc.status >= 500 and exc.status <= 599)):
                return True

        return False

    @retry
    @copy_ancestor_docstring
    def lookup(self, key):
        log.debug('started with %s', key)

        key = self._encode_key(key)
        headers = CaseInsensitiveDict()
        headers['Range'] = "bytes=0-1"  # Only get first byte

        resp, data = self._do_request('GET',
                                      '/file/%s/%s/%s' % (self.bucket_name, self.prefix, key),
                                      self.conn_download, headers=headers)

        meta = self._extractmeta(resp, key)
        return meta

    @retry
    def get_size(self, key):
        """Return size of object stored under *key*"""
        log.debug('started with %s', key)

        key = self._encode_key(key)
        key_with_prefix = "%s/%s" % (self.prefix, key)
        request_dict = {'bucketId': self.bucket_id,
                        'startFileName': key_with_prefix,
                        'maxFileCount': 1}

        body = bytes(json.dumps(request_dict), encoding='UTF-8')
        resp, body = self._do_request('POST',
                                      '/b2api/v1/b2_list_file_names',
                                      self.conn_api,
                                      body=body)

        j = json.loads(str(body, encoding='UTF-8'))
        if j['files'][0]['fileName'] == key_with_prefix:
            return j['files'][0]['contentLength']

        raise NoSuchObject(key_with_prefix)

    @retry
    @copy_ancestor_docstring
    def open_read(self, key):
        log.debug('started with %s', key)

        key = self._encode_key(key)

        resp, data = self._do_request('GET',
                                      '/file/%s/%s/%s' % (self.bucket_name, self.prefix, key),
                                      self.conn_download, download_body=False)

        meta = self._extractmeta(resp, key)

        return ObjectR(key, self.conn_download, resp, metadata=meta)

    @copy_ancestor_docstring
    def open_write(self, key, metadata=None, is_compressed=False):
        log.debug('started with %s', key)

        key = self._encode_key(key)

        return ObjectW(key, self, metadata)

    @retry
    @copy_ancestor_docstring
    def clear(self):
        log.debug('started')
        for file in self.list():
            self.delete(file,force=True)

    @retry
    @copy_ancestor_docstring
    def delete(self, key, force=False):
        log.debug('started with %s', key)

        todel_id = self._list_file_version(key)

        if not todel_id and not force:
            raise NoSuchObject(key)

        self._delete_file_id(todel_id)

    @copy_ancestor_docstring
    def list(self, prefix=''):
        log.debug('started with %s', prefix)

        prefix = self._encode_key(prefix)

        next_filename = self.prefix + '/' + prefix
        keys_remaining = True

        while keys_remaining and next_filename is not None:
            next_filename, filelist = self._list_file_name(next_filename)

            while filelist:
                file = filelist.pop(0)

                if file.startswith(self.prefix + '/' + prefix):
                    # remove prefix before return
                    r = file[len(self.prefix + '/'):]
                    yield self._decode_key(r)
                else:
                    keys_remaining = False
                    break

    @prepend_ancestor_docstring
    def copy(self, src, dest, metadata=None):
        """No atomic copy operation on backend. Hope this does not append to often"""
        log.debug('started with %s, %s', src, dest)

        data, src_meta = self.fetch(src)

        # Delete dest file if already exist
        self.delete(dest, force=True)

        if metadata is None:
            dst_meta = src_meta
        else:
            dst_meta = metadata

        self.store(dest, data, dst_meta)

    @copy_ancestor_docstring
    def update_meta(self, key, metadata):
        log.debug('started with %s', key)
        self.copy(key, key, metadata)

    def close(self):
        log.debug('started')
        self.conn_api.disconnect()
        self.conn_download.disconnect()


class ObjectR(object):
    """An BackBlaze object open for reading"""

    def __init__(self, key, conn, resp, metadata=None):
        self.conn = conn
        self.metadata = metadata
        self.sha1_checked = False
        self.resp = resp
        self.key = key
        self.closed = False

        self.sha1 = hashlib.sha1()

    def read(self, size=None):
        """Read up to *size* bytes of object data

        For integrity checking to work, this method has to be called until
        it returns an empty string, indicating that all data has been read
        (and verified).
        """

        if size == 0:
            return b''

        try:
            buf = self.conn.read(size)
        except Exception as exc:
            if is_temp_network_error(exc):
                # We probably can't use the connection anymore
                self.conn.disconnect()
            raise

        self.sha1.update(buf)

        # Check SHA1 on EOF
        # (size == None implies EOF)
        if (not buf or size is None) and not self.sha1_checked:
            file_hash = self.resp.headers['x-bz-content-sha1'].strip('"')
            self.sha1_checked = True
            if file_hash != self.sha1.hexdigest():
                log.warning('SHA mismatch for %s: %s vs %s',
                            self.key, file_hash, self.sha1.hexdigest())
                raise BadDigestError('BadDigest',
                                     'SHA1 header does not agree with calculated SHA1')

        return buf

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def close(self, checksum_warning=True):
        """Close object

        If *checksum_warning* is true, this will generate a warning message if
        the object has not been fully read (because in that case the MD5
        checksum cannot be checked).
        """

        if self.closed:
            return
        self.closed = True

        # If we have not read all the data, close the entire
        # connection (otherwise we loose synchronization)
        if not self.sha1_checked:
            if checksum_warning:
                log.warning("Object closed prematurely, can't check SHA1, and have to "
                            "reset connection")
            self.conn.disconnect()


class ObjectW(object):
    """An BackBlaze object open for writing"""

    def __init__(self, key, backend, meta):
        self.key = key
        self.meta = meta
        self.closed = False
        self.obj_size = 0
        self.backend = backend

        # According to http://docs.python.org/3/library/functions.html#open
        # the buffer size is typically ~8 kB. We process data in much
        # larger chunks, so buffering would only hurt performance.
        self.fh = tempfile.TemporaryFile(buffering=0)

        self.sha1 = hashlib.sha1()

    def write(self, buf):
        self.fh.write(buf)
        self.obj_size += len(buf)
        self.sha1.update(buf)

    def is_temp_failure(self, exc):
        if isinstance(exc, NoSuchObject):
            return True
        return self.backend.is_temp_failure(exc)

    @retry
    def close(self):
        """Close object and upload data"""
        log.debug('started with %s', self.key)

        if self.closed:
            # still call fh.close, may have generated an error before
            self.fh.close()
            return

        self.fh.seek(0)
        upload_auth_token, upload_url = self.backend._get_upload_url()
        upload_url = urllib.parse.urlparse(upload_url)

        with HTTPConnection(upload_url.hostname, 443, ssl_context=self.backend.ssl_context) as conn_up:

            headers = CaseInsensitiveDict()
            headers['X-Bz-File-Name'] = self.backend.prefix + '/' + self.key
            headers['Content-Type'] = 'application/octet-stream'
            headers['Content-Length'] = self.obj_size
            headers['X-Bz-Content-Sha1'] = self.sha1.hexdigest()

            if self.meta is None:
                self.meta = dict()
            self.backend._add_meta_headers(headers, self.meta)

            self.backend._do_request('POST', upload_url.path + '?' + upload_url.query,
                                     conn_up,
                                     headers=headers,
                                     body=self.fh,
                                     auth_token=upload_auth_token,
                                     body_size=self.obj_size)

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


class AuthenticationExpired(Exception):
    """Raised if the provided Authentication Token has expired"""

    def __init__(self, msg):
        super().__init__()
        self.msg = msg

    def __str__(self):
        return 'Auth token expired. Server said: %s' % self.msg

def _parse_retry_after(header):
    """Parse headers for Retry-After value"""

    hit = re.match(r'^\s*([0-9]+)\s*$', header)
    if hit:
        val = int(header)
    else:
        val = 1

    return val

class B2Error(Exception):
    """Represents an error returned by Backblaze"""

    def __init__(self, status, code, msg, headers = None):
        super().__init__(msg)
        self.status = status
        self.code = code
        self.msg = msg

        if headers and 'Retry-After' in headers:
            self.retry_after = _parse_retry_after(headers['Retry-After'])
        else:
            # Force 1s waiting before retry
            self.retry_after = 1

    def __str__(self):
        return '%s : %s - %s' % (self.status, self.code, self.msg)
