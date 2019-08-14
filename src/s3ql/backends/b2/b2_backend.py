import base64
from dugong import (HTTPConnection, CaseInsensitiveDict, is_temp_network_error, BodyFollowing,
                    ConnectionClosed, ConnectionTimedOut)
from urllib.parse import urlparse
import urllib
import json
import re
import ssl
import binascii
from ast import literal_eval
from itertools import count
import io
import os
from shutil import copyfileobj

from  typing import Dict, Tuple, Union, List

from ..common import (AbstractBackend, get_ssl_context, retry, checksum_basic_mapping,
                      CorruptedObjectError, NoSuchObject, DanglingStorageURLError)
from ...inherit_docstrings import (ABCDocstMeta, copy_ancestor_docstring, prepend_ancestor_docstring)
from ...logging import logging, QuietError
from ... import BUFSIZE
from .object_r import ObjectR
from .object_w import ObjectW
from .b2_error import B2Error, BadDigestError

log = logging.getLogger(__name__)

class B2Backend(AbstractBackend, metaclass=ABCDocstMeta):
    '''A backend to store data in backblaze b2 cloud storage.
    '''

    known_options = { 'account-id', 'disable-versions', 'retry-on-cap-exceeded',
                      'test-mode-fail-some-uploads', 'test-mode-expire-some-tokens', 'test-mode-force-cap-exceeded',
                      'tcp-timeout' }

    available_upload_url_infos = []

    def __init__(self, options):
        '''Initialize backend object
        '''

        super().__init__()

        self.ssl_context = get_ssl_context(options.backend_options.get('ssl-ca-path', None))

        self.options = options.backend_options

        self.b2_application_key_id = options.backend_login
        self.b2_application_key = options.backend_password

        self.tcp_timeout = self.options.get('tcp-timeout', 20)

        self.account_id = self.options.get('account-id', None)

        self.disable_versions = self.options.get('disable-versions', False)
        self.retry_on_cap_exceeded = self.options.get('retry-on-cap-exceeded', False)

        # Test modes
        self.test_mode_fail_some_uploads = self.options.get('test-mode-fail-some-uploads', False)
        self.test_mode_expire_some_tokens = self.options.get('test-mode-expire-some-tokens', False)
        self.test_mode_force_cap_exceeded = self.options.get('test-mode-force-cap-exceeded', False)

        (bucket_name, prefix) = self._parse_storage_url(options.storage_url, self.ssl_context)
        self.bucket_name = bucket_name
        self.bucket_id = None
        self.prefix = prefix

        self.api_version = 2
        self.api_url_prefix = '/b2api/v' + str(self.api_version) + '/'

        # are set by _authorize_account
        self.api_url = None
        self.download_url = None
        self.authorization_token = None
        self.api_connection = None
        self.download_connection = None

        self.info_header_prefix = 'X-Bz-Info-'

    def _get_key_with_prefix(self, key):
        return self._b2_url_encode('%s%s' % (self.prefix, key))

    @staticmethod
    def _parse_storage_url(storage_url, ssl_context):
        '''Extract information from storage URL

        Return a tuple * (bucket_name, prefix) * .
        '''

        hit = re.match(r'^b2://([^/]+)(?:/(.*))?$', storage_url)

        if not hit:
            raise QuietError('Invalid storage URL', exitcode=2)

        bucket_name = hit.group(1)

        # Backblaze has bucket name restrictions:
        if not re.match(r'^(?!b2-)[a-z0-9A-Z\-]{6,50}$', bucket_name):
            raise QuietError('Invalid bucket name.', exitcode=2)

        prefix = hit.group(2) or ''
        if prefix != '':
            # add trailing slash if there is none
            prefix = prefix + '/' if prefix[-1] != '/' else prefix

        return (bucket_name, prefix)

    def _reset_authorization_values(self):
        self.api_url = None
        self.download_url = None
        self.authorization_token = None
        self._close_connections()

    def _close_connections(self):
        if self.api_connection:
            self.api_connection.disconnect()
            self.api_connection = None

        if self.download_connection:
            self.download_connection.disconnect()
            self.download_connection = None

    def _reset_connections(self):
        if self.api_connection:
            self.api_connection.reset()

        if self.download_connection:
            self.download_connection.reset()

    def _get_download_connection(self):
        if self.download_connection is None:
            self._authorize_account()

        return self.download_connection

    def _get_api_connection(self):
        if self.api_connection is None:
            self._authorize_account()

        return self.api_connection

    def _get_account_id(self):
        if self.account_id is None:
            self._authorize_account()

        return self.account_id

    @retry
    def _authorize_account(self):
        '''Authorize API calls'''

        authorize_host = 'api.backblazeb2.com'
        authorize_url = self.api_url_prefix + 'b2_authorize_account'

        id_and_key = self.b2_application_key_id + ':' + self.b2_application_key
        basic_auth_string = 'Basic ' + str(base64.b64encode(bytes(id_and_key, 'UTF-8')), encoding='UTF-8')

        with HTTPConnection(authorize_host, 443, ssl_context=self.ssl_context) as connection:

            headers = CaseInsensitiveDict()
            headers['Authorization'] = basic_auth_string

            connection.send_request('GET', authorize_url, headers=headers, body=None)
            response = connection.read_response()
            response_body = connection.readall()

            j = json.loads(response_body.decode('utf-8'))

            if self.account_id is None:
                self.account_id = j['accountId']

            self.api_url = urlparse(j['apiUrl'])
            self.download_url = urlparse(j['downloadUrl'])
            self.authorization_token = j['authorizationToken']

            self.api_connection = HTTPConnection(self.api_url.hostname, 443, ssl_context=self.ssl_context)
            self.api_connection.timeout = self.tcp_timeout

            self.download_connection = HTTPConnection(self.download_url.hostname, 443, ssl_context=self.ssl_context)
            self.download_connection.timeout = self.tcp_timeout

    def _do_request(self, connection, method, path, headers=None, body=None, download_body=True):
        '''Send request, read and return response object'''

        log.debug('started with %s %s', method, path)

        if headers is None:
            headers = CaseInsensitiveDict()

        if self.authorization_token is None:
            self._authorize_account()

        if 'Authorization' not in headers:
            headers['Authorization'] = self.authorization_token

        if self.test_mode_expire_some_tokens:
            headers['X-Bz-Test-Mode'] = 'expire_some_account_authorization_tokens'

        if self.test_mode_force_cap_exceeded:
            headers['X-Bz-Test-Mode'] = 'force_cap_exceeded'

        log.debug('REQUEST: %s %s %s %s', connection.hostname, method, path, headers)

        if body is None or isinstance(body, (bytes, bytearray, memoryview)):
            connection.send_request(method, path, headers=headers, body=body)
        else:
            body_length = os.fstat(body.fileno()).st_size
            connection.send_request(method, path, headers=headers, body=BodyFollowing(body_length))

            copyfileobj(body, connection, BUFSIZE)

        response = connection.read_response()

        if download_body is True or response.status != 200: # Backblaze always returns a json with error information in body
            response_body = connection.readall()
        else:
            response_body = None

        log.debug('RESPONSE: %s %s %s', response.status, response.headers, response_body)

        # File not found
        if response.status == 404 or (response.status != 200 and method == 'HEAD'):
            raise HTTPError(response.status, response.reason, response.headers)

        if response.status != 200:
            json_error_response = json.loads(response_body.decode('utf-8'))

            b2_error = B2Error(json_error_response['status'], json_error_response['code'], json_error_response['message'], response.headers)

            # Not authorized
            if (b2_error.code == 'bad_auth_token' or
                b2_error.code == 'expired_auth_token'):
                self._reset_authorization_values()

            raise b2_error

        return response, response_body

    def _do_api_call(self, api_call_name, data_dict):
        api_call_url_path = self.api_url_prefix + api_call_name
        body = json.dumps(data_dict).encode('utf-8')

        log.debug('API REQUEST: %s %s', api_call_name, json.dumps(data_dict, indent=2))

        response, body = self._do_request(self._get_api_connection(), 'POST', api_call_url_path, headers=None, body=body)

        json_response = json.loads(body.decode('utf-8'))

        log.debug('API RESPONSE: %s', json.dumps(json_response, indent=2))

        return json_response

    def _do_download_request(self, method, key):
        path = '/file/' + self.bucket_name + '/' + self._get_key_with_prefix(key)

        try:
            response, body = self._do_request(self._get_download_connection(), method, path, download_body=False)
        except HTTPError as exc:
            if exc.status == 404:
                raise NoSuchObject(key)
            else:
                raise

        return response

    def _do_upload_request(self, headers=None, body=None):
        upload_url_info = self._get_upload_url_info()
        headers['Authorization'] = upload_url_info['authorizationToken']

        if self.test_mode_fail_some_uploads:
            headers['X-Bz-Test-Mode'] = 'fail_some_uploads'

        upload_url_info['isUploading'] = True

        try:
            response, response_body = self._do_request(upload_url_info['connection'], 'POST', upload_url_info['path'], headers, body)
        except B2Error as exc:
            if exc.status == 503:
                # storage url too busy, change it
                self._invalidate_upload_url(upload_url_info)

            raise

        except (ConnectionClosed, ConnectionTimedOut):
            # storage url too busy, change it
            self._invalidate_upload_url(upload_url_info)
            raise

        except:
            upload_url_info['isUploading'] = False
            raise

        upload_url_info['isUploading'] = False

        json_response = json.loads(response_body.decode('utf-8'))

        log.debug('UPLOAD RESPONSE: %s', json.dumps(json_response, indent=2))

        return json_response

    @retry
    def _get_bucket_id(self):
        if not self.bucket_id:
            request_data = {
                'accountId': self._get_account_id(),
                'bucketName': self.bucket_name
            }
            response = self._do_api_call('b2_list_buckets', request_data)

            buckets = response['buckets']
            for bucket in buckets:
                if bucket['bucketName'] == self.bucket_name:
                    self.bucket_id = bucket['bucketId']

            if not self.bucket_id:
                raise DanglingStorageURLError(self.bucket_name)

        return self.bucket_id

    @property
    @copy_ancestor_docstring
    def has_native_rename(self):
        return False

    @property
    @copy_ancestor_docstring
    def has_delete_multi(self):
        return False

    @copy_ancestor_docstring
    def is_temp_failure(self, exc):
        if is_temp_network_error(exc) or isinstance(exc, ssl.SSLError):
            # We better reset our connections
            self._reset_connections()

        if is_temp_network_error(exc):
            return True

        elif (isinstance(exc, B2Error) and
              (exc.code == 'bad_auth_token' or
               exc.code == 'expired_auth_token')):
            return True

        elif (isinstance(exc, B2Error) and exc.code == 'cap_exceeded' and self.retry_on_cap_exceeded):
            return True

        elif (isinstance(exc, B2Error) and
              ((500 <= exc.status <= 599) or
               exc.status == 408 or
               exc.status == 429)):
            return True

        elif (isinstance(exc, HTTPError) and
              (exc.status == 408 or
               exc.status == 401)):
            return True

        # Consider all SSL errors as temporary. There are a lot of bug
        # reports from people where various SSL errors cause a crash
        # but are actually just temporary. On the other hand, we have
        # no information if this ever revealed a problem where retrying
        # was not the right choice.
        elif isinstance(exc, ssl.SSLError):
            return True

        return False

    @retry
    @copy_ancestor_docstring
    def lookup(self, key):
        log.debug('started with %s', key)

        try:
            response =  self._do_download_request('HEAD', key)
        except HTTPError as exc:
            if exc.status == 404:
                raise NoSuchObject(key)
            else:
                raise

        return self._extract_b2_metadata(response, key)

    @retry
    @copy_ancestor_docstring
    def get_size(self, key):
        log.debug('started with %s', key)

        try:
            response = self._do_download_request('HEAD', key)
        except HTTPError as exc:
            if exc.status == 404:
                raise NoSuchObject(key)
            else:
                raise

        try:
            return int(response.headers['Content-Length'])
        except KeyError:
            raise RuntimeError('HEAD request did not return Content-Length')

    @retry
    @copy_ancestor_docstring
    def open_read(self, key):
        try:
            response = self._do_download_request('GET', key)
        except NoSuchObject:
            raise

        try:
            metadata = self._extract_b2_metadata(response, key)
        except (BadDigestError, CorruptedObjectError):
            # If there's less than 64 kb of data, read and throw
            # away. Otherwise re-establish connection.
            if response.length is not None and response.length < 64 * 1024:
                self._get_download_connection().discard()
            else:
                self._close_connections()
            raise

        return ObjectR(key, response, self, metadata)

    @prepend_ancestor_docstring
    def open_write(self, key, metadata=None, is_compressed=False):
        '''
        The returned object will buffer all data and only start the uploads
        when its `close` method is called.
        '''

        log.debug('started with %s', key)

        headers = CaseInsensitiveDict()
        if metadata is None:
            metadata = dict()

        self._add_b2_metadata_to_headers(headers, metadata)

        return ObjectW(key, self, headers)

    @retry
    @copy_ancestor_docstring
    def delete(self, key, force=False, is_retry=False):
        log.debug('started with %s', key)

        if self.disable_versions:
            response =  self._do_download_request('HEAD', key)
            file_id = self._b2_url_decode(response.headers['X-Bz-File-Id'])
            file_name = self._b2_url_decode(response.headers['X-Bz-File-Name'])
            file_ids = [{
                'fileName': file_name.replace('\\', '=5C'),
                'fileId': file_id
            }]
        else:
            file_ids = self._list_file_versions(key)

        if not file_ids:
            raise NoSuchObject(key)

        for file_id in file_ids:
            try:
                self._delete_file_id(file_id['fileName'], file_id['fileId'])
            except B2Error as exc:
                if exc.code == 'file_not_present':
                    # Server may have deleted the object even though we did not
                    # receive the response.
                    if force or is_retry:
                        pass
                    else:
                        raise exc

    @retry
    def _delete_file_id(self, file_name, file_id):
        request_dict = {
            'fileName': file_name,
            'fileId': file_id
        }
        response = self._do_api_call('b2_delete_file_version', request_dict)
        return response

    def _list_file_versions(self, key):
        next_filename = self._get_key_with_prefix(key)
        next_file_id = None

        keys_remaining = True

        versions = []
        while keys_remaining and next_filename is not None:
            next_filename, next_file_id, versions_list = self._list_file_versions_page(next_filename, next_file_id)

            for version in versions_list:
                decoded_file_name = self._b2_url_decode(version['fileName'], decode_plus=False)
                if decoded_file_name == self.prefix + key:
                    versions.append({ 'fileName': version['fileName'], 'fileId': version['fileId']})
                else:
                    keys_remaining = False
                    break

        return versions

    @retry
    def _list_file_versions_page(self, next_filename=None, next_file_id=None):
        request_dict = {
            # maximum is 10000, but will get billed in steps of 1000
            'maxFileCount': 1000,
            'bucketId': self._get_bucket_id()
        }

        if next_filename is not None:
            request_dict['startFileName'] = next_filename

        if next_file_id is not None:
            request_dict['startFileId'] = next_file_id

        response = self._do_api_call('b2_list_file_versions', request_dict)
        file_versions_list = [ { 'fileName': file_version['fileName'], 'fileId': file_version['fileId'] } for file_version in response['files'] ]

        return response['nextFileName'], response['nextFileId'], file_versions_list

    @copy_ancestor_docstring
    def list(self, prefix=''):
        next_filename = self._get_key_with_prefix(prefix)
        keys_remaining = True

        while keys_remaining and next_filename is not None:
            next_filename, filelist = self._list_file_names_page(next_filename)

            for file_ in filelist:
                if file_.startswith(self.prefix + prefix):
                    # remove prefix before return
                    r = file_[len(self.prefix):]
                    decoded_r = self._b2_url_decode(r, decode_plus=False)
                    yield decoded_r
                else:
                    keys_remaining = False
                    break

    @retry
    def _list_file_names_page(self, next_filename=None):
        request_dict = {
            # maximum is 10000, but will get billed in steps of 1000
            'maxFileCount': 1000,
            'bucketId': self._get_bucket_id()
        }

        if next_filename is not None:
            request_dict['startFileName'] = next_filename

        response = self._do_api_call('b2_list_file_names', request_dict)
        filelist = [ file_['fileName'] for file_ in response['files'] ]

        return response['nextFileName'], filelist

    @retry
    @copy_ancestor_docstring
    def copy(self, src, dest, metadata=None):
        log.debug('started with %s, %s', src, dest)

        head_request_response = self._do_download_request('HEAD', src)
        source_file_id = head_request_response.headers['X-Bz-File-Id']

        request_dict = {
            'sourceFileId': source_file_id,
            'fileName': self.prefix + dest.replace('\\', '=5C') # self._get_key_with_prefix(dest)
        }

        if metadata is None:
            request_dict['metadataDirective'] = 'COPY'
        else:
            request_dict['metadataDirective'] = 'REPLACE'
            request_dict['contentType'] = 'application/octet-stream'

            fileInfo = self._create_metadata_dict(metadata)
            request_dict['fileInfo'] = fileInfo

        response = self._do_api_call('b2_copy_file', request_dict)

    @copy_ancestor_docstring
    def update_meta(self, key, metadata):
        # Backblaze has no API call to change existing metadata of an object,
        # we have to copy it, but it is done remotely
        self.copy(key, key, metadata)

    @copy_ancestor_docstring
    def close(self):
        self._reset_connections()

    def _get_upload_url_info(self):

        def find(f, seq):
            for item in seq:
                if f(item):
                    return item

        upload_url_info = find(lambda info: info['isUploading'] == False, self.available_upload_url_infos)

        if upload_url_info is None:
            upload_url_info = self._request_upload_url_info()
            self.available_upload_url_infos.append(upload_url_info)

        return upload_url_info

    @retry
    def _request_upload_url_info(self):
        request_data = {
            'bucketId': self._get_bucket_id()
        }
        response = self._do_api_call('b2_get_upload_url', request_data)

        new_upload_url = urlparse(response['uploadUrl'])
        new_authorization_token = response['authorizationToken']

        upload_connection = HTTPConnection(new_upload_url.hostname, 443, ssl_context=self.ssl_context)
        upload_connection.timeout = self.tcp_timeout

        return {
            'hostname': new_upload_url.hostname,
            'connection': upload_connection,
            'path': new_upload_url.path,
            'authorizationToken': new_authorization_token,
            'isUploading': False
        }

    def _invalidate_upload_url(self, upload_url_info):
        try:
            self.available_upload_url_infos.remove(upload_url_info)
        except ValueError:
            pass

    @staticmethod
    def _b2_url_encode(s):
        '''URL-encodes a unicode string to be sent to B2 in an HTTP header.
        '''

        s = str(s)
        encoded_s = urllib.parse.quote(s.encode('utf-8'), safe='/\\')
        encoded_s = encoded_s.replace('\\', '=5C')

        return encoded_s

    @staticmethod
    def _b2_url_decode(s, decode_plus=True):
        '''Decodes a Unicode string returned from B2 in an HTTP header.

        Returns a Python unicode string.
        '''

        # Use str() to make sure that the input to unquote is a str, not
        # unicode, which ensures that the result is a str, which allows
        # the decoding to work properly.

        s = str(s)
        decoded_s = s.replace('=5C', '\\')
        if decode_plus:
            decoded_s = urllib.parse.unquote_plus(decoded_s)
        else:
            decoded_s = urllib.parse.unquote(decoded_s)
        return decoded_s

    def _create_metadata_dict(self, metadata, chunksize=2048):
        metadata_dict = {}

        info_header_count = 0
        for key in metadata.keys():
            if not isinstance(key, str):
                raise ValueError('dict keys must be str, not %s' % type(key))

            value = metadata[key]
            if (not isinstance(value, (str, bytes, int, float, complex, bool))
                and value is not None):
                raise ValueError('value for key %s (%s) is not elementary' % (key, value))

            if isinstance(value, (bytes, bytearray)):
                value = base64.b64encode(value)

            buffer = ('%s: %s' % (repr(key), repr(value)))
            buffer = urllib.parse.quote(buffer, safe='!@#$^*()=+/?-_\'"><\\| `.,;:~')

            if len(buffer) < chunksize:
                metadata_dict['meta-%03d' % info_header_count] = buffer
                info_header_count += 1
            else:
                i = 0
                while i * chunksize < len(buffer):
                    k = 'meta-%03d' % info_header_count
                    v = buffer[i * chunksize : (i + 1) * chunksize]
                    metadata_dict[k] = v
                    i += 1
                    info_header_count += 1

        # Backblaze B2 only allows 10 metadata headers (2 are used by format and md5)
        assert info_header_count <= 8

        metadata_dict['meta-format'] = 'raw2'
        md5 = base64.b64encode(checksum_basic_mapping(metadata)).decode('ascii')
        metadata_dict['meta-md5'] = md5

        return metadata_dict

    def _add_b2_metadata_to_headers(self, headers, metadata, chunksize=2048):
        '''Add metadata to HTTP headers

        Theoretically there is no limit for the header size but we limit to
        2014 bytes just to be sure the receiving server will handle it.
        '''

        metadata_dict = self._create_metadata_dict(metadata)

        for key in metadata_dict.keys():
            header_key = '%s%s' % (self.info_header_prefix, key)
            encoded_value = self._b2_url_encode(metadata_dict[key])

            headers[header_key] = encoded_value

    def _extract_b2_metadata(self, response, obj_key):
        '''Extract metadata from HTTP response object'''

        headers = CaseInsensitiveDict()
        for k, v in response.headers.items():
            # we convert to lower case in order to do case-insensitive comparison
            if k.lower().startswith(self.info_header_prefix.lower() + 'meta-'):
                headers[k] = self._b2_url_decode(v)

        format_ = headers.get('%smeta-format' % self.info_header_prefix, 'raw')
        if format_ != 'raw2': # Current metadata format
            raise CorruptedObjectError('invalid metadata format: %s' % format_)

        parts = []
        for i in count():
            part = headers.get('%smeta-%03d' % (self.info_header_prefix, i), None)
            if part is None:
                break
            parts.append(part)

        buffer = urllib.parse.unquote(','.join(parts))
        meta = literal_eval('{ %s }' % buffer)

        # Decode bytes values
        for (k, v) in meta.items():
            if not isinstance(v, bytes):
                continue
            try:
                meta[k] = base64.b64decode(v)
            except binascii.Error:
                # This should trigger a MD5 mismatch below
                meta[k] = None

        # Check MD5. There is a case to be made for treating a mismatch as a
        # `CorruptedObjectError` rather than a `BadDigestError`, because the MD5
        # sum is not calculated on-the-fly by the server but stored with the
        # object, and therefore does not actually verify what the server has
        # sent over the wire. However, it seems more likely for the data to get
        # accidentally corrupted in transit than to get accidentally corrupted
        # on the server (which hopefully checksums its storage devices).
        md5 = base64.b64encode(checksum_basic_mapping(meta)).decode('ascii')
        if md5 != headers.get('%smeta-md5' % self.info_header_prefix, None):
            log.warning('MD5 mismatch in metadata for %s', obj_key)

            # When trying to read file system revision 23 or earlier, we will
            # get a MD5 error because the checksum was calculated
            # differently. In order to get a better error message, we special
            # case the s3ql_passphrase and s3ql_metadata object (which are only
            # retrieved once at program start).
            if obj_key in ('s3ql_passphrase', 's3ql_metadata'):
                raise CorruptedObjectError('Meta MD5 for %s does not match' % obj_key)
            raise BadDigestError(400, 'bad_digest', 'Meta MD5 for %s does not match' % obj_key)

        return meta

    def __str__(self):
        return 'b2://%s/%s' % (self.bucket_name, self.prefix)


def _parse_retry_after_header(header):
    try:
        value = int(header)
    except ValueError:
        value = 1
    return value


class HTTPError(Exception):
    '''Represents an HTTP error returned by Backblaze B2
    '''

    def __init__(self, status, message, headers=None):
        super().__init__()
        self.status = status
        self.message = message
        self.headers = headers

        if headers and 'Retry-After' in headers:
            self.retry_after = _parse_retry_after_header(headers['Retry-After'])
        else:
            self.retry_after = None

    def __str__(self):
        return '%d %s' % (self.status, self.message)