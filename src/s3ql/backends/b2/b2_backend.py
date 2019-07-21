import base64
from dugong import (HTTPConnection, CaseInsensitiveDict, is_temp_network_error, BodyFollowing,
                    ConnectionClosed)
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
from ...inherit_docstrings import (ABCDocstMeta, copy_ancestor_docstring)
from ...logging import logging, QuietError
from ... import BUFSIZE
from .object_r import ObjectR
from .object_w import ObjectW

log = logging.getLogger(__name__)

class B2Backend(AbstractBackend, metaclass=ABCDocstMeta):
    """A backend to store data in backblaze b2 cloud storage.
    """

    def __init__(self, options):
        '''Initialize backend object
        '''

        super().__init__()

        self.ssl_context = get_ssl_context(options.backend_options.get('ssl-ca-path', None))

        self.options = options.backend_options

        self.b2_application_key_id = options.backend_login
        self.b2_application_key = options.backend_password

        self.account_id = None

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

        self.upload_url = None
        self.upload_authorization_token = None
        self.upload_connection = None

        self.info_header_prefix = 'X-Bz-Info-'

    def _get_key_with_prefix(self, key):
        return self._b2_url_encode('%s%s' % (self.prefix, key))

    @staticmethod
    def _parse_storage_url(storage_url, ssl_context):
        '''Extract information from storage URL

        Return a tuple * (host, port, bucket_name, prefix) * .
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
        self.api_connection.reset()
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

    def _authorize_account(self):
        """Authorize API calls
        """

        authorize_host = 'api.backblazeb2.com'
        authorize_url = self.api_url_prefix + 'b2_authorize_account'

        id_and_key = self.b2_application_key_id + ':' + self.b2_application_key
        basic_auth_string = 'Basic ' + str(base64.b64encode(bytes(id_and_key, 'UTF-8')), encoding = 'UTF-8')

        with HTTPConnection(authorize_host, 443, ssl_context = self.ssl_context) as connection:

            headers = CaseInsensitiveDict()
            headers['Authorization'] = basic_auth_string

            connection.send_request('GET', authorize_url, headers = headers, body = None)
            response = connection.read_response()
            response_body = connection.readall()

            j = json.loads(response_body.decode('utf-8'))

            self.account_id = j['accountId']
            self.api_url = urlparse(j['apiUrl'])
            self.download_url = urlparse(j['downloadUrl'])
            self.authorization_token = j['authorizationToken']

            self.api_connection = HTTPConnection(self.api_url.hostname, 443, ssl_context = self.ssl_context)
            self.download_connection = HTTPConnection(self.download_url.hostname, 443, ssl_context = self.ssl_context)

    def _do_request(self, connection, method, path, headers = None, body = None, download_body = True):
        '''Send request, read and return response object'''

        log.debug('started with %s %s', method, path)

        if headers is None:
            headers = CaseInsensitiveDict()

        if self.authorization_token is None:
            self._authorize_account()

        if 'Authorization' not in headers:
            headers['Authorization'] = self.authorization_token

        print('\nREQ:', connection.hostname, method, path, headers)

        if body is None or isinstance(body, (bytes, bytearray, memoryview)):
            connection.send_request(method, path, headers = headers, body = body)
        else:
            body_length = os.fstat(body.fileno()).st_size
            connection.send_request(method, path, headers = headers, body = BodyFollowing(body_length))

            try:
                copyfileobj(body, connection, BUFSIZE)
            except ConnectionClosed:
                # Server closed connection while w were writing body data -
                # but we may still be able to read an error response
                try:
                    response = connection.read_response()
                except ConnectionClosed: # No server response available
                    pass
                else:
                    if response.status >= 400: # Got error response
                        return response
                    log.warning('Server broke connection during upload, but signaled '
                                '%d %s', response.status, response.reason)

                # Re-raise first ConnectionClosed exception
                raise

        response = connection.read_response()

        if download_body is True or response.status != 200:
            response_body = connection.readall()
        else:
            response_body = None

        print('\nRES:', response.status, response.headers)

        # Not authorized
        if response.status == 401:
            self._reset_authorization_values()

        # File not found
        if response.status == 404:
            pass

        # TODO       if isinstance(body, (bytes, bytearray, memoryview)):
        #            headers['Content-MD5'] = md5sum_b64(body)

        # TODO ERROR HANDLING

        #return response
        return response, response_body

    def _do_api_call(self, api_call_name, data_dict):
        api_call_url_path = self.api_url_prefix + api_call_name
        body = json.dumps(data_dict).encode('utf-8')

        print('\nAPI REQ:', api_call_name, '\n', json.dumps(data_dict, indent = 2))

        request_response, request_body = self._do_request(self._get_api_connection(), 'POST', api_call_url_path, headers = None, body = body)

        response = json.loads(request_body.decode('utf-8'))

        print('\nAPI RES:', json.dumps(response, indent = 2))

        return response

    def _do_download_request(self, method, key):
        path = '/file/' + self.bucket_name + '/' + self._get_key_with_prefix(key)

        try:
            request_response, request_body = self._do_request(self._get_download_connection(), method, path, download_body = False)
        except NoSuchObject:
            raise

        # TODO -- check if everything is right this way
        return request_response

    def _do_upload_request(self, headers = None, body = None):
        connection = self._get_upload_connection()

        headers['Authorization'] = self.upload_authorization_token

        response = self._do_request(connection, 'POST', self.upload_url.path, headers, body)
        return response

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

        elif (isinstance(exc, B2Error) and ((500 <= exc.status <= 599) or exc.status == 408)):
            return True

        # lots of clients reported SSL problems, ignoring them here # TODO
        if isinstance(exc, ssl.SSLError):
            return True

        # TODO

        return False

    @retry
    @copy_ancestor_docstring
    def lookup(self, key: str):
        log.debug('started with %s', key)

        try:
            response =  self._do_download_request('HEAD', key)
        except HTTPError as exc:
            if exc.status == 404:
                raise NoSuchObject(key)
            else:
                raise

        return self._extract_b2_metadata(response, key)
        # TODO

    @retry
    @copy_ancestor_docstring
    def get_size(self, key: str) -> int:
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
        # TODO

    @retry
    @copy_ancestor_docstring
    def open_read(self, key: str):
        try:
            response = self._do_download_request('GET', key)
        except NoSuchKeyError:
            raise NoSuchObject(key)

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

    @copy_ancestor_docstring
    def open_write(self, key: str, metadata = None, is_compressed = False):
        log.debug('started with %s', key)

        headers = CaseInsensitiveDict()
        if metadata is None:
            metadata = dict()

        self._add_b2_metadata_to_headers(headers, metadata)

        return ObjectW(key, self, headers)

    @copy_ancestor_docstring
    def delete(self, key: str, force = False):
        log.debug('started with %s', key)

        file_ids = self._list_file_versions(key)
        for file_id in file_ids:
            self._delete_file_id(file_id['fileName'], file_id['fileId'])

    def _delete_file_id(self, file_name, file_id):
        request_dict = {
            'fileName': file_name,
            'fileId': file_id
        }
        response = self._do_api_call('b2_delete_file_version', request_dict)
        return response

    @copy_ancestor_docstring
    def list(self, prefix = ''):
        next_filename = self._get_key_with_prefix(prefix)
        keys_remaining = True

        while keys_remaining and next_filename is not None:
            next_filename, filelist = self._list_file_names_page(next_filename)

            for file_ in filelist:
                if file_.startswith(self.prefix + prefix):
                    # remove prefix before return
                    r = file_[len(self.prefix):]
                    decoded_r = self._b2_url_decode(r)
                    yield decoded_r
                else:
                    keys_remaining = False
                    break

    @retry
    def _list_file_names_page(self, next_filename = None):
        request_dict: Dict[str, Union[str, int]] = {
            'maxFileCount': 1000, # maximum is 10000, but will get billed in steps of 1000
            'bucketId': self._get_bucket_id()
        }
        if next_filename is not None:
            request_dict['startFileName'] = next_filename

        response = self._do_api_call('b2_list_file_names', request_dict)
        filelist = [ file_['fileName'] for file_ in response['files'] ]

        return response['nextFileName'], filelist

    def _list_file_versions(self, key):
        next_filename = self._get_key_with_prefix(key)
        next_file_id = None

        next_filename, next_file_id, versions_list = self._list_file_versions_page(next_filename, next_file_id)

        versions = []
        for version in versions_list:
            decoded_file_name = self._b2_url_decode(version['fileName'])
            if decoded_file_name == self.prefix + key:
                versions.append({ 'fileName': version['fileName'], 'fileId': version['fileId']})

        return versions

    def _list_file_versions_page(self, next_filename = None, next_file_id = None):
        request_dict = {
            'maxFileCount': 1000, # maximum is 10000, but will get billed in steps of 1000
            'bucketId': self._get_bucket_id()
        }
        if next_filename is not None:
            request_dict['startFileName'] = next_filename
        if next_file_id is not None:
            request_dict['startFileId'] = next_file_id

        response = self._do_api_call('b2_list_file_versions', request_dict)
        file_versions_list = [ { 'fileName': file_version['fileName'], 'fileId': file_version['fileId'] } for file_version in response['files'] ]

        return response['nextFileName'], response['nextFileId'], file_versions_list

    @retry
    @copy_ancestor_docstring
    def copy(self, src, dest, metadata = None):
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

            fileInfo = CaseInsensitiveDict()
            self._add_b2_metadata_to_headers(headers, fileInfo)
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

    def _get_upload_connection(self):
        if self.upload_connection is None:
            self._get_upload_url()

        return self.upload_connection

    def _get_upload_url(self):
        if self.upload_url == None:
            request_data = {
                'bucketId': self._get_bucket_id()
            }
            response = self._do_api_call('b2_get_upload_url', request_data)

            self.upload_url = urlparse(response['uploadUrl'])
            self.upload_authorization_token = response['authorizationToken']
            self.upload_connection = HTTPConnection(self.upload_url.hostname, 443, ssl_context = self.ssl_context)

        return self.upload_authorization_token, self.upload_url

    @staticmethod
    def _b2_url_encode(s):
        """URL-encodes a unicode string to be sent to B2 in an HTTP header.
        """

        #return urllib.parse.quote(s.encode('utf-8'))
        s = str(s)
        encoded_s = urllib.parse.quote(s.encode('utf-8'), safe = '/\\')
        encoded_s = encoded_s.replace('\\', '=5C')

        return encoded_s

    @staticmethod
    def _b2_url_decode(s):
        """Decodes a Unicode string returned from B2 in an HTTP header.

        Returns a Python unicode string.
        """

        # Use str() to make sure that the input to unquote is a str, not
        # unicode, which ensures that the result is a str, which allows
        # the decoding to work properly.

        s = str(s)
        decoded_s = s.replace('=5C', '\\')
        decoded_s = urllib.parse.unquote_plus(decoded_s)
        return decoded_s

    def _add_b2_metadata_to_headers(self, headers, metadata, chunksize = 2048):
        '''Add metadata to HTTP headers

        Theoretically there is no limit for the header size but we limit to
        2014 bytes just to be sure the receiving server will handle it.
        '''

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
            buffer = urllib.parse.quote(buffer, safe = '!@#$^*()=+/?-_\'"><\\| `.,;:~')

            if len(buffer) < chunksize:
                encoded_buffer = self._b2_url_encode(buffer)
                headers['%smeta-%03d' % (self.info_header_prefix, info_header_count)] = encoded_buffer
                info_header_count += 1
            else:
                i = 0
                while i * chunksize < len(buffer):
                    k = '%smeta-%03d' % (self.info_header_prefix, info_header_count)
                    v = buffer[i * chunksize : (i + 1) * chunksize]
                    encoded_v = self._b2_url_encode(v)
                    headers[k] = encoded_v
                    i += 1
                    info_header_count += 1

        assert info_header_count <= 10 # TODO count correctly!
        headers[self.info_header_prefix + 'meta-format'] = 'raw2'
        md5 = base64.b64encode(checksum_basic_mapping(metadata)).decode('ascii')
        headers[self.info_header_prefix + 'meta-md5'] = md5

    def _extract_b2_metadata(self, response, obj_key):
        '''Extract metadata from HTTP response object'''

        format_ = response.headers.get('%smeta-format' % self.info_header_prefix, 'raw')
        if format_ != 'raw2': # Current metadata format
            raise CorruptedObjectError('invalid metadata format: %s' % format_)

        parts = []
        for i in count():
            part = response.headers.get('%smeta-%03d' % (self.info_header_prefix, i), None)
            if part is None:
                break
            parts.append(self._b2_url_decode(part))

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
        if md5 != response.headers.get('%smeta-md5' % self.info_header_prefix, None):
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
        # TODO

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

    def __init__(self, status, message, headers = None):
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


class B2Error(Exception):
    '''
    Represents an error returned by Backblaze B2 API call

    For possible codes, see https://www.backblaze.com/b2/docs/calling.html
    '''

    def __init__(self, status, code, message, headers = None):
        super.__init__(message)
        self.status = status
        self.code = code
        self.message = message

        if headers and 'Retry-After' in headers:
            self.retry_after = _parse_retry_after_header(headers['Retry-After'])
        else:
            # Force 1s waiting time before retry
            self.retry_after = 1

    def __str__(self):
        return '%s : %s - %s' % (self.status, self.code, self.message)


class BadDigestError(B2Error): pass
