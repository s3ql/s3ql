'''
backends/gdrive.py - this file is part of S3QL.

Copyright © 2017 Isaac Aymerich <isaac.aymerich@gmail.com>

This work can be distributed under the terms of the GNU GPLv3.
'''


from ..logging import logging # Ensure use of custom logger class
from ..inherit_docstrings import (copy_ancestor_docstring,prepend_ancestor_docstring, ABCDocstMeta)
from .common import (AbstractBackend, DanglingStorageURLError, NoSuchObject,retry,retry_generator,AuthenticationError)
import apiclient.discovery
import apiclient.http
import time
import base64
import hashlib
import httplib2
import io
import json
import oauth2client.client
import tempfile

log = logging.getLogger(__name__)



class Backend(AbstractBackend, metaclass=ABCDocstMeta):
    '''
    A backend that stores data in Google Drive.
    '''

    known_options = set()

    MIME_TYPE_BINARY = 'application/octet-stream'
    MIME_TYPE_FOLDER = 'application/vnd.google-apps.folder'
    TOKEN_URI = 'https://accounts.google.com/o/oauth2/token'
    USER_AGENT = 'S3QL-Backend'

    MAX_PROPERTY_SIZE = 124
    MAX_REF_CHUNK_SIZE = 100
    VALUE_BASE64 = 'b64:'
    VALUE_REF = 'ref:'

    def __init__(self, storage_url, login, password, options):
        '''Initialize local backend
        Login and password are ignored.
        '''
        # Unused argument
        #pylint: disable=W0613

        super().__init__()
        self.login = login
        self.password = password
        client_secret_and_refresh_token = self.password.split(':')
        log.info(len(client_secret_and_refresh_token))
        if len(client_secret_and_refresh_token) != 2:
            raise AuthenticationError("Invalid Password format, must be  secretApplication:refreshToken")
        self.client_secret = client_secret_and_refresh_token[0]
        self.refresh_token = client_secret_and_refresh_token[1]
        self.options = options

        # get google drive service
        credentials = self._get_credentials()
        http = credentials.authorize(httplib2.Http())

        self.service = apiclient.discovery.build('drive', 'v3', http=http)

        # get gdrive folder
        folderPath = storage_url[len('gdrive://'):].rstrip('/')
        self.folder = self._lookup_file(folderPath)
        if self.folder != None:
            if self.folder['mimeType'] != Backend.MIME_TYPE_FOLDER:
                raise DanglingStorageURLError(folderPath)
        else:
            self.folder = self.service.files().create(body={'name' : folderPath,'mimeType' : Backend.MIME_TYPE_FOLDER},fields='id').execute()


    def _get_credentials(self):
        return oauth2client.client.OAuth2Credentials(access_token="1234",client_id=self.login,client_secret=self.client_secret,
        refresh_token=self.refresh_token,token_expiry="0000-00-00T00:00:00Z",token_uri=Backend.TOKEN_URI,user_agent=Backend.USER_AGENT)

    @copy_ancestor_docstring
    def is_temp_failure(self, exc): #IGNORE:W0613
        if isinstance(exc, apiclient.errors.HttpError):
            #handle only in case response are:
            if exc.resp.status == 403 and exc.resp.reason in ["Forbidden","userRateLimitExceeded","rateLimitExceeded","backendError"]:                
                return True
            #Google best practice error says error 500 could be temporal
            elif exc.resp.status == 500:
                return True
        elif isinstance(exc,(BrokenPipeError,ConnectionResetError)):
            return True
        return False

    @staticmethod
    def _escape_string(name):
        '''Escape string'''
        return name.replace("\\", "\\\\").replace("'", "\\'")

    def _list_files(self, folder, fields, query=None):
        '''List folder contents
        Arguments:
        folder -- Google Drive folder to list files in
        fields -- file properties to return
        query -- files.list query (optional)
        '''
        # build query
        if query is None:
            query = ''
        else:
            query += ' and '
        query += "'{0}' in parents".format(folder['id'])

        # iterate over results
        page_token = None
        while True:
            results = self.service.files().list(
                q=query, pageToken=page_token,pageSize=1000,
                fields="nextPageToken, files(%s)" % fields).execute()
            yield from results.get('files', [])

            # get next page
            page_token = results.get('nextPageToken', None)
            if page_token is None:
                break

    def _lookup_file(self, name, folder=None):
        '''Lookup file by name
        Arguments:
        name -- file name. May contain absolute or relative file name with '/' as path separator
        folder -- folder to start lookup from (root folder if omitted)
        '''
        if folder is None:
            folder = { 'id': 'root', 'path': '/', 'mimeType': Backend.MIME_TYPE_FOLDER }

        # find next path component
        if len(name) == 0:
            return folder
        if name[0] == '/':
            name = name[1:]
        n = name.split('/', 1)
        if len(n[0]) == 0:
            return folder

        # iterate over folder children
        query = "name = '{0}'".format(self._escape_string(n[0]))
        for f in self._list_files(folder, "id, name, mimeType, size,md5Checksum, properties", query):
            f['path'] = folder['path'] + f['name']
            if f['mimeType'] == Backend.MIME_TYPE_FOLDER:
                f['path'] += '/'

            log.debug("{0} ({1}: {2})".format(f['path'], f['id'], f['mimeType']))
            if len(n) == 1:
                return f
            elif f['mimeType'] == Backend.MIME_TYPE_FOLDER:
                return self._lookup_file(n[1], f)
            else:
                return None

        # nothing found
        return None

    @staticmethod
    def _chunk_property(k, i):
        '''Make chunk property'''
        return 'ref({0}.{1})'.format(k, i)

    def _encode_metadata(self, metadata):
        #
        # Google Drive properties are limited to 124 bytes for key+value
        #
        # 'bytes' metadata properties are base64-encoded and are assumed
        # the only properties that may not fit 124 bytes
        #
        # if 'bytes' property fits it is stored as 'b64:' + <base64 value>
        # otherwise it is stored as 'ref:' + <number of chunks>
        #
        # 'ref' data is stored in chunks (of MAX_REF_CHUNK_SIZE) in properties
        # with name 'ref(name.chunk-index)'
        if metadata is None:
            return None
        properties = dict()
        for k, v in metadata.items():
            if isinstance(v, bytes):
                b64 = base64.b64encode(v).decode()
                value = Backend.VALUE_BASE64 + b64
                if len(k+value) <= Backend.MAX_PROPERTY_SIZE:
                    properties[k] = value
                else:
                    # create chunked metadata property
                    chunks = [b64[i:i+Backend.MAX_REF_CHUNK_SIZE]
                        for i in range(0, len(b64), Backend.MAX_REF_CHUNK_SIZE)]
                    properties[k] = Backend.VALUE_REF + str(len(chunks))
                    for i, chunk in enumerate(chunks):
                        properties[self._chunk_property(k, i)] = chunk
            else:
                properties[k] = v
        return properties

    def _decode_metadata(self, f):
        metadata = dict()
        properties = f.get('properties', dict())
        for k, v in properties.items():
            if k.startswith('ref('):
                continue
            if v.startswith(Backend.VALUE_BASE64):
                metadata[k] = base64.b64decode(v[len(Backend.VALUE_BASE64):])
            elif v.startswith(Backend.VALUE_REF):
                b64 = ''
                for i in range(int(v[len(Backend.VALUE_REF):])):
                    b64 += properties[self._chunk_property(k, i)]
                metadata[k] = base64.b64decode(b64)
            elif v.isdigit():
                metadata[k] = int(v)
            else:
                metadata[k] = v
        log.debug('{0}: metadata {1}'.format(f['name'], metadata))
        return metadata

    @property
    @copy_ancestor_docstring
    def has_native_rename(self):
        return True

    def __str__(self):
        return 'gdrive folder %s@%s' % (self.login, self.prefix)

    @retry
    @copy_ancestor_docstring
    def lookup(self, key):
        log.debug("key: {0}".format(key))
        f = self._lookup_file(key, self.folder)
        if f is None:
            raise NoSuchObject(key)    
        return self._decode_metadata(f)

    @retry
    @copy_ancestor_docstring
    def get_size(self, key):
        log.debug("key: {0}".format(key))
        f = self._lookup_file(key, self.folder)
        if f is None:
            raise NoSuchObject(key)
        return int(f.get('size', u'0'))
    @retry
    @copy_ancestor_docstring
    def open_read(self, key):
        log.info("key: {0}".format(key))
        f = self._lookup_file(key, self.folder)
        if f is None:
            raise NoSuchObject(key)  
        return ObjectR(self.service, f, self._decode_metadata(f))

    @prepend_ancestor_docstring
    def open_write(self, key, metadata=None, is_compressed=False):
        log.debug("key: {0}".format(key))
        if metadata is None:
            metadata = dict()
        elif not isinstance(metadata, dict):
            raise TypeError('*metadata*: expected dict or None, got %s' % type(metadata))

        body = {
            'name': key,
            'mimeType': Backend.MIME_TYPE_BINARY,
            'parents': [ self.folder['id'] ],
            'properties': self._encode_metadata(metadata),
        }
        log.debug("metadata: {0}, body: {1}".format(metadata, body))
        return ObjectW(self.service,self, key,body)

    @prepend_ancestor_docstring
    def clear(self):
        log.debug("")
        for f in self._list_files(self.folder, "id"):
            self.service.files().delete(fileId=f['id']).execute()

        
    @retry
    @copy_ancestor_docstring
    def delete(self, key, force=False, is_retry=False):
        log.debug("key: {0}".format(key))
        query = "name = '{0}'".format(self._escape_string(key))
        found = False
        for f in self._list_files(self.folder, "id", query):
            found = True            
            self.service.files().delete(fileId=f['id']).execute()
        if not found:
            if force or is_retry:
                pass
            else:
                raise NoSuchObject(key)

    @retry_generator
    @copy_ancestor_docstring
    def list(self, prefix=''):
        log.debug("prefix: {0}".format(prefix))
        # Google Drive "contains" operator does prefix match for "name"
        query = "name contains '{0}'".format(self._escape_string(prefix))
        yield from map(lambda f: f['name'], self._list_files(self.folder, "name", query))

    @copy_ancestor_docstring
    def update_meta(self, key, metadata):
        log.debug("key: {0}, metadata: {1}".format(key, metadata))
        if not isinstance(metadata, dict):
            raise TypeError('*metadata*: expected dict, got %s' % type(metadata))
        self.copy(key, key, metadata)

    @retry
    @copy_ancestor_docstring
    def rename(self, src, dest, metadata=None,is_retry=False):
        if not (metadata is None or isinstance(metadata, dict)):
            raise TypeError('*metadata*: expected dict or None, got %s' % type(metadata))

        f = self._lookup_file(src, self.folder)
        if f is None:
            if is_retry:
                f = self._lookup_file(dest, self.folder)
                if f!=None:
                    return
            raise NoSuchObject(src)
        if metadata==None:
            metadata = self._decode_metadata(f)
        body = {
            'name': dest,
            'properties': self._encode_metadata(metadata),
        }
        log.debug("metadata: {0}, body: {1}".format(metadata, body))
        new_f = self.service.files().update(fileId=f['id'], body=body).execute()
        # delete other files with the same name
        query = "name = '{0}'".format(self._escape_string(new_f['name']))
        for other_f in self._list_files(self.folder, "id", query):
            if other_f['id'] == new_f['id']:
                continue
            self.service.files().delete(fileId=other_f['id']).execute()

    @retry
    @copy_ancestor_docstring
    def copy(self, src, dest, metadata=None):
        log.debug("{0} -> {1}".format(src, dest))
        if not (metadata is None or isinstance(metadata, dict)):
            raise TypeError('*metadata*: expected dict or None, got %s' % type(metadata))

        f = self._lookup_file(src, self.folder)
        if f is None:
            raise NoSuchObject(src)
        if src == dest:
            body = {
                'properties': self._encode_metadata(metadata),
            }
            log.debug("metadata update: {0}, body: {1}".format(metadata, body))
            self.service.files().update(fileId=f['id'], body=body).execute()
        else:
            if metadata==None:
                metadata = self._decode_metadata(f)
            body = {
                'name': dest,
                'mimeType': Backend.MIME_TYPE_BINARY,
                'parents': [ self.folder['id'] ],
                'properties': self._encode_metadata(metadata),
            }
            log.debug("metadata: {0}, body: {1}".format(metadata, body))
            new_f = self.service.files().copy(fileId=f['id'], body=body).execute()

            # delete other files with the same name
            query = "name = '{0}'".format(self._escape_string(new_f['name']))
            for other_f in self._list_files(self.folder, "id", query):
                if other_f['id'] == new_f['id']:
                    continue
                self.service.files().delete(fileId=other_f['id']).execute()


class ObjectR(object):
    '''A Google Drive object opened for reading'''

    def __init__(self, service, f, metadata):
        self.service = service
        self.f = f
        self.closed=False
        self.metadata = metadata
        self.md5_checked = False
        self.md5 = hashlib.md5()

        # check size - get_media fails for zero-sized files
        if int(f['size']) > 0:
            request = service.files().get_media(fileId=f['id'])
            self.buf = io.BytesIO()     # current read buffer
            self.buf.length = 0         # current read buffer length
            self.done = False
            self.download = apiclient.http.MediaIoBaseDownload(self.buf, request,chunksize=101*1024*1024)
            
        else:
            self.download = None

    def read(self, size=None):
        '''Read up to *size* bytes of object data
        For integrity checking to work, this method has to be called until
        it returns an empty string, indicating that all data has been read
        (and verified).
        '''

        if size == 0 or self.download is None:            
            return b''

        if self.buf.tell() >= self.buf.length:
            # try to read more data

            if self.done:
                self.md5_checked = True
                log.info(str(self.f))
                if self.f['md5Checksum']!=self.md5.hexdigest():
                    log.warning('MD5 mismatch for %s: %s vs %s',self.f['title'], self.f['md5Checksum'], self.md5.hexdigest())
                    raise BadDigestError('BadDigest', 'MD5 mismatch for %s (received: %s, sent: %s)' % (self.f['title'], self.f['md5Checksum'], self.md5.hexdigest()))
                return b''

            # read next chunk
            self.buf.truncate(0)
            self.buf.seek(0)
            download_progress, self.done = self.download.next_chunk()            
            self.buf.length = self.buf.tell()
            self.buf.seek(0)            

        readed=self.buf.read(size)
        self.md5.update(readed)
        return readed

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()
        return False

    def close(self, checksum_warning=True):
        if self.closed:
            return
        self.closed = True
        if not self.md5_checked:
            if checksum_warning:
                log.warning("Object closed prematurely, can't check MD5, and have to reset connection")
        

class ObjectW(object):
    '''A Google Drive object opened for writing'''

    def __init__(self, service,backend, key,metadata):
        self.service = service
        self.backend=backend
        self.key = key
        self.metadata = metadata
        self.retries = 0
        self.md5 = hashlib.md5()

        # According to http://docs.python.org/3/library/functions.html#open
        # the buffer size is typically ~8 kB. We process data in much
        # larger chunks, so buffering would only hurt performance.
        self.fh = tempfile.TemporaryFile(buffering=0)
        self.closed = False
        self.obj_size = 0

    def write(self, buf):
        '''Write object data'''

        self.fh.write(buf)
        self.md5.update(buf)
        self.obj_size += len(buf)

    def is_temp_failure(self, exc):
        return self.backend.is_temp_failure(exc)

    def rollbackUpload(self,message):
            # delete may fail, but we don't want to loose the BadDigest exception
        try:
            self.backend.delete(self.key)
        finally:
            raise BadDigestError('BadDigest', message) 

    @retry
    def close(self):
        if self.closed:
            # still call fh.close, may have generated an error before
            self.fh.close()
            return
        
        self.fh.seek(0)
        binaryData = apiclient.http.MediaIoBaseUpload(self.fh, Backend.MIME_TYPE_BINARY, resumable=True,chunksize=101*1024*1024)        
        request = self.service.files().create(media_body=binaryData,body=self.metadata,fields='id,md5Checksum,name,properties')
        response = None
        while response is None:          
            status, response = request.next_chunk()       

        if len(self.metadata['properties']) != len(response['properties']):
            self.rollbackUpload('metadata is invalid %s (received: %s, sent: %s)' % (self.key, response['properties'], self.metadata['properties'])) 
        if response['md5Checksum'] != self.md5.hexdigest():
            self.rollbackUpload('MD5 mismatch for %s (received: %s, sent: %s)' % (self.key, checksumMD5, self.md5.hexdigest()))            
        # close underlying file
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


class GDriveError(Exception):
    '''
    Represents an error returned by Gdrive. For possible codes, see
    https://developers.google.com/drive/v3/web/handle-errors
    '''

    def __init__(self, code, msg):
        super().__init__(msg)
        self.code = code
        self.msg = msg
    def __str__(self):
        return '%s: %s' % (self.code, self.msg)


class BadDigestError(GDriveError): pass