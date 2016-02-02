'''
gdrive.py - this file is part of S3QL.

Copyright © 2016 Max Khon <fjoe@samodelkin.net>

This work can be distributed under the terms of the GNU GPLv3.
'''

from ..logging import logging # Ensure use of custom logger class
from ..inherit_docstrings import (copy_ancestor_docstring, ABCDocstMeta)
from .common import (AbstractBackend, DanglingStorageURLError, NoSuchObject)
import apiclient
import base64
import httplib2
import io
import json
import oauth2client.client

log = logging.getLogger(__name__)

class Backend(AbstractBackend, metaclass=ABCDocstMeta):
    '''
    A backend that stores data in Google Drive.
    '''

    known_options = set()

    MIME_TYPE_BINARY = 'application/octet-stream'
    MIME_TYPE_FOLDER = 'application/vnd.google-apps.folder'

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
        self.options = options

        # get google drive service
        credentials = self._get_credentials()
        http = credentials.authorize(httplib2.Http())
        self.service = apiclient.discovery.build('drive', 'v3', http=http)

        # get gdrive folder
        path = storage_url[len('gdrive://'):].rstrip('/')
        self.folder = self._lookup_file(path)
        if self.folder is None or self.folder['mimeType'] != Backend.MIME_TYPE_FOLDER:
            raise DanglingStorageURLError(self.path)

    def _get_credentials(self):
        credentials_filename = oauth2client.client._get_environment_variable_file()
        if not credentials_filename:
            credentials_filename = oauth2client.client._get_well_known_file()
        if not credentials_filename:
            return None
        with open(credentials_filename) as f:
            creds = json.load(f)

        credentials = oauth2client.client.SignedJwtAssertionCredentials(
            creds['client_email'],
            creds['private_key'],
            'https://www.googleapis.com/auth/drive',
            sub=self.login)
        return credentials

    def _list_files(self, folder, fields):
        # list folder children
        page_token = None
        while True:
            results = self.service.files().list(
                q="'{0}' in parents".format(folder['id']),
                pageToken=page_token,
                fields="nextPageToken, files(%s)" % fields).execute()
            for f in results.get('files', []):
                yield f

            # get next page
            page_token = results.get('nextPageToken', None)
            if page_token is None:
                break

    def _lookup_file(self, name, folder=None):
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
        for f in self._list_files(folder, "id, name, mimeType, size, properties"):
            f['path'] = folder['path'] + f['name']
            if f['mimeType'] == Backend.MIME_TYPE_FOLDER:
                f['path'] += '/'

            #log.debug("{0} ({1}: {2})".format(f['path'], f['id'], f['mimeType']))
            if f['name'] != n[0]:
                continue

            # found matching child
            if len(n) == 1:
                return f
            elif f['mimeType'] == Backend.MIME_TYPE_FOLDER:
                return self._lookup_file(n[1], f)
            else:
                return None

        # nothing found
        return None

    def _create_file(self, body):
        return self.service.files().create(body=body).execute()

    def _copy_file(self, f, body):
        return self.service.files().copy(fileId=f['id'], body=body).execute()

    def _delete_file(self, f):
        self.service.files().delete(fileId=f['id']).execute()

    def _delete_other(self, f):
        for _f in self._list_files(self.folder, "id, name"):
            if _f['id'] == f['id'] or _f['name'] != f['name']:
                continue
            self._delete_file(_f)

    @staticmethod
    def _chunk_property(k, i):
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
        return False

    def __str__(self):
        return 'gdrive folder %s@%s' % (self.login, self.prefix)

    @copy_ancestor_docstring
    def is_temp_failure(self, exc): #IGNORE:W0613
        return False

    @copy_ancestor_docstring
    def lookup(self, key):
        log.debug("lookup {0}".format(key))
        f = self._lookup_file(key, self.folder)
        if f is None:
            raise NoSuchObject(key)
        return self._decode_metadata(f)

    @copy_ancestor_docstring
    def get_size(self, key):
        log.debug("get_size {0}".format(key))
        f = self._lookup_file(key, self.folder)
        if f is None:
            raise NoSuchObject(key)
        return int(f.get('size', u'0'))

    @copy_ancestor_docstring
    def open_read(self, key):
        log.debug("open_read {0}".format(key))
        f = self._lookup_file(key, self.folder)
        if f is None:
            raise NoSuchObject(key)
        return ObjectR(self.service, f, self._decode_metadata(f))

    @copy_ancestor_docstring
    def open_write(self, key, metadata=None, is_compressed=False):
        log.debug("open_write {0}".format(key))
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
        f = self._create_file(body)
        self._delete_other(f)
        return ObjectW(self.service, f)

    @copy_ancestor_docstring
    def clear(self):
        log.debug("clear")
        for f in self._list_files(self.folder, "id"):
            self._delete_file(f)

    @copy_ancestor_docstring
    def contains(self, key):
        log.debug("contains {0}".format(key))
        f = self._lookup_file(key, self.folder)
        return f is not None

    @copy_ancestor_docstring
    def delete(self, key, force=False):
        log.debug("delete {0}".format(key))
        f = self._lookup_file(key, self.folder)
        if f is not None:
            self._delete_file(f)
        elif not force:
            raise NoSuchObject(key)

    @copy_ancestor_docstring
    def list(self, prefix=''):
        log.debug("list {0}".format(prefix))
        for f in self._list_files(self.folder, "id, name"):
            name = f['name']
            if not prefix or name.startswith(prefix):
                yield name

    @copy_ancestor_docstring
    def update_meta(self, key, metadata):
        log.debug("update_meta {0}: {1}".format(key, metadata))
        if not isinstance(metadata, dict):
            raise TypeError('*metadata*: expected dict, got %s' % type(metadata))
        self.copy(key, key, metadata)

    @copy_ancestor_docstring
    def copy(self, src, dest, metadata=None):
        log.debug("copy {0} -> {1}: {2}".format(src, dest, metadata))
        if not (metadata is None or isinstance(metadata, dict)):
            raise TypeError('*metadata*: expected dict or None, got %s' % type(metadata))

        f = self._lookup_file(src, self.folder)
        if f is None:
            raise NoSuchObject(src)
        body = {
            'name': dest,
            'mimeType': Backend.MIME_TYPE_BINARY,
            'parents': [ self.folder['id'] ],
            'properties': self._encode_metadata(metadata),
        }
        log.debug("metadata: {0}, body: {1}".format(metadata, body))
        new_f = self._copy_file(f, body)
        if src == dest:
            self._delete_other(new_f)

class ObjectR(io.BytesIO):
    '''A Google Drive object opened for reading'''

    def __init__(self, service, f, metadata):
        super().__init__()
        self.metadata = metadata
        request = service.files().get_media(fileId=f['id'])
        if int(f['size']) > 0:
            d = apiclient.http.MediaIoBaseDownload(self, request)
            while True:
                download_progress, done = d.next_chunk()
                if download_progress:
                    log.debug('Download Progress: %d%%' % int(download_progress.progress() * 100))
                if done:
                    break
        self.seek(0)

    def close(self, checksum_warning=True):
        '''Close object

        The *checksum_warning* parameter is ignored.
        '''
        super().close()

class ObjectW(io.BytesIO):
    '''A Google Drive object opened for writing'''

    def __init__(self, service, f):
        super().__init__()
        self.service = service
        self.f = f

    def close(self):
        self.seek(0)
        u = apiclient.http.MediaIoBaseUpload(self, Backend.MIME_TYPE_BINARY, resumable=True)
        self.service.files().update(fileId=self.f['id'], media_body=u).execute()

# vi: ts=4:sw=4:et:
