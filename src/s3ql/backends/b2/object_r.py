import hashlib

from ...logging import logging

from .b2_error import BadDigestError

log = logging.getLogger(__name__)

class ObjectR(object):
    '''A Backblaze B2 object open for reading'''

    def __init__(self, key, response, backend, metadata=None):
        self.key = key
        self.response = response
        self.backend = backend
        self.metadata = metadata
        self.sha1_checked = False
        self.closed = False

        self.sha1 = hashlib.sha1()

    def read(self, size=None):
        '''Read up to *size* bytes of object data

        For integrity checking to work, this method has to be called until
        it returns an empty string, indicating that all data has been read
        (and verified)
        '''

        if size == 0:
            return b''

        connection = self.backend._get_download_connection()

        # This may raise an exception, in which case we probably can't
        # re-use the connection. However, we rely on the caller
        # to still close the file-like object, so that we can do
        # cleanup in close().
        buffer = connection.read(size)
        self.sha1.update(buffer)

        # Check SHA1 on EOF
        # (size == None implies EOF)
        if (not buffer or size is None) and not self.sha1_checked:
            remote_sha1 = self.response.headers['X-Bz-Content-Sha1'].strip('"')
            self.sha1_checked = True
            if remote_sha1 != self.sha1.hexdigest():
                log.warning('SHA1 mismatch for %s: %s vs %s', self.key, remote_sha1, self.sha1.hexdigest())
                raise BadDigestError(400, 'bad_digest', 'SHA1 header does not agree with calculated SHA1')

        return buffer

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()
        return False

    def close(self, checksum_warning=True):
        '''Close object

        If *checksum_warning* is true, this will generate a warning message if
        the object has not been fully read (because in that case the SHA1
        checksum cannot be checked).
        '''

        if self.closed:
            return
        self.closed = True

        # If we have not read all the data, close the entire
        # connection (otherwise we loose synchronization)
        if not self.sha1_checked:
            if checksum_warning:
                log.warning('Object closed prematurely, can\'t check SHA1, and have to reset connection')

            self.backend._close_connections()
