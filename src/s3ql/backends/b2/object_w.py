import tempfile
import hashlib

from ...logging import logging
from ..common import retry

from .b2_error import BadDigestError

log = logging.getLogger(__name__)

class ObjectW(object):
    '''A Backblaze B2 object open for writing

    All data is first cached in memory, upload only starts when
    the close() method is called.
    '''

    def __init__(self, key, backend, headers):
        self.key = key
        self.backend = backend
        self.headers = headers
        self.closed = False
        self.obj_size = 0

        # According to http://docs.python.org/3/library/functions.html#open
        # the buffer size is typically ~8 kB. We process data in much
        # larger chunks, so buffering would only hurt performance.
        self.fh = tempfile.TemporaryFile(buffering=0)

        # Backblaze uses sha1 hashes
        self.sha1 = hashlib.sha1()

    def write(self, buf):
        '''Write object data'''

        self.fh.write(buf)
        self.obj_size += len(buf)
        self.sha1.update(buf)

    def is_temp_failure(self, exc):
        return self.backend.is_temp_failure(exc)

    @retry
    def close(self):
        '''Close object and upload data'''

        log.debug('started with %s', self.key)

        if self.closed:
            # still call fh.close, may have generated an error before
            self.fh.close()
            return

        self.fh.seek(0)

        key_with_prefix = self.backend._get_key_with_prefix(self.key)

        self.headers['X-Bz-File-Name'] = self.backend._b2_url_encode(key_with_prefix)
        self.headers['Content-Type'] = 'application/octet-stream'
        self.headers['Content-Length'] = self.obj_size
        self.headers['X-Bz-Content-Sha1'] = self.sha1.hexdigest()

        try:
            response = self.backend._do_upload_request(self.headers, self.fh)
        except BadDigestError:
            # delete may fail, but we don't want to loose the BadDigest exception
            try:
                self.backend.delete(self.key)
            finally:
                raise BadDigestError(400, 'bad_digest', 'Sha1 mismatch for %s' % self.key)

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
