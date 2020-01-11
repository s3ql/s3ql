from ..s3c import HTTPError

class B2Error(HTTPError):
    '''
    Represents an error returned by Backblaze B2 API call

    For possible codes, see https://www.backblaze.com/b2/docs/calling.html
    '''

    def __init__(self, status, code, message, headers=None):
        super().__init__(status, message, headers)
        self.code = code

        # Force 1s waiting time before retry
        if not self.retry_after:
            self.retry_after = 1

    def __str__(self):
        return '%s : %s - %s' % (self.status, self.code, self.msg)


class BadDigestError(B2Error): pass
