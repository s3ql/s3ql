class B2Error(Exception):
    '''
    Represents an error returned by Backblaze B2 API call

    For possible codes, see https://www.backblaze.com/b2/docs/calling.html
    '''

    def __init__(self, status, code, message, headers=None):
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message
        self.headers = headers

        if headers and 'Retry-After' in headers:
            self.retry_after = _parse_retry_after_header(headers['Retry-After'])
        else:
            # Force 1s waiting time before retry
            self.retry_after = 1

    def __str__(self):
        return '%s : %s - %s' % (self.status, self.code, self.message)


class BadDigestError(B2Error): pass

def _parse_retry_after_header(header):
    try:
        value = int(header)
    except ValueError:
        value = 1
    return value
