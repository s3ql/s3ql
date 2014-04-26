'''
oauth_client.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from .logging import logging, setup_logging, QuietError
from .parse_args import ArgumentParser
import sys
import textwrap
import requests
import time

log = logging.getLogger(__name__)

# S3QL client id and client secret for Google APIs.
# Don't get your hopes up, this isn't truly secret.
CLIENT_ID = '381875429714-6pch5vnnmqab454c68pkt8ugm86ef95v.apps.googleusercontent.com'
CLIENT_SECRET = 'HGl8fJeVML-gZ-1HSZRNZPz_'

def parse_args(args):
    '''Parse command line'''

    parser = ArgumentParser(
        description=textwrap.dedent('''\
        Obtain OAuth2 refresh token for Google Storage
        '''))

    parser.add_debug_modules()
    parser.add_quiet()
    parser.add_version()

    return parser.parse_args(args)

def _log_response(r):
    '''Log server response'''

    if not log.isEnabledFor(logging.DEBUG):
        return

    s = [ 'Server response:',
          '%03d %s' % (r.status_code, r.reason) ]
    for tup in r.headers.items():
        s.append('%s: %s' % tup)

    s.append('')
    s.append(r.text)
    
    log.debug('\n'.join(s))

def _parse_response(r):
    
    _log_response(r)
    if r.status_code != requests.codes.ok:
        raise QuietError('Connection failed with: %d %s'
                         % (r.status_code, r.reason))

    return r.json()
    
def main(args=None):

    if args is None:
        args = sys.argv[1:]

    options = parse_args(args)
    setup_logging(options)

    cli = requests.Session()
    
    r = cli.post('https://accounts.google.com/o/oauth2/device/code',
                 data={ 'client_id': CLIENT_ID,
                        'scope': 'https://www.googleapis.com/auth/devstorage.read_write' },
                 verify=True, allow_redirects=False, timeout=20)
    req_json = _parse_response(r)
    
    print(textwrap.fill('Please open %s in your browser and enter the following '
                        'user code: %s' % (req_json['verification_url'],
                                           req_json['user_code'])))

    while True:
        log.debug('polling..')
        time.sleep(req_json['interval'])

        r = cli.post('https://accounts.google.com/o/oauth2/token',
                     data={ 'client_id': CLIENT_ID,
                            'client_secret': CLIENT_SECRET,
                            'code': req_json['device_code'],
                            'grant_type': 'http://oauth.net/grant_type/device/1.0' },
                     verify=True, allow_redirects=False, timeout=20)
        resp_json = _parse_response(r)
        r.close()
        
        if 'error' in resp_json:
            if resp_json['error'] == 'authorization_pending':
                continue
            else:
                raise QuietError('Authentication failed: ' + resp_json['error'])
        else:
            break
        
    print('Success. Your refresh token is:\n',
          resp_json['refresh_token'])
    
