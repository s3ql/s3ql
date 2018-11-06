'''
oauth_client.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

from .logging import logging, setup_logging, QuietError
from .parse_args import ArgumentParser

from oauth2client.client import OAuth2WebServerFlow
from oauth2client.client import AccessTokenCredentials
from  oauth2client.client import OOB_CALLBACK_URN
from .common import OAUTH_CLIENT_ID, OAUTH_CLIENT_SECRET

import sys
import textwrap
import requests
import time

log = logging.getLogger(__name__)


def parse_args(args):
    '''Parse command line'''

    parser = ArgumentParser(
        description=textwrap.dedent('''\
        Obtain OAuth2 refresh token for Google Storage
        '''))

    parser.add_debug()
    parser.add_quiet()
    parser.add_version()
    parser.add_oauth()

    options =  parser.parse_args(args)
    if options.client_id == '':
        options.client_id = OAUTH_CLIENT_ID
    if options.client_secret == '':
        options.client_secret = OAUTH_CLIENT_SECRET
    return options

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



def googleDrive(options):
    flow = OAuth2WebServerFlow(client_id=options.client_id,
                           client_secret=options.client_secret,
                           scope='https://www.googleapis.com/auth/drive',
                           redirect_uri=OOB_CALLBACK_URN)

    authorize_url = flow.step1_get_authorize_url()
    print(textwrap.fill('Go to the following link in your browser: %s' % authorize_url))
    code = input('Enter verification code: ').strip()
    credentials = flow.step2_exchange(code)
    print("your S3QL Credentials are:")
    print("user: %s" % options.client_id)
    print("password: %s:%s" % (options.client_secret,credentials.refresh_token))




def googleStorage(options):
    cli = requests.Session()

    # We need full control in order to be able to update metadata
    # cf. https://stackoverflow.com/questions/24718787
    r = cli.post('https://accounts.google.com/o/oauth2/device/code',
                 data={ 'client_id': options.client_id,
                        'scope': 'https://www.googleapis.com/auth/devstorage.full_control' },
                 verify=True, allow_redirects=False, timeout=20)
    req_json = _parse_response(r)

    print(textwrap.fill('Please open %s in your browser and enter the following '
                        'user code: %s' % (req_json['verification_url'],
                                           req_json['user_code'])))

    while True:
        log.debug('polling..')
        time.sleep(req_json['interval'])

        r = cli.post('https://accounts.google.com/o/oauth2/token',
                     data={ 'client_id': options.client_id,
                            'client_secret': options.client_secret,
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


def main(args=None):

    if args is None:
        args = sys.argv[1:]

    options = parse_args(args)
    setup_logging(options)
    if options.oauth_type == 'google-storage':
        googleStorage(options)
    elif options.oauth_type == 'google-drive':
        googleDrive(options)
    else:
        raise QuietError('Invalid oauth type : ' + options.oauth_type)
