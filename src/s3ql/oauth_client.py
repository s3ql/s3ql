'''
oauth_client.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

from .logging import logging, setup_logging, QuietError
from .parse_args import ArgumentParser
from .common import OAUTH_CLIENT_ID, OAUTH_CLIENT_SECRET
from google_auth_oauthlib.flow import InstalledAppFlow
import sys
import textwrap
import requests

log = logging.getLogger(__name__)


def parse_args(args):
    '''Parse command line'''

    parser = ArgumentParser(
        description=textwrap.dedent('''\
        Obtain OAuth2 refresh token for Google Storage
        '''))

    parser.add_log()
    parser.add_debug()
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

    # We need full control in order to be able to update metadata
    # cf. https://stackoverflow.com/questions/24718787
    flow = InstalledAppFlow.from_client_config(
        client_config={
              "installed": {
                  "client_id": OAUTH_CLIENT_ID,
                  "client_secret": OAUTH_CLIENT_SECRET,
                  "redirect_uris": ["http://localhost", "urn:ietf:wg:oauth:2.0:oob"],
                  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                  "token_uri": "https://accounts.google.com/o/oauth2/token"
              }
        },
        scopes=['https://www.googleapis.com/auth/devstorage.full_control'])

    credentials = flow.run_local_server(open_browser=True)

    print('Success. Your refresh token is:\n', credentials.refresh_token)
