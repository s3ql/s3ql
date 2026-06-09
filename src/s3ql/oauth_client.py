'''
oauth_client.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import annotations

import logging
import sys
from collections.abc import Sequence

from google_auth_oauthlib.flow import InstalledAppFlow

from .common import OAUTH_CLIENT_ID, OAUTH_CLIENT_SECRET
from .logging import setup_logging
from .parse_args import (
    DebugFlag,
    DebugModules,
    LogDest,
    QuietFlag,
    make_app,
    run_app,
    trio_command,
)

log: logging.Logger = logging.getLogger(__name__)

app = make_app()


@app.command()
@trio_command
async def oauth_client(
    log_target: LogDest = None,
    debug: DebugFlag = False,
    debug_modules: DebugModules = None,
    quiet: QuietFlag = False,
) -> None:
    '''Obtain OAuth2 refresh token for Google Storage.'''
    setup_logging(quiet=quiet, log=log_target, debug=debug, debug_modules=debug_modules)

    # We need full control in order to be able to update metadata
    # cf. https://stackoverflow.com/questions/24718787
    flow = InstalledAppFlow.from_client_config(
        client_config={
            "installed": {
                "client_id": OAUTH_CLIENT_ID,
                "client_secret": OAUTH_CLIENT_SECRET,
                "redirect_uris": ["http://localhost", "urn:ietf:wg:oauth:2.0:oob"],
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://accounts.google.com/o/oauth2/token",
            }
        },
        scopes=['https://www.googleapis.com/auth/devstorage.full_control'],
    )

    credentials = flow.run_local_server(open_browser=True)

    print('Success. Your refresh token is:\n', credentials.refresh_token)


def main(args: Sequence[str] | None = None) -> None:
    run_app(app, args, prog_name='s3ql_oauth_client')


if __name__ == '__main__':
    main(sys.argv[1:])
