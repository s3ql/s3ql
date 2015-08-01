'''
swiftks.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

from ..logging import logging, QuietError # Ensure use of custom logger class
from . import swift
from dugong import HTTPConnection, CaseInsensitiveDict
from .common import AuthorizationError, retry, DanglingStorageURLError
from .s3c import HTTPError
from ..inherit_docstrings import copy_ancestor_docstring
from urllib.parse import urlsplit
import json
import re
import urllib.parse

log = logging.getLogger(__name__)

class Backend(swift.Backend):

    def __init__(self, storage_url, login, password, options):
        self.region = None
        super().__init__(storage_url, login, password, options)

    @copy_ancestor_docstring
    def _parse_storage_url(self, storage_url, ssl_context):

        hit = re.match(r'^[a-zA-Z0-9]+://' # Backend
                       r'([^/:]+)' # Hostname
                       r'(?::([0-9]+))?' # Port
                       r'/([a-zA-Z0-9._-]+):' # Region
                       r'([^/]+)' # Bucketname
                       r'(?:/(.*))?$', # Prefix
                       storage_url)
        if not hit:
            raise QuietError('Invalid storage URL', exitcode=2)

        hostname = hit.group(1)
        if hit.group(2):
            port = int(hit.group(2))
        elif ssl_context:
            port = 443
        else:
            port = 80
        region = hit.group(3)
        containername = hit.group(4)
        prefix = hit.group(5) or ''

        self.hostname = hostname
        self.port = port
        self.container_name = containername
        self.prefix = prefix
        self.region = region

    @retry
    def _get_conn(self):
        '''Obtain connection to server and authentication token'''

        log.debug('started')

        if 'no-ssl' in self.options:
            ssl_context = None
        else:
            ssl_context = self.ssl_context

        headers = CaseInsensitiveDict()
        headers['Content-Type'] = 'application/json'
        headers['Accept'] = 'application/json; charset="utf-8"'

        if ':' in self.login:
            (tenant,user) = self.login.split(':')
        else:
            tenant = None
            user = self.login

        auth_body = { 'auth':
                          { 'passwordCredentials':
                                { 'username': user,
                                  'password': self.password } }}
        if tenant:
            auth_body['auth']['tenantName'] = tenant

        with HTTPConnection(self.hostname, port=self.port, proxy=self.proxy,
                            ssl_context=ssl_context) as conn:
            conn.timeout = int(self.options.get('tcp-timeout', 20))

            conn.send_request('POST', '/v2.0/tokens', headers=headers,
                              body=json.dumps(auth_body).encode('utf-8'))
            resp = conn.read_response()

            if resp.status == 401:
                raise AuthorizationError(resp.reason)

            elif resp.status > 299 or resp.status < 200:
                raise HTTPError(resp.status, resp.reason, resp.headers)

            cat = json.loads(conn.read().decode('utf-8'))
            self.auth_token = cat['access']['token']['id']

        avail_regions = []
        for service in cat['access']['serviceCatalog']:
            if service['type'] != 'object-store':
                continue

            for endpoint in service['endpoints']:
                if endpoint['region'] != self.region:
                    avail_regions.append(endpoint['region'])
                    continue

                o = urlsplit(endpoint['publicURL'])
                self.auth_prefix = urllib.parse.unquote(o.path)
                if o.scheme == 'https':
                    ssl_context = self.ssl_context
                elif o.scheme == 'http':
                    ssl_context = None
                else:
                    # fall through to scheme used for authentication
                    pass

                conn = HTTPConnection(o.hostname, o.port,  proxy=self.proxy,
                                      ssl_context=ssl_context)
                conn.timeout = int(self.options.get('tcp-timeout', 20))
                return conn

        if len(avail_regions) < 10:
            raise DanglingStorageURLError(self.container_name,
                'No accessible object storage service found in region %s'
                ' (available regions: %s)' % (self.region, ', '.join(avail_regions)))
        else:
            raise DanglingStorageURLError(self.container_name,
                'No accessible object storage service found in region %s'
                % self.region)
