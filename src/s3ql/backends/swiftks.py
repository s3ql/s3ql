'''
swiftks.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from ..logging import logging # Ensure use of custom logger class
from . import swift
from ..common import QuietError
from .common import AuthorizationError, http_connection, retry
from .s3c import HTTPError
from ..inherit_docstrings import copy_ancestor_docstring
from urllib.parse import urlsplit
import json
import re
import urllib.parse

log = logging.getLogger(__name__)

class Backend(swift.Backend):

    def __init__(self, storage_url, login, password, ssl_context):
        # Unused argument
        #pylint: disable=W0613
        
        (host, port, region,
         container_name, prefix) = self._parse_storage_url(storage_url, ssl_context)
            
        self.hostname = host
        self.port = port
        self.region = region
        self.container_name = container_name
        self.prefix = prefix
        self.password = password
        self.login = login
        self.auth_token = None
        self.auth_prefix = None
        self.conn = None
        self.ssl_context = ssl_context
        
        self._container_exists()

    @staticmethod
    @copy_ancestor_docstring
    def _parse_storage_url(storage_url, ssl_context):

        hit = re.match(r'^[a-zA-Z0-9]+://' # Backend
                       r'([^/:]+)' # Hostname
                       r'(?::([0-9]+))?' # Port 
                       r'/([a-zA-Z0-9._-]+):' # Region
                       r'([^/]+)' # Bucketname
                       r'(?:/(.*))?$', # Prefix
                       storage_url)
        if not hit:
            raise QuietError('Invalid storage URL')

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
        
        return (hostname, port, region, containername, prefix)

    @retry
    def _get_conn(self):
        '''Obtain connection to server and authentication token'''

        log.debug('_get_conn(): start')
        
        conn = http_connection(self.hostname, port=self.port,
                               ssl_context=self.ssl_context)

        headers={ 'Content-Type': 'application/json',
                  'Accept': 'application/json; charset="utf-8"' }

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

        conn.request('POST', '/v2.0/tokens', json.dumps(auth_body).encode('utf-8'), headers)
        resp = conn.getresponse()
            
        if resp.status == 401:
            raise AuthorizationError(resp.read())
            
        elif resp.status > 299 or resp.status < 200:
            raise HTTPError(resp.status, resp.reason, resp.getheaders(), resp.read())

        cat = json.loads(resp.read().decode('utf-8'))
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
                conn.close()

                return http_connection(o.hostname, o.port, self.ssl_context)

        if len(avail_regions) < 10:
            raise QuietError('No accessible object storage service found in region %s'
                             ' (available regions: %s)' 
                             % (self.region, ', '.join(avail_regions)))
        else:
            raise QuietError('No accessible object storage service found in region %s'
                             % self.region)
            

