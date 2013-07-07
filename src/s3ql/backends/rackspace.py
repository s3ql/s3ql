'''
rackspace.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from ..common import QuietError
from .s3c import HTTPError
from .common import (AuthorizationError, http_connection)
from . import swift
import json
import re
import logging
from urllib.parse import urlsplit
import urllib.parse

log = logging.getLogger("backend.rackspace")

class Backend(swift.Backend):
    """A backend to store data in Rackspace CloudFiles.
    
    The backend guarantees get after create consistency, i.e. a newly created
    object will be immediately retrievable. 
    """

    def __init__(self, storage_url, login, password, ssl_context):
        # Unused argument
        #pylint: disable=W0613
        
        (region, container_name, prefix) = self._parse_storage_url(storage_url)
            
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

    def __str__(self):
        return 'rackspace container %s, prefix %s' % (self.container_name, self.prefix)
        
                    
    @staticmethod
    def _parse_storage_url(storage_url):
        '''Extract information from storage URL
        
        Return a tuple *(region, container_name, prefix)* .
        '''

        hit = re.match(r'^rackspace://' # Backend
                       r'([^/:]+)' # Region
                       r'/([^/]+)' # Bucketname
                       r'(?:/(.*))?$', # Prefix
                       storage_url)
        if not hit:
            raise QuietError('Invalid storage URL')

        region = hit.group(1)
        containername = hit.group(2)
        prefix = hit.group(3) or ''
        
        return (region, containername, prefix)

    def _get_conn(self):
        '''Obtain connection to server and authentication token'''

        log.debug('_get_conn(): start')

        conn = http_connection('auth.api.rackspacecloud.com',
                               ssl_context=self.ssl_context)
        headers={ 'Content-Type': 'application/json',
                  'Accept': 'application/json; charset="utf-8"' }

        auth_body = { 'auth':
                          { 'passwordCredentials':
                                { 'username': self.login,
                                  'password': self.password, } } }
                                  
        conn.request('POST', '/v2.0/tokens', json.dumps(auth_body).encode('utf-8'), headers)
        resp = conn.getresponse()
            
        if resp.status == 401:
            raise AuthorizationError(resp.read())
            
        elif resp.status > 299 or resp.status < 200:
            raise HTTPError(resp.status, resp.reason, resp.getheaders(), resp.read())

        cat = json.loads(resp.read().decode('utf-8'))
        self.auth_token = cat['access']['token']['id']

        for service in cat['access']['serviceCatalog']:
            if service['type'] != 'object-store':
                continue

            for endpoint in service['endpoints']:
                if endpoint['region'] != self.region:
                    continue
                
                o = urlsplit(endpoint['publicURL'])
                self.auth_prefix = urllib.parse.unquote(o.path)
                conn.close()

                return http_connection(o.hostname, o.port, self.ssl_context)

        raise QuietError('No accessible object storage service found in region %s'
                         % self.region)

