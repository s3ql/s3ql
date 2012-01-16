'''
backends/s3cs.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import division, print_function, absolute_import
from . import s3c
from s3ql.common import QuietError
import httplib
import re
import os

# Pylint goes berserk with false positives
#pylint: disable=E1002,E1101,W0232


class Bucket(s3c.Bucket):
    """A bucket stored in some S3 compatible storage service.
    
    This class uses secure (SSL) connections.
    
    The bucket guarantees only immediate get after create consistency.
    """

    def _get_conn(self):
        '''Return connection to server'''

        if 'https_proxy' in os.environ:
            conn = httplib.HTTPSConnection(os.environ['https_proxy'])
            conn.set_tunnel(self.hostname, self.port)
            return conn
        else:
            return httplib.HTTPSConnection(self.hostname, self.port)

    def __str__(self):
        return 's3cs://%s/%s/%s' % (self.hostname, self.bucket_name, self.prefix)
    
    @staticmethod
    def _parse_storage_url(storage_url):
        '''Extract information from storage URL
        
        Return a tuple * (host, port, bucket_name, prefix) * .
        '''

        hit = re.match(r'^[a-zA-Z0-9]+://' # Backend
                       r'([^/:]+)' # Hostname
                       r'(?::([0-9]+))?' # Port 
                       r'/([^/]+)' # Bucketname
                       r'(?:/(.*))?$', # Prefix
                       storage_url)
        if not hit:
            raise QuietError('Invalid storage URL')

        hostname = hit.group(1)
        port = int(hit.group(2) or '443')
        bucketname = hit.group(3)
        prefix = hit.group(4) or ''

        return (hostname, port, bucketname, prefix)