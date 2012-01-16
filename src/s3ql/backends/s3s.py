'''
s3s.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import division, print_function, absolute_import
from . import s3
from ..common import QuietError
import httplib
import re
import os

# Pylint goes berserk with false positives
#pylint: disable=E1002,E1101,W0232

class Bucket(s3.Bucket):
    """A bucket stored in Amazon S3
    
    This class uses secure (SSL) connections to connect to S3.
    
    The bucket guarantees get after create consistency, i.e. a newly created
    object will be immediately retrievable. Additional consistency guarantees
    may or may not be available and can be queried for with instance methods.    
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
        return 's3s://%s/%s' % (self.bucket_name, self.prefix)

    @staticmethod
    def _parse_storage_url(storage_url):
        hit = re.match(r'^s3s://([^/]+)(?:/(.*))?$', storage_url)
        if not hit:
            raise QuietError('Invalid storage URL')

        bucket_name = hit.group(1)
        hostname = '%s.s3.amazonaws.com' % bucket_name
        prefix = hit.group(2) or ''
        return (hostname, 443, bucket_name, prefix)
