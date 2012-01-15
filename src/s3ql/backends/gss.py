'''
backends/gss.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import division, print_function, absolute_import
from . import gs
from s3ql.common import QuietError
import httplib
import re

# Pylint goes berserk with false positives
#pylint: disable=E1002,E1101,W0232

class Bucket(gs.Bucket):
    """A bucket stored in Google Storage
    
    This class uses secure (SSL) connections to connect to GS.
    
    The bucket guarantees immediate get consistency and eventual list
    consistency.    
    """

    @staticmethod
    def _parse_storage_url(storage_url):
        hit = re.match(r'^gss://([^/]+)(?:/(.*))?$', storage_url)
        if not hit:
            raise QuietError('Invalid storage URL: %s' % storage_url)

        bucket_name = hit.group(1)
        hostname = '%s.commondatastorage.googleapis.com' % bucket_name
        prefix = hit.group(2) or ''
        return (hostname, 443, bucket_name, prefix)

    def _get_conn(self):
        '''Return connection to server'''

        return httplib.HTTPSConnection(self.hostname, self.port)

    def __str__(self):
        return 'gss://%s/%s' % (self.bucket_name, self.prefix)
