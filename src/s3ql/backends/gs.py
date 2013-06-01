'''
backends/gs.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from . import s3c
from ..logging import logging # Ensure use of custom logger class
from s3ql.common import QuietError
import re

# Pylint goes berserk with false positives
#pylint: disable=E1002,E1101,W0201

log = logging.getLogger(__name__)

class Backend(s3c.Backend):
    """A backend to store data in Google Storage
    
    This class uses standard HTTP connections to connect to GS.
    
    The backend guarantees immediate get consistency and eventual list
    consistency.
    """

    def __init__(self, storage_url, gs_key, gs_secret, ssl_context):
        super().__init__(storage_url, gs_key, gs_secret, ssl_context)

        self.namespace = 'http://doc.s3.amazonaws.com/2006-03-01'

    @staticmethod
    def _parse_storage_url(storage_url, ssl_context):
        hit = re.match(r'^gs://([^/]+)(?:/(.*))?$', storage_url)
        if not hit:
            raise QuietError('Invalid storage URL')

        bucket_name = hit.group(1)
        hostname = '%s.commondatastorage.googleapis.com' % bucket_name
        prefix = hit.group(2) or ''
        port = 443 if ssl_context else 80
        return (hostname, port, bucket_name, prefix)        

    def __str__(self):
        return 'gs://%s/%s' % (self.bucket_name, self.prefix)

