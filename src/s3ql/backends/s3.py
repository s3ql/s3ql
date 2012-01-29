'''
s3.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import division, print_function, absolute_import
from . import s3c
from s3ql.backends.common import retry
from s3ql.common import QuietError
import xml.etree.cElementTree as ElementTree
import logging
import re

log = logging.getLogger("backend.s3")

# Pylint goes berserk with false positives
#pylint: disable=E1002,E1101
              
class Bucket(s3c.Bucket):
    """A bucket stored in Amazon S3
    
    This class uses standard HTTP connections to connect to S3.
    
    The bucket guarantees get after create consistency, i.e. a newly created
    object will be immediately retrievable. Additional consistency guarantees
    may or may not be available and can be queried for with instance methods.    
    """

    @staticmethod
    def _parse_storage_url(storage_url, use_ssl):
        hit = re.match(r'^s3s?://([^/]+)(?:/(.*))?$', storage_url)
        if not hit:
            raise QuietError('Invalid storage URL')

        bucket_name = hit.group(1)
        hostname = '%s.s3.amazonaws.com' % bucket_name
        prefix = hit.group(2) or ''
        port = 443 if use_ssl else 80
        return (hostname, port, bucket_name, prefix)

    def __str__(self):
        return 's3://%s/%s' % (self.bucket_name, self.prefix)
