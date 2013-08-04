'''
rackspace.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from ..common import QuietError
from . import swiftks
from ..inherit_docstrings import copy_ancestor_docstring
import re
import logging

log = logging.getLogger("backend.rackspace")

class Backend(swiftks.Backend):
    """A backend to store data in Rackspace CloudFiles"""

    @staticmethod
    @copy_ancestor_docstring
    def _parse_storage_url(storage_url, ssl_context):

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

        if ssl_context:
            port = 443
        else:
            port = 80
            
        return ('auth.api.rackspacecloud.com', port, region, containername, prefix)

