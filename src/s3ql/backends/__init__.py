'''
backends/__init__.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

from s3ql.backends.common import AbstractBackend, AsyncBackend

from . import gs, local, rackspace, s3, s3c, s3c4, swift, swiftks
from .b2 import b2_backend as b2

#: Mapping from storage URL prefixes to backend classes
prefix_map: dict[str, type[AbstractBackend]] = {
    's3': s3.Backend,
    'local': local.Backend,
    'gs': gs.Backend,
    's3c': s3c.Backend,
    's3c4': s3c4.Backend,
    'swift': swift.Backend,
    'swiftks': swiftks.Backend,
    'rackspace': rackspace.Backend,
    'b2': b2.B2Backend,
}

async_prefix_map: dict[str, type[AsyncBackend]] = {
    's3': s3.AsyncBackend,
    'local': local.AsyncBackend,
    'gs': gs.AsyncBackend,
    's3c': s3c.AsyncBackend,
    's3c4': s3c4.AsyncBackend,
    'swift': swift.AsyncBackend,
    'swiftks': swiftks.AsyncBackend,
    'rackspace': rackspace.AsyncBackend,
    'b2': b2.AsyncB2Backend,
}
