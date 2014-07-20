'''
backends/__init__.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright © 2008 Nikolaus Rath <Nikolaus.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from . import local, s3, gs, s3c, swift, rackspace, swiftks

#: Mapping from storage URL prefixes to backend classes
prefix_map = { 's3': s3.Backend,
               'local': local.Backend,
               'gs': gs.Backend,
               's3c': s3c.Backend,
               'swift': swift.Backend,
               'swiftks': swiftks.Backend,
               'rackspace': rackspace.Backend }

__all__ = [ 'common', 'pool', 'comprenc' ] + list(prefix_map.keys())

