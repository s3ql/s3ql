'''
backends/gss.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import division, print_function, absolute_import

from . import gs
import httplib

# Pylint goes berserk with false positives
#pylint: disable=E1002,E1101,W0232


class Bucket(gs.Bucket):
    """A bucket stored in Google Storage
    
    This class uses secure (SSL) connections to connect to GS.
    
    The bucket guarantees immediate get consistency and eventual list
    consistency.    
    """

    def _get_conn(self):
        '''Return connection to server'''

        return httplib.HTTPSConnection('%s.commondatastorage.googleapis.com' % self.bucket_name)

    def __str__(self):
        return 'gss://%s/%s' % (self.bucket_name, self.prefix)