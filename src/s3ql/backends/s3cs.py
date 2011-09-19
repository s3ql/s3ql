'''
backends/s3cs.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import division, print_function, absolute_import

from . import s3c
import httplib

# Pylint goes berserk with false positives
#pylint: disable=E1002,E1101,W0232


class Bucket(s3c.Bucket):
    """A bucket stored in some S3 compatible storage service.
    
    This class uses secure (SSL) connections.
    
    The bucket guarantees only immediate get after create consistency.
    """
            
    def _get_conn(self):
        '''Return connection to server'''
        
        return httplib.HTTPSConnection(self.bucket_name)
    
    def __str__(self):
        return 's3cs://%s/%s' % (self.bucket_name, self.prefix)