'''
s3s.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import division, print_function, absolute_import

from . import s3 
import httplib

class Bucket(s3.Bucket):
    """A bucket stored in Amazon S3
    
    This class uses secure (SSL) connections to connect to S3.
    
    The bucket guarantees get after create consistency, i.e. a newly created
    object will be immediately retrievable. Additional consistency guarantees
    may or may not be available and can be queried for with instance methods.    
    """
            
    def _get_conn(self, use_ssl):
        '''Return connection to server'''
        
        return httplib.HTTPSConnection('%s.s3.amazonaws.com' % self.bucket_name)
    
    def __str__(self):
        return 's3s://%s/%s' % (self.bucket_name, self.prefix)            