'''
backends/s3c.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import division, print_function, absolute_import
from . import s3
import httplib
import logging

log = logging.getLogger("backends.s3c")

# Pylint goes berserk with false positives
#pylint: disable=E1002,E1101

class Bucket(s3.Bucket):
    """A bucket stored in some S3 compatible storage service.
    
    This class uses standard HTTP connections to connect to GS.
    
    The bucket guarantees only immediate get after create consistency.
    """


    def __init__(self, bucket_name, login, password):

        try:
            idx = bucket_name.index('/')
        except ValueError:
            idx = len(bucket_name)
        self.hostname = bucket_name[:idx]
        bucket_name = bucket_name[idx + 1:]

        super(Bucket, self).__init__(bucket_name, login, password)

        if ':' in self.hostname:
            idx = self.hostname.index(':')
            self.conn = httplib.HTTPConnection(self.hostname[:idx],
                                               port=int(self.hostname[idx + 1:]))
        else:
            self.conn = httplib.HTTPConnection(self.hostname)

        self.namespace = 'http://s3.amazonaws.com/doc/2006-03-01/'

    def _init(self):
        '''Additional initialization code
        
        Called by constructor and provided solely for easier subclassing.
        '''
        pass

    def _do_request(self, method, url, subres=None, query_string=None,
                    headers=None, body=None):
        '''Send request, read and return response object'''

        # Need to add bucket name
        url = '/%s%s' % (self.bucket_name, url)
        return super(Bucket, self)._do_request(method, url, subres,
                                               query_string, headers, body)

    def _add_auth(self, method, url, headers, subres=None, query_string=None):
        '''Add authentication to *headers*
        
        Note that *headers* is modified in-place. As a convenience,
        this method returns the encoded URL with query string and
        sub resource appended.
        
        *query_string* must be a dict or *None*. *subres* must be a
        string or *None*.
        '''

        # Bucket name is already part of URL
        url = url[len(self.bucket_name) + 1:]
        return '/%s%s' % (self.bucket_name,
                          super(Bucket, self)._add_auth(method, url, headers,
                                                        subres, query_string))

    def is_get_consistent(self):
        '''If True, objects retrievals are guaranteed to be up-to-date
        
        If this method returns True, then creating, deleting, or overwriting an
        object is guaranteed to be immediately reflected in subsequent object
        retrieval attempts.
        '''
        return False

    def is_list_create_consistent(self):
        '''If True, new objects are guaranteed to show up in object listings
        
        If this method returns True, creation of objects will immediately be
        reflected when retrieving the list of available objects.
        '''
        return False

    def __str__(self):
        return 's3c://%s/%s/%s' % (self.hostname, self.bucket_name, self.prefix)

