'''
backends/gs.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import division, print_function, absolute_import
from . import s3
from .s3 import retry
import httplib
import logging
import xml.etree.cElementTree as ElementTree

# Pylint goes berserk with false positives
#pylint: disable=E1002,E1101,W0201

log = logging.getLogger("backends.gs")
    
class Bucket(s3.Bucket):
    """A bucket stored in Google Storage
    
    This class uses standard HTTP connections to connect to GS.
    
    The bucket guarantees immediate get consistency and eventual list
    consistency.
    """

    def __init__(self, bucket_name, gs_key, gs_secret):
        super(Bucket, self).__init__(bucket_name, gs_key, gs_secret)
        
        self.namespace = 'http://doc.s3.amazonaws.com/2006-03-01'
    
    def _init(self):
        '''Additional initialization code
        
        Called by constructor and provided solely for easier subclassing.
        '''
        
        self.conn = self._get_conn()
        
    def _get_conn(self):
        '''Return connection to server'''
        
        return httplib.HTTPConnection('%s.commondatastorage.googleapis.com' % self.bucket_name)          

    @retry
    def _get_region(self):
        ''''Return bucket region'''
        
        log.debug('_get_region()')
        resp = self._do_request('GET', '/', subres='location')
        region = ElementTree.parse(resp).getroot().text
        
        return region
        
    def is_get_consistent(self):
        '''If True, objects retrievals are guaranteed to be up-to-date
        
        If this method returns True, then creating, deleting, or overwriting an
        object is guaranteed to be immediately reflected in subsequent object
        retrieval attempts.
        '''
        
        return True
                    
    def is_list_create_consistent(self):
        '''If True, new objects are guaranteed to show up in object listings
        
        If this method returns True, creation of objects will immediately be
        reflected when retrieving the list of available objects.
        '''

        return False
        
    def __str__(self):
        return 'gs://%s/%s' % (self.bucket_name, self.prefix)

