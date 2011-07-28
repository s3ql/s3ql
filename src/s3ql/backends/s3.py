'''
s3.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import division, print_function, absolute_import

from .common import AbstractBucket, NoSuchBucket, NoSuchObject
from s3ql.common import (TimeoutError, AsyncFn)
import logging
import errno
import httplib
import re
import time
import threading
from base64 import b64encode
import hmac
import hashlib
import urllib
from datetime import datetime

log = logging.getLogger("backend.s3etal")

C_DAY_NAMES = [ 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun' ]
C_MONTH_NAMES = [ 'Jan', 'Feb', 'Mar', 'Apr', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec' ]

class Bucket(AbstractBucket):
    """A bucket stored in Amazon S3 and compatible services

    This class is threadsafe. All methods (except for internal methods
    starting with underscore) may be called concurrently by different
    threads.    
    """

    def __init__(self, aws_key_id, aws_key,  
                 storage_url, bucket_passphrase, compression, use_ssl):
        super(Bucket, self).__init__(bucket_passphrase, compression)
        
        self.bucket_name = bucket_name
        self.aws_key = aws_key
        self.aws_key_id = aws_key_id
        if use_ssl:
            self.conn = httplib.HTTPSConnection('%s.s3.amazonaws.com' % bucket_name)
        else:
            self.conn = httplib.HTTPConnection('%s.s3.amazonaws.com' % bucket_name)
           

         
        
    def delete(self, key):
        '''Delete the specified object'''
        
        self._auth_request('DELETE', '/%s' % key)
        
        
    def clear(self):
        """Delete all objects in bucket
        
        This function starts multiple threads."""

        threads = list()
        for (no, s3key) in enumerate(self):
            if no != 0 and no % 1000 == 0:
                log.info('Deleted %d objects so far..', no)

            log.debug('Deleting key %s', s3key)

            # Ignore missing objects when clearing bucket
            t = AsyncFn(self.delete, s3key, True)
            t.start()
            threads.append(t)

            if len(threads) > 50:
                log.debug('50 threads reached, waiting..')
                threads.pop(0).join_and_raise()

        log.debug('Waiting for removal threads')
        for t in threads:
            t.join_and_raise()

    def __str__(self):
        return '<s3 bucket, name=%r>' % self.bucket_name

    def _auth_request(self, method, url, query_string=None, 
                      body=None, headers=None):
        '''Make authenticated request
        
        *query_string* and *headers* must be dictionaries or *None*.
        '''
             
        # See http://docs.amazonwebservices.com/AmazonS3/latest/dev/RESTAuthentication.html
        
        # Lowercase headers
        if headers:
            headers = dict((x.lower(), y) for (x,y) in headers.iteritems())
        else:
            headers = dict()
        
        if 'date' not in headers:
            now = time.gmtime()
            # Can't use strftime because it's locale dependent
            headers['date'] = ('%s, %02d %s %04d %02d:%02d:%02d GMT' 
                               % (C_DAY_NAMES[now.tm_wday],
                                  now.tm_mday,
                                  C_MONTH_NAMES[now.tm_mon - 1],
                                  now.tm_year, now.tm_hour, 
                                  now.tm_min, now.tm_sec))
            
        auth_strs = [method, '\n']
        
        for hdr in ('content-md5', 'content-type', 'date'):
            if hdr in headers:
                auth_strs.append(headers[hdr])
            auth_strs.append('\n')
    
        for hdr in sorted(x for x in headers if x.startswith('x-amz-')):
            val = ' '.join(re.split(r'\s*\n\s*', headers[hdr].strip()))
            auth_strs.append('%s:%s\n' % (hdr,val))
    
        auth_strs.append('/' + self.bucket_name)
        auth_strs.append(url)
        
        # False positive, hashlib *does* have sha1 member
        #pylint: disable=E1101
        signature = b64encode(hmac.new(self.aws_key, ''.join(auth_strs), hashlib.sha1).digest())
         
        headers['Authorization'] = 'AWS %s:%s' % (self.aws_key_id, signature)
    
        full_url = urllib.quote(url)
        if query_string:
            full_url += '?%s' % urllib.urlencode(query_string, doseq=True)
            
        return self.conn.request(method, full_url, body, headers)
