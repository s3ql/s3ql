
from __future__ import unicode_literals
import os
import stat
import sys

def get():
    '''Return AWS credentials if ~/.awssecret exists and is secure.
    
    Otherwise return none and print warning message.
    '''
    
    aws_credentials = None
    keyfile = os.path.join(os.environ["HOME"], ".awssecret")
    if os.path.isfile(keyfile):
        mode = os.stat(keyfile).st_mode
        kfile = open(keyfile, "r")
        key = kfile.readline().rstrip()
    
        if mode & (stat.S_IRGRP | stat.S_IROTH):
            sys.stderr.write("~/.awssecret has insecure permissions, skipping remote tests.\n")        
        else:    
            pw = kfile.readline().rstrip()
            aws_credentials = (key, pw)
            print 'Will use credentials from ~/.awssecret to access AWS.'
        kfile.close()  
    else:
        print '~/.awssecret does not exist. Will skip tests requiring valid AWS credentials.'
        
    return aws_credentials