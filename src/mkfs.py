#!/usr/bin/env python
#
#    Copyright (C) 2008  Nikolaus Rath <Nikolaus@rath.org>
#
#    This program can be distributed under the terms of the GNU LGPL.
#

from __future__ import unicode_literals
import sys
import os
from getpass import getpass
import shutil 
from optparse import OptionParser
import logging

from s3ql import mkfs, s3
from s3ql.common import init_logging, get_credentials, get_cachedir, get_dbfile
from s3ql.database import WrappedConnection
import apsw

#
# Parse options
#
parser = OptionParser(
    usage="%prog  [options] <bucketname>\n" \
        "       %prog --help",
    description="Initializes on Amazon S3 bucket for use as a filesystem")

parser.add_option("--awskey", type="string",
                  help="Amazon Webservices access key to use. The password is "
                  "read from stdin. If this option is not specified, both access key "
                  "and password are read from ~/.awssecret (separated by newlines).")
parser.add_option("-L", type="string", default='', help="Filesystem label",
                  dest="label")
parser.add_option("--blocksize", type="int", default=10240,
                  help="Maximum size of s3 objects in KB (default: %default)")
parser.add_option("-f", action="store_true", default=False,
                  dest="force", help="Force creation and remove any existing data")
parser.add_option("--encrypt", action="store_true", default=None,
                  help="Create an AES encrypted filesystem")
parser.add_option("--debug", action="append", 
                  help="Activate debugging output from specified facility."
                      "This option can be specified multiple times.")
parser.add_option("--quiet", action="store_true", default=False,
                  help="Be really quiet")


(options, pps) = parser.parse_args()

if not len(pps) == 1:
    parser.error("bucketname not specificed")
bucket = pps[0]


#
# Read password(s)
#
(awskey, awspass) = get_credentials(options.awskey)

if options.encrypt:
    if sys.stdin.isatty():
        options.encrypt = getpass("Enter encryption password: ")
        if not options.encrypt == getpass("Confirm encryption password: "):
            sys.stderr.write("Passwords don't match\n.")
            sys.exit(1)
    else:
        options.encrypt = sys.stdin.readline().rstrip()

# Activate logging
init_logging(True, options.quiet, options.debug)
log = logging.getLogger("frontend")

#
# Setup bucket
#
conn = s3.Connection(awskey, awspass, options.encrypt)
if conn.bucket_exists(bucket):
    if options.force:
        log.info("Removing existing bucket...")
        conn.delete_bucket(bucket, True)
    else:
        log.warn(
            "Bucket already exists!\n" 
            "Use -f option to remove the existing bucket.\n")
        sys.exit(1)


#
# Setup database
#
dbfile = get_dbfile(bucket)
cachedir = get_cachedir(bucket)

if os.path.exists(dbfile) or \
        os.path.exists(cachedir):
    if options.force:
        if os.path.exists(dbfile):
            os.unlink(dbfile)
        if os.path.exists(cachedir):
            shutil.rmtree(cachedir)
        log.info("Removed existing metadata.")
    else:
        log.warn(
            "Local metadata file already exists!\n" 
            "Use -f option to really remove the existing filesystem.")
        sys.exit(1)
        
try:
    log.info('Creating metadata tables...')
    mkfs.setup_db(WrappedConnection(apsw.Connection(dbfile), retrytime=0),
                  options.blocksize * 1024, options.label)

    log.info('Uploading database...')
    bucket = conn.get_bucket(bucket)
    bucket.store_from_file('s3ql_metadata', dbfile)
    bucket['s3ql_bgcommit'] = 'no'
    bucket['s3ql_dirty'] = "no"

finally:
    os.unlink(dbfile)
