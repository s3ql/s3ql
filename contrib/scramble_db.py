#!/usr/bin/env python3
'''
scramble_db.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

# Data can be restored with:
#from s3ql.metadata import restore_metadata
#from s3ql.database import Connection
#restore_metadata(open('s3ql_metadata.dat', 'rb+'), 'data.sqlite')

import pickle
import os
import shutil
import sys
import tempfile
import hashlib

# We are running from the S3QL source directory, make sure
# that we use modules from this directory
basedir = os.path.abspath(os.path.join(os.path.dirname(sys.argv[0]), '..'))
if (os.path.exists(os.path.join(basedir, 'setup.py')) and
    os.path.exists(os.path.join(basedir, 'src', 's3ql', '__init__.py'))):
    sys.path = [os.path.join(basedir, 'src')] + sys.path

from s3ql.logging import logging, setup_logging, QuietError
from s3ql import CURRENT_FS_REV
from s3ql.common import get_backend_cachedir
from s3ql.database import Connection
from s3ql.metadata import dump_metadata
from s3ql.parse_args import ArgumentParser

log = logging.getLogger(__name__)

DBNAME = 's3ql_metadata.dat'

def parse_args(args):

    parser = ArgumentParser(
        description="Create metadata copy where all file- and directory names, "
                    "and extended attribute names and values have been scrambled. "
                    "This is intended to preserve privacy when a metadata copy "
                    "needs to be provided to the developers for debugging.")

    parser.add_debug()
    parser.add_quiet()
    parser.add_version()
    parser.add_cachedir()
    parser.add_storage_url()

    options = parser.parse_args(args)

    return options

def main(args=None):

    if args is None:
        args = sys.argv[1:]

    options = parse_args(args)
    setup_logging(options)

    # Check for cached metadata
    cachepath = get_backend_cachedir(options.storage_url, options.cachedir)
    if not os.path.exists(cachepath + '.params'):
        raise QuietError("No local metadata found.")

    with open(cachepath + '.params', 'rb') as fh:
        param = pickle.load(fh)

    # Check revision
    if param['revision'] < CURRENT_FS_REV:
        raise QuietError('File system revision too old.')
    elif param['revision'] > CURRENT_FS_REV:
        raise QuietError('File system revision too new.')

    if os.path.exists(DBNAME):
        raise QuietError('%s exists, aborting.' % DBNAME)

    log.info('Copying database...')
    dst = tempfile.NamedTemporaryFile()
    with open(cachepath + '.db', 'rb') as src:
        shutil.copyfileobj(src, dst)
    dst.flush()
    db = Connection(dst.name)

    log.info('Scrambling...')
    md5 = lambda x: hashlib.md5(x).hexdigest()
    for (id_, name) in db.query('SELECT id, name FROM names'):
        db.execute('UPDATE names SET name=? WHERE id=?',
                   (md5(name), id_))

    for (id_, name) in db.query('SELECT inode, target FROM symlink_targets'):
        db.execute('UPDATE symlink_targets SET target=? WHERE inode=?',
                   (md5(name), id_))

    for (id_, name) in db.query('SELECT rowid, value FROM ext_attributes'):
        db.execute('UPDATE ext_attributes SET value=? WHERE rowid=?',
                   (md5(name), id_))

    log.info('Saving...')
    with open(DBNAME, 'wb+') as fh:
        dump_metadata(db, fh)

if __name__ == '__main__':
    main(sys.argv[1:])
