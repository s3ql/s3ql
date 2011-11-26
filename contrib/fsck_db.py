#!/usr/bin/env python
'''
fsck_db.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C)  Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import division, print_function, absolute_import
from argparse import ArgumentTypeError
import cPickle as pickle
import logging
import os
import sys

# We are running from the S3QL source directory, make sure
# that we use modules from this directory
basedir = os.path.abspath(os.path.join(os.path.dirname(sys.argv[0]), '..'))
if (os.path.exists(os.path.join(basedir, 'setup.py')) and
    os.path.exists(os.path.join(basedir, 'src', 's3ql', '__init__.py'))):
    sys.path = [os.path.join(basedir, 'src')] + sys.path
    
from s3ql import CURRENT_FS_REV
from s3ql.common import setup_logging
from s3ql.fsck import ROFsck
from s3ql.parse_args import ArgumentParser
from s3ql.adm import _add_name
from s3ql.database import Connection    

log = logging.getLogger("fsck")

def parse_args(args):

    parser = ArgumentParser(
        description="Checks S3QL file system metadata")

    parser.add_log('~/.s3ql/fsck_db.log')
    parser.add_debug_modules()
    parser.add_quiet()
    parser.add_version()

    def db_path(s):
        s = os.path.splitext(s)[0]
        if not os.path.exists(s + '.db'):
            raise ArgumentTypeError('Unable to read %s.db' % s)
        if not os.path.exists(s + '.params'):
            raise ArgumentTypeError('Unable to read %s.params' % s)
        return s
    
    parser.add_argument("path", metavar='<path>', type=db_path, 
                        help='Database to be checked')
        
    options = parser.parse_args(args)

    return options

def main(args=None):

    if args is None:
        args = sys.argv[1:]

    options = parse_args(args)
    setup_logging(options)
      
    # Temporary hack to allow checking previous revision with most recent fsck
    param = pickle.load(open(options.path + '.params', 'rb'))
    if param['revision'] == CURRENT_FS_REV-1:
        log.info('Upgrading...')
        db = Connection(options.path + '.db')  
        db.execute("""
        CREATE TABLE ext_attributes_new (
            inode     INTEGER NOT NULL REFERENCES inodes(id),
            name_id   INTEGER NOT NULL REFERENCES names(id),
            value     BLOB NOT NULL,
     
            PRIMARY KEY (inode, name_id)               
        )""")        
        for (inode, name, val) in db.query('SELECT inode, name, value FROM ext_attributes'):
            db.execute('INSERT INTO ext_attributes_new (inode, name_id, value) VALUES(?,?,?)',
                       (inode, _add_name(db, name), val))
        db.execute('DROP TABLE ext_attributes')
        db.execute('ALTER TABLE ext_attributes_new RENAME TO ext_attributes')
        db.execute("""
        CREATE VIEW ext_attributes_v AS
        SELECT * FROM ext_attributes JOIN names ON names.id = name_id
        """)  
        db.close()  
        param['revision'] = CURRENT_FS_REV
        pickle.dump(param, open(options.path + '.params', 'wb'), 2)
           
    fsck = ROFsck(options.path)
    fsck.check()
        
if __name__ == '__main__':
    main(sys.argv[1:])

