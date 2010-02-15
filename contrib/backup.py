#!/usr/bin/env python
'''
Back up a directory to an S3QL file system and intelligently remove old
backups.

The actual copying is done with rsync, so that only changed files are transferred.
'''

from __future__ import division, print_function, absolute_import

import sys
import subprocess
import time
import os

# === You should configure the following variables ===

# Path to the S3QL commands
s3ql_path = '/home/nikratio/lib/s3ql/bin'

# The directory that should be backed up
source_dir = "/home/nikratio/misc"

# A list of exclusion patterns that will be passed to rsync
# with --exclude
exclusions = ['.cache/',
              '.s3ql/',
              '.gconf/',
              '.gconfd/',
              '.thunderbird-3.0/',
              '.local/',
              '.thumbnails/',
              '.mozilla/firefox/pw5by90g.default/Cache/',
              'lib/archive/index.swish-e',
              'tmp/',
              'nobackup/',
              'lib/EclipseWorkspace/.metadata/']


# The name of the bucket that contains the S3QL file system
bucketname = "nikratio-backup"

# The sub-directory of the S3QL file system for the backups
backup_dir = "inspiron"

# The directory where the S3QL file system will be mounted during backup
mountpoint = "/home/nikratio/tmp/mnt"

# mount.s3ql will try to read the passphrase from this file if the
# file system is encrypted. 
passfile = '/home/nikratio/.s3ql_backup_passphrase'

# === End of configurable section ===

current_backup = time.strftime('%Y-%m-%d_%H:%M:%S')

if not os.path.exists(mountpoint):
    os.mkdir(mountpoint)

# Mount file system
print('== Mounting filesystem ==')
with open(passfile, 'r') as pass_fh:
    subprocess.check_call([os.path.join(s3ql_path, 'mount.s3ql'),
                           bucketname, mountpoint], stdin=pass_fh.fileno())

try:
    if not os.path.exists(os.path.join(mountpoint, backup_dir)):
        os.mkdir(os.path.join(mountpoint, backup_dir))

    all_backups = sorted(os.listdir(os.path.join(mountpoint, backup_dir)))
    if all_backups:
        last_backup = all_backups[-1]

        print('== Replicating previous backup (%r) ==')
        print('...')
        subprocess.check_call([os.path.join(s3ql_path, 'cp.s3ql'),
                               os.path.join(mountpoint, backup_dir, last_backup),
                               os.path.join(mountpoint, backup_dir, current_backup)])

    print('== Creating new backup %r ==' % current_backup)
    rsync_args = ['rsync', '-aHvxW', '--delete']
    for pat in exclusions:
        rsync_args.append('--exclude')
        rsync_args.append(pat)

    rsync_args.append(source_dir + '/')
    rsync_args.append(os.path.join(mountpoint, backup_dir, current_backup) + '/')

    subprocess.check_call(rsync_args)


finally:
    print('== Unmounting ==')
    subprocess.check_call([os.path.join(s3ql_path, 'umount.s3ql'),
                           mountpoint])

