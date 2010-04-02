#!/usr/bin/env python
'''
Back up a directory to an S3QL file system and intelligently remove old
backups.

The actual copying is done with rsync, so that only changed files are transferred.
'''

from __future__ import division, print_function, absolute_import

# === You should configure the following variables ===

# Path to the S3QL commands
s3ql_path = '/home/nikratio/projekte/s3ql/bin'

# The directory that should be backed up
source_dir = "/home/nikratio"

# A list of exclusion patterns that will be passed to rsync
# with --exclude
exclusions = ['/.cache/',
              '/.s3ql/',
              '/.thumbnails/' ]


# The name of the bucket that contains the S3QL file system
bucketname = "nikratio_43"

# The sub-directory of the S3QL file system for the backups
import platform
backup_dir = platform.node() # Returns the hostname

# The directory where the S3QL file system will be mounted during backup
mountpoint = "/home/nikratio/tmp/mnt"

# mount.s3ql will try to read the passphrase from this file if the
# file system is encrypted. 
passfile = '/home/nikratio/.s3ql_backup_passphrase'

# The backup generations determines when backups will be deleted. Each backup generation is identified by
# its relative age compared to the earlier generation. So the setting
backup_generations = [ '1d', '1d', '7d', '14d', '31d', '150d', '150d' ]
# defines 9 generations:
#  - The first generation is at least 1 day older than the current backup
#  - The 2nd generation is at least 1 day older than the youngest backup in the 1st generation, so
#    backups in this generation are at least 2 days old.
#  - The 3rd generation is at least 7 days older than the youngest backup in the 2nd generation, so
#    backups in this generation are at least 9 days old.
#  - The 4th generation is at least 14 days older than the youngest backup in the 3rd generation, so
#    backups in this generation are at least 23 days old.
# etc. 
#
# You can also specify the age in hours as e.g. '23h'.
#
# This script ensures that there is exactly one backup in each generation at all times.
# Backups that are no longer needed to satisfy this condition will be deleted.
#

# For more verbose output
verbose = True

# === End of configurable section ===

import sys
import subprocess
import time
import os
from datetime import datetime, timedelta
import logging
import random
import re
import shutil

# We are running from the S3QL source directory, make sure
# that we use modules from this directory
basedir = os.path.abspath(os.path.join(os.path.dirname(sys.argv[0]), '..'))
if (os.path.exists(os.path.join(basedir, 'setup.py')) and
    os.path.exists(os.path.join(basedir, 'src', 's3ql', '__init__.py'))):
    sys.path = [os.path.join(basedir, 'src')] + sys.path

from s3ql.common import init_logging

if verbose:
    init_logging(None, stdout_level=logging.INFO)
else:
    init_logging(None, stdout_level=logging.WARN)
log = logging.getLogger("backup")

def main():

    current_backup = time.strftime('%Y-%m-%d_%H:%M:%S')

    if not os.path.exists(mountpoint):
        os.mkdir(mountpoint)

    # Mount file system
    mount_args = [os.path.join(s3ql_path, 'mount.s3ql'), bucketname, mountpoint]
    if not verbose:
        mount_args.append('--quiet')
    with open(passfile, 'r') as pass_fh:
        subprocess.check_call(mount_args, stdin=pass_fh.fileno())
    try:
        if not os.path.exists(os.path.join(mountpoint, backup_dir)):
            os.mkdir(os.path.join(mountpoint, backup_dir))

        all_backups = sorted([ x for x in os.listdir(os.path.join(mountpoint, backup_dir))
                              if re.match(r'^\d{4}-\d\d-\d\d_\d\d:\d\d:\d\d$', x) ])
        if all_backups:
            last_backup = all_backups[-1]

            log.info('Replicating previous backup (%r)', last_backup)
            subprocess.check_call([os.path.join(s3ql_path, 'cp.s3ql'),
                                   os.path.join(mountpoint, backup_dir, last_backup),
                                   os.path.join(mountpoint, backup_dir, current_backup)])

        log.info('Creating new backup %r', current_backup)
        rsync_args = ['rsync', '-aHxW', '--delete-during', '--delete-excluded',
                      '--partial']
        if verbose:
            rsync_args.append('-v')
        for pat in exclusions:
            rsync_args.append('--exclude')
            rsync_args.append(pat)

        rsync_args.append(source_dir + '/')
        rsync_args.append(os.path.join(mountpoint, backup_dir, current_backup) + '/')

        subprocess.call(rsync_args)

        for name in expire_backups(all_backups):
            path = os.path.join(mountpoint, backup_dir, name)
            log.info('Removing backup %r', name)
            shutil.rmtree(path)

    finally:
        umount_args = [os.path.join(s3ql_path, 'umount.s3ql'), mountpoint]
        if not verbose:
            umount_args.append('--quiet')
        subprocess.check_call(umount_args)


def expire_backups(available_txt):
    # Get rid of microseconds
    now = datetime.strptime(datetime.now().strftime('%Y-%m-%d_%H:%M:%S'), '%Y-%m-%d_%H:%M:%S')

    # Relative generation ages
    generations = list()
    for txt_gen in backup_generations:
        if txt_gen.endswith('h'):
            generations.append(timedelta(seconds=int(txt_gen[:-1]) * 3600))
        elif txt_gen.endswith('d'):
            generations.append(timedelta(days=int(txt_gen[:-1])))
        else:
            raise RuntimeError('Cannot parse backup generation %r' % txt_gen)
    step = min(generations)
    log.info('For this backup strategy you need to backup every %s', step)

    # Backups that are available 
    available = list()
    for name in available_txt:
        age = now - datetime.strptime(name, '%Y-%m-%d_%H:%M:%S')
        available.append((name, age))
        log.info('Backup %s is %s old', name, age)
    available.sort(key=lambda x: x[1])

    if available[0][1] < step:
        log.warn('Most previous backup is %s old, but according to your backup strategy '
                 'it should be %s old before creating a new backup', available[0][1], step)

    # Backups that need to be kept
    keep = dict()

    # Go forward in time to see what backups need to be kept
    simulated = timedelta(0)
    while True:
        log.debug('Considering situation on %s', now + simulated)

        # Go through generations
        cur_age = timedelta(0)
        done = True
        for (i, min_rel_age) in enumerate(generations):
            if cur_age < simulated: # Future backup 
                cur_age = cur_age + min_rel_age
                continue
            done = False
            min_age = cur_age + min_rel_age
            for (j, age) in enumerate((a[1] for a in available)):
                if age >= min_age:
                    if j not in keep:
                        log.info('Keeping backup %s (abs. age %s, rel. age %s) '
                                 'for generation %d (abs. min. age %s, rel. min. age %s) on %s',
                                 available[j][0], age, age - cur_age, i + 1, min_age, min_rel_age,
                                 now + simulated)
                    cur_age = age
                    keep[j] = True
                    break
            else:
                if available:
                    keep[len(available) - 1] = True
                    if not simulated: # print only for current day
                        log.warn('Found no backup for generation %d (min. age: %s), using oldest available',
                                 i + 1, min_age)
                else:
                    if not simulated: # print only for current day
                        log.warn('Found no backup for generation %d (min. age: %s)',
                                 i + 1, min_age)
                break

        # Update backup ages
        simulated += step
        available = [ (a[0], a[1] + step) for a in available ]

        if done:
            break

    # Remove what's left
    return [ available[i][0] for i in range(len(available)) if i not in keep ]

def test():
    hour = timedelta(days=0, seconds=3600)
    now = datetime.strptime(datetime.now().strftime('%Y-%m-%d_%H:%M:%S'), '%Y-%m-%d_%H:%M:%S')

    all_backups = []

    for i in range(15):
        now += (24 + random.randint(-6, 6)) * hour
        for name in expire_backups(all_backups, now):
            log.warn('Deleting %s', name)
            all_backups.remove(name)
        all_backups.append(now.strftime('%Y-%m-%d_%H:%M:%S'))


if __name__ == '__main__':
    main()
    #all_backups = sorted([ x for x in os.listdir(os.path.join(mountpoint, backup_dir))
    #                          if re.match(r'^\d{4}-\d\d-\d\d_\d\d:\d\d:\d\d$', x) ])
    #all_backups.pop()
    #for name in expire_backups(all_backups):
    #        log.warn('Deleting %s', name)
    #test()
