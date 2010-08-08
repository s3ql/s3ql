#!/bin/bash

# Abort entire script if any command fails
set -e

# Backup destination  (storage url)
bucket="s3://my_backup_bucket"

# Recover cache if e.g. system was shut down while fs was mounted
fsck.s3ql --batch "$bucket"

# Create a temporary mountpoint and mount file system
mountpoint="/tmp/s3ql_backup_$$"
mkdir "$mountpoint"
mount.s3ql "$bucket" "$mountpoint"

# Make sure the file system is unmounted when we are done
trap "cd /; umount.s3ql '$mountpoint'; rmdir '$mountpoint'" EXIT

# Figure out the most recent backup
cd "$mountpoint"
last_backup=`python <<EOF
import os
import re
backups=sorted(x for x in os.listdir('.') if re.match(r'^[\\d-]{10}_[\\d:]{8}$', x))
if backups:
    print backups[-1]
EOF`

# Duplicate the most recent backup unless this is the first backup
new_backup=`date "+%Y-%m-%d_%H:%M:%S"`
if [ -n "$last_backup" ]; then
    echo "Copying $last_backup to $new_backup..."
    s3qlcp "$last_backup" "$new_backup"

    # Make the last backup immutable
    # (in case the previous backup was interrupted prematurely)
    s3qllock "$last_backup"
fi

# ..and update the copy
rsync -aHAXx --delete-during --delete-excluded --partial -v \
    --exclude /.cache/ \
    --exclude /.s3ql/ \
    --exclude /.thumbnails/ \
    --exclude /tmp/ \
    "/home/my_username/" "./$new_backup/"

# Make the new backup immutable
s3qllock "$new_backup"

# Expire old backups, keep 6 generations with specified relative ages
expire_backups --use-s3qlrm 9h 1d 7d 14d 31d 150d 150d
