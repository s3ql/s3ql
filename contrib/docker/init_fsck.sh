#!/usr/bin/env bash

# This file is part of S3QL.
#
# Copyright (c) 2008 Nikolaus Rath <Nikolaus@rath.org>
#
# This work can be distributed under the terms of the GNU GPLv3.

# credits to Daniel Jagszent, from: https://github.com/s3ql/s3ql/issues/191#issuecomment-662550891
function verify_clean_mountpoint() {
  echo "$S3QL_MOUNTPOINT exists already, verifying that it isn't still mounted."

  CRASHED_MOUNTPOINT_CHECK="$(ls "$S3QL_MOUNTPOINT" 2>&1)"
  if echo "$CRASHED_MOUNTPOINT_CHECK" | grep -Fq 'Transport endpoint is not connected'; then
    echo "It seems like $S3QL_MOUNTPOINT was not cleanly unmounted! Trying to unmount..."
    if fusermount3 -u "$S3QL_MOUNTPOINT"; then
      echo "Unmounted crashed filesystem"
    else
      FUSERMOUNT_CRASHED_RETCODE=$?
      echo "Failed to unmount crashed filesystem with return code $FUSERMOUNT_CRASHED_RETCODE"
      exit $FUSERMOUNT_CRASHED_RETCODE
    fi
  fi
}

function run_fsck() {
  fsck.s3ql "$S3QL_STORAGE_URL"
  FSCK_RESULT=$?
  if [[ $FSCK_RESULT != 0 && $FSCK_RESULT != 128 ]]; then
    echo "fsck.s3ql reported errors! exit code $FSCK_RESULT"
    exit $FSCK_RESULT
  fi
}

if [ -d "$S3QL_MOUNTPOINT" ]; then
  verify_clean_mountpoint
fi

run_fsck
