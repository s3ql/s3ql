#!/usr/bin/env bash

# This file is part of S3QL.
#
# Copyright (c) 2008 Nikolaus Rath <Nikolaus@rath.org>
#
# This work can be distributed under the terms of the GNU GPLv3.

function debug_log() {
  if [ "$VERBOSE" == "true" ]; then
    echo "$1"
  fi
}

umount_args=()

if [ -n "$LOG_FILE" ]; then
  debug_log "LOG_FILE: $LOG_FILE"
  umount_args+=("--log" "$LOG_FILE")
fi

if [ "$UMOUNT_LAZY" == "true" ]; then
  debug_log "UMOUNT_LAZY: $UMOUNT_LAZY"
  umount_args+=("--lazy")
fi

if [ "$VERBOSE" == "true" ]; then
  debug_log "VERBOSE: $VERBOSE"
  umount_args+=("--debug")
fi

(
  set -x
  umount.s3ql \
    "${umount_args[@]}" \
    "$MOUNT_POINT"
)
UMOUNT_RETCODE=$?

if [ $UMOUNT_RETCODE == 0 ]; then
  echo "Successfully unmounted."
  exit 0
fi

echo "Failed to unmount using umount.s3ql, exited with code: $UMOUNT_RETCODE. Will attempt to unmount using fusermount..."
(
  set -x
  fusermount3 -u "$MOUNT_POINT"
)

FUSERMOUNT_RETCODE=$?
if [ $FUSERMOUNT_RETCODE != 0 ]; then
  echo "Failed to unmount using fusermount, exited with code: $FUSERMOUNT_RETCODE."
  echo "Assuming filesystem is busy or backend is unreachable, and executing lazy unmount."
  (
    set -x
    fusermount3 -u -z "$MOUNT_POINT"
  )

  FUSERMOUNT_LAZY_RETCODE=$?
  if [ $FUSERMOUNT_LAZY_RETCODE != 0 ]; then
    echo "Failed to lazily unmount filesystem, exited with code: $FUSERMOUNT_LAZY_RETCODE"
  else
    echo "Lazily unmounted filesystem. Will finish syncing as soon as possible."
  fi
else
  echo "Succeeded unmounting with fusermount. However, this is an asynchronous process, so waiting for it "
fi
