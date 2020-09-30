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

function validate_logfile() {
  if [ "$LOG_FILE" == "syslog" ]; then
    echo "LOG_FILE: $LOG_FILE <- syslog is not supported!"
    exit 1
  fi
}

# credits to Daniel Jagszent, from: https://github.com/s3ql/s3ql/issues/191#issuecomment-662550891
function verify_clean_mountpoint() {
  echo "$MOUNT_POINT exists already, verifying that it isn't still mounted."

  CRASHED_MOUNTPOINT_CHECK="$(ls "$MOUNT_POINT" 2>&1)"
  if echo "$CRASHED_MOUNTPOINT_CHECK" | grep -Fq 'Transport endpoint is not connected'; then
    echo "It seems like $MOUNT_POINT was not cleanly unmounted! Trying to unmount..."
    if fusermount3 -u "$MOUNT_POINT"; then
      echo "Unmounted crashed filesystem"
    else
      FUSERMOUNT_CRASHED_RETCODE=$?
      echo "Failed to unmount crashed filesystem with return code $FUSERMOUNT_CRASHED_RETCODE"
      exit $FUSERMOUNT_CRASHED_RETCODE
    fi
  fi
}

function run_fsck() {
  fsck_args=()

  if [ -n "$LOG_FILE" ]; then
    debug_log "LOG_FILE: $LOG_FILE"
    fsck_args+=("--log" "$LOG_FILE")
  fi

  if [ -n "$CACHE_DIR" ]; then
    debug_log "CACHE_DIR: $CACHE_DIR"
    fsck_args+=("--cachedir" "$CACHE_DIR")
  fi

  if [ -n "$BACKEND_OPTIONS" ]; then
    debug_log "BACKEND_OPTIONS: $BACKEND_OPTIONS"
    fsck_args+=("--backend-options" "$BACKEND_OPTIONS")
  fi

  if [ -n "$AUTH_FILE" ]; then
    debug_log "AUTH_FILE: $AUTH_FILE"
    chmod 600 "$AUTH_FILE"
    fsck_args+=("--authfile" "$AUTH_FILE")
  fi

  if [ -n "$COMPRESS" ]; then
    debug_log "COMPRESS: $COMPRESS"
    fsck_args+=("--compress" "$COMPRESS")
  fi

  if [ "$KEEP_CACHE" == "true" ]; then
    debug_log "KEEP_CACHE: $KEEP_CACHE"
    fsck_args+=("--keep-cache")
  fi

  if [ "$FSCK_FORCE" == "true" ]; then
    debug_log "FSCK_FORCE: $FSCK_FORCE"
    fsck_args+=("--force")
  fi

  if [ "$VERBOSE" == "true" ]; then
    debug_log "VERBOSE: $VERBOSE"
    fsck_args+=("--debug")
  fi

  (
    set -x
    fsck.s3ql \
      --batch \
      "${fsck_args[@]}" \
      "$STORAGE_URL"
  )

  FSCK_RESULT=$?
  if [[ $FSCK_RESULT != 0 && $FSCK_RESULT != 128 ]]; then
    echo "fsck.s3ql reported errors! exit code $FSCK_RESULT"
    exit $FSCK_RESULT
  fi
}

validate_logfile

if [ -d "$MOUNT_POINT" ]; then
  verify_clean_mountpoint
fi

run_fsck
