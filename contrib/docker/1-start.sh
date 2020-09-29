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

mount_s3ql_args=()

if [ -n "$LOG_FILE" ]; then
  debug_log "LOG_FILE: $LOG_FILE"
  mount_s3ql_args+=("--log" "$LOG_FILE")
fi

if [ -n "$CACHE_DIR" ]; then
  debug_log "CACHE_DIR: $CACHE_DIR"
  mount_s3ql_args+=("--cachedir" "$CACHE_DIR")
fi

if [ -n "$BACKEND_OPTIONS" ]; then
  debug_log "BACKEND_OPTIONS: $BACKEND_OPTIONS"
  mount_s3ql_args+=("--backend-options" "$BACKEND_OPTIONS")
fi

if [ -n "$AUTH_FILE" ]; then
  debug_log "AUTH_FILE: $AUTH_FILE"
  mount_s3ql_args+=("--authfile" "$AUTH_FILE")
fi

if [ -n "$COMPRESS" ]; then
  debug_log "COMPRESS: $COMPRESS"
  mount_s3ql_args+=("--compress" "$COMPRESS")
fi

if [ -n "$CACHE_SIZE" ]; then
  debug_log "CACHE_SIZE: $CACHE_SIZE"
  mount_s3ql_args+=("--cachesize" "$CACHE_SIZE")
fi

if [ -n "$MAX_CACHE_ENTRIES" ]; then
  debug_log "MAX_CACHE_ENTRIES: $MAX_CACHE_ENTRIES"
  mount_s3ql_args+=("--max-cache-entries" "$MAX_CACHE_ENTRIES")
fi

if [ "$KEEP_CACHE" == "true" ]; then
  debug_log "KEEP_CACHE: $KEEP_CACHE"
  mount_s3ql_args+=("--keep-cache")
fi

if [ "$ALLOW_OTHER" == "true" ]; then
  debug_log "ALLOW_OTHER: $ALLOW_OTHER"
  mount_s3ql_args+=("--allow-other")
fi

if [ "$ALLOW_ROOT" == "true" ]; then
  debug_log "ALLOW_ROOT: $ALLOW_ROOT"
  mount_s3ql_args+=("--allow-root")
fi

if [ -n "$FS_NAME" ]; then
  debug_log "FS_NAME: $FS_NAME"
  mount_s3ql_args+=("--fs-name" "$FS_NAME")
fi

if [ -n "$METADATA_UPLOAD_INTERVAL" ]; then
  debug_log "METADATA_UPLOAD_INTERVAL: $METADATA_UPLOAD_INTERVAL"
  mount_s3ql_args+=("--metadata-upload-interval" "$METADATA_UPLOAD_INTERVAL")
fi

if [ -n "$UPLOAD_THREADS" ]; then
  debug_log "UPLOAD_THREADS: $UPLOAD_THREADS"
  mount_s3ql_args+=("--threads" "$UPLOAD_THREADS")
fi

if [ "$NFS" == "true" ]; then
  debug_log "NFS: $NFS"
  mount_s3ql_args+=("--nfs")
fi

if [ "$VERBOSE" == "true" ]; then
  debug_log "VERBOSE: $VERBOSE"
  mount_s3ql_args+=("--debug")
fi

if ! (
  set -x
  mount.s3ql \
    "${mount_s3ql_args[@]}" \
    "$STORAGE_URL" \
    "$MOUNT_POINT"
); then
  echo "Failed mount!"
  exit 1
fi
