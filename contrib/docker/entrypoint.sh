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

function shutdown() {
  echo ""
  echo "*** Shutdown ***"
  if /s3ql/bin/2-shutdown.sh; then
    echo "Shutdown successful."
  else
    echo "Shutdown failed!"
    exit 1
  fi
}

VERSION="$(fsck.s3ql --version)"
echo "Starting $VERSION"
echo ""

echo "Will use storage-url '$STORAGE_URL' and mountpoint '$MOUNT_POINT'"
echo ""

echo "*** Init ***"
trap shutdown TERM INT # set shutdown hook on SIGTERM/SIGINT (`docker stop` and Ctrl+C)
if /s3ql/bin/0-init.sh; then
  echo "Init done."
else
  echo "Init failed."
  exit 1
fi
echo ""

echo "*** Start ***"
if /s3ql/bin/1-start.sh; then
  echo "Successfully started."
else
  echo "Start failed."
  exit 1
fi
echo ""

while true; do
  LOGS="${LOG_FILE:-"~/.s3ql/mount.log"}"
  tail -f "$LOGS"
  wait $!
done
