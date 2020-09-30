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
    exit $?
  fi
}

VERSION="$(fsck.s3ql --version)"
echo "Starting $VERSION"
echo ""

echo "Will use storage-url '$STORAGE_URL' and mountpoint '$MOUNT_POINT'"
echo ""

echo "*** Init ***"
trap shutdown INT # set shutdown hook on keyboard interrupt, SIGINT (`docker stop`'s SIGTERM is rewritten as SIGINT by dumb-init)
if /s3ql/bin/0-init.sh; then
  echo "Init done."
else
  exit $?
fi
echo ""

echo "*** Start ***"
if /s3ql/bin/1-start.sh; then
  echo "Successfully started."
else
  exit $?
fi
echo ""

LOGS="${LOG_FILE:-"~/.s3ql/mount.log"}"
tail -f "$LOGS"
wait $!
