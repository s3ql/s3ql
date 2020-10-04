#!/usr/bin/env bash

# This file is part of S3QL.
#
# Copyright (c) 2008 Nikolaus Rath <Nikolaus@rath.org>
#
# This work can be distributed under the terms of the GNU GPLv3.

VERSION="$(fsck.s3ql --version)"
echo "Starting $VERSION"
echo ""

echo "Will use storage-url '$S3QL_STORAGE_URL' and mountpoint '$S3QL_MOUNTPOINT'"
echo ""

echo "*** Init ***"
/s3ql/bin/init_fsck.sh || exit $?
echo ""

echo "*** Start ***"
mount.s3ql "$S3QL_STORAGE_URL" "$S3QL_MOUNTPOINT"
