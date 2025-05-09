#!/bin/bash

set -e

os="$(lsb_release --short --id)-$(lsb_release --short --release)"

sudo apt install -y \
     fuse3 \
     libfuse3-dev \
     libsqlite3-dev \
     psmisc

echo "Current libsqlite3-dev version: $(dpkg-query --show --showformat='${Version}' libsqlite3-dev)"
