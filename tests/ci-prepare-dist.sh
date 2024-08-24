#!/bin/bash

set -e

PIP="sudo python3 -m pip"
if [ -f venv/bin/activate ]; then
  . venv/bin/activate
  PIP="pip"
fi

os="$(lsb_release --short --id)-$(lsb_release --short --release)"

sudo apt install -y \
     cython3 \
     fuse3 \
     libfuse3-dev \
     libsqlite3-dev \
     psmisc \
     python3-apsw \
     python3-cryptography \
     python3-defusedxml \
     python3-dev \
     python3-google-auth \
     python3-pip \
     python3-pytest \
     python3-requests \
     python3-trio

# The python3-attr that is shipped with Ubuntu 20.04 is incompatible
# with python3-trio. *sigh*
if [ "${os}" = "Ubuntu-20.04" ]; then
    $PIP install "attrs >= 20.1.0, < 21.0.0 "
else
    sudo apt install -y python3-attr
fi


# Where packages are not available, download from pypi instead (but pin to specific versions).
if [ "${os}" = "Ubuntu-24.04" ]; then
  sudo apt install -y python3-pytest-trio
else
  $PIP install "pytest_trio == 0.6.0"
fi

if [ "${os}" = "Ubuntu-20.04" ]; then
    $PIP install \
         "pyfuse3 >= 3.2.0, < 4.0" \
         "google-auth-oauthlib >= 0.4.0, < 0.5.0"
else
    sudo apt install -y \
         python3-pyfuse3 \
         python3-google-auth-oauthlib
fi

echo "Current libsqlite3-dev version: $(dpkg-query --show --showformat='${Version}' libsqlite3-dev)"

echo "Installed PIP versions:"
$PIP freeze
