#!/bin/bash

set -e

os="$(lsb_release --short --id)-$(lsb_release --short --release)"

sudo -- apt-get install -y \
     libsqlite3-dev \
     meson \
     ninja-build \
     psmisc


# Install latest libfuse3
(
    wget https://github.com/libfuse/libfuse/archive/master.zip
    unzip master.zip
    cd libfuse-master
    mkdir build
    cd build
    meson ..
    ninja
    sudo -H -- ninja install
)
test -e /usr/local/lib/pkgconfig || sudo mkdir /usr/local/lib/pkgconfig
sudo -- mv /usr/local/lib/*/pkgconfig/* /usr/local/lib/pkgconfig/
ls -d1 /usr/local/lib/*-linux-gnu | sudo tee /etc/ld.so.conf.d/usrlocal.conf
sudo -- ldconfig


# Upgrading cryptography results in an AttributeError in OpenSSL/crypto.py. My
# guess is that this is due to a non-declared version dependency on OpenSSL.
if [ "${os}" = "Ubuntu-20.04" ]; then
    sudo -- apt-get install -y \
         python3-cryptography
else
    pip install --upgrade --upgrade-strategy eager \
         cryptography
fi

pip install --upgrade --upgrade-strategy eager \
     apsw \
     attrs \
     cython \
     defusedxml \
     google-auth \
     google-auth-oauthlib \
     pyfuse3 \
     pytest \
     pytest_trio \
     requests \
     sphinx \
     trio

echo "Current libsqlite3-dev version: $(dpkg-query --show --showformat='${Version}' libsqlite3-dev)"

echo "Installed PIP versions:"
pip freeze
