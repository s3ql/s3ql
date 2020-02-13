#!/bin/sh

set -e

# Install fuse
sudo -H python3 -m pip install meson==0.44 --no-cache-dir
wget https://github.com/libfuse/libfuse/archive/master.zip
unzip master.zip
cd libfuse-master
mkdir build
cd build
meson ..
ninja
sudo -H ninja install
test -e /usr/local/lib/pkgconfig || sudo mkdir /usr/local/lib/pkgconfig
sudo mv /usr/local/lib/*/pkgconfig/* /usr/local/lib/pkgconfig/
ls -d1 /usr/local/lib/*-linux-gnu | sudo tee /etc/ld.so.conf.d/usrlocal.conf
sudo ldconfig

pip install https://github.com/rogerbinns/apsw/releases/download/3.8.2-r1/apsw-3.8.2-r1.zip

pip install defusedxml \
            cython \
            sphinx \
            cryptography \
            requests \
            "pyfuse3 >= 1.0, < 2.0" \
            "dugong >= 3.4, < 4.0" \
            "pytest == 4.6.5" \
            google-auth \
            google-auth-oauthlib \
            pytest_trio \
            "trio == 0.11.0" \
            "attrs == 19.3.0"

echo "Current libsqlite3-dev version: $(dpkg-query --show --showformat='${Version}' libsqlite3-dev)"

echo "Installed PIP versions:"
pip freeze
