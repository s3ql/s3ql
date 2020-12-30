#!/bin/sh

set -e

# Install fuse
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

# We're pinning most packages to specific versions to prevent the CI from failing when
# testing eg merge requests because some of those packages have started emitting
# depreciation warnings or made backwards incompatible changes.
pip install defusedxml \
            cython \
            sphinx \
            cryptography \
            requests \
            google-auth \
            google-auth-oauthlib \
            "attrs >= 19.3.0, < 20.0.0 " \
            "pyfuse3 >= 3.2.0, < 4.0" \
            "dugong >= 3.4, < 4.0" \
            "pytest >= 4.6.5, < 5.0.0" \
            "pytest_trio == 0.6.0" \
            "trio == 0.15"

echo "Current libsqlite3-dev version: $(dpkg-query --show --showformat='${Version}' libsqlite3-dev)"

echo "Installed PIP versions:"
pip freeze
