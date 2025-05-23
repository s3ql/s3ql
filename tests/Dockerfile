# This is not a general purpose Dockerfile
# It's tailored for running tests and building documentation.

FROM ubuntu:jammy AS build
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
 && apt-get dist-upgrade -y \
 && apt-get install -y lsb-release sudo curl build-essential manpages \
                       git libbz2-dev libssl-dev libreadline-dev libffi-dev libsqlite3-dev \
                       zlib1g-dev libsqlite3-dev wget curl llvm libncurses5-dev xz-utils libxml2-dev libxmlsec1-dev liblzma-dev \
                       libsystemd-dev gcc psmisc pkg-config libattr1-dev libsqlite3-dev libjs-sphinxdoc texlive-latex-base \
                       texlive-latex-recommended texlive-latex-extra texlive-fonts-recommended python3-pip python3-setuptools \
                       ninja-build udev libudev1 libudev-dev meson \
 && mkdir /build \
 && ln -sf /usr/bin/pip3 /usr/bin/pip && ln -sf /usr/bin/python3 /usr/bin/python \
 && pip install "setuptools >= 40.3.0"

ADD tests/ci-prepare-latest.sh /ci-prepare-latest.sh
RUN /ci-prepare-latest.sh ubuntu-22.04

ADD . /build
WORKDIR /build
RUN ln -sf /usr/local/bin/fusermount3 /bin/fusermount \
 && python setup.py build_cython \
 && python setup.py build_ext --inplace
