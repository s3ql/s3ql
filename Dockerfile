FROM fedora:32

# system dependencies
RUN dnf install -y \
  dumb-init \
  fuse3 \
  fuse3-devel \
  git \
  gcc \
  procps-ng \
  python \
  python-pip \
  python-devel \
  psmisc \
  sqlite \
  sqlite-devel \
  && dnf clean all

# install python requirements
ENV PATH /root/.local/bin:$PATH
RUN pip install --user wheel setuptools
ADD requirements.txt /requirements.txt
RUN pip install --user -r requirements.txt

# build and install s3ql
ADD . /s3ql-src
WORKDIR /s3ql-src
RUN python setup.py build_cython \
  && python3 setup.py build_ext --inplace \
  && python3 setup.py install --user \
  && cd /root \
  && rm -rf /s3ql-src
WORKDIR /root

# add docker entrypoint and scripts
ADD contrib/docker/*.sh /s3ql/bin/

# default docker-specific overrides
ENV S3QL_LOG "none"
ENV S3QL_MOUNT_ALLOW_OTHER "true"
ENV S3QL_MOUNT_FG "true"
ENV S3QL_FSCK_BATCH "true"

# use dumb-init to make sure signals to the container are proxied to the entrypoint script
ENTRYPOINT [ "dumb-init", "--rewrite", "15:2", "--" ]
CMD [ "/s3ql/bin/entrypoint.sh" ]
