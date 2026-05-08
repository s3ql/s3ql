.. -*- mode: rst -*-


==============
 Installation
==============


Dependencies
============

* Kernel: Linux 3.9 or newer.

* The `psmisc <http://psmisc.sf.net/>`_ utilities.

* The development headers for `SQLite <http://www.sqlite.org/>`_, version 3.7.0 or newer.

* `Python <http://www.python.org/>`_ 3.10 or newer, installed with development headers.

* Additional Python dependencies are listed in `pyproject.toml` (and your Python package installer
  should normally be able to install them automatically)

Installing from a source release
================================

Download the release tarball ``s3ql-A.B.C.tar.gz`` and its detached signature
``s3ql-A.B.C.tar.gz.sig`` from
`GitHub <https://github.com/s3ql/s3ql/releases>`_ and validate them with
`signify <https://github.com/aperezdc/signify>`_::

  signify -V -m s3ql-A.B.C.tar.gz -p s3ql-A.B.pub

The same public key is used for every bugfix release in the ``A.B`` series, so
the key file is named after the major.minor portion only (e.g. ``s3ql-6.1.pub``
for any 6.1.x release).

You need to obtain ``s3ql-A.B.pub`` from a trustworthy source for the first
release you install. After that, the trust chain is self-sustaining: each
release tarball contains the public key for the next release in its
``signify/`` directory under the name ``s3ql-next.pub``. After verifying the
tarball, extract that file and use it as ``-p`` for the following release.

After validating the tarball, unpack it and change into the newly created `s3ql-X.Y.Z` directory. At
this point, you can use any tool that can work with Python `pyproject.toml` files to install S3QL.
If you don't know which one to use, `pipx <https://pipx.pypa.io/latest/>`_ is a good choice. To install
S3QL with `pipx`, run ::

   pipx install .

This will download all necessary Python dependencies and install S3QL in its own separate
virtual environment.

For a minor performance boost, set the `PYTHONOPTIMIZE` environment variable before running S3QL commands
(perhaps by using a wrapper script). This disables the checking of debug assertions. Surprisingly,
there does not seem any way to do this automatically for a Python application (otherwise S3QL
would set this up by default).


Installing from Git / Developing S3QL
=====================================

Clone the S3QL repository and take a look at `developer_notes/setup.md`.

