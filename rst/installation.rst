.. -*- mode: rst -*-


==============
 Installation
==============


Dependencies
============

* Kernel: Linux 3.9 or newer.

* The `psmisc <http://psmisc.sf.net/>`_ utilities.

* The development headers for `SQLite <http://www.sqlite.org/>`_, version 3.7.0 or newer.

* `Python <http://www.python.org/>`_ 3.8 or newer, installed with development headers.

* Additional Python dependencies are listed in `pyproject.toml` (and your Python package installer
  should normally be able to install them automatically)

Installing from a source release
================================

Download the release tarball from
`GitHub <https://github.com/s3ql/s3ql/releases>`_ and validate it with
`signify <https://github.com/aperezdc/signify>`_::

  signify -V -m s3ql-XX.tar.gz -p s3ql-XX.pub

The `s3ql-XX.pub` file needs to be obtained from a trustworthy source (it contains the
signing key). Each S3QL release contains the signing key for the release after it in the
`signify` directory, so you only need to manually acquire this file once when you install
S3QL for the first time).

After validating the tarball, unpack it and change into the newly created `s3ql-X.Y.Z` directory. At
this point, you can use any tool that can work with Python `pyproject.toml` files to install S3QL.
If you don't know which one to use, `pipx <https://pipx.pypa.io/latest/>`_ is a good choice. To install
S3QL with `pipx`, run ::

   pipx install .

This will download all necessary Python dependencies and install S3QL in its own separate
virtual environment.


Installing from Git / Developing S3QL
=====================================

Clone the S3QL repository and take a look at `developer_notes/setup.md`.

