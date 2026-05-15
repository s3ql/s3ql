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

  signify -V -m s3ql-A.B.C.tar.gz -p <public-key>

The public key must be obtained from a trustworthy source for the first installation. After that,
the trust chain is self-sustaining: each release tarball's ``signify/`` directory contains public
keys for future releases. Bugfix releases (``A.B.C``) are signed with ``s3ql-A.B.pub``; new major
or minor releases (``A.B.0``) are signed with ``s3ql-next.pub``. After verifying a tarball, extract
the relevant key from its ``signify/`` directory and keep it for the next verification.

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

