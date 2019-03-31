.. -*- mode: rst -*-


==============
 Installation
==============

S3QL depends on several other programs and libraries that have to be
installed first. The best method to satisfy these dependencies depends
on your distribution.


Dependencies
============

The following is a list of the programs and libraries required for
running S3QL. Generally, you should first check if your distribution
already provides a suitable packages and only install from source if
that is not the case.

* Kernel: Linux 2.6.9 or newer or FreeBSD with `FUSE4BSD
  <http://www.freshports.org/sysutils/fusefs-kmod/>`_. Starting with
  kernel 2.6.26 you will get significantly better write performance,
  so under Linux you should actually use *2.6.26 or newer whenever
  possible*.

* The `psmisc <http://psmisc.sf.net/>`_ utilities.

* `SQLite <http://www.sqlite.org/>`_ version 3.7.0 or newer. SQLite
  has to be installed as a *shared library* with development headers.

* `Python <http://www.python.org/>`_ 3.5.0 or newer. Make sure to also
  install the development headers.

* The following Python modules:

  * `setuptools <https://pypi.python.org/pypi/setuptools>`_, version 1.0 or newer.
  * `cryptography <https://cryptography.io/en/latest/installation/>`_
  * `defusedxml <https://pypi.python.org/pypi/defusedxml/>`_
  * `apsw <https://github.com/rogerbinns/apsw>`_, version 3.7.0 or
    newer.
  * `llfuse <https://pypi.org/project/llfuse/>`_, any
    version between 1.0 (inclusive) and 2.0 (exclusive)
  * `dugong <https://pypi.org/project/dugong/>`_, any
    version between 3.4 (inclusive) and 4.0 (exclusive)
  * `pytest <http://pytest.org/>`_, version 3.7 or newer (optional, to run unit tests)
  * `systemd <https://github.com/systemd/python-systemd>`_ (optional,
    for enabling systemd support). Do *not* install the module from
    PyPi, this is from a third-party developer and incompatible with
    the official module from the systemd developers.
  * `requests <https://pypi.python.org/pypi/requests/>`_ (optional,
    required for OAuth2 authentication with Google Storage)
  * `google-auth <https://pypi.python.org/project/google-auth/>`_
    (optional, required for ADC authentication with Google Storage)
  * `google-auth-oauthlib <https://pypi.python.org/project/google-auth-oauthlib/>`_
    (optional, required for browser-based authentication with Google Storage)

  To check if a specific module :var:`<module>` is installed, execute
  :samp:`python3 -c 'import {<module>};
  print({<module>}.__version__)'`. This will result in an
  `ImportError` if the module is not installed, and will print the
  installed version if the module is installed.


.. _inst-s3ql:

Installing S3QL
===============

To build and install S3QL itself, proceed as follows:

1. Download S3QL from https://github.com/s3ql/s3ql/releases
2. Unpack it into a folder of your choice
3. Run `python3 setup.py build_ext --inplace` to build S3QL.
4. Run `python3 -m pytest tests/` to run a self-test. If this fails, ask
   for help on the `mailing list
   <http://groups.google.com/group/s3ql>`_ or report a bug in the
   `issue tracker <https://github.com/s3ql/s3ql/issues>`_.

Now you have three options:

* You can run the S3QL commands from the `bin/` directory.

* You can install S3QL system-wide for all users. To do that, you
  have to run `sudo python3 setup.py install`.

* You can install S3QL into `~/.local` by executing `python3
  setup.py install --user`. In this case you should make sure that
  `~/.local/bin` is in your `$PATH` environment variable.


Development Version
===================

If you have checked out the unstable development version from the
Mercurial repository, a bit more effort is required. You'll also need:

* Version 0.24 or newer of the Cython_ compiler.

* Version 1.2b1 or newer of the Sphinx_ document processor.

With these additional dependencies installed, S3QL can be build and
tested with ::

  python3 setup.py build_cython
  python3 setup.py build_ext --inplace
  python3 -m pytest test

Note that when building from the Mercurial or Git repository, building
and testing is done with several additional checks. This may cause
compilation and/or tests to fail even though there are no problems
with functionality. For example, any use of functions that are
scheduled for deprecation in future Python version will cause tests to
fail. If you would rather just check for functionality, you can delete
the :file:`MANIFEST.in` file. In that case, the build system will
behave as it does for a regular release.

The HTML and PDF documentation can be generated with ::

  python3 setup.py build_sphinx

and S3QL can be installed as usual with ::

  python3 setup.py install [--user]


Running tests requiring remote servers
======================================

By default, tests requiring a connection to a remote storage backend
are skipped. If you would like to run these tests too (which is always
a good idea), you have to create additional entries in your
`~/.s3ql/authinfo2` file that tell S3QL what server and credentials to
use for these tests. These entries have the following form::

  [<BACKEND>-test]
  backend-login: <user>
  backend-password: <password>
  test-fs: <storage-url>

Here *<BACKEND>* specifies the backend that you want to test
(e.g. *s3*, *s3c*, *gs*, or *swift*), *<user>* and *<password>* are
the backend authentication credentials, and *<storage-url>* specifies
the full storage URL that will be used for testing. **Any existing
S3QL file system in this storage URL will be destroyed during
testing**.

For example, to run tests that need connection to a Google Storage
server, you would add something like ::

  [gs-test]
  backend-login: GOOGIGWLONT238MD7HZ4
  backend-password: rmEbstjscoeunt1249oes1298gauidbs3hl
  test-fs: gs://joes-gs-bucket/s3ql_tests/

On the next run of `runtest.py` (or `py.test` when using the
development version), the additional tests will be run. If the tests
are still skipped, you can get more information about why tests are
being skipped by passing the :cmdopt:`-rs` argument to
`runtest.py`/`py.test`.


.. _Cython: http://www.cython.org/
.. _Sphinx: http://sphinx.pocoo.org/
