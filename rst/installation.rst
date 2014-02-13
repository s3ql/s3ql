.. -*- mode: rst -*-


==============
 Installation
==============

S3QL depends on several other programs and libraries that have to be
installed first. The best method to satisfy these dependencies depends
on your distribution. In some cases S3QL and all its dependencies can
be installed with as little as three commands, while in other cases more work
may be required.

The `S3QL Wiki <https://bitbucket.org/nikratio/s3ql/wiki/Home>`_
contains installation instructions for quite a few different Linux
distributions. You should only use the generic instructions in this
manual if your distribution is not included in the
`distribution-specific installation instructions
<https://bitbucket.org/nikratio/s3ql/wiki/Installation>`_ on the wiki.


Note that there are two branches of S3QL. The *maint-1.x* branch
(version numbers *1.x*) is no longer actively developed and receives
only selected high-impact bugfixes. It is provided for systems without
Python 3 support. For systems with Python 3.3 or newer, it is
recommended run the *default* S3QL branch (with version numbers
*2.x*). This branch is actively developed and has a number of new
features that are not available in the *1.x* versions.

The following instructions are for S3QL |version|.

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

* `Python <http://www.python.org/>`_ 3.3.0 or newer. Make sure to also
  install the development headers.

* The `setuptools Python Module
  <https://pypi.python.org/pypi/setuptools>`_, version 1.0 or newer.
  To check if with version (if any) of this module is installed, try
  to execute ::

    python -c 'import setuptools; print(setuptools.__version__)'

* The `PyCrypto Python Module
  <https://www.dlitz.net/software/pycrypto/>`_. To check if this
  module is installed, try to execute `python -c 'import Crypto'`.
  
* `SQLite <http://www.sqlite.org/>`_ version 3.7.0 or newer. SQLite
  has to be installed as a *shared library* with development headers.

* The `APSW Python Module <http://code.google.com/p/apsw/>`_. To check
  which (if any) version of APWS is installed, run the command ::

    python -c 'import apsw; print(apsw.apswversion())'

  The printed version number should be at least 3.7.0. 

* The `Python LLFUSE module
  <http://code.google.com/p/python-llfuse/>`_. To check if this module
  is installed, execute `python -c 'import llfuse;
  print(llfuse.__version__)'`. This should print a version number. You
  need at least version 0.39.

* The `Python dugong module
  <https://bitbucket.org/nikratio/python-dugong/>`_. To check if this module
  is installed, try to execute `python -c 'import dugong'`.
  
.. _inst-s3ql:

Installing S3QL
===============

To install S3QL itself, proceed as follows:

1. Download S3QL from https://bitbucket.org/nikratio/s3ql/downloads
2. Unpack it into a folder of your choice
3. Run `python3 setup.py build_ext --inplace` to build S3QL.
4. Run `python3 runtests.py tests` to run a self-test. If this fails, ask
   for help on the `mailing list
   <http://groups.google.com/group/s3ql>`_ or report a bug in the
   `issue tracker <https://bitbucket.org/nikratio/s3ql/issues>`_.

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

* Version 0.17 or newer of the Cython_ compiler.

* Version 1.2b1 or newer of the Sphinx_ document processor.

* The `py.test`_ testing tool, version 2.3.3 or newer.

With these additional dependencies installed, S3QL can be build and
tested with ::

  python3 setup.py build_cython
  python3 setup.py build_ext --inplace
  py.test tests/

Note that when building from the Mercurial repository, building and
testing is done with several additional checks. This may cause
compilation and/or tests to fail even though there are no problems
with functionality. For example, when building from the Mercurial
repository, any use of functions that are scheduled for deprecation in
future Python version will cause tests to fail. If you would rather
just check for functionality, you can delete the :file:`MANIFEST.in`
file. In that case, the build system will behave as it does for a
regular release.

The HTML and PDF documentation can be generated with ::
  
  python3 setup.py build_sphinx

and S3QL can be installed as usual with ::

  python3 setup.py install [--user]


Running tests requiring remote servers
======================================

By default, the `runtest.py` (or `py.test`) script skips all tests
that require connection to a remote storage backend. If you would like
to run these tests too (which is always a good idea), you have to
create additional entries in your `~/.s3ql/authinfo2` file that tell
S3QL what server and credentials to use for these tests. These entries
have the following form::

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
.. _py.test: http://pytest.org/
