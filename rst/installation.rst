.. -*- mode: rst -*-


==============
 Installation
==============

S3QL depends on several other programs and libraries that have to be
installed first. The best method to satisfy these dependencies depends
on your distribution. In some cases S3QL and all its dependencies can
be installed with as little as three commands, while in other cases more work
may be required.

The `S3QL Wiki <http://code.google.com/p/s3ql/w/list>`_ contains
installation instructions for quite a few different Linux
distributions. You should only use the generic instructions in this
manual if your distribution is not included in the `distribution-specific
installation instructions
<http://code.google.com/p/s3ql/w/list?q=label:Installation>`_ on the wiki.


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

* `Python <http://www.python.org/>`_, version 2.6.6 or newer, but not
  Python 3.x.
  
* The `PyCrypto++ Python Module
  <http://pypi.python.org/pypi/pycryptopp>`_. To check if this module
  is installed, try to execute `python -c 'import pycryptopp'`. 

* The `argparse Python Module
  <http://pypi.python.org/pypi/argparse>`_. To check if this module is
  installed, try to execute `python -c 'import argparse; print
  argparse.__version__'`. If argparse is installed, this will print
  the version number. You need version 1.1 or later.
  
* The `APSW Python Module <http://code.google.com/p/apsw/>`_. To check
  which (if any) version of APWS is installed, run the command ::

    python -c 'import apsw; print apsw.apswversion(), apsw.sqlitelibversion()'

  If APSW is installed, this should print two version numbers which
  both have to be at least 3.7.0.

* The `PyLibLZMA Python module
  <http://pypi.python.org/pypi/pyliblzma>`_. To check if this module
  is installed, execute `python -c 'import lzma; print
  lzma.__version__'`. This should print a version number. You need at
  least version 0.5.3.

* The `Python LLFUSE module
  <http://code.google.com/p/python-llfuse/>`_. To check if this module
  is installed, execute `python -c 'import llfuse; print
  llfuse.__version__'`. This should print a version number. You need at
  least version 0.29.

  Note that earlier S3QL versions shipped with a builtin version of
  this module. If you are upgrading from such a version, make sure to
  completely remove the old S3QL version first.

.. _inst-s3ql:

Installing S3QL
===============

To install S3QL itself, proceed as follows:

1. Download S3QL from http://code.google.com/p/s3ql/downloads/list
2. Unpack it into a folder of your choice
3. Run `python setup.py test` to run a self-test. If this fails, ask
   for help on the `mailing list
   <http://groups.google.com/group/s3ql>`_ or report a bug in the
   `issue tracker <http://code.google.com/p/s3ql/issues/list>`_.

Now you have three options:

* You can run the S3QL commands from the `bin/` directory.

* You can install S3QL system-wide for all users. To do that, you
  have to run `sudo python setup.py install`.

* You can install S3QL into `~/.local` by executing `python
  setup.py install --user`. In this case you should make sure that
  `~/.local/bin` is in your `$PATH` environment variable.
