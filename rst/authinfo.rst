.. -*- mode: rst -*-

.. _authinfo:

=============================================
 Storing Backend Information and Credentials
=============================================

Normally, S3QL reads username and password for the backend as well as
an encryption passphrase for the file system from the
terminal. Alternatively, these (and some other backend parameters) may
be specified in the ``~/.s3ql/authinfo2`` file, or a file specified by the
:cmdopt:`--authfile` parameter.

The authinfo file consists of sections, led by a ``[section]``
header and followed by ``name: value`` entries. The section headers
themselves are not used by S3QL but have to be unique within the file.

In each section, the following entries can be defined:

:storage-url:
  Specifies the storage url to which this section applies. If a
  storage url starts with the value of this entry, the section is
  considered applicable.

:backend-login:
  Specifies the username to use for authentication with the backend.

:backend-password:
  Specifies the password to use for authentication with the backend.

:fs-passphrase:
  Specifies the passphrase to use to decrypt the file system (if
  it is encrypted).

Furthermore, command line parameters that are shared between all S3QL
commands that work with storage URL may be specified in this file as
well by omitting the leading dashes, e.g. the ``--backend-options
notls`` parameter may be written into the authinfo file as
``backend-options: notls``.

When reading the authinfo file, S3QL considers every applicable
section in order and uses the last value that it found for each entry.
For example, consider the following authentication file::

  [s3]
  storage-url: s3://
  backend-login: joe
  backend-password: notquitesecret

  [fs1]
  storage-url: s3://joes-first-bucket
  fs-passphrase: neitheristhis

  [fs2]
  storage-url: s3://joes-second-bucket
  fs-passphrase: swordfish

  [fs3]
  storage-url: s3://joes-second-bucket/with-prefix
  backend-login: bill
  backend-password: bi23ll
  fs-passphrase: ll23bi

With this authentication file, S3QL would try to log in as "joe"
whenever the s3 backend is used, except when accessing a storage url
that begins with "s3://joes-second-bucket/with-prefix". In that case,
the last section becomes active and S3QL would use the "bill"
credentials. Furthermore, file system encryption passphrases will be used
for storage urls that start with "s3://joes-first-bucket" or
"s3://joes-second-bucket".

The authentication file is parsed by the `Python ConfigParser
module <http://docs.python.org/library/configparser.html>`_.
