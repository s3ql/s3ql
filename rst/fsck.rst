.. -*- mode: rst -*-

Checking for Errors
===================

It is recommended to periodically run the :program:`fsck.s3ql` and
:program:`s3ql_verify` commands (in this order) to ensure that the
file system is consistent, and that there has been no data corruption
or data loss in the storage backend.

:program:`fsck.s3ql` is intended to detect and correct problems with
the internal file system structure, caused by e.g. a file system crash
or a bug in S3QL. It assumes that the storage backend can be fully
trusted, i.e. if the backend reports that a specific storage object
exists, :program:`fsck.s3ql` takes that as proof that the data is
present and intact.

In contrast to that, the :program:`s3ql_verify` command is intended to
check the consistency of the storage backend. It assumes that the
internal file system data is correct, and verifies that all data can
actually be retrieved from the backend. Running :program:`s3ql_verify`
may therefore take much longer than running :program:`fsck.s3ql`.


Checking and repairing internal file system errors
--------------------------------------------------

:program:`fsck.s3ql` checks that the internal file system structure is
consistent and attempts to correct any problems it finds. If an S3QL
file system has not been unmounted correcly for any reason, you need
to run :program:`fsck.s3ql` before you can mount the file system
again.

The :program:`fsck.s3ql` command has the following syntax::

 fsck.s3ql [options] <storage url>

This command accepts the following options:

.. pipeinclude:: python ../bin/fsck.s3ql --help
   :start-after: show this help message and exit


.. _s3ql_verify:

Detecting and handling backend data corruption
----------------------------------------------

The :program:`s3ql_verify` command verifies all data in the file
system.  In contrast to :program:`fsck.s3ql`, :program:`s3ql_verify`
does not trust the object listing returned by the backend, but
actually attempts to retrieve every object. By default,
:program:`s3ql_verify` will attempt to retrieve just the metadata for
every object (for e.g. the S3-compatible or Google Storage backends
this corresponds to a ``HEAD`` request for each object), which is
generally sufficient to determine if the object still exists. When
specifying the :cmdopt:`--data` option, :program:`s3ql_verify` will
instead read every object entirely. To determine how much data will be
transmitted in total when using :cmdopt:`--data`, look at the *After
compression* row in the :ref:`s3qlstat <s3qlstat>` output.

:program:`s3ql_verify` is not able to correct any data corruption that
it finds. Instead, a list of the corrupted and/or missing objects is
written to a file and the decision about the proper course of action
is left to the user. If you have administrative access to the backend
server, you may want to investigate the cause of the corruption or
check if the missing/corrupted objects can be restored from
backups. If you believe that the missing/corrupted objects are indeed
lost irrevocably, you can use the :ref:`remove_objects` script (from
the :file:`contrib` directory of the S3QL distribution) to explicitly
delete the objects from the storage backend. After that, you should
run :program:`fsck.s3ql`. Since the (now explicitly deleted) objects
should now no longer be included in the object index reported by the
backend, :program:`fsck.s3ql` will identify the objects as missing,
update the internal file system structures accordingly, and move the
affected files into the :file:`lost+found` directory.

The :program:`s3ql_verify` command has the following syntax::

 s3ql_verify [options] <storage url>

This command accepts the following options:

.. pipeinclude:: python ../bin/s3ql_verify --help
   :start-after: show this help message and exit

