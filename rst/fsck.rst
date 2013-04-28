.. -*- mode: rst -*-


Checking for Errors
===================

If, for some reason, the filesystem has not been correctly unmounted,
or if you suspect that there might be errors, you should run the
`fsck.s3ql` utility. It has the following syntax::

 fsck.s3ql [options] <storage url>

This command accepts the following options:

.. pipeinclude:: python ../bin/fsck.s3ql --help
   :start-after: show this help message and exit
