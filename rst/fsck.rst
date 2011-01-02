.. -*- mode: rst -*-


Checking for Errors
===================

If, for some reason, the filesystem has not been correctly unmounted,
or if you suspect that there might be errors, you should run the
`fsck.s3ql` utility. It has the following syntax::

 fsck.s3ql [options] <storage url>

This command accepts the following options:

.. include:: autogen/fsck-help.rst
   :start-after: show this help message and exit
