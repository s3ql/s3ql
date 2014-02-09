.. -*- mode: rst -*-


=================================
The :program:`pcp` command
=================================

Synopsis
========

::

   pcp [options] <source> [<source> ...] <destination>

   
Description
===========

The |command| command is a is a wrapper that starts several
:program:`sync` processes to copy directory trees in parallel. This is
allows much better copying performance on file system that have
relatively high latency when retrieving individual files like S3QL.

**Note**: Using this program only improves performance when copying
*from* an S3QL file system. When copying *to* an S3QL file system,
using |command| is more likely to *decrease* performance.


Options
=======

The |command| command accepts the following options:

.. pipeinclude:: python ../../contrib/pcp.py --help
   :start-after: show this help message and exit

Exit Status
===========

|command| returns exit code 0 if the operation succeeded and 1 if some
error occured.


See Also
========

|command| is shipped as part of S3QL, https://bitbucket.org/nikratio/s3ql/.

.. |command| replace:: :command:`pcp` 

