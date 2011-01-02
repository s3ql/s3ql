.. -*- mode: rst -*-


Checking for Errors
===================

If, for some reason, the filesystem has not been correctly unmounted,
or if you suspect that there might be errors, you should run the
`fsck.s3ql` utility. It has the following syntax::

 fsck.s3ql [options] <storage url>

Important options are:

  --batch           If user input is required, exit without prompting.
  --force           Force checking even if file system is marked clean.
  
For a full list of available options, run `fsck.s3ql --help`.

