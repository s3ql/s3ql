#!/usr/bin/env python3
'''
t5_cp.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright © 2008 Nikolaus Rath <Nikolaus.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

if __name__ == '__main__':
    import pytest
    import sys
    sys.exit(pytest.main([__file__] + sys.argv[1:]))

from common import populate_dir, skip_without_rsync
import os.path
import shutil
import subprocess
from subprocess import check_output, CalledProcessError
import t4_fuse
import tempfile
import pytest


@pytest.mark.usefixtures('s3ql_cmd_argv')
class cpTests(t4_fuse.fuse_tests):

    def runTest(self):
        skip_without_rsync()
        self.mkfs()
        self.mount()
        self.tst_cp()
        self.umount()
        self.fsck()

    def tst_cp(self):

        tempdir = tempfile.mkdtemp(prefix='s3ql-cp-')
        try:
            populate_dir(tempdir)

            # Rsync
            subprocess.check_call(['rsync', '-aHAX', tempdir + '/',
                                   os.path.join(self.mnt_dir, 'orig') + '/'])

            # copy
            subprocess.check_call(self.s3ql_cmd_argv('s3qlcp') +
                                  [ '--quiet', os.path.join(self.mnt_dir, 'orig'),
                                    os.path.join(self.mnt_dir, 'copy')])

            # compare
            try:
                out = check_output(['rsync', '-anciHAX', '--delete', tempdir + '/',
                                    os.path.join(self.mnt_dir, 'copy') + '/'],
                                   universal_newlines=True, stderr=subprocess.STDOUT)
            except CalledProcessError as exc:
                self.fail('rsync failed with ' + exc.output)

            if out:
                self.fail('Copy not equal to original, rsync says:\n' + out)

        finally:
            shutil.rmtree(tempdir)
