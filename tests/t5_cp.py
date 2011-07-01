'''
t5_cp.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import division, print_function
import os.path
from s3ql.cli.cp import main as s3qlcp
import subprocess
import tarfile
import tempfile
import errno
import unittest2 as unittest
import t4_fuse


class cpTests(t4_fuse.fuse_tests):
    
    def runTest(self):
        try:
            subprocess.call(['rsync', '--version'],
                            stderr=subprocess.STDOUT,
                            stdout=open('/dev/null', 'wb'))
        except OSError as exc:
            if exc.errno == errno.ENOENT:
                raise unittest.SkipTest('rsync not installed')
            raise

        self.mount()
        self.tst_cp()
        
        self.umount()

    def tst_cp(self):

        # Extract tar
        data_file = os.path.join(os.path.dirname(__file__), 'data.tar.bz2')
        tempdir = tempfile.mkdtemp()
        tarfile.open(data_file).extractall(tempdir)

        # Rsync
        subprocess.check_call(['rsync', '-aHAX', tempdir + '/',
                               os.path.join(self.mnt_dir, 'orig') + '/'])

        # copy
        try:
            s3qlcp([os.path.join(self.mnt_dir, 'orig'),
                              os.path.join(self.mnt_dir, 'copy')])
        except BaseException as exc:
            self.fail("s3qlcp failed: %s" % exc)

        # compare
        rsync = subprocess.Popen(['rsync', '-anciHAX', '--delete',
                                  tempdir + '/',
                                  os.path.join(self.mnt_dir, 'copy') + '/'],
                                  stdout=subprocess.PIPE,
                                  stderr=subprocess.STDOUT)
        out = rsync.communicate()[0]
        if out:
            self.fail('Copy not equal to original, rsync says:\n' + out)
        elif rsync.returncode != 0:
            self.fail('rsync failed with ' + out)


# Somehow important according to pyunit documentation
def suite():
    return unittest.makeSuite(cpTests)


# Allow calling from command line
if __name__ == "__main__":
    unittest.main()
