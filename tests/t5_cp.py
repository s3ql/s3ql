'''
t5_cp.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

import os.path
import subprocess
import t4_fuse
from t4_fuse import populate_dir
import tempfile
import unittest
import shutil

class cpTests(t4_fuse.fuse_tests):

    def runTest(self):
        try:
            subprocess.call(['rsync', '--version'],
                            stderr=subprocess.STDOUT,
                            stdout=open('/dev/null', 'wb'))
        except FileNotFoundError:
            raise unittest.SkipTest('rsync not installed')

        self.mkfs()
        self.mount()
        self.tst_cp()
        self.umount()
        self.fsck()

    def tst_cp(self):

        tempdir = tempfile.mkdtemp()
        try:
            populate_dir(tempdir)

            # Rsync
            subprocess.check_call(['rsync', '-aHAX', tempdir + '/',
                                   os.path.join(self.mnt_dir, 'orig') + '/'])

            # copy
            subprocess.check_call([os.path.join(t4_fuse.BASEDIR, 'bin', 's3qlcp'),
                                   '--quiet',
                                   os.path.join(self.mnt_dir, 'orig'),
                                   os.path.join(self.mnt_dir, 'copy')])

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

        finally:
            shutil.rmtree(tempdir)

# Somehow important according to pyunit documentation
def suite():
    return unittest.makeSuite(cpTests)


# Allow calling from command line
if __name__ == "__main__":
    unittest.main()
