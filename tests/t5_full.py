'''
t5_full.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import division, print_function
import errno
import os.path
import subprocess
import t4_fuse
import tarfile
import tempfile
import unittest2 as unittest
import shutil

class FullTests(t4_fuse.fuse_tests):

    def runTest(self):
        try:
            subprocess.call(['rsync', '--version'],
                            stderr=subprocess.STDOUT,
                            stdout=open('/dev/null', 'wb'))
        except OSError as exc:
            if exc.errno == errno.ENOENT:
                raise unittest.SkipTest('rsync not installed')
            raise

        data_file = os.path.join(os.path.dirname(__file__), 'data.tar.bz2')
        ref_dir = tempfile.mkdtemp()
        try:
            tarfile.open(data_file).extractall(ref_dir)

            # Copy source data
            self.mkfs()
            self.mount()
            subprocess.check_call(['rsync', '-aHAX', ref_dir + '/',
                                   self.mnt_dir + '/'])
            self.umount()
            self.fsck()

            # Delete cache, run fsck and compare
            shutil.rmtree(self.cache_dir)
            self.cache_dir = tempfile.mkdtemp()
            self.fsck()
            self.mount()
            rsync = subprocess.Popen(['rsync', '-anciHAX', '--delete',
                                      ref_dir + '/', self.mnt_dir + '/'],
                                     stdout=subprocess.PIPE,
                                     stderr=subprocess.STDOUT)
            out = rsync.communicate()[0]
            if out:
                self.fail('Copy not equal to original, rsync says:\n' + out)
            elif rsync.returncode != 0:
                self.fail('rsync failed with ' + out)

            self.umount()

            # Delete cache and mount
            shutil.rmtree(self.cache_dir)
            self.cache_dir = tempfile.mkdtemp()
            self.mount()
            self.umount()


        finally:
            shutil.rmtree(ref_dir)


# Somehow important according to pyunit documentation
def suite():
    return unittest.makeSuite(FullTests)

# Allow calling from command line
if __name__ == "__main__":
    unittest.main()