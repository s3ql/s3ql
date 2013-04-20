'''
t5_full.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

import subprocess
import t4_fuse
from t4_fuse import populate_dir
import tempfile
import unittest
import shutil

class FullTests(t4_fuse.fuse_tests):

    def runTest(self):
        try:
            subprocess.call(['rsync', '--version'],
                            stderr=subprocess.STDOUT,
                            stdout=open('/dev/null', 'wb'))
        except FileNotFoundError:
            raise unittest.SkipTest('rsync not installed')

        ref_dir = tempfile.mkdtemp()
        try:
            populate_dir(ref_dir)
            
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
                                      '--exclude', '/lost+found',
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