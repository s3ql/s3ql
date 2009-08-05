#!/usr/bin/env python
#
#    Copyright (C) 2008  Nikolaus Rath <Nikolaus@rath.org>
#
#    This program can be distributed under the terms of the GNU LGPL.
#

from __future__ import unicode_literals
import sys
import os
import unittest
from optparse import OptionParser
from s3ql.common import init_logging
import _awscred
                              
#
# Parse commandline
#
parser = OptionParser(
    usage="%prog  [options] <testnames>\n" \
        "       %prog --help",
    description="Runs unit tests for s3ql")

parser.add_option("--debug", action="append", 
                  help="Activate debugging output from specified facility. Valid facility names "
                        "are: mkfs, fsck, fs, fuse, s3, frontend. "
                        "This option can be specified multiple times.")
(options, test_names) = parser.parse_args()

# Init Logging
init_logging(True, True, options.debug)
    
# Get credentials for remote tests.
aws_credentials = _awscred.get()

# Find and import all tests
testdir = os.path.dirname(__file__)
modules_to_test =  [ name[:-3] for name in os.listdir(testdir) 
                    if name.endswith(".py") and name.startswith('t')]
modules_to_test.sort()
self = sys.modules["__main__"]
sys.path.insert(0, testdir)
for name in modules_to_test:
    # Note that __import__ itself does not add the modules to the namespace
    module = __import__(name)
    setattr(self, name, module)

if not test_names:
    test_names = modules_to_test
        
# Run tests
runner = unittest.TextTestRunner(verbosity=2)
result = runner.run(unittest.defaultTestLoader.loadTestsFromNames(test_names))
sys.exit(not result.wasSuccessful())    
