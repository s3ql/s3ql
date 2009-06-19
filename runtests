#!/usr/bin/env python
#
#    Copyright (C) 2008  Nikolaus Rath <Nikolaus@rath.org>
#
#    This program can be distributed under the terms of the GNU LGPL.
#


# Python boto uses several deprecated modules
import warnings
warnings.filterwarnings("ignore", "", DeprecationWarning, "boto")

import unittest
import os
import sys
from optparse import OptionParser
from s3ql.common import init_logging

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

# Find and import all tests
testdir = os.path.join(os.path.dirname(__file__), "tests")
modules_to_test =  [ name[:-3] for name in os.listdir(testdir) if name.endswith(".py") ]
modules_to_test.sort()
self = __import__("__main__")
sys.path.insert(0, testdir)
for name in modules_to_test:
    # Note that __import__ itself does not add the modules to the namespace
    setattr(self, name, __import__(name))

if not test_names:
    test_names = modules_to_test
    
# Run tests
result = unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromNames(test_names))
sys.exit(not result.wasSuccessful())    
