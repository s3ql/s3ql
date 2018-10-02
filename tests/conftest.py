'''
conftest.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.


This module is loaded automatically by py.test and is used to
initialize logging and adjust the load path before running
any tests.
'''

# Python version check
import sys
if sys.version_info < (3,3):
    raise SystemExit('Python version is %d.%d.%d, but S3QL requires Python 3.3 or newer'
                     % sys.version_info[:3])

import logging.handlers
import sys
import os.path
import pytest
import faulthandler
import signal
import gc
import time

# If a test fails, wait a moment before retrieving the captured
# stdout/stderr. When using a server process (like in t4_fuse.py), this makes
# sure that we capture any potential output of the server that comes *after* a
# test has failed. For example, if a request handler raises an exception, the
# server first signals an error to FUSE (causing the test to fail), and then
# logs the exception. Without the extra delay, the exception will go into
# nowhere.
@pytest.mark.hookwrapper
def pytest_pyfunc_call(pyfuncitem):
    outcome = yield
    failed = outcome.excinfo is not None
    if failed:
        time.sleep(1)

@pytest.fixture(scope="class")
def s3ql_cmd_argv(request):
    '''Provide argument list to execute s3ql commands in tests'''

    if request.config.getoption('installed'):
        request.cls.s3ql_cmd_argv = lambda self, cmd: [ cmd ]
    else:
        basedir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        request.cls.s3ql_cmd_argv = lambda self, cmd: [ sys.executable,
                                                        os.path.join(basedir, 'bin', cmd) ]

# Enable output checks
pytest_plugins = ('pytest_checklogs')

# Ignore DeprecationWarnings when running unit tests.  They are
# unfortunately quite often a result of indirect imports via third party
# modules, so we can't actually fix them.
@pytest.fixture(autouse=True)
def ignore_depreciation_warnings(reg_output):
    reg_output(r'(Pending)?DeprecationWarning', count=0)

@pytest.fixture()
def pass_reg_output(request, reg_output):
    '''Provide reg_output function to UnitTest instances'''
    request.instance.reg_output = reg_output

def pytest_addoption(parser):
    group = parser.getgroup("terminal reporting")
    group._addoption("--logdebug", action="append", metavar='<module>',
                     help="Activate debugging output from <module> for tests. Use `all` "
                          "to get debug messages from all modules. This option can be "
                          "specified multiple times.")

    group = parser.getgroup("general")
    group._addoption("--installed", action="store_true", default=False,
                     help="Test the installed package.")

def pytest_configure(config):
    # If we are running from the S3QL source directory, make sure that we
    # load modules from here
    basedir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    if not config.getoption('installed'):
        if (os.path.exists(os.path.join(basedir, 'setup.py')) and
            os.path.exists(os.path.join(basedir, 'src', 's3ql', '__init__.py'))):
            sys.path = [os.path.join(basedir, 'src')] + sys.path

    # When running from HG repo, enable all warnings
    if os.path.exists(os.path.join(basedir, 'MANIFEST.in')):
        import warnings
        warnings.resetwarnings()
        warnings.simplefilter('default')

    # Enable faulthandler
    faultlog_fd = os.open(os.path.join(basedir, 'test_crit.log'),
                          flags=os.O_APPEND|os.O_CREAT|os.O_WRONLY, mode=0o644)
    faulthandler.enable(faultlog_fd)
    faulthandler.register(signal.SIGUSR1, file=faultlog_fd)

    # Configure logging. We don't set a default handler but rely on
    # the catchlog pytest plugin.
    logdebug = config.getoption('logdebug')
    root_logger = logging.getLogger()
    if logdebug is not None:
        logging.disable(logging.NOTSET)
        if 'all' in logdebug:
            root_logger.setLevel(logging.DEBUG)
        else:
            for module in logdebug:
                logging.getLogger(module).setLevel(logging.DEBUG)
    else:
        root_logger.setLevel(logging.INFO)
        logging.disable(logging.DEBUG)
    logging.captureWarnings(capture=True)

# Run gc.collect() at the end of every test, so that we get ResourceWarnings
# as early as possible.
def pytest_runtest_teardown(item, nextitem):
    gc.collect()
