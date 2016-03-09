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

# Converted to autouse fixture below if capture is activated
def check_test_output(request, capfd):
    request.capfd = capfd
    def raise_on_exception_in_out():
        # Ensure that capturing has been set up (this may not be the case if one
        # of the test fixtures raises an exception)
        try:
            (stdout, stderr) = capfd.readouterr()
        except AttributeError:
            return

        # Write back what we've read (so that it will still be printed.
        sys.stdout.write(stdout)
        sys.stderr.write(stderr)

        if ('exception' in stderr.lower()
            or 'exception' in stdout.lower()):
            raise AssertionError('Suspicious output to stderr')

    request.addfinalizer(raise_on_exception_in_out)


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

    # Enable stdout and stderr analysis, unless output capture is disabled
    if config.getoption('capture') != 'no':
        global check_test_output
        check_test_output = pytest.fixture(autouse=True)(check_test_output)

    logdebug = config.getoption('logdebug')

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
        warnings.simplefilter('error')

    # Enable faulthandler
    global faultlog_fh
    faultlog_fh = open(os.path.join(basedir, 'tests', 'test_crit.log'), 'a')
    faulthandler.enable(faultlog_fh)
    faulthandler.register(signal.SIGUSR1, file=faultlog_fh)

    # Enable logging
    import s3ql.logging
    root_logger = logging.getLogger()
    if root_logger.handlers:
        root_logger.warning("Logging already initialized.")
    else:
        handler = logging.handlers.RotatingFileHandler(
            os.path.join(basedir, 'tests', 'test.log'),
            maxBytes=10 * 1024 ** 2, backupCount=0)
        if logdebug is None:
            formatter = logging.Formatter(
                '%(asctime)s.%(msecs)03d [%(process)s] %(threadName)s: '
                '[%(name)s] %(message)s', datefmt="%Y-%m-%d %H:%M:%S")
        else:
            formatter = logging.Formatter(
                '%(asctime)s.%(msecs)03d [%(process)s] %(threadName)s: '
                '[%(name)s.%(funcName)s] %(message)s', datefmt="%Y-%m-%d %H:%M:%S")

        handler.setFormatter(formatter)
        root_logger.addHandler(handler)

        if logdebug is not None:
            if 'all' in logdebug:
                root_logger.setLevel(logging.DEBUG)
            else:
                for module in logdebug:
                    logging.getLogger(module).setLevel(logging.DEBUG)
            logging.disable(logging.NOTSET)
        else:
            root_logger.setLevel(logging.WARNING)

        logging.captureWarnings(capture=True)

    # Make errors and warnings fatal
    s3ql.logging.EXCEPTION_SEVERITY = logging.WARNING


# Run gc.collect() at the end of every test, so that we get ResourceWarnings
# as early as possible.
def pytest_runtest_teardown(item, nextitem):
    gc.collect()
