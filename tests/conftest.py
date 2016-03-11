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
import re

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

@pytest.fixture()
def pass_capfd(request, capfd):
    '''Provide capfd object to UnitTest instances'''
    request.instance.capfd = capfd

# Fail tests if the result in log messages of severity WARNING or more.
# Previously (as of Mercurial commit 192dd923daa8, 2016-03-10) we instead
# installed a custom Logger class that would immediately raise an
# exception. However, this solution seemed ugly because normally the logging
# methods suppress any exceptions. Therefore, exception handlers (which are
# especially likely to log warnings) may rely on that and an unexpected
# exception may result in improper clean-up. Furthermore, the custom logger
# class required a second hack to allow logging the "unexpected warning message"
# exception itself from sys.excepthook. Checking the logs after execution has
# the drawback that we abort later, and that the failure seems to come from the
# teardown method, but seems like an overall better solution.
def check_test_log(caplog):
    for record in caplog.records():
        if (record.levelno >= logging.WARNING and
            not getattr(record, 'caplog_ignore', False)):
            raise AssertionError('Logger received warning messages')

def check_test_output(capfd):
    (stdout, stderr) = capfd.readouterr()

    # Write back what we've read (so that it will still be printed.
    sys.stdout.write(stdout)
    sys.stderr.write(stderr)

    # Strip out false positives
    for (pattern, flags, count) in capfd.false_positives:
        cp = re.compile(pattern, flags)
        (stdout, cnt) = cp.subn('', stdout, count=count)
        if count == 0 or count - cnt > 0:
            stderr = cp.sub('', stderr, count=count - cnt)

    for pattern in ('exception', 'error', 'warning', 'fatal',
                    'fault', 'crash(?:ed)?', 'abort(?:ed)'):
        cp = re.compile(r'\b{}\b'.format(pattern), re.IGNORECASE | re.MULTILINE)
        hit = cp.search(stderr)
        if hit:
            raise AssertionError('Suspicious output to stderr (matched "%s")' % hit.group(0))
        hit = cp.search(stdout)
        if hit:
            raise AssertionError('Suspicious output to stdout (matched "%s")' % hit.group(0))


def register_output(self, pattern, count=1, flags=re.MULTILINE):
    '''Register *pattern* as false positive for output checking

    This prevents the test from failing because the output otherwise
    appears suspicious.
    '''

    self.false_positives.append((pattern, flags, count))

# This is a terrible hack that allows us to access the fixtures from the
# pytest_runtest_call hook. Among a lot of other hidden assumptions, it probably
# relies on tests running sequential (i.e., don't dare to use e.g. the xdist
# plugin)
current_cap_fixtures = None
@pytest.yield_fixture(autouse=True)
def save_cap_fixtures(request, capfd, caplog):
    global current_cap_fixtures
    capfd.false_positives = []
    type(capfd).register_output = register_output

    # Ignore DeprecationWarnings when running unit tests.  They are
    # unfortunately quite often a result of indirect imports via third party
    # modules, so we can't actually fix them.
    capfd.register_output(r'^(Pending)?DeprecationWarning: .+$', count=0)

    if request.config.getoption('capture') == 'no':
        capfd = None
    current_cap_fixtures = (capfd, caplog)
    bak = current_cap_fixtures
    yield
    # Try to catch problems with this hack (e.g. when running
    # tests simultaneously)
    assert bak is current_cap_fixtures
    current_cap_fixtures = None

@pytest.hookimpl(trylast=True)
def pytest_runtest_call(item):
    (capfd, caplog) = current_cap_fixtures
    check_test_log(caplog)
    if capfd is not None:
        check_test_output(capfd)

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
    global faultlog_fh
    faultlog_fh = open(os.path.join(basedir, 'tests', 'test_crit.log'), 'a')
    faulthandler.enable(faultlog_fh)
    faulthandler.register(signal.SIGUSR1, file=faultlog_fh)

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
