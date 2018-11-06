#!/usr/bin/env python3
'''
pytest_checklogs.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.

py.test plugin to look for suspicious phrases in messages emitted on
stdout/stderr or via the logging module.

False positives can be registered via a new `reg_output` fixture (for messages
to stdout/stderr), and a `assert_logs` function (for logging messages).
'''

import pytest
import re
import functools
import sys
import logging
from contextlib import contextmanager
from distutils.version import LooseVersion

def pytest_configure(config):
    # pytest-catchlog was integrated in pytest 3.3.0
    if (LooseVersion(pytest.__version__) < "3.3.0" and
        not config.pluginmanager.hasplugin('pytest_catchlog')):
        raise ImportError('pytest catchlog plugin not found')

# Fail tests if they result in log messages of severity WARNING or more.
def check_test_log(caplog):
    for record in caplog.records:
        if (record.levelno >= logging.WARNING and
            not getattr(record, 'checklogs_ignore', False)):
            raise AssertionError('Logger received warning messages')

class CountMessagesHandler(logging.Handler):
    def __init__(self, level=logging.NOTSET):
        super().__init__(level)
        self.count = 0

    def emit(self, record):
        self.count += 1

@contextmanager
def assert_logs(pattern, level=logging.WARNING, count=None):
    '''Assert that suite emits specified log message

    *pattern* is matched against the *unformatted* log message, i.e. before any
    arguments are merged.

    If *count* is not None, raise an exception unless exactly *count* matching
    messages are caught.

    Matched log records will also be flagged so that the caplog fixture
    does not generate exceptions for them (no matter their severity).
    '''

    def filter(record):
        if (record.levelno == level and
            re.search(pattern, record.msg)):
            record.checklogs_ignore = True
            return True
        return False

    handler = CountMessagesHandler()
    handler.setLevel(level)
    handler.addFilter(filter)
    logger = logging.getLogger()
    logger.addHandler(handler)
    try:
        yield

    finally:
        logger.removeHandler(handler)

        if count is not None and handler.count != count:
            raise AssertionError('Expected to catch %d %r messages, but got only %d'
                                 % (count, pattern, handler.count))

def check_test_output(capfd, item):
    (stdout, stderr) = capfd.readouterr()

    # Write back what we've read (so that it will still be printed)
    sys.stdout.write(stdout)
    sys.stderr.write(stderr)

    # Strip out false positives
    try:
        false_pos = item.checklogs_fp
    except AttributeError:
        false_pos = ()
    for (pattern, flags, count) in false_pos:
        cp = re.compile(pattern, flags)
        (stdout, cnt) = cp.subn('', stdout, count=count)
        if count == 0 or count - cnt > 0:
            stderr = cp.sub('', stderr, count=count - cnt)

    for pattern in (r'exception\b', r'error\b', r'warning\b',
                    r'\bfatal\b', r'\btraceback\b',
                    r'(:?seg)?fault\b', r'\bcrash(?:ed)?\b', r'\babort(?:ed)\b',
                    r'\bfishy\b'):
        cp = re.compile(pattern, re.IGNORECASE | re.MULTILINE)
        hit = cp.search(stderr)
        if hit:
            raise AssertionError('Suspicious output to stderr (matched "%s")' % hit.group(0))
        hit = cp.search(stdout)
        if hit:
            raise AssertionError('Suspicious output to stdout (matched "%s")' % hit.group(0))

def register_output(item, pattern, count=1, flags=re.MULTILINE):
    '''Register *pattern* as false positive for output checking

    This prevents the test from failing because the output otherwise
    appears suspicious.
    '''

    item.checklogs_fp.append((pattern, flags, count))

@pytest.fixture()
def reg_output(request):
    assert not hasattr(request.node, 'checklogs_fp')
    request.node.checklogs_fp = []
    return functools.partial(register_output, request.node)

def check_output(item):
    pm = item.config.pluginmanager
    cm = pm.getplugin('capturemanager')
    capmethod = (getattr(cm, '_capturing', None) or
                 getattr(item, '_capture_fixture', None) or
                 getattr(cm, '_global_capturing', None))
    check_test_output(capmethod, item)
    check_test_log(item.catch_log_handler)

@pytest.hookimpl(trylast=True)
def pytest_runtest_setup(item):
    check_output(item)
@pytest.hookimpl(trylast=True)
def pytest_runtest_call(item):
    check_output(item)
@pytest.hookimpl(trylast=True)
def pytest_runtest_teardown(item, nextitem):
    check_output(item)
