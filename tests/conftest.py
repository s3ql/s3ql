'''
conftest.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (c) Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.


This module is loaded automatically by py.test and is used to
initialize logging and adjust the load path before running
any tests.
'''

import logging.handlers
import sys
import os.path

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

        # Resource warnings can be emitted from __del__, so turning them into
        # exceptions doesn't help (they'll get printed to stdout and captured
        # by py.test)
        warnings.simplefilter('default', ResourceWarning)

    # Enable logging
    import s3ql.logging
    root_logger = logging.getLogger()
    if root_logger.handlers:
        root_logger.warning("Logging already initialized.")
    else:
        handler = logging.handlers.RotatingFileHandler(os.path.join(basedir, 'tests', 'test.log'),
                                                       maxBytes=10 * 1024 ** 2, backupCount=0)
        formatter = logging.Formatter('%(asctime)s.%(msecs)03d [%(process)s] %(threadName)s: '
                                      '[%(name)s] %(message)s', datefmt="%Y-%m-%d %H:%M:%S")
        handler.setFormatter(formatter)
        root_logger.addHandler(handler)

        if logdebug is not None:
            root_logger.setLevel(logging.DEBUG)
            if 'all' not in logdebug:
                root_logger.addFilter(s3ql.logging.LoggerFilter(logdebug, logging.WARNING))
        else:
            root_logger.setLevel(logging.WARNING)
        logging.captureWarnings(capture=True)

    # Make errors and warnings fatal
    s3ql.logging.EXCEPTION_SEVERITY = logging.WARNING
