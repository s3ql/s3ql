'''
logging.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

import logging
import warnings
import sys

# Logging messages with severities larger or equal
# than this value will raise exceptions.
EXCEPTION_SEVERITY = logging.CRITICAL+1


class LoggingError(Exception):
    '''
    Raised when a `Logger` instance is used to log a message with
    a severity larger than its `exception_severity`.
    '''

    formatter = logging.Formatter('%(message)s')

    def __init__(self, record):
        super().__init__()
        self.record = record

    def __str__(self):
        return 'Unexpected log message: ' + self.formatter.format(self.record)


class QuietError(Exception):
    '''
    QuietError is the base class for exceptions that should not result
    in a stack trace being printed.

    It is typically used for exceptions that are the result of the user
    supplying invalid input data. The exception argument should be a
    string containing sufficient information about the problem.
    '''

    def __init__(self, msg=''):
        super().__init__()
        self.msg = msg

    def __str__(self):
        return self.msg


def setup_logging(options):
    root_logger = logging.getLogger()
    if root_logger.handlers:
        root_logger.debug("Logging already initialized.")
        return

    stdout_handler = add_stdout_logging(options.quiet)
    if hasattr(options, 'log') and options.log:
        root_logger.addHandler(options.log)
    elif options.debug and (not hasattr(options, 'log') or not options.log):
        # When we have debugging enabled but no separate log target,
        # make stdout logging more detailed.
        formatter = logging.Formatter('%(asctime)s.%(msecs)03d [pid=%(process)r, '
                                      'thread=%(threadName)r, module=%(name)r, '
                                      'fn=%(funcName)r, line=%(lineno)r]: %(message)s',
                                      datefmt="%Y-%m-%d %H:%M:%S")
        stdout_handler.setFormatter(formatter)
        stdout_handler.setLevel(logging.NOTSET)

    setup_excepthook()

    if options.debug:
        if 'all' in options.debug:
            root_logger.setLevel(logging.DEBUG)
        else:
            for module in options.debug:
                logging.getLogger(module).setLevel(logging.DEBUG)

        logging.disable(logging.NOTSET)
    else:
        root_logger.setLevel(logging.INFO)
        logging.disable(logging.DEBUG)


    if hasattr(options, 'fatal_warnings') and options.fatal_warnings:
        global EXCEPTION_SEVERITY
        EXCEPTION_SEVERITY = logging.WARNING

        # When ResourceWarnings are emitted, any exceptions thrown
        # by warning.showwarning() are lost (not even printed to stdout)
        # so we cannot rely on our logging mechanism to turn these
        # warnings into (visible) exceptions. Instead, we assume that
        # --fatal-warnings implies that ResourceWarnings should be
        # enabled and tell the warnings module to raise exceptions
        # immediately (so that they're at least printed to stderr).
        warnings.simplefilter('error', ResourceWarning)

    else:
        logging.captureWarnings(capture=True)
        
        
    return stdout_handler

def setup_excepthook():
    '''Modify sys.excepthook to log exceptions

    Also makes sure that exceptions derived from `QuietException`
    do not result in stacktraces.
    '''

    def excepthook(type_, val, tb):
        root_logger = logging.getLogger()
        if isinstance(val, QuietError):
            # force_log attribute ensures that logging handler will
            # not raise exception (if EXCEPTION_SEVERITY is set)
            root_logger.error(val.msg, extra={ 'force_log': True })
        else:
            # force_log attribute ensures that logging handler will
            # not raise exception (if EXCEPTION_SEVERITY is set)
            root_logger.error('Uncaught top-level exception:',
                              exc_info=(type_, val, tb),
                              extra={ 'force_log': True})

    sys.excepthook = excepthook


def add_stdout_logging(quiet=False):
    '''Add stdout logging handler to root logger

    If *quiet* is True, logging is sent to stderr rather than stdout, and only
    messages with severity WARNING are printed.
    '''

    root_logger = logging.getLogger()
    formatter = logging.Formatter('%(message)s')
    if quiet:
        handler = logging.StreamHandler(sys.stderr)
        handler.setLevel(logging.WARNING)
    else:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.INFO)
    handler.setFormatter(formatter)
    root_logger.addHandler(handler)
    return handler

class Logger(logging.getLoggerClass()):
    '''
    This class has the following features in addition to `logging.Logger`:

    * Loggers can automatically raise exceptions when a log message exceeds
      a specified severity. This is useful when running unit tests.
    '''

    def __init__(self, name):
        super().__init__(name)

    def handle(self, record):
        if (record.levelno >= EXCEPTION_SEVERITY
            and not hasattr(record, 'force_log')):
            raise LoggingError(record)

        # Do not call superclass method directly so that we can
        # re-use this method when monkeypatching the root logger.
        return self._handle_real(record)

    def _handle_real(self, record):
        return super().handle(record)


# Ensure that no handlers have been created yet
loggers = logging.Logger.manager.loggerDict
if len(loggers) != 0:
    raise ImportError('%s must be imported before loggers are created! '
                      'Existing loggers: %s' % (__name__, loggers.keys()))

# Monkeypatch the root logger
#pylint: disable=W0212
root_logger_class = type(logging.getLogger())
root_logger_class._handle_real = root_logger_class.handle
root_logger_class.handle = Logger.handle

logging.setLoggerClass(Logger)
