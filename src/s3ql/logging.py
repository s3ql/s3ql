'''
logging.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

import logging
import logging.handlers
import warnings
import sys
import os.path

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

    def __init__(self, msg='', exitcode=1):
        super().__init__()
        self.msg = msg

        #: Exit code to use when terminating process
        self.exitcode = exitcode

    def __str__(self):
        return self.msg


def create_handler(target):
    '''Create logging handler for given target'''

    if target.lower() == 'syslog':
        handler = logging.handlers.SysLogHandler('/dev/log')
        formatter = logging.Formatter(os.path.basename(sys.argv[0])
                                       + '[%(process)s:%(threadName)s] '
                                       + '%(name)s.%(funcName)s: %(message)s')

    else:
        fullpath = os.path.expanduser(target)
        dirname = os.path.dirname(fullpath)
        if dirname and not os.path.exists(dirname):
            try:
                os.makedirs(dirname)
            except PermissionError:
                raise QuietError('No permission to create log file %s' % fullpath,
                                 exitcode=10)

        try:
            handler = logging.handlers.RotatingFileHandler(fullpath,
                                                           maxBytes=1024 ** 2, backupCount=5)
        except PermissionError:
            raise QuietError('No permission to write log file %s' % fullpath,
                             exitcode=10)

        formatter = logging.Formatter('%(asctime)s.%(msecs)03d %(process)s:%(threadName)s '
                                      '%(name)s.%(funcName)s: %(message)s',
                                      datefmt="%Y-%m-%d %H:%M:%S")

    handler.setFormatter(formatter)
    return handler

def setup_logging(options):
    root_logger = logging.getLogger()
    if root_logger.handlers:
        root_logger.debug("Logging already initialized.")
        return

    stdout_handler = add_stdout_logging(options.quiet)
    if hasattr(options, 'log') and options.log:
        root_logger.addHandler(create_handler(options.log))
    elif options.debug and (not hasattr(options, 'log') or not options.log):
        # When we have debugging enabled but no separate log target,
        # make stdout logging more detailed.
        formatter = logging.Formatter('%(asctime)s.%(msecs)03d %(process)s %(threadName)s '
                                      '%(name)s.%(funcName)s: %(message)s',
                                      datefmt="%Y-%m-%d %H:%M:%S")
        stdout_handler.setFormatter(formatter)
        stdout_handler.setLevel(logging.NOTSET)

    setup_excepthook()

    if options.debug:
        if 'all' in options.debug:
            root_logger.setLevel(logging.DEBUG)
        else:
            root_logger.setLevel(logging.INFO)
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
            sys.exit(val.exitcode)
        else:
            # force_log attribute ensures that logging handler will
            # not raise exception (if EXCEPTION_SEVERITY is set)
            root_logger.error('Uncaught top-level exception:',
                              exc_info=(type_, val, tb),
                              extra={ 'force_log': True})
            sys.exit(1)

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

    * Log messages that are emitted with an *log_once* attribute in the
      *extra* parameter are only emitted once per logger.
    '''

    def __init__(self, name):
        super().__init__(name)
        self.log_cache = set()

    def handle(self, record):
        if (record.levelno >= EXCEPTION_SEVERITY
            and not hasattr(record, 'force_log')):
            raise LoggingError(record)

        if hasattr(record, 'log_once') and record.log_once:
            id_ = hash((record.name, record.levelno, record.msg,
                        record.args, record.exc_info))
            if id_ in self.log_cache:
                return
            self.log_cache.add(id_)

        # Do not call superclass method directly so that we can
        # re-use this method when monkeypatching the root logger.
        return self._handle_real(record)

    def _handle_real(self, record):
        return super().handle(record)


# Convenience object for use in logging calls, e.g.
# log.warning('This will be printed only once', extra=LOG_ONCE)
LOG_ONCE = { 'log_once': True }

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
