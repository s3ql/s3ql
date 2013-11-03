'''
exit_stack.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) Nikolaus Rath <Nikolaus@rath.org>
Copyright (C) the Python Software Foundation.

This module provides the ExitStack class.

For Python versions 3.3.3 or newer, it simply exports the vanilla version from
contextlib.

For Python 3.3.0 - 3.3.2, it provides a derived class that overwrites the
__exit__ method with the version from CPython revision 423736775f6b to fix
Python issue 19092 (cf. http://bugs.python.org/issue19092).

This program can be distributed under the terms of the GNU GPLv3.
'''

import sys
from contextlib import ExitStack as _ExitStack

if sys.version_info < (3,3):
    raise RuntimeError("Unsupported Python version: %s" % sys.version_info)

if sys.version_info >= (3,3,3):
    ExitStack = _ExitStack

else:   
    class ExitStack(_ExitStack):
        def __exit__(self, *exc_details):
            received_exc = exc_details[0] is not None

            # We manipulate the exception state so it behaves as though
            # we were actually nesting multiple with statements
            frame_exc = sys.exc_info()[1]
            def _fix_exception_context(new_exc, old_exc):
                while 1:
                    exc_context = new_exc.__context__
                    if exc_context in (None, frame_exc):
                        break
                    new_exc = exc_context
                new_exc.__context__ = old_exc

            # Callbacks are invoked in LIFO order to match the behaviour of
            # nested context managers
            suppressed_exc = False
            pending_raise = False
            while self._exit_callbacks:
                cb = self._exit_callbacks.pop()
                try:
                    if cb(*exc_details):
                        suppressed_exc = True
                        pending_raise = False
                        exc_details = (None, None, None)
                except:
                    new_exc_details = sys.exc_info()
                    # simulate the stack of exceptions by setting the context
                    _fix_exception_context(new_exc_details[1], exc_details[1])
                    pending_raise = True
                    exc_details = new_exc_details
            if pending_raise:
                try:
                    # bare "raise exc_details[1]" replaces our carefully
                    # set-up context
                    fixed_ctx = exc_details[1].__context__
                    raise exc_details[1]
                except BaseException:
                    exc_details[1].__context__ = fixed_ctx
                    raise
            return received_exc and suppressed_exc


