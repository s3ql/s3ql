'''
block_cache.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2011 Nikolaus Rath <Nikolaus@rath.org>
Copyright (C) 2011 Jan Kaliszewski <zuo@chopin.edu.pl>
 
This program can be distributed under the terms of the GNU GPLv3.
'''


class CleanupManager(object):

    def __init__(self, logger=None, initial_callbacks=()):
        self.cleanup_callbacks = list(initial_callbacks)
        self.logger = logger

    def register(self, callback, *args, **kwargs):
        self.cleanup_callbacks.append((callback, args, kwargs))

    def unregister(self, callback, *args, **kwargs):
        self.cleanup_callbacks.remove((callback, args, kwargs))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self._next_callback()

    def _next_callback(self):
        if self.cleanup_callbacks:
            callback, args, kwargs = self.cleanup_callbacks.pop()
            try:
                callback(*args, **kwargs)
            except:
                if self.logger:
                    self.logger.exception('Exception during cleanup:')
            finally:
                # all cleanup callbacks to be used
                # Py3.x: all errors to be reported
                self._next_callback()
