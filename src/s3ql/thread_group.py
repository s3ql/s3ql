'''
thread_group.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2010 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function, absolute_import

import threading
import sys
import logging
from .common import EmbeddedException

log = logging.getLogger("thread_group")

class Thread(threading.Thread):

    def __init__(self, group):
        super(Thread, self).__init__()

        self._exc = None
        self._tb = None
        self._joined = False
        self._group = group
    
    def run(self):
        try:
            self.run_protected()
        except BaseException as exc:
            self._exc = exc
            self._tb = sys.exc_info()[2] # This creates a circular reference chain
        finally:
            log.debug('thread: waiting for lock')
            with self._group.lock:
                log.debug('thread: calling notify()')
                self._group.active_threads.remove(self)
                self._group.finished_threads.append(self)
                self._group.lock.notify()

    def join_and_raise(self):
        '''Wait for the thread to finish, raise any occurred exceptions'''
        
        self._joined = True
        self.join()
        if self._exc is not None:
            # Break reference chain
            tb = self._tb
            del self._tb
            raise EmbeddedException(self._exc, tb, self.name)


    def __del__(self):
        if not self._joined:
            raise RuntimeError("Thread was destroyed without calling join_and_raise()!")



class ThreadGroup(object):
    '''Represents a group of threads'''
    
    def __init__(self, max_threads):
        '''Initialize thread group
        
        `max_active` specifies the maximum number of running threads
        in the group. 
        '''
        
        self.max_threads = max_threads
        self.active_threads = list()
        self.finished_threads = list()
        self.lock = threading.Condition(threading.Lock())
        
    def add(self, fn, *a, **kw):
        '''Add new thread that runs given function
        
        If the group already contains the maximum number of threads, the method will first call
        `join_one()`. There is a race condition between checking the number of threads and starting
        the new thread, so the maximum number of threads is not exactly obeyed.
        '''
        
        t = Thread(self)
        t.run_protected = lambda: fn(*a, **kw)
        
        if len(self) >= self.max_threads:
            self.join_one()
        self.active_threads.append(t)
        t.start()
        
        
    def join_one(self):
        '''Wait for any one thread to finish
        
        If the thread terminated with an exception, the exception
        is encapsulated in `EmbeddedException` and raised again.
        '''
        
        log.debug('controller: waiting for lock')
        with self.lock:
            # Loop is required because notify() may wake up more than
            # one thread in wait() state.
            while True:
                # See if any thread has terminated
                log.debug('controller: looking for terminated threads')
                try:
                    self.finished_threads.pop().join_and_raise()
                    return    
                except IndexError:
                    pass 
                
                # Wait for thread to terminate
                log.debug('controller: wait()')
                self.lock.wait()
                
        
    def join_all(self):
        '''Call `join_one` until all threads are finished'''
        
        while self.active_threads or self.finished_threads:
            self.join_one()
    
    def __len__(self):
        return len(self.active_threads) + len(self.finished_threads)
