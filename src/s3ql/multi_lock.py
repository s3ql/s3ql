#!/usr/bin/env python
#
#    Copyright (C) 2008-2009  Nikolaus Rath <Nikolaus@rath.org>
#
#    This program can be distributed under the terms of the GNU LGPL.
#

import threading
import logging
from functools import partial 

__all__ = [ "MultiLock" ]

log = logging.getLogger("MultiLock")

class ContextManager(object):
    '''A very simple context manager class.
    
    This class does not provide any methods. Instances set 
    the `__enter__` and `__exit__` methods as true instance 
    attributes (i.e., function objects) when initialized.
    '''
    
    def __init__(self, enter, exit_):
        self.__enter__ = enter 
        self.__exit__ = exit_
        
            
class MultiLock(object):
    """Provides locking for multiple objects.
    
    This class provides locking for a dynamically changing set of objects:
    The `acquire` and `release` methods have an additional argument, the
    locking key. Only locks with the same key can actually see each other,
    so that several threads can hold locks with different locking keys
    at the same time.
    
    MultiLock instances can be used with `with` statements as
    
    lock = MultiLock()
    with lock(key):
        pass
    """
    
    def __init__(self):
        self.locked_keys = set()
        self.cond = threading.Condition()
         
      
    def __call__(self, key):
        return ContextManager(partial(self.acquire, key),
                              partial(self.release, key))      
            
    def acquire(self, key):
        # Lock set of lockedkeys (global lock)
        with self.cond:
            
            # Wait for given s3 key becoming unused
            while key in self.locked_keys:
                self.cond.wait()

            # Mark it as used (local lock)
            self.locked_keys.add(key)

    def release(self, key):
        """Releases lock on given s3key
        """

        # Lock set of locked keys (global lock)
        with self.cond:

            # Mark key as free (release local lock)
            self.locked_keys.remove(key)

            # Notify other threads
            self.cond.notifyAll()