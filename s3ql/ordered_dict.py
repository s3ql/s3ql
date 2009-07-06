#!/usr/bin/env python
#
#    Copyright (C) 2008-2009  Nikolaus Rath <Nikolaus@rath.org>
#
#    This program can be distributed under the terms of the GNU LGPL.
#

import threading
import collections


__all__ = [ "OrderedDict" ]

class OrderedDictElement(object):
    """An element in an OrderedDict
    

    Attributes:
    -----------

    :next:      Next element in list (closer to last element)
    :prev:      Previous element in list (closer to first element) 
    :key:       Dict key of the element
    :value:     Dict value of the element
    """
    
    __slots__ = [ "next", "prev", "key", "value" ]
    
    def __init__(self, key, value, next=None, prev=None):
        self.key = key
        self.value = value
        self.next = next
        self.prev = prev
    
    
class OrderedDict(collections.MutableMapping):
    """Implements an ordered dictionary
    
    The order is maintained by wrapping dictionary elements in
    OrderedDictElement objects which are kept in a linked list
    and a dict at the same time. 
    
    When new elements are added to the ordered dictionary by an
    obj[key] = val assignment, the new element is added at the 
    beginning of the list. If the key already exists, the position
    of the element does not change.
    
    Attributes:
    -----------
    :data:    Backend dict object that holds OrderedDictElement instances
    :lock:    Global lock, required when rearranging the order
    :first:   First element in list
    :last:    Last element in list
    
    """
    
    def __init__(self):
        self.data = dict()
        self.lock = threading.Lock()
        
        # Sentinels
        self.first = OrderedDictElement(None, None)
        self.last = OrderedDictElement(None, None)
        self.first.next = self.last
        self.last.prev = self.first

    def __setitem__(self, key, value):
        if key in self.data:
            self.data[key].value = value
        else:
            with self.lock:
                el = OrderedDictElement(key, value, next=self.first.next, prev=self.first)
                self.first.next.prev = el
                self.first.next = el
                self.data[key] = el
                
    def __delitem__(self, key):
        with self.lock:
            el = self.data.pop(key)  # exception can be passed on
            el.prev.next = el.next
            el.next.prev = el.prev
        
    def __getitem__(self, key):
        return self.data[key].value
    
    def __len__(self):
        return len(self.data)
    
    def __iter__(self):
        cur = self.first.next
        while cur is not self.last:
            yield cur.key
            cur = cur.next
    
    def __reversed__(self):
        cur = self.last.prev
        while cur is not self.first:
            yield cur.key
            cur = cur.prev
                    
    def __contains__(self, key):
        return key in self.data
    
    def to_front(self, key):
        """Moves `key` to the front in the ordering
        """
        with self.lock:
            el = self.data[key]
            # Splice out
            el.prev.next = el.next
            el.next.prev = el.prev
            
            # Insert back at front
            el.next = self.first.next
            el.prev = self.first
            
            self.first.next.prev = el
            self.first.next = el
        
    def to_end(self, key):
        """Moves `key` to the end in the ordering
        """
        with self.lock:
            el = self.data[key]
            # Splice out
            el.prev.next = el.next
            el.next.prev = el.prev
            
            # Insert back at end
            el.next = self.last 
            el.prev = self.last.prev
            
            self.last.prev.next = el
            self.last.prev = el

    def pop_last(self):
        """Fetch and remove last element 
        """
        with self.lock:
            el = self.last.prev
            if el is self.first:
                raise IndexError()
            
            del self.data[el.key]
            self.last.prev = el.prev
            el.prev.next = self.last
                            
        return el.value
        
    def get_last(self):
        """Fetch last element 
        """
        if self.last.prev is self.first:
            raise IndexError()

        return self.last.prev.value

    def pop_first(self):
        """Fetch and remove first element 
        """
        with self.lock:
            el = self.first.next
            if el is self.last:
                raise IndexError
            del self.data[el.key]
            self.first.next = el.next 
            el.next.prev = self.first
            
        return el.value
        
    def get_first(self):
        """Fetch last element 
        """
        if self.first.next is self.last:
            raise IndexError()

        return self.first.next.value

        