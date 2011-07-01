'''
ordered_dict.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import division, print_function

import threading
import collections


__all__ = [ "OrderedDict" ]

class OrderedDictElement(object):
    """An element in an OrderedDict
    

    Attributes:
    -----------

    :next:      Next element in list (closer to tail)
    :prev:      Previous element in list (closer to head) 
    :key:       Dict key of the element
    :value:     Dict value of the element
    """

    __slots__ = [ "next", "prev", "key", "value" ]

    def __init__(self, key, value, next_=None, prev=None):
        self.key = key
        self.value = value
        self.next = next_
        self.prev = prev

class HeadSentinel(object):
    '''Sentinel that marks the head of a linked list
    '''

    __slots__ = [ 'next' ]

    def __init__(self, next_=None):
        self.next = next_

    def __str__(self):
        return '<head sentinel>'

class TailSentinel(object):
    '''Sentinel that marks the tail of a linked list
    '''

    __slots__ = [ 'prev' ]

    def __init__(self, prev=None):
        self.prev = prev

    def __str__(self):
        return '<tail sentinel>'

class OrderedDict(collections.MutableMapping):
    """Implements an ordered dictionary
    
    The order is maintained by wrapping dictionary elements in
    OrderedDictElement objects which are kept in a linked list
    and a dict at the same time. 
    
    When new elements are added to the ordered dictionary by an
    obj[key] = val assignment, the new element is added at the 
    beginning of the list. If the key already exists, the position
    of the element does not change.
    
    All methods are threadsafe and may be called concurrently
    from several threads.
    
    Attributes:
    -----------
    :data:    Backend dict object that holds OrderedDictElement instances
    :lock:    Global lock, required when rearranging the order
    :head:    First element in list
    :tail:    Last element in list
    
    """

    def __init__(self):
        self.data = dict()
        self.lock = threading.Lock()
        self.head = HeadSentinel()
        self.tail = TailSentinel(self.head)
        self.head.next = self.tail

    def __setitem__(self, key, value):
        with self.lock:
            if key in self.data:
                self.data[key].value = value
            else: 
                el = OrderedDictElement(key, value, next_=self.head.next, prev=self.head)
                self.head.next.prev = el
                self.head.next = el
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
        cur = self.head.next
        while cur is not self.tail:
            yield cur.key
            cur = cur.next

    def __reversed__(self):
        cur = self.tail.prev
        while cur is not self.head:
            yield cur.key
            cur = cur.prev

    def values_rev(self):
        '''Iterator over all values, starting from tail'''
        cur = self.tail.prev
        while cur is not self.head:
            yield cur.value
            cur = cur.prev

    def __contains__(self, key):
        return key in self.data

    def to_head(self, key):
        """Moves `key` to the head in the ordering
        """
        with self.lock:
            el = self.data[key]
            # Splice out
            el.prev.next = el.next
            el.next.prev = el.prev

            # Insert back at front
            el.next = self.head.next
            el.prev = self.head

            self.head.next.prev = el
            self.head.next = el

    def to_tail(self, key):
        """Moves `key` to the end in the ordering
        """
        with self.lock:
            el = self.data[key]
            # Splice out
            el.prev.next = el.next
            el.next.prev = el.prev

            # Insert back at end
            el.next = self.tail
            el.prev = self.tail.prev

            self.tail.prev.next = el
            self.tail.prev = el

    def pop_last(self):
        """Fetch and remove last element 
        """
        with self.lock:
            el = self.tail.prev
            if el is self.head:
                raise IndexError()

            del self.data[el.key]
            self.tail.prev = el.prev
            el.prev.next = self.tail

        return el.value

    def get_last(self):
        """Fetch last element"""
        with self.lock:
            if self.tail.prev is self.head:
                raise IndexError()
    
            return self.tail.prev.value

    def pop_first(self):
        """Fetch and remove first element"""
        with self.lock:
            el = self.head.next
            if el is self.tail:
                raise IndexError
            del self.data[el.key]
            self.head.next = el.next
            el.next.prev = self.head

            return el.value

    def get_first(self):
        """Fetch first element"""
        
        with self.lock:
            if self.head.next is self.tail:
                raise IndexError()
    
            return self.head.next.value

    def clear(self):
        '''Delete all elements'''
 
        with self.lock:
            self.data.clear()
            self.head = HeadSentinel()
            self.tail = TailSentinel(self.head)
            self.head.next = self.tail
