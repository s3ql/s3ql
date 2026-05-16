# cython: language_level=3
'''
_linked_dict.py - this file is part of S3QL.

Copyright © 2026 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import annotations

import cython

# Private sentinel stored as `_Node.key` on `mutable_values()` cursor nodes.
_CURSOR_KEY: object = object()


@cython.freelist(10)
@cython.cclass
class _Node:
    key: object
    value: object
    prev: _Node
    next: _Node

    def __init__(self, key, value):
        self.key = key
        self.value = value
        self.prev = None
        self.next = None


@cython.cclass
class LinkedDict:
    """Ordered mapping with mutation-safe iteration.

    New entries are added at the tail. Re-assignment and access do not change order. Entries can be
    moved to the tail. `mutable_values()` yields values in order and allows concurrent mutation, but
    (under mutation) may yield values more than once or skip values.

    The plain `__iter__` / `keys` / `values` / `items` iterate in arbitrary order and raise an
    exception if the mapping is mutated during iteration.
    """

    _data: dict[object, _Node]
    _head: _Node
    _tail: _Node

    def __init__(self):
        """Create an empty mapping."""
        self._data = {}
        self._head = _Node(None, None)
        self._tail = _Node(None, None)
        self._head.next = self._tail
        self._tail.prev = self._head

    def __class_getitem__(cls, item):
        """Return the class unchanged so `LinkedDict[K, V]` works at runtime.

        Type-checkers read the generic parameters from the `_linked_dict.pyi`
        stub; *item* is ignored here.
        """
        return cls

    def __len__(self):
        """Return the number of entries in the mapping."""
        return len(self._data)

    def __contains__(self, key):
        """Return whether *key* is present in the mapping."""
        return key in self._data

    def __getitem__(self, key):
        """Return the value stored under *key*; raise `KeyError` if absent."""
        node: _Node = self._data[key]
        return node.value

    @cython.locals(node=_Node, prev=_Node)
    def __setitem__(self, key, value):
        """Set the value for *key*.

        Inserting a new key appends it at the tail of the linked list.
        Overwriting an existing key updates the value in place and leaves
        the linked-list order unchanged.
        """
        try:
            node = self._data[key]
        except KeyError:
            pass
        else:
            node.value = value
            return

        node = _Node(key, value)
        prev = self._tail.prev
        node.prev = prev
        node.next = self._tail
        prev.next = node
        self._tail.prev = node
        self._data[key] = node

    def __delitem__(self, key):
        """Remove *key* from the mapping; raise `KeyError` if absent."""
        self._unlink(self._data.pop(key))

    @cython.ccall
    @cython.locals(node=_Node)
    def get(self, key, default=None):
        """Return the value stored under *key*, or *default* if absent.

        *default* defaults to `None`. Unlike `__getitem__`, this never
        raises for a missing key.
        """
        node = self._data.get(key)
        if node is None:
            return default
        return node.value

    @cython.ccall
    def pop(self, key):
        """Remove *key* from the mapping and return its value.

        Raises `KeyError` if *key* is absent.
        """
        node: _Node = self._data.pop(key)
        self._unlink(node)
        return node.value

    @cython.ccall
    @cython.locals(node=_Node, prev=_Node)
    def move_to_tail(self, key):
        """Move *key* to the tail of the linked list."""
        node = self._data[key]
        self._unlink(node)
        prev = self._tail.prev
        node.prev = prev
        node.next = self._tail
        prev.next = node
        self._tail.prev = node

    @cython.cfunc
    def _unlink(self, node: _Node):
        prev: _Node = node.prev
        nxt: _Node = node.next
        prev.next = nxt
        nxt.prev = prev
        node.prev = None
        node.next = None

    @cython.locals(node=_Node, count=cython.Py_ssize_t)
    def _linked_list_length(self) -> int:
        """Return the number of nodes between the head and tail sentinels.

        Includes any cursor sentinels left over from aborted `mutable_values()`
        calls, so a mismatch with `len(self)` flags a cursor leak. Used by
        tests as a leak-detection invariant.
        """
        count = 0
        node = self._head.next
        while node is not self._tail:
            count += 1
            node = node.next
        return count

    def __iter__(self):
        """Iterate keys in arbitrary order."""
        return iter(self._data)

    def keys(self):
        """Iterate keys in arbitrary order."""
        return self._data.keys()

    def values(self):
        """Iterate values in arbitrary order.

        In contrast to `mutable_values()`, this will return every value exactly once, but the order
        is arbitrary and the iterator will raise an exception if the mapping is mutated during
        iteration.
        """
        for node in self._data.values():
            yield cython.cast(_Node, node).value

    def items(self):
        """Yield `(key, value)` pairs in arbitrary order.

        Returns every `(key, value)` pair exactly once, but the order is arbitrary and the iterator
        will raise an exception if the mapping is mutated during iteration.
        """
        for key, node in self._data.items():
            yield (key, cython.cast(_Node, node).value)

    @cython.locals(cursor=_Node, head_next=_Node, nxt=_Node, prev=_Node, after=_Node)
    def mutable_values(self):
        """Yield values in order, allowing concurrent mutation.

        Under mutation, a value may be yielded more than once or not at all.
        """

        # This method works by inserting a placeholder node (the *cursor*) into the linked list and
        # advancing it between yields. The cursor is tagged with the private `_CURSOR_KEY` sentinel
        # so that it can be identified and skipped by identity. The cursor is unlinked on iterator
        # exit.

        cursor = _Node(_CURSOR_KEY, None)
        head_next = self._head.next
        cursor.prev = self._head
        cursor.next = head_next
        head_next.prev = cursor
        self._head.next = cursor
        try:
            while True:
                nxt = cursor.next
                if nxt is self._tail:
                    return

                prev = cursor.prev
                after = nxt.next
                prev.next = nxt
                nxt.prev = prev
                nxt.next = cursor
                cursor.prev = nxt
                cursor.next = after
                after.prev = cursor
                if nxt.key is _CURSOR_KEY:
                    continue
                yield nxt.value
        finally:
            prev = cursor.prev
            nxt = cursor.next
            prev.next = nxt
            nxt.prev = prev
            cursor.prev = None
            cursor.next = None
