#!/usr/bin/env python3
'''
t2_linked_dict.py - this file is part of S3QL.

Copyright © 2026 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

import pytest

from s3ql._linked_dict import LinkedDict


def assert_cursor_unlinked(d):
    '''Assert no `mutable_values` cursor is still parked in the linked list.

    `_linked_list_length` counts every node between the head and tail
    sentinels (cursor sentinels included), so a mismatch with `len(d)`
    flags a leaked cursor.
    '''
    assert d._linked_list_length() == len(d)


def make_dict(n):
    d = LinkedDict()
    for i in range(n):
        d[i] = f'v{i}'
    return d


@pytest.mark.parametrize('n', [0, 5])
def test_mutable_values_in_order(n):
    d = make_dict(n)
    assert list(d.mutable_values()) == [f'v{i}' for i in range(n)]
    assert_cursor_unlinked(d)


@pytest.mark.parametrize('n', [1, 5])
def test_move_to_tail(n):
    d = make_dict(n)
    d.move_to_tail(0)
    expected = [f'v{i}' for i in range(1, n)] + ['v0']
    assert list(d.mutable_values()) == expected
    assert_cursor_unlinked(d)


def test_concurrent_iterators_with_overtake():
    n = 5
    d = make_dict(n)

    a = d.mutable_values()
    b = d.mutable_values()
    b_yielded = [next(b), next(b)]  # b walks ahead of a
    a_yielded = list(a)
    b_yielded.extend(b)

    assert a_yielded == [f'v{i}' for i in range(n)]
    assert b_yielded == [f'v{i}' for i in range(n)]
    assert_cursor_unlinked(d)


def test_cursor_cleanup_on_exhaustion():
    d = make_dict(5)
    for _ in d.mutable_values():
        pass
    assert_cursor_unlinked(d)


def test_cursor_cleanup_on_abort():
    d = make_dict(5)

    def iterate():
        it = d.mutable_values()
        for i, _ in enumerate(it):
            if i == 2:
                return

    iterate()
    assert_cursor_unlinked(d)
