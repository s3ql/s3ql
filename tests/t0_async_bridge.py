#!/usr/bin/env python3
'''
t0_async_bridge.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

if __name__ == '__main__':
    import sys

    import pytest

    sys.exit(pytest.main([__file__] + sys.argv[1:]))

from collections.abc import Generator

import pytest
import trio

from s3ql.async_bridge import run_async, shutdown_portal


@pytest.fixture(autouse=True)
def cleanup_portal() -> Generator[None, None, None]:
    '''Ensure portal is shut down before and after each test.'''
    shutdown_portal()
    yield
    shutdown_portal()


@pytest.mark.trio
async def test_run_async_from_async_context_raises() -> None:
    '''run_async() called from async context should raise RuntimeError.'''

    async def dummy_async_fn() -> str:
        return 'result'

    with pytest.raises(RuntimeError, match='cannot be called from async context'):
        run_async(dummy_async_fn)


def test_run_async_no_event_loop_uses_portal() -> None:
    '''run_async() from main thread with no Trio loop should use blocking portal.'''

    async def async_add(a: int, b: int) -> int:
        await trio.sleep(0)
        return a + b

    result = run_async(async_add, 3, 5)
    assert result == 8


def test_run_async_from_trio_thread() -> None:
    '''run_async() from a Trio-spawned thread should use trio.from_thread.run().'''
    result_holder: list[int] = []

    async def async_double(x: int) -> int:
        return x * 2

    async def main() -> None:
        def sync_call_from_thread() -> None:
            result = run_async(async_double, 21)
            result_holder.append(result)

        await trio.to_thread.run_sync(sync_call_from_thread)

    trio.run(main)
    assert result_holder == [42]


def test_run_async_propagates_exceptions() -> None:
    '''Exceptions from async functions should propagate through run_async().'''

    async def async_raise() -> None:
        raise ValueError('test error from async')

    with pytest.raises(ValueError, match='test error from async'):
        run_async(async_raise)


def test_shutdown_portal_when_active() -> None:
    '''shutdown_portal() should cleanly shut down an active portal.'''

    async def dummy() -> str:
        return 'ok'

    run_async(dummy)
    shutdown_portal()

    result = run_async(dummy)
    assert result == 'ok'


def test_run_async_with_args() -> None:
    '''run_async() should correctly pass positional arguments.'''

    async def async_concat(a: str, b: str, c: str) -> str:
        return f'{a}-{b}-{c}'

    result = run_async(async_concat, 'x', 'y', 'z')
    assert result == 'x-y-z'
