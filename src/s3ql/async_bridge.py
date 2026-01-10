'''
async_bridge.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import annotations

import logging
import threading
from contextlib import AbstractContextManager
from typing import TYPE_CHECKING

import trio
from anyio.from_thread import BlockingPortal, start_blocking_portal

log = logging.getLogger(__name__)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from typing_extensions import TypeVar, TypeVarTuple, Unpack

    T = TypeVar('T')
    Ts = TypeVarTuple('Ts')

_portal_lock = threading.Lock()
_portal: BlockingPortal | None = None
_portal_cm: AbstractContextManager[BlockingPortal] | None = None


def _get_or_create_portal() -> BlockingPortal:
    '''Get or create a blocking portal in a daemon thread.'''
    global _portal, _portal_cm

    with _portal_lock:
        if _portal is not None:
            return _portal

        log.debug('Creating new blocking portal')
        _portal_cm = start_blocking_portal(backend='trio')
        _portal = _portal_cm.__enter__()
        return _portal


def shutdown_portal() -> None:
    '''Shut down the blocking portal if it exists.'''

    global _portal, _portal_cm

    with _portal_lock:
        if _portal_cm is not None:
            log.debug('Shutting down blocking portal')
            _portal_cm.__exit__(None, None, None)
            _portal = None
            _portal_cm = None


def run_async(
    afn: Callable[[Unpack[Ts]], Awaitable[T]],
    *args: Unpack[Ts],
) -> T:
    '''Run an awaitable from synchronous code.

    This function handles three scenarios in order:
    1. If called from async context (main Trio thread): raise RuntimeError
    2. If in a trio worker thread OR pyfuse3.trio_token exists: use trio.from_thread.run()
    3. Otherwise: use anyio blocking portal in a daemon thread
    '''

    # Check if we're in the main Trio thread (async context)
    try:
        trio.lowlevel.current_trio_token()
    except RuntimeError:
        pass
    else:
        raise RuntimeError('run_async() cannot be called from async context. Use await instead.')

    # Try to submit to the Trio main loop
    try:
        return trio.from_thread.run(afn, *args)
    except RuntimeError as e:
        # Unfortunately there's no more specific exception for "no trio event loop running
        # in any parent thread'.
        if "this thread wasn't created by trio" not in str(e).lower():
            raise

        # Use (or start) or own event loop in a separate thread
        portal = _get_or_create_portal()
    return portal.call(afn, *args)
