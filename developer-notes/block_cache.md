# Concurrency in BlockCache

## 1. Introduction

`BlockCache` (`src/s3ql/block_cache.py`) manages a local disk cache of file blocks backed by cloud storage objects. It handles downloading blocks on cache miss, uploading dirty blocks to the backend, content-based deduplication, cache eviction under space pressure, and block removal when files are deleted or truncated.

Multiple Trio tasks interact with `BlockCache` concurrently:

- **FUSE operation tasks** call `get()` to read/write blocks, `remove()` to delete blocks, and `flush_local()` for fsync.
- **CommitTask** (`src/s3ql/mount.py`) periodically iterates the cache and calls `upload_if_dirty()` on blocks that have been dirty longer than a configured delay.
- **Upload worker tasks** (spawned by `BlockCache.create()`) consume from the upload channel and execute `_do_upload()` to write objects to the backend.
- **Removal worker tasks** consume from the removal channel and execute backend deletions, either individually or in batches.

These tasks share mutable state: the cache dictionary, per-entry `in_transit` flags, the SQLite database, and the backend. Correctness depends on a locking protocol built on `MultiLock`, Trio's cooperative scheduling guarantees, and the atomicity of synchronous code between `await` points. This document describes that protocol in enough detail to reason about correctness when modifying `block_cache.py` or code that interfaces with it.


## 2. Trio Concurrency Model

### 2.1 Cooperative Scheduling and Checkpoints

Trio uses cooperative multitasking. A task runs until it hits an `await` that is a *checkpoint* (any `await` that can actually suspend the task). Between checkpoints, a task has exclusive access to all in-process Python state. This means that a sequence of synchronous operations — dictionary lookups, set mutations, attribute assignments — cannot be interleaved with operations from another task, regardless of how many tasks are runnable.

This property is used extensively in `block_cache.py`. For example, `upload_if_dirty()` performs multiple database queries and mutations (checking `inode_blocks`, inserting into `objects`, updating refcounts) between checkpoints. These execute atomically with respect to other tasks without any explicit lock protecting the database.

**Consequence:** any change that introduces a new `await` between operations that were previously adjacent breaks the implicit atomicity. When adding `await` points, audit the surrounding code for invariants that depend on non-interleaving.

### 2.2 Database Operations Are Synchronous

All SQLite operations go through the `Connection` class (`src/s3ql/database.py`), which wraps APSW. Every method — `execute()`, `get_val()`, `get_row()`, `rowid()`, `has_val()`, and all typed accessors — makes blocking calls to SQLite without yielding to the Trio scheduler. They are not `async` and contain no checkpoints.

This means a sequence of database calls within a single task behaves like a critical section with no explicit lock needed. The `Connection` class is documented as not thread-safe, but this is irrelevant in practice: only one task executes between checkpoints, and database calls are never offloaded to threads.

The database uses WAL mode with exclusive locking. Transactions are managed through `BatchedTransactionManager` for grouping writes and periodic checkpointing. On crash, SQLite's WAL recovery automatically restores the database to the last committed transaction boundary.

### 2.3 Thread Offloading

Two operations use `trio.to_thread.run_sync` to offload work to an OS thread:

1. **SHA-256 hashing** in `upload_if_dirty()`: `await trio.to_thread.run_sync(sha256_fh, el)`
2. **fsync** in `_get_entry()`: `await trio.to_thread.run_sync(os.fsync, tmpfh.fileno())`

These are checkpoints — the calling task yields and other tasks can run while the thread executes. In the hashing case, the `CacheEntry` already has its `in_transit` flag set and its `block_lock` held, so no other task can access or evict it. The thread only touches the entry's file handle (seeking and reading), not any shared Python data structures. See Section 8 for further thread safety analysis.


## 3. Synchronization Primitives

### 3.1 MultiLock

`MultiLock` (`src/s3ql/multi_lock.py`) is an async key-based lock built on `trio.Condition`. It maintains a set of currently-held keys. Any hashable value can serve as a key.

`BlockCache` uses a single `MultiLock` instance as **`block_lock`**. It is keyed by `(inode, blockno)` and protects a cache entry and its associated database rows in `inode_blocks`.

API:

| Method | Behavior |
|--------|----------|
| `await lock.acquire(key...)` | Suspends until `key` is not held, then adds it to the locked set. |
| `await lock.release(key..., noerror=False)` | Removes `key` from the locked set. Raises `KeyError` if not held. With `noerror=True`, silently succeeds even if not held. |
| `lock.acquire_nowait(key...)` | Non-blocking. Returns `True` if acquired, `False` if already held. |
| `async with lock(key...)` | Context manager: acquires on entry, releases on exit. |

Properties to keep in mind:

- **Not reentrant.** Acquiring a key that the same task already holds will deadlock.
- **Cross-task release is intentional.** `upload_if_dirty()` acquires the block lock, then sends work to an upload worker via a channel. The worker's `_do_upload()` releases the lock. This is documented in `multi_lock.py` as a feature, not a bug.
- **`notify_all()` on release.** All waiting tasks on the same `MultiLock` instance wake up and re-check whether their specific key is now free.
- **`noerror=True`** is used in exception handlers where the lock may or may not have been acquired (e.g., the `block_lock.release(..., noerror=True)` call in `upload_if_dirty`'s exception path).

### 3.2 `in_flight_uploads` (dict[int, trio.Event])

A dictionary mapping `obj_id` to a `trio.Event` that is set when the upload of that object completes (whether successfully or not). An entry exists in the dict if and only if an upload for that `obj_id` is in flight.

The event is created and inserted into the dict in `upload_if_dirty()` immediately after the `objects` row is inserted into the database, before any `await` that could let another task observe the new `obj_id`. The upload worker pops the entry and sets the event in `_do_upload()`'s `finally` block, ensuring that waiters wake on both success and failure.

Used by `_get_entry()` (before downloading) and `_deref_obj()` (before queuing a removal) to wait until the upload of a specific object has finished. Both callers look up the event by `obj_id`; if it is present, they await it. Waiters are only woken when their specific upload completes, not on every upload.

### 3.3 `transfer_completed` (trio.Condition)

A `trio.Condition` signaled by `_do_upload()` whenever an upload finishes (successfully or not). Used by `wait()`, which is called from `evict()` when dirty entries need to be flushed before eviction can proceed.

The `wait()` method uses a loop with a 5-second timeout:

```python
while True:
    if not self.transfer_in_progress():
        return
    with trio.move_on_after(5):
        async with self.transfer_completed:
            await self.transfer_completed.wait()
            return
```

The timeout guards against a race: a transfer could complete between the `transfer_in_progress()` check and entering `wait()`. Without the timeout, `wait()` would block forever in that case.

### 3.4 Memory Channels

Two Trio memory channels distribute work to worker tasks:

- **`_upload_send` / upload workers** — Zero-capacity channel (`trio.open_memory_channel[...](0)`). A send blocks until a worker is ready to receive. This provides natural backpressure: `upload_if_dirty()` blocks if all upload workers are busy.
- **`_remove_send` / removal workers** — Bounded channel with capacity 1000. Removal is less latency-sensitive, so items can accumulate. `_removal_task_multi` drains all available items without blocking after the first receive, then issues a single `delete_multi` call.

Each worker receives a **clone** of the receive end. When a worker exits (normally or due to an unhandled exception), its clone closes. Once all clones are closed, subsequent sends raise `trio.BrokenResourceError` instead of blocking — preventing deadlocks when all workers have died.

The number of workers equals `backend_pool.max_connections`, matching the available backend connection pool capacity.

### 3.5 BackendPool

`BackendPool` (`src/s3ql/backends/pool.py`) is a connection pool governed by a `trio.CapacityLimiter`. Acquired via `async with self.backend_pool() as backend:`. The limiter ensures that at most `max_connections` backend operations run concurrently. Since upload and removal workers each acquire a backend connection for their operation, the pool bounds total concurrent backend I/O.


## 4. Key Data Structures and Invariants

### 4.1 `cache` (CacheDict)

An `OrderedDict` mapping `(inode, blockno)` to `CacheEntry`. Ordering serves as an LRU list: the right end holds the most-recently-used entries, the left end the least-recently-used. `move_to_end((inode, blockno), last=True)` marks an entry as recently used (both `get()` fast path and `_get_entry()` do this), and newly inserted entries land on the right by default. Eviction in `evict()` iterates from the left end, so the least-recently-used entries are considered first.

`CacheDict` tracks aggregate state:

- **`size`**: sum of all entry sizes. Updated when entries are added, removed, or resized. Must stay consistent — `get()` updates it after yielding the entry to the caller (`cache.size += el.size - oldsize`).
- **`max_size`**, **`max_entries`**: eviction thresholds. `is_full()` returns `True` when either is exceeded.

**Invariant:** `cache.size` equals the sum of `el.size` for all entries in the dict. Violations cause eviction to behave incorrectly (evicting too much or too little).

### 4.2 CacheEntry

A file-backed block with the following concurrency-relevant attributes:

| Attribute | Description |
|-----------|-------------|
| `dirty` | `True` if modified since last upload. Set by `write()` and `truncate()`. Cleared by `upload_if_dirty()` (on dedup match) or `_do_upload()` (on successful upload). |
| `inode`, `blockno` | Immutable identifiers. Used as the cache dict key and the `block_lock` key. |
| `size` | Current logical size of the block. Updated by `write()` and `truncate()`. |
| `last_write` | Timestamp of last `write()` call. Used by `CommitTask` to delay uploads until the block has been idle for a configured period. |
| `fh` | Unbuffered file handle to the backing file on disk (`open(filename, mode, 0)`). |

The backing file is named `{inode}-{blockno}` in the cache directory. Its contents represent the current block data, whether or not it has been uploaded to the backend.

**Invariant:** A dirty entry always remains in `cache`. Its `in_transit` flag may additionally be set while it is being uploaded. An entry must never be dirty without being in `cache` — if it were, the dirty data would be silently lost.

### 4.3 `in_transit` flag and counter

Each `CacheEntry` has a boolean `in_transit` attribute that records whether the entry is currently being uploaded. `BlockCache` keeps a single counter `_in_transit_count` tracking how many entries have the flag set. Two helpers, `_mark_in_transit()` and `_unmark_in_transit()`, keep the flag and counter in lockstep.

The flag is set in `upload_if_dirty()` before hashing begins and cleared either:

- In `upload_if_dirty()` itself, if a dedup match is found (no upload needed).
- In `_do_upload()`, in the `finally` block, after the upload succeeds or fails.

The entry remains in `cache` throughout — the flag is an additional marker, not an alternative location. This is harmless: the entry's `block_lock` prevents concurrent modification, and `evict()` skips in-transit entries rather than trying to evict them.

While `el.in_transit` is `True`:

- The entry's `block_lock` is held — by the upload pipeline once the entry has been handed off, not by the task that initiated the upload. If a new object was created, an entry also exists in `in_flight_uploads` for that `obj_id` until the upload completes.
- `evict()` skips it (cannot evict an entry being uploaded).
- `upload_if_dirty()` returns `True` immediately if called again for the same entry.
- `get()` on the same `(inode, blockno)` blocks on `block_lock` until the upload completes and the lock is released.


### 4.4 Database Tables

Two tables track the mapping between file blocks and backend objects:

**`objects`** — one row per deduplicated storage object:

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER PK | Auto-increment object identifier. |
| `hash` | BLOB(32) UNIQUE | SHA-256 of content. `NULL` = upload failed (sentinel). |
| `refcount` | INT | Number of `inode_blocks` rows pointing to this object. |
| `length` | INT | Logical size of the block data. |
| `phys_size` | INT | Physical size in backend after compression. `-1` = not yet uploaded (sentinel). |

**`inode_blocks`** — maps file blocks to objects:

| Column | Type | Description |
|--------|------|-------------|
| `inode` | INTEGER | References `inodes(id)`. |
| `blockno` | INT | Block number within the file. |
| `obj_id` | INTEGER | References `objects(id)`. |
| | | Primary key is `(inode, blockno)`. |

Multiple `inode_blocks` rows can reference the same `objects` row — this is how deduplication works. The `refcount` column in `objects` tracks the number of such references.

**Sentinel values:**

| Value | Meaning | Set by | Resolved by |
|-------|---------|--------|-------------|
| `phys_size = -1` | Object created in DB but not yet uploaded to backend | `upload_if_dirty()` on INSERT | `_do_upload()` on success; `fsck.check_objects_phys_size()` after crash |
| `hash = NULL` | Upload failed; object may or may not exist in backend | `_do_upload()` on failure | `fsck.check_objects_hash()` deletes the row |


## 5. Locking Protocol

### 5.1 Lock Lifecycle in the Upload Path

The upload path spans two tasks and involves a cross-task lock handoff via the upload channel:

```
upload_if_dirty()                        _do_upload()
─────────────────                        ────────────
block_lock.acquire(inode, blockno)
_mark_in_transit(el)
hash (yields to thread)
INSERT INTO objects
INSERT INTO inode_blocks
in_flight_uploads[obj_id] = Event()
send to upload channel ──────────────►  receive from channel
                                         write to backend
                                         UPDATE objects SET phys_size=...
                                         el.dirty = False
                                         _unmark_in_transit(el)
                                         pop in_flight_uploads[obj_id].set()
                                         block_lock.release(inode, blockno)
                                         notify transfer_completed
```

The initiating task (FUSE handler, CommitTask, or `evict()`) acquires the block lock and registers the completion event, but does not release the lock or set the event. The upload worker performs both after the backend write completes (or fails). The cross-task release of `block_lock` is the reason `MultiLock` explicitly supports it.

On **dedup match** (existing object with the same hash), the path is shorter and stays within a single task:

```
upload_if_dirty()
─────────────────
block_lock.acquire(inode, blockno)
_mark_in_transit(el)
hash (yields to thread)
find matching object in DB
UPDATE objects SET refcount=refcount+1
INSERT OR REPLACE INTO inode_blocks
el.dirty = False
_unmark_in_transit(el)
block_lock.release(inode, blockno)
_deref_obj(old_obj_id)
```

No event is registered because no upload occurs.

### 5.2 Waiting for Upload Completion in `_get_entry()`

In `_get_entry()`, when downloading an object from the backend, the code waits for any in-flight upload of that object to finish:

```python
if (event := self.in_flight_uploads.get(obj_id)) is not None:
    await event.wait()
```

If no upload is in flight for `obj_id`, the lookup returns `None` and the wait is skipped. If an upload is in flight, the await blocks until the upload finishes and `_do_upload` sets the event. This ensures the download reads the fully-written object. Deletion is not a concern because the block lock `(inode, blockno)` guarantees a reference to the object exists for as long as the cache entry exists.

### 5.3 Waiting for Upload Completion in `_deref_obj()`

`_deref_obj()` uses the same pattern before queuing a removal:

```python
if (event := self.in_flight_uploads.get(obj_id)) is not None:
    await event.wait()
```

This ensures that the object is no longer being uploaded before it is deleted. After the wait, the upload has either succeeded (object exists in backend, ready for deletion) or failed (object may not exist; `_deref_obj` checks `phys_size == -1` and skips the deletion in that case).

The object row has already been deleted from the `objects` table before this wait. This is safe because:

1. The object can no longer be found by deduplication (hash lookup will not match a deleted row).
2. No new upload can target this `obj_id`.
3. `_deref_obj` only runs when `refcount` drops to zero, meaning no `inode_blocks` rows reference this object — so no concurrent download for this object is possible.

### 5.4 Block Lock in `get()`

`get()` has two paths, each with different lock behavior:

**Fast path** (`max_write` is not `None` and `max_write < el.size`): The block lock is held via `async with self.block_lock(inode, blockno)` for the duration of the caller's use of the entry. This path is taken when the caller promises not to grow the block, so cache size accounting is not affected and no expiration check is needed.

**Slow path**: The block lock is acquired with `await self.block_lock.acquire(inode, blockno)` and released in a `finally` block after the caller is done. Between yield and release, the cache size is updated (`cache.size += el.size - oldsize`). This path may trigger `evict()` before acquiring the lock if the cache is full. The lock is held across the caller's operations, guaranteeing exclusive access to the entry.

In both paths, the lock is held while the entry is yielded to the caller. This means only one task can read or write a given block at a time.


## 6. Concurrency Scenarios

This section traces through the most important concurrent interactions step by step. Each scenario identifies the tasks involved and the sequence of lock acquisitions, checkpoints, and state mutations.

### 6.1 Read/Write via `get()` — Fast Path

**Tasks:** A single FUSE handler task.

1. Caller invokes `get(inode, blockno, max_write=N)` where `N < el.size`.
2. `async with self.block_lock(inode, blockno)` — acquires block lock.
3. Entry found in `cache`. `max_write < el.size` confirmed.
4. `move_to_end()` — marks as recently used (synchronous, no checkpoint).
5. Entry yielded to caller. Caller reads or writes (synchronous file I/O on the cache file).
6. Context manager exits — releases block lock.

No expiration check. No backend interaction. No checkpoint while the caller holds the entry (unless the caller itself awaits, which FUSE handlers do not do within a single `get()` block).

### 6.2 Read/Write via `get()` — Slow Path

**Tasks:** A FUSE handler task, potentially interacting with upload workers via lock contention.

1. `cache.is_full()` may be true — calls `evict()` (see 6.6) before acquiring any lock.
2. `await self.block_lock.acquire(inode, blockno)` — acquires block lock.
3. Calls `_get_entry()`:
   - **Cache hit:** `move_to_end()`, return entry.
   - **Cache miss, no DB row:** Create new empty `CacheEntry`, add to cache, return.
   - **Cache miss, DB row exists:** Download from backend. If an upload of `obj_id` is in flight, waits on the entry's completion event in `in_flight_uploads` (see 5.2). Opens a `.tmp` file, downloads, fsyncs, renames to final name. Creates `CacheEntry`, adds to cache and updates `cache.size`.
4. `oldsize = el.size` recorded.
5. Entry yielded to caller. Caller reads or writes.
6. `finally` block: `cache.size += el.size - oldsize`, then releases block lock.

The `cache.size` update in step 6 requires that no other task modified `el.size` concurrently. This is guaranteed because the block lock was held throughout.

### 6.3 Upload of a New Object

**Tasks:** An initiating task (FUSE handler via `evict()`, or CommitTask) and an upload worker.

**Initiating task** (`upload_if_dirty`):

1. Check: `el.in_transit` is false and `el.dirty` is true.
2. `await self.block_lock.acquire(inode, blockno)` — acquires block lock.
3. Re-check after acquiring lock: entry still in cache, not in transit, still dirty.
4. `_mark_in_transit(el)`
5. `await trio.to_thread.run_sync(sha256_fh, el)` — **checkpoint**. Hash computed in OS thread. Other tasks can run, but the block lock prevents them from accessing this block.
6. Query `inode_blocks` for existing `obj_id` (synchronous).
7. Query `objects` for hash match — no match found.
8. `INSERT INTO objects` with `phys_size=-1` (synchronous).
9. `INSERT OR REPLACE INTO inode_blocks` (synchronous).
10. `self.in_flight_uploads[obj_id] = trio.Event()` (synchronous). The event is registered before the next checkpoint so that any waiter that discovers the `obj_id` via the database will also find the event.
11. `await self._upload_send.send((el, obj_id))` — blocks until a worker is ready. **Checkpoint.** Initiating task is now done; control passes to the worker.

**Upload worker** (`_do_upload`):

12. `async with self.backend_pool() as backend` — acquires backend connection. **Checkpoint.**
13. `await backend.write_fh(...)` — writes object to backend. **Checkpoint.**
14. `UPDATE objects SET phys_size=...` (synchronous).
15. `el.dirty = False`.
16. `_unmark_in_transit(el)`
17. `self.in_flight_uploads.pop(obj_id).set()` — wakes any task waiting on this upload.
18. `await self.block_lock.release(inode, blockno)` — releases block lock.
19. Signal `transfer_completed`.

Between steps 4 and 16, the entry's `in_transit` flag is set. Between steps 2 and 18, the block lock is held (across a task boundary). Between steps 10 and 17, an entry exists in `in_flight_uploads` for `obj_id`.

### 6.4 Deduplication (Hash Matches Existing Object)

**Tasks:** A single initiating task. No upload worker involved.

Steps 1–6 are identical to 6.3. Then:

7. Query `objects` for hash match — **match found**, returns `obj_id`.
8. If `old_obj_id != obj_id`: increment refcount on matched object, `INSERT OR REPLACE INTO inode_blocks` (synchronous).
9. `el.dirty = False`.
10. `_unmark_in_transit(el)`
11. `await self.block_lock.release(inode, blockno)` — releases block lock.
12. If there was an `old_obj_id` and it differs from `obj_id`: call `_deref_obj(old_obj_id)` which may decrement refcount and queue the old object for backend deletion.

No completion event is registered because no upload occurs. The entry spends less time with `in transit` set (steps 4–10 only, no backend I/O).

Note that if `old_obj_id == obj_id` (the block was re-dirtied with the same content), no DB changes are made and the method returns `False`.

### 6.5 Download Racing with Upload

**Tasks:** Task A is uploading object X. Task B wants to read a block backed by object X.

1. Task A has registered `in_flight_uploads[obj_id_X]` (step 10 in 6.3) and is uploading.
2. Task B enters `get()` slow path, acquires block lock for its block.
3. Task B enters `_get_entry()`, finds its block is not in cache, queries DB, finds it maps to `obj_id_X`.
4. Task B looks up `in_flight_uploads[obj_id_X]`, finds the event, and `await event.wait()` — **blocks**.
5. Task A's upload worker completes the upload, pops the entry, and sets the event.
6. Task B's wait returns.
7. Task B downloads the object (which now exists in the backend).

Without this wait, Task B could attempt to download an object that has not yet been written.

### 6.6 Cache Eviction (`evict()`)

**Tasks:** The task calling `evict()` (typically from `get()` slow path), plus any upload workers it triggers.

1. Compute `need_size` and `need_entries` from cache limits.
2. Iterate a **snapshot** (`list(self.cache.values())`) — a copy is necessary because the dict may change during iteration.
3. For each entry:
   - If dirty: call `upload_if_dirty(el)`. If it returns `True` (upload started or already in transit), set `sth_in_transit = True` and skip eviction.
   - If clean: `await self.block_lock.acquire(inode, blockno)`, then re-validate (entry may have been removed or re-dirtied while waiting for the lock). If still clean and still in cache, call `self.cache.remove(...)` which closes the file handle, decrements `cache.size`, and unlinks the backing file.
4. If any entries were in transit, call `await self.wait()` (which blocks until an upload completes), then loop back to step 1.

The re-validation in step 3 is essential: between iterating the snapshot and acquiring the lock, another task may have evicted the entry, re-dirtied it, or replaced it entirely.

### 6.7 Block Removal (`remove()`)

**Tasks:** A FUSE handler (file deletion or truncation), potentially interacting with upload workers.

Uses a **two-pass** strategy to avoid unnecessary blocking:

**Pass 1** (`timeout=0`): For each blockno, attempt `block_lock.acquire_nowait(inode, blockno)`. If the lock is already held (e.g., the block is being uploaded), skip it. For acquired blocks: remove from cache if present, delete `inode_blocks` row, release block lock, then call `_deref_obj(obj_id)`.

**Pass 2** (`timeout=None`): For blocks skipped in pass 1, `await self.block_lock.acquire(inode, blockno)`. This blocks until any in-progress upload finishes. Then proceed identically: remove from cache, delete DB row, release lock, deref object.

The two-pass design matters for a common pattern: creating a file and immediately deleting it. Without pass 1, removal of every block would wait behind the upload of the first block, serializing the entire operation. Pass 1 allows immediate removal of all blocks that aren't currently locked.

Note that `_deref_obj` is called **after** releasing the block lock. This is safe because the `inode_blocks` row has already been deleted, so no other task can re-discover this block-to-object mapping.

### 6.8 CommitTask Interaction

**Tasks:** The CommitTask (background task in `mount.py`), running concurrently with FUSE handlers and upload workers.

CommitTask runs in a loop, sleeping for 5 seconds between iterations. On each iteration it:

1. Iterates `list(self.block_cache.cache.values())` — a snapshot, same as `evict()`.
2. For each entry: if `el.dirty` and `not el.in_transit` and `el.last_write` is older than the configured delay, calls `upload_if_dirty(el)`.

This is safe because `upload_if_dirty` performs all necessary locking internally. CommitTask merely acts as a trigger — it does not hold any locks across iterations, and it skips entries that are already being uploaded. The `last_write` check avoids uploading blocks that are still being actively written, reducing unnecessary uploads of data that will change again soon.

CommitTask runs concurrently with FUSE handlers that may be dirtying the same entries. The locking within `upload_if_dirty` handles this: if an entry gets re-dirtied between CommitTask's check and the lock acquisition, `upload_if_dirty` sees the current state after acquiring the lock and proceeds correctly.


## 7. Failure Modes and Crash Recovery

This section covers both runtime error handling (the process stays alive) and crash recovery (the process dies and `fsck.s3ql` repairs the filesystem on next mount).

A key property underpinning crash recovery: **backends guarantee atomic writes**. A backend object is either written completely or not at all. `fsck.s3ql` relies on this — it never needs to handle partially-written objects.

### 7.1 Upload Failure (Runtime)

When `_do_upload()` raises an exception during the backend write:

1. The entry's in-transit flag is cleared (in `finally` block).
2. `hash` set to `NULL` in the `objects` table. This prevents future deduplication against an object that may not exist in the backend. The row is not deleted because its `id` is already referenced by `inode_blocks`; deleting it would leave dangling references or allow the id to be reused for a different object.
3. `in_flight_uploads[obj_id]` is popped and the event is set, waking any waiters. `block_lock` is released.
4. `transfer_completed` signaled.
5. `el.dirty` remains `True` — the entry stays dirty in the cache for a future upload attempt.

On the next upload attempt for the same block, `upload_if_dirty` computes the hash again and creates a new `objects` row (since the old row has `NULL` hash, it won't match). The old row with `NULL` hash becomes an orphan once the `inode_blocks` row is updated to point to the new object, and will be cleaned up by `_deref_obj` or by `fsck` if a crash occurs first.

### 7.2 Crash During Upload

The process dies while `_do_upload()` is writing to the backend. Possible states on disk:

- **Backend:** Object either fully written (atomic write guarantee) or absent.
- **Database:** `objects` row exists with `phys_size = -1` (the `UPDATE objects SET phys_size=...` hasn't executed). `inode_blocks` row points to this object. `hash` is non-NULL (it was set at insert time; the `NULL` fallback only runs in the exception handler, which didn't execute).
- **Cache:** The dirty cache file `{inode}-{blockno}` persists on disk.

`fsck.s3ql` recovery:

1. **`check_cache()`**: Reads each cache file, computes its SHA-256, compares with the hash in `objects`. If mismatched (or if the object's hash is `NULL`), re-uploads the block to the backend, creating or updating the `objects` row as needed.
2. **`check_objects_phys_size()`**: Finds objects with `phys_size = -1`. If the object exists in the backend (upload completed before the crash), retrieves its actual size and updates the row. If the object does not exist, `check_objects_id()` handles it (see below).
3. **`check_objects_id()`**: Compares database rows against actual backend contents. Objects in the database but missing from the backend are deleted from the database. Orphaned backend objects (present in backend but not in database) are deleted from the backend.

Because the cache file persists, **no data is lost**. The block data can always be re-uploaded from the cache file.

### 7.3 Crash During Deduplication Relink

The process dies after `upload_if_dirty()` has linked a block to an existing object (incremented refcount, updated `inode_blocks`) but before `_deref_obj()` decremented the old object's refcount.

State on disk:

- **Database:** Old object has an inflated `refcount` (one higher than the actual number of `inode_blocks` references). The `inode_blocks` row already points to the new object.
- **Backend:** Old object still exists (deletion was never attempted).

`fsck.s3ql` recovery:

- **`check_objects_refcount()`**: Recalculates every object's refcount by counting actual `inode_blocks` references. Fixes the old object's refcount to match reality. If the refcount drops to zero, the object is deleted from the backend.

### 7.4 Crash After `_deref_obj` Deletes DB Row but Before Backend Deletion

The process dies after `_deref_obj()` deleted the `objects` row (refcount was 1) but before the removal worker deleted the object from the backend.

State on disk:

- **Database:** No `objects` row for this object. No `inode_blocks` references.
- **Backend:** Object still exists.

`fsck.s3ql` recovery:

- **`check_objects_id()`**: Finds the backend object with no corresponding database row. Deletes it from the backend.

### 7.5 Dirty Cache Files on Restart

When S3QL starts, `BlockCache.load_cache()` loads all files matching `{inode}-{blockno}` from the cache directory. These are loaded as clean entries (the `CacheEntry` constructor sets `dirty = False`). This is just for operational use — the entries are assumed to match the backend.

Before mounting, `fsck.s3ql` must run if the filesystem was not cleanly unmounted. Its `check_cache()` method:

1. Reads each cache file and computes its SHA-256.
2. Looks up the corresponding object in the database.
3. If the hash matches: the cache file is consistent. Kept if `--keep-cache` was specified, deleted otherwise.
4. If the hash does not match (or the object has `NULL` hash): the cache file contains dirty data. Re-uploads it to the backend and updates the database.
5. If no corresponding `inode_blocks` row exists: re-uploads regardless. Later fsck passes (`check_inode_blocks_inode()`) clean up any dangling references if the inode itself no longer exists.
6. Temporary `.tmp` files (incomplete downloads) are deleted unconditionally.

### 7.6 Failsafe Mode

`self.fs.failsafe` is set to `True` when a backend object is unexpectedly missing during a removal attempt (`_removal_task_multi`, `_removal_task_simple`) or when an object referenced by a cache miss has `NULL` hash (`_get_entry`). Once set, the filesystem operations layer prevents further data-mutating operations to limit damage. The filesystem must be unmounted and `fsck.s3ql` must be run before it can be used again.

### 7.7 Summary of DB-vs-Backend Inconsistencies

SQLite WAL recovery guarantees that the database is always in a transaction-consistent state after a crash. The only inconsistencies that survive a crash are between database state and backend state. The following table summarizes them:

| Inconsistency | Cause | fsck repair |
|---------------|-------|-------------|
| `objects` row with `phys_size = -1`, object exists in backend | Crash after successful backend write but before DB update | `check_objects_phys_size()` retrieves size from backend |
| `objects` row with `phys_size = -1`, object absent from backend | Crash during or before backend write | `check_objects_id()` deletes DB row |
| `objects` row with `hash = NULL` | Upload failed at runtime (exception handler ran before crash) | `check_objects_hash()` deletes DB row |
| Object in backend but not in DB | Crash after DB row deletion but before backend deletion | `check_objects_id()` deletes from backend |
| Object refcount too high | Crash between dedup relink and `_deref_obj` | `check_objects_refcount()` recalculates |
| Dirty cache file | Crash before or during upload | `check_cache()` re-uploads |


## 8. Thread Safety Notes

Only two operations offload work to OS threads via `trio.to_thread.run_sync`:

### 8.1 SHA-256 Hashing

In `upload_if_dirty()`:

```python
sha = await trio.to_thread.run_sync(sha256_fh, el)
```

`sha256_fh` (`src/s3ql/common.py`) seeks the file handle to the start, reads the entire file, and returns the hash object. The thread accesses `el.fh` (the underlying OS file descriptor via `read()` and `seek()`).

This is safe because:

- The block lock `(inode, blockno)` is held, preventing any other Trio task from calling `get()` on this block.
- The entry's `in_transit` flag is set, so `evict()` skips it.
- No other thread accesses the same file descriptor.
- `sha256_fh` does not read or write any Python-level shared state — it only touches the `CacheEntry`'s file handle and returns a new hash object.

One subtlety: `sha256_fh` calls `el.seek(0)` internally, which updates `el.pos`. But `upload_if_dirty` also calls `el.seek(0)` at line 435 before dispatching to the thread. After the thread returns, `el.pos` reflects the thread's final position (end of file). No code between the thread return and the next checkpoint depends on `el.pos` being at a specific position, so this is harmless.

### 8.2 fsync

In `_get_entry()`:

```python
await trio.to_thread.run_sync(os.fsync, tmpfh.fileno())
```

This fsyncs a temporary file (`.tmp`) that was just downloaded from the backend. The file descriptor is a raw integer passed to `os.fsync` — no Python objects are shared with the thread. The block lock `(inode, blockno)` is held, and the `.tmp` file is local to this code path (not yet visible in the cache), so no other task can interact with it.

### 8.3 General Rule

The `Connection` object (database) is never accessed from worker threads. All database operations happen in Trio tasks. The same applies to `self.cache`, `self._in_transit_count`, the per-entry `in_transit` flag, and all other `BlockCache` attributes — they are only accessed from Trio task context, where cooperative scheduling provides mutual exclusion between checkpoints.

If future changes need to offload additional work to threads, the offloaded code must only access data that is either:

- Immutable or task-local (e.g., a file descriptor, a bytes object).
- Protected by the block lock with the entry's `in_transit` flag set, ensuring no Trio task will touch it concurrently.


## 9. Quick Reference

### 9.1 Synchronization Reference

| Primitive | Key | Purpose | Set/acquired by | Cleared/released by |
|-----------|-----|---------|-----------------|---------------------|
| `block_lock` | `(inode, blockno)` | Exclusive access to a cache entry, its backing file, and its `inode_blocks` row | `get()`, `upload_if_dirty()`, `evict()`, `remove()` | Same task, or `_do_upload()` worker (cross-task) |
| `in_flight_uploads[obj_id]` | `obj_id` | Wait for upload of a specific object to complete | `upload_if_dirty()` after `INSERT INTO objects` (new-object path) | `_do_upload()` worker on success or failure |

### 9.2 CacheEntry State Transitions

```
                ┌─────────────────────────────────────────┐
                │                                         │
                ▼                                         │
        ┌──────────────┐   write()/     ┌──────────────┐  │
 new ──►│ clean        │──truncate()──► │ dirty        │  │
        │ in cache     │                │ in cache     │  │
        └──────┬───────┘                └──────┬───────┘  │
               │                               │          │
               │ evict()                      │ upload_if_dirty()
               │ remove()                      │          │
               │                               ▼          │
               │                        ┌──────────────┐  │
               │                        │ dirty        │  │
               │                        │ in cache     │  │
               │                        │ + in_transit │  │
               │                        └──────┬───────┘  │
               │                               │          │
               │              ┌────────────────┼──────────┘
               │              │                │
               │          _do_upload()     dedup match
               │          succeeds         in upload_if_dirty()
               │              │                │
               │              ▼                │
               │       ┌──────────────┐        │
               │       │ clean        │◄───────┘
               │       │ in cache     │
               │       └──────┬───────┘
               │              │
               │              │ evict()
               │              │ remove()
               ▼              ▼
        ┌──────────────────────┐
        │ removed from cache   │
        │ file handle closed   │
        │ backing file deleted │
        └──────────────────────┘
```

On upload failure, `_do_upload()` clears the entry's `in_transit` flag but leaves `dirty = True`, returning the entry to the "dirty, in cache" state for a future retry.

### 9.3 Sentinel Values

| Value | Location | Meaning | Set by | Resolved by |
|-------|----------|---------|--------|-------------|
| `phys_size = -1` | `objects` table | Object not yet uploaded | `upload_if_dirty()` on INSERT | `_do_upload()` on success; `fsck.check_objects_phys_size()` after crash |
| `hash = NULL` | `objects` table | Upload failed | `_do_upload()` on exception | `fsck.check_objects_hash()` deletes the row |
| `.tmp` suffix | Cache directory | Incomplete download | `_get_entry()` during download | `_get_entry()` deletes on error; `fsck.check_cache()` deletes after crash |

### 9.4 Checkpoint Awareness Checklist

When modifying `block_cache.py`, check for these patterns:

- **Adding an `await` between synchronous operations?** Verify that no invariant depends on those operations being atomic. In particular, sequences of database calls that must see consistent state should not be split by a checkpoint.
- **Accessing shared state after an `await`?** Re-validate assumptions — another task may have modified `cache`, the `in_transit` flag, or database rows during the checkpoint. The existing code demonstrates this pattern: `upload_if_dirty()` and `evict()` both re-check entry state after acquiring locks.
- **Holding a lock across an `await`?** This is sometimes necessary (e.g., the upload path holds the block lock across the hash computation). Ensure the locked resource cannot be needed by the task you might yield to, or deadlock results.
- **Offloading to a thread?** The thread must not access `cache`, the `in_transit` flag or counter, the database, or any shared Python object. Only file descriptors and immutable data are safe. See Section 8.

