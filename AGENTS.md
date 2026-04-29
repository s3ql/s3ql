# AI Agent Instructions

This file provides guidance to AI Coding Agents when working with code in this repository.

## Project Overview

S3QL is a POSIX file system that stores data in cloud storage (Amazon S3, Google Cloud Storage, OpenStack Swift, Backblaze B2). It provides compression, encryption, deduplication, and snapshots. Uses FUSE for filesystem operations and Trio for async I/O.

## Build and Development Commands

```bash
# Install dependencies (using uv)
uv sync --locked

# Run all tests
uv run pytest -r s -x tests/

# Run specific test file with debug logging
uv run pytest -x --logdebug=all tests/t3_fs_api.py

# Code quality (run in this order)
uv run ruff check --fix .
uv run ty check --output-format=concise
uv run ruff format .
```

Test files are organized by level: t0 (HTTP), t1 (backends/database), t2 (block cache), t3 (filesystem), t4 (admin/FUSE), t5-t6 (integration).

## Architecture

```
FUSE Operations (fs.py)
        ↓
   FileSystem
        ↓
Database (SQLite via database.py)
        ↓
BlockCache ↔ InodeCache
        ↓
Backend (S3, GCS, Swift, B2, Local)
        ↓
HTTP Client (custom implementation)
```

**Key modules in `src/s3ql/`:**

| Module | Purpose |
|--------|---------|
| fs.py | FUSE operations (Operations class) |
| database.py | SQLite layer, FsAttributes dataclass |
| mount.py | Main mounting logic |
| mkfs.py | Format/initialize filesystem |
| fsck.py | Consistency checking and repair |
| block_cache.py | Local block caching with async uploads |
| inode_cache.py | In-memory inode caching |
| http.py | Custom HTTP client (100-continue, chunked encoding) |
| backends/ | Storage backend implementations |

**Backend architecture:** `AsyncBackend` base class in `backends/common.py`. `AsyncComprencBackend` wraps backends to add compression/encryption.

**Entry points:** `mkfs.s3ql`, `mount.s3ql`, `umount.s3ql`, `fsck.s3ql`, `s3qladm`, `s3qlcp`, `s3qlstat`, `s3qlctrl`, `s3qllock`, `s3qlrm`, `s3ql_verify`

## Key Constants

From `src/s3ql/__init__.py`:
- `CURRENT_FS_REV = 26` - filesystem revision
- `BUFSIZE = 256KB` - object upload buffer
- `CTRL_INODE = ROOT_INODE + 1` - special control file

## Technical Notes

- Uses Trio (not asyncio) for async I/O
- Custom HTTP client in http.py (not using requests for core operations)
- SQLite extension (sqlite3ext.pyx) tracks dirty blocks for upload
- pytest-trio for async test support
- Uses Jujutsu (jj) for version control, not git

## Workflow


- Before completing a task, run `ruff check --fix`, `ruff format`, `ty check` and `pytest` (in this
  order). Fix any reported issues before running the next tool.



