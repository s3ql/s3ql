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

**Database schema:** The on-disk SQLite schema is defined in `create_tables()` in `src/s3ql/database.py`. Each table and column carries inline comments that explain its semantics, including sentinel values (e.g. `objects.phys_size == -1`, `objects.hash IS NULL`), invariants (e.g. `inodes.size == len(symlink_targets.target)` for symlinks), denormalised counters, and special handling such as the AUTOINCREMENT-protected ids in `objects` and `contents`. Read those comments before writing or modifying any code that queries these tables.

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


- Before completing a task, run `ruff check --fix`, `ruff format`, `ty check`, `pytest`, and
  `./build_docs.sh` (in this order). Fix any reported issues before running the next tool.

## Releases

Cutting a new release follows `developer-notes/release-process.md`. Some
parts of that workflow can be done locally by an agent; others require
credentials or access to external systems and must be handed off to the
user.

**Agent does locally:**
- Draft the `ChangeLog.rst` entry from the commits since the last release.
- Bump `VERSION` in `src/s3ql/__init__.py` and `version` in `pyproject.toml`.
- `uv lock`.
- For non-bugfix releases that rotate signing keys: rename the public key
  `mv signify/s3ql-next.pub signify/s3ql-A.B.pub`. The matching `.sec` file
  lives in `$S3QL_SIGNING_KEYS_DIR` (outside the repo, not accessible to the
  agent), so the user handles the secret-key rename and the `signify -G -n`
  that generates the fresh `s3ql-next.{pub,sec}` pair (see "User does"
  below).
- Refresh `AUTHORS` (see command below).
- There's no need to run formatters, linters, or unit test when making
  a release. The CI has done this all for us.
- `jj_ai describe -m "Released s3ql-A.B.C"`, `jj_ai tag set s3ql-A.B.C`,
  then `jj_ai bookmark move main --to=s3ql-A.B.C`.
  DO NOT repeat the contents of ChangeLog.rst in the changeset
  description. The only thing worth mentioning there is if the
  signing key was rotated, otherwise the single line summary
  is sufficient.

**User does:**
- For non-bugfix releases that rotate signing keys, before running
  `make_release.sh`:
  - `mv "$S3QL_SIGNING_KEYS_DIR/s3ql-next.sec" "$S3QL_SIGNING_KEYS_DIR/s3ql-A.B.sec"`
  - `signify -G -n -p signify/s3ql-next.pub -s "$S3QL_SIGNING_KEYS_DIR/s3ql-next.sec"`
    (overwrites `signify/s3ql-next.pub` with the freshly generated public
    key; commit it).
- `./make_release.sh s3ql-A.B.C` — builds and signs the tarball, rsyncs
  documentation to `ebox.rath.org` (needs SSH credentials), and prints
  the announcement email body at the end with version, changelog
  excerpt, and contributor list filled in. Requires
  `$S3QL_SIGNING_KEYS_DIR` to point at the directory holding the secret
  signing keys; the script aborts otherwise.
- `jj git push` (publishes the moved `main` bookmark), then
  `git push --tags` (publishes the release tag — `jj git push` does not
  push git tags).
- Create the GitHub release at https://github.com/s3ql/s3ql/releases
  and upload the tarball plus its `.sig`.
- Copy the email body printed by `make_release.sh` and send it to
  `s3ql@googlegroups.com`.

### Refreshing AUTHORS

The file has an 11-line header (current maintainer + autogenerated-list
preamble) followed by an alphabetised contributor list. A blanket
`sort -u` over the whole file would scramble the header, and the previous
`>> AUTHORS` form left duplicates. Refresh the contributor list while
preserving the header:

```bash
{ head -n 11 AUTHORS;
  { tail -n +12 AUTHORS;
    git log --all --pretty="format:%an <%aE>" | grep -v '<none@none>$';
  } | grep -vEi 'copilot|claude|\[bot\]' \
    | sort -u --ignore-case;
} > AUTHORS.new && mv AUTHORS.new AUTHORS
```

The AI-agent filter is applied to the combined stream so previously-recorded
bot entries in `AUTHORS` (e.g. `Copilot`, `dependabot[bot]`) also drop out on
the next refresh.

Verify afterwards that `head -n 11 AUTHORS` is unchanged from the
pre-refresh state and the lines below are alphabetised with no
duplicates. If the header line count ever changes, update the `head -n
11` and `tail -n +12` constants here in lockstep.



