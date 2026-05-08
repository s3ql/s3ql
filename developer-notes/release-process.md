# Steps for Releasing a New Version

* `jj new`
* Go through commits since last release, and document user-visible changes in
  `ChangeLog.rst`. Decide on an appropriate version number.
* Update version in `src/s3ql/__init__.py`
* Update version in `pyproject.toml`
* `uv lock`
* `set TAG s3ql-A.B.C`
* When creating a non-bugfix release, rotate the signing keys. Each release is
  signed with the `signify/s3ql-next.{pub,sec}` pair that was prepared during
  the previous release. Promote it to the current version, then prepare a new
  `s3ql-next` pair for the release after this one:
  * `mv signify/s3ql-next.pub signify/s3ql-A.B.pub`
  * `mv signify/s3ql-next.sec signify/s3ql-A.B.sec`
  * `signify -G -n -p signify/s3ql-next.pub -s signify/s3ql-next.sec`
  * `git add signify/*.pub`
  * Expire old release signing keys (keep one around just in case).
* Refresh the `AUTHORS` file to include any new contributors, preserving the
  file header. See `AGENTS.md` for the exact command.
* `jj describe -m "Released $TAG"`
* `jj tag set $TAG`
* `./make_release.sh`. The script builds and signs the release tarball,
  uploads the documentation, and prints the announcement email body at the
  end with the version, changelog excerpt, and contributor list filled in.
* `jj bookmark move main --to=$TAG`
* `jj git push`
* Create release on GitHub.
* Copy the announcement email body printed by `make_release.sh` into your
  mail client and send it to `s3ql@googlegroups.com`.
