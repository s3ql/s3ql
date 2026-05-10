# Steps for Releasing a New Version

* Start a new changeset
* Go through commits since last release, and document user-visible changes in
 `ChangeLog.rst`. Decide on an appropriate version number, $VERSION:
 * `export VERSION=XX.YY.Z`
 * `export MAJOR_REV=${VERSION%.*}`
* When creating a new minor or major release, rotate the signing keys.
   * `mv signify/s3ql-{next, $MAJOR_REV}.pub`
   * `mv $S3QL_SIGNING_KEYS_DIR/s3ql-{next, $MAJOR_REV}.pub`
   * `signify -G -n -p signify/s3ql-next.pub -s $S3QL_SIGNING_KEYS_DIR/s3ql-next.sec`
   * Expire old release signing keys
* Update version in `src/s3ql/__init__.py`
* Update version in `pyproject.toml`
* `uv lock`
* Refresh the `AUTHORS` file to include any new contributors, preserving the
  file header.
* `jj describe -m "Released $VERSION"`
* `jj tag set s3ql-$VERSION`
* `jj bookmark move main --to=$VERSION`
* `./make_release.sh`. The script builds and signs the release tarball,
  uploads the documentation, and prints the announcement email body at the
  end with the version, changelog excerpt, and contributor list filled in.
* `jj git push && git push --tags`
* Create release on GitHub.
* Send announcement email to `s3ql@googlegroups.com`.
