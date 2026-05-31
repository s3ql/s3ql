# Steps for Releasing a New Version

2. Go through commits since last release, and document user-visible changes in
  `ChangeLog.rst`. Decide on an appropriate version number, and add the corresponding
   release heading (The version number in this heading is what the release
   script will use.).

3. Make sure ``$S3QL_SIGNING_KEYS_DIR`` points at the directory holding the
   signify secret keys (containing at minimum ``s3ql-next.sec`` and
   ``s3ql-<current-major>.sec``).

4. Run ``util/make_release.py``. The script:

   * rotates the signify keys when the new version starts a new minor or
     major release (otherwise it reuses the existing key for bugfix releases),
   * commits changes and creates release tag
   * builds the HTML docs and release tarball
   * signs the sdist with ``signify``
   * uploads the HTML documentation
   * writes an announcement template

6. Push to GitHub: ``git push --tags && jj git push -b main``.

7. Create a release on GitHub at
   https://github.com/s3ql/s3ql/releases/.

8. Send announcement email to `s3ql@googlegroups.com`.
