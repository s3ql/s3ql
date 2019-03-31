# Steps for Releasing a New Version =

* Bump version in `src/s3ql/__init__.py`
* Add release date to `Changes.txt`
* `./setup.py build_cython build_ext --inplace`
* `./setup.py build_sphinx sdist`
* Test tarball:
  * `./setup.py build_ext --inplace`
  * `./setup.py build_sphinx`
  * `python3 -m pytest tests/`
* Generate signature: `gpg -sb --armor XXXX.tar.bz2`
* `git commit --all -m "Released XXX"`
* `git tag -s release-x.y -m "Tagged release"`
* `git push && git push --tags`, create release on Github
* `./setup.py upload_docs`
* Write announcement to mailing list
  * Get contributors: `git log --pretty="format:%an <%aE>" "${PREV_TAG}..${TAG}" | sort -u`
  * Update authors: `git log --all --pretty="format:%an <%aE>" | sort -u >> AUTHOR`

## Email template

```
From: Nikolaus Rath <Nikolaus@rath.org>
Subject: [s3ql] [ANNOUNCE] S3QL X.XX has been released
To: s3ql@googlegroups.com

Dear all,

I am pleased to announce a new release of S3QL, version XXX.

From the changelog:

[PASTE CHANGELOG ENTRY]

The following people have contributed code to this release:

[PASTE HERE]

(The full list of contributors is available in the AUTHORS file).

The release is available for download from
https://github.com/s3ql/s3ql/releases.

Please report any bugs on the mailing list (s3ql@googlegroups.com) or
the issue tracker (https://github.com/s3ql/s3ql/issues).

Best,
-Nikolaus
```
