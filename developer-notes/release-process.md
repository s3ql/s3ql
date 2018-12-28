# Steps for Releasing a New Version =

* Bump version in `src/s3ql/__init__.py`
* Add release date to `Changes.txt`
* Check `hg status -u`, if necessary run `hg purge` to avoid undesired files in the tarball.
* `./setup.py build_cython build_ext --inplace`
* `./setup.py build_sphinx sdist`
* Test tarball:
  * `./setup.py build_ext --inplace`
  * `./setup.py build_sphinx`
  * `python3 -m pytest tests/`
* Generate signature: `gpg -sb --armor XXXX.tar.bz2`
* hg commit, hg tag
* `./setup.py upload_docs`
* Upload source distribution and signature to BitBucket
* `hg push && hg push github`
* Create release on Github
* Write announcement to mailing list

## Email template

```
From: Nikolaus Rath <Nikolaus@rath.org>
Subject: [s3ql] [ANNOUNCE] S3QL X.XX has been released
To: s3ql@googlegroups.com

Dear all,

I am pleased to announce a new release of S3QL, version XXX.

From the changelog:

[PASTE CHANGELOG ENTRY]

The release is available for download from
https://github.com/s3ql/s3ql/releases.

Please report any bugs on the mailing list (s3ql@googlegroups.com) or
the issue tracker (https://github.com/s3ql/s3ql/issues).

Best,
-Nikolaus
```
