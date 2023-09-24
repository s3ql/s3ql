# Steps for Releasing a New Version

* `set TAG s3ql-A.B.C`
* Bump version in `src/s3ql/__init__.py`
* Add release date to `ChangeLog.rst`
* When creating non-bugfix release:
  * Create signing key for the next release: `P=s3ql-<A.B+1> signify-openbsd -G -n -p signify/$P.pub -s
  signify/$P.sec`
  * Expire old release signing keys (keep one around just in case)
* Update authors: `git log --all --pretty="format:%an <%aE>" | grep -v '<none@none>$' | sort -u >> AUTHORS`
* `git commit --all -m "Released $TAG"`
* `git tag $TAG`
* `./make_release.sh`
* Test tarball:
  * `./setup.py build_ext --inplace`
  * `python3 -m pytest tests/`
* `git checkout master`
* `git push && git push --tags`, create release on Github
* `./setup.py upload_docs`
* Write announcement to mailing list

## Email template

```
From: Nikolaus Rath <Nikolaus@rath.org>
Subject: [s3ql] [ANNOUNCE] S3QL X.XX has been released
To: s3ql@googlegroups.com

Dear all,

I am pleased to announce a new release of S3QL, version [PASTE VERSION].

From the changelog:

[PASTE CHANGELOG ENTRY]

The following people have contributed code to this release:

[PASTE HERE]

(The full list of contributors is available in the AUTHORS file).

The release is available for download from
https://github.com/s3ql/s3ql/releases

Please report any bugs on the mailing list (s3ql@googlegroups.com) or
the issue tracker (https://github.com/s3ql/s3ql/issues).

Best,
-Nikolaus
```
