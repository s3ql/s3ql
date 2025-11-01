# Steps for Releasing a New Version

* Add release date and new version number to `ChangeLog.rst` 
* Update version in `src/s3ql/__init__.py`
* Update version in `pyproject.toml`
* `uv lock`
* `set TAG s3ql-A.B.C`
* When creating non-bugfix release:
  * Create signing key for the next release: `P=s3ql-<A.B+1> signify-openbsd -G -n -p signify/$P.pub -s
  signify/$P.sec`
  * `git add signify/*.pub`
  * Expire old release signing keys (keep one around just in case)
* Update authors: `git log --all --pretty="format:%an <%aE>" | grep -v '<none@none>$' | sort -u --ignore-case >> AUTHORS`
* `git commit --all -m "Released $TAG"`
* `git tag $TAG`
* `./make_release.sh`
* `git checkout main`
* `git push && git push --tags`, create release on Github
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
