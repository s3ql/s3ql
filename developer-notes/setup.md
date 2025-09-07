# How to run/develop S3QL from Git

To run unit tests, build the documentation, and make changes to S3QL, the recommended procedure is
to create a virtual environment and install S3QL, build dependencies, and development tools into
this environment.

You can do this using a tool like [uv](https://docs.astral.sh/uv/getting-started/installation/) or
by hand as follows:

```sh
$ python3 -m venv .venv   # create the venv
$ . .venv/bin/activate    # activate it
$ pip install --upgrade pip # upgrade pip
$ pip install --group dev # install build dependencies
$ pip install --no-build-isolation --editable .  # install s3ql in editable mode
```

As long as the venv is active, you can run tests with

```sh
$ pytest tests/
```

and build the HTML documentation and manpages with:
```sh
$ ./build_docs.sh
```

S3QL commands are available in `.venv/bin` even when the venv is not explicitly activated, e.g.:

```sh
$ .venv/bin/mkfs.s3ql --help
```

## Running tests requiring remote servers

By default, tests requiring a connection to a remote storage backend
are skipped. If you would like to run these tests too (which is always
a good idea), you have to create additional entries in your
`~/.s3ql/authinfo2` file that tell S3QL what server and credentials to
use for these tests. These entries have the following form::

```ini
[<BACKEND>-test]
backend-login: <user>
backend-password: <password>
test-fs: <storage-url>
```

Here *<BACKEND>* specifies the backend that you want to test
(e.g. *s3*, *s3c*, *gs*, or *swift*), *<user>* and *<password>* are
the backend authentication credentials, and *<storage-url>* specifies
the full storage URL that will be used for testing. **Any existing
S3QL file system in this storage URL will be destroyed during
testing**.

For example, to run tests that need connection to a Google Storage
server, you would add something like

```ini
[gs-test]
backend-login: GOOGIGWLONT238MD7HZ4
backend-password: rmEbstjscoeunt1249oes1298gauidbs3hl
test-fs: gs://joes-gs-bucket/s3ql_tests/
```
