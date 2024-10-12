#!/bin/sh

set -e

if [ -z "$1" ]; then
    TAG="$(git tag --list 's3ql-*' --sort=-creatordate | head -1)"
else
    TAG="$1"
fi
PREV_TAG="$(git tag --list 's3ql-*' --sort=-creatordate --merged "${TAG}^"| head -1)"
MAJOR_REV=${TAG%.*}

echo "Creating release tarball for ${TAG}..."

git checkout -q "${TAG}"

# check if we have a recent enough Cython version so that the release tarball is compatible with Python 3.12
if ! python3 -c 'import sys; from Cython.Compiler.Version import version as cython_version; sys.exit(1) if tuple(map(int, (cython_version.split(".")[0:3]))) < (3, 0, 0) else sys.exit(0)'; then
  printf "You need to install Cython >= 3.0.0. You have version %s installed\n" "$(python3 -c 'from Cython.Compiler.Version import version as cython_version; print(cython_version)')"
  exit 1
fi

python3 setup.py build_cython build_ext --inplace
./build_docs.sh
(cd doc/pdf && latexmk)
python3 ./setup.py sdist

# Ideally we'd use -z here to embed the signature in the gz header.
# However, this is currently buggy: bugs.debian.org/1042837
signify-openbsd -S -s signify/$MAJOR_REV.sec -m dist/$TAG.tar.gz
#mv -f dist/$TAG.tar.gz.sig dist/$TAG.tar.gz

echo "Contributors from ${PREV_TAG} to ${TAG}:"
git log --pretty="format:%an <%aE>" "${PREV_TAG}..${TAG}" | \
    grep -v '<none@none>$' | \
    sort -u
