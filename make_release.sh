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
