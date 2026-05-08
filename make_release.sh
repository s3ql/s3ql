#!/bin/sh

set -e

if [ -z "${S3QL_SIGNING_KEYS_DIR}" ] || [ ! -d "${S3QL_SIGNING_KEYS_DIR}" ]; then
    echo "S3QL_SIGNING_KEYS_DIR must point to the directory containing the release" >&2
    echo "signing .sec files (see developer-notes/release-process.md)." >&2
    exit 1
fi

if [ -z "$1" ]; then
    TAG="$(git tag --list 's3ql-*' --sort=-creatordate | head -1)"
else
    TAG="$1"
fi
PREV_TAG="$(git tag --list 's3ql-*' --sort=-creatordate --merged "${TAG}^"| head -1)"
MAJOR_REV=${TAG%.*}
VERSION="${TAG#s3ql-}"

echo "Creating release tarball for ${TAG}..."

git checkout -q "${TAG}"

uv sync --frozen
. .venv/bin/activate

# check if we have a recent enough Cython version so that the release tarball is compatible with Python 3.12
if ! python3 -c 'import sys; from Cython.Compiler.Version import version as cython_version; sys.exit(1) if tuple(map(int, (cython_version.split(".")[0:3]))) < (3, 0, 0) else sys.exit(0)'; then
  printf "You need to install Cython >= 3.0.0. You have version %s installed\n" "$(python3 -c 'from Cython.Compiler.Version import version as cython_version; print(cython_version)')"
  exit 1
fi

./build_docs.sh
uv build --sdist

# Ideally we'd use -z here to embed the signature in the gz header.
# However, this is currently buggy: bugs.debian.org/1042837
signify -S -s "${S3QL_SIGNING_KEYS_DIR}/${MAJOR_REV}.sec" -m "dist/${TAG}.tar.gz"
#mv -f dist/$TAG.tar.gz.sig dist/$TAG.tar.gz

echo "Uploading documentation..."
rsync -aHv --del doc/html/ ebox.rath.org:/srv/www.rath.org/s3ql-docs/

# Build the announcement email by interpolating the changelog excerpt and
# contributor list into the template. The user copies this block into the
# mailing-list email.
CHANGELOG=$(awk -v ver="$VERSION" '
    /^S3QL [0-9]/ { in_section = ($0 ~ "^S3QL " ver " "); next }
    /^=+$/ && in_section { next }
    in_section { print }
' ChangeLog.rst)

CONTRIBUTORS=$(git log --pretty="format:%an" "${PREV_TAG}..${TAG}" \
    | grep -vEi 'copilot|claude|\[bot\]' \
    | sort -u)

echo
echo "================================================================"
echo "Announcement email body for ${TAG} (copy between the markers):"
echo "================================================================"
cat <<EOF
From: Nikolaus Rath <Nikolaus@rath.org>
Subject: [s3ql] [ANNOUNCE] S3QL ${VERSION} has been released
To: s3ql@googlegroups.com

Dear all,

I am pleased to announce a new release of S3QL, version ${VERSION}.

From the changelog:
${CHANGELOG}

The following people have contributed code to this release:

${CONTRIBUTORS}

(The full list of contributors is available in the AUTHORS file).

The release is available for download from
https://github.com/s3ql/s3ql/releases

Please report any bugs on the mailing list (s3ql@googlegroups.com) or
the issue tracker (https://github.com/s3ql/s3ql/issues).

Best,
-Nikolaus
EOF
echo "================================================================"
