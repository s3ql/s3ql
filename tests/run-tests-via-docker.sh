#!/usr/bin/env bash

# always use S3QL source root as working dir
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR/.." || exit 1

set -e

# build a fresh container (with current sources, will mostly get cached in subsequent calls)
docker build -f ./tests/Dockerfile  -t s3ql-tests .
# run tests in docker, pass down arguments to pytest
if [ "$#" -eq 0 ]; then
  PY_TEST_ARGS=( tests )
else
  PY_TEST_ARGS=( "$@" )
fi
docker run --rm \
           --device /dev/fuse \
           --cap-add SYS_ADMIN \
           s3ql-tests python -m pytest "${PY_TEST_ARGS[@]}"
