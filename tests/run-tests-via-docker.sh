#!/usr/bin/env bash

# always use S3QL source root as working dir
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR/.." || exit 1

# run tests in docker and copy documentation to /build/
# use BuildKit when available so that the --output option works on macOS
DOCKER_BUILDKIT=1 docker build -f ./tests/Dockerfile -o "./build" .
