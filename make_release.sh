#!/bin/sh

set -e

python3 setup.py build_cython build_ext --inplace`
./build_docs.sh
(cd doc/pdf && latexmk)
python3 ./setup.py sdist
