#!/usr/bin/env python3
import os


def get_version():
    pyproject = os.path.join(os.path.dirname(__file__), '../pyproject.toml')
    with open(pyproject) as fid:
        data = fid.readlines()

    version_line = next(line for line in data if line.startswith('version ='))

    version = version_line.strip().split(' = ')[1]
    version = version.replace('"', '').replace("'", '')

    return version


if __name__ == '__main__':
    print(get_version())
