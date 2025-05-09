# Meson Build Experiment

This directory contains files to build S3QL with meson-python rather than setuptools. Theoretically,
this is cleaner because it's declarative builds out-of-source. In practice, there's a number of
issues that made the switch inadvisable for now:


## Pylance / VSCode / IDE Support

meson-python uses Python code in .pth files to enable editable installs to find both (in source)
Python code and (out of source) extension modules. This unfortunately means that many static code
analysis tools will not be able to find the S3QL sources (see [pylance
note](https://github.com/microsoft/pylance-release/blob/main/TROUBLESHOOTING.md#editable-install-modules-not-found),
[meson-python bug](https://github.com/mesonbuild/meson-python/issues/555)).

As a workaround, it's recommended to just add the `src` directory as an additional
import path to the tool. This means the tool will be able to find the pure Python
sources, but not be able to find the compiled extension module.

## Tarball generation

It's not clear how to generate a source tarball that includes the generated documentation
and the Cython generated C++ file.




