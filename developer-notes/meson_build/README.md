# Meson Build Experiment

This directory contains files to build S3QL with meson-python rather than setuptools. Theoretically,
this is cleaner because the build configuration is more declarative (no more setup.py executing
arbitrary code), and the build process happens out-of-tree (i.e., artifacts are placed in a
separate directory instead of next to source files).

In practice, there's a number of issues that made the switch inadvisable for now:


## Editable installs don't support static analysis

meson-python uses Python code in `.pth` files to enable editable installs to find both (in source)
Python code and (out of source) extension modules. This means that many static code analysis tools
will not be able to find the S3QL sources (see [pylance
note](https://github.com/microsoft/pylance-release/blob/main/TROUBLESHOOTING.md#editable-install-modules-not-found),
[meson-python bug](https://github.com/mesonbuild/meson-python/issues/555)).

As a workaround, one can to add the `src` directory as an additional import path to the tool.
However, this then results in warnings because compiled extension modules seemingly do not exist.

## Source tarball generation

Support for generating the source tarball feels very barebones. It was not immediately clear
to me how to include the the generated documentation and C++ files.




