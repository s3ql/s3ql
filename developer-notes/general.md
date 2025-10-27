- The .git-blame-ignore-revs file contains Git commits with cleanup changes
  (e.g. formatting only). To use it, pass it to `git blame` through `--ignore-revs-file`.

- Please format all Python code with [Ruff](https://docs.astral.sh/ruff/)

- Before submitting merge requests, make sure that static analysis with
  and `uv run pyright` and `uv run mypy` passes.