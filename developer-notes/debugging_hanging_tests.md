To debug a hanging test, the following may help:

- Send SIGUSR1 to the Python process. This will dump stack traces into
  tests/test_crit.log.

- Use the `--log-cli-level` option to see live debug output.

- Deactivate the pytest_checklogs plugin (comment out the `pytest_plugins` line in
  conftest.py) and pass the `-s` option to immediately see output written to stderr/stdin.
