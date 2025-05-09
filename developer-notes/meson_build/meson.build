project('s3ql', 'cpp', 'cython',
 meson_version: '>= 1.5.0',
  version: run_command(['util/getversion.py'], check: true).stdout().strip(),
  default_options : [
    'warning_level=3',
    'default_library=shared',
  ]
)


py = import('python').find_installation(pure: false)
dep_py = py.dependency()

subdir('src')