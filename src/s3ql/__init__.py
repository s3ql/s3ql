# Enfore correct Python version
import sys

if sys.version_info[0] < 2 or \
    (sys.version_info[0] == 2 and sys.version_info[1] < 6):
    sys.stderr.write('Python version too old, must be between 2.6.0 and 3.0!\n') 
    sys.exit(1)
    
if sys.version_info[0] > 2:
    sys.stderr.write('Python version too new, must be between 2.6.0 and 3.0!\n')
    sys.exit(1)

import apsw
tmp = apsw.apswversion()
tmp = tmp[:tmp.index('-')]
apsw_ver = tuple([ int(x) for x in tmp.split('.') ])
if apsw_ver < (3, 6, 14):    
    sys.stderr.write('APSW version too old, must be 3.6.14 or newer!\n')
    sys.exit(1)
    
    
sqlite_ver = tuple([ int(x) for x in apsw.sqlitelibversion().split('.') ])
if sqlite_ver < (3, 6, 17):    
    sys.stderr.write('SQLite version too old, must be 3.6.17 or newer!\n')
    sys.exit(1)


# Python boto uses several deprecated modules, deactivate warnings for them
import warnings
warnings.filterwarnings("ignore", "", DeprecationWarning, "boto")
    
__all__ = [ "common", "fs", "fsck", 'fuse', 'isodate', 'database'
            "mkfs", 'multi_lock', 'ordered_dict', "s3", 's3cache' ]
