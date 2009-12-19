# Enfore correct Python version
import sys

if sys.version_info[0] < 2 or \
    (sys.version_info[0] == 2 and sys.version_info[1] < 6):
    raise StandardError('Python version too old, must be between 2.6.0 and 3.0!\n') 
    
if sys.version_info[0] > 2:
    raise StandardError('Python version too new, must be between 2.6.0 and 3.0!\n')

import apsw
tmp = apsw.apswversion()
tmp = tmp[:tmp.index('-')]
apsw_ver = tuple([ int(x) for x in tmp.split('.') ])
if apsw_ver < (3, 6, 14):    
    raise StandardError('APSW version too old, must be 3.6.14 or newer!\n')
    
    
sqlite_ver = tuple([ int(x) for x in apsw.sqlitelibversion().split('.') ])
if sqlite_ver < (3, 6, 17):    
    raise StandardError('SQLite version too old, must be 3.6.17 or newer!\n')


# Python boto uses several deprecated modules, deactivate warnings for them
import warnings
warnings.filterwarnings("ignore", "", DeprecationWarning, "boto")
    
__all__ = [ "common", "fs", "fsck", 'fuse', 'isodate', 'database'
            "mkfs", 'multi_lock', 'ordered_dict', "s3", 's3cache' ]
