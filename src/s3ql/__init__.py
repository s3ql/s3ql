# Enfore correct Python version
import sys

if sys.version_info[0] < 2 or \
    (sys.version_info[0] == 2 and sys.version_info[1] < 6):
    sys.stderr.write('Python version too old, must be between 2.6.0 and 3.0!\n') 
    sys.exit(1)
    
if sys.version_info[0] > 2:
    sys.stderr.write('Python version too new, must be between 2.6.0 and 3.0!\n')
    sys.exit(1)

# TODO: Enforce apsw and sqlite version    
    

# Python boto uses several deprecated modules, deactivate warnings for them
import warnings
warnings.filterwarnings("ignore", "", DeprecationWarning, "boto")
    
__all__ = [ "common", "fs", "fsck", 'fuse', 'isodate'
            "mkfs", 'multi_lock', 'ordered_dict', "s3", 's3cache' ]
