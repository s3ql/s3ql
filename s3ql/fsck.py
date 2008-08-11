#!/usr/bin/env python
#
#    Copyright (C) 2008  Nikolaus Rath <Nikolaus@rath.org>
#
#    This program can be distributed under the terms of the GNU LGPL.
#


def check_cache(cursor, checkonly):
    """Verifies that the s3 table agrees with the cache.

    Checks that:
    - For each file in the cache, there is an entry in the table
    - For each entry in the table, there is a cache file

    If `checkonly` is disabled, it also:
    - Commits all cache entries to S3 and deletes them

    Returns `False` if any errors have been found.
    """


    return True

def check_parameters(cursor, checkonly):
    """Check that filesystem parameters are set

    Returns `False` if any errors have been found.
    """
    pass
    return True

def check_contents(cursor, checkonly):
    """Check contents table

    Checks that:
    - parent_inode and filename are consistent
    - for each path all the path components exist and are
       directories

    Returns `False` if any errors have been found.
    """
    pass
    return True

def check_inodes(cursor, checkonly):
    """Check inode table

    Checks that:
    - refcounts are correct
    - each inode has a content entry

    Returns `False` if any errors have been found.
    """
    pass
    return True

def check_s3(cursor, checkonly):
    """Checks s3_objects table.

    Checks that:
    - offsets are blocksize apart

    Returns `False` if any errors have been found.
    """
    pass
    return True


def check_keylist(cursor, keylist, checkonly):
    """Checks the list of S3 objects.

    Checks that:
    - no s3 object is larger than the blocksize
    - all s3 objects are referred in the s3 table
    - all objects in the s3 table exist
    - etags match (update metadata in case of conflict)


    Returns `False` if any errors have been found.
    """
    pass
    return True
