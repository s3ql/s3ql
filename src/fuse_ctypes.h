/* Necessary to prevent gccxml from complaining about
 * an undefined type */
#define __builtin_va_arg_pack_len int

/* FUSE */
#define FUSE_USE_VERSION 26
#include <fuse_lowlevel.h>

#include <attr/xattr.h>

/* Make #define's visible to GCCXML */
const int D_FUSE_SET_ATTR_MODE = FUSE_SET_ATTR_MODE;
const int D_FUSE_SET_ATTR_UID = FUSE_SET_ATTR_UID;
const int D_FUSE_SET_ATTR_GID = FUSE_SET_ATTR_GID;
const int D_FUSE_SET_ATTR_SIZE = FUSE_SET_ATTR_SIZE;
const int D_FUSE_SET_ATTR_ATIME = FUSE_SET_ATTR_ATIME;
const int D_FUSE_SET_ATTR_MTIME = FUSE_SET_ATTR_MTIME;
const int D_XATTR_CREATE = XATTR_CREATE;
const int D_XATTR_REPLACE = XATTR_REPLACE;
const int D_ENOATTR = ENOATTR;
