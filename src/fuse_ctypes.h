/* Necessary to prevent gccxml from complaining about
 * an undefined type */
#define __builtin_va_arg_pack_len int

/* FUSE */
#define FUSE_USE_VERSION 26
#include <fuse_lowlevel.h>

/* Make #define's visible to GCCXML */
const int fuse_set_attr_mode = FUSE_SET_ATTR_MODE;
const int fuse_set_attr_uid = FUSE_SET_ATTR_UID;
const int fuse_set_attr_gid = FUSE_SET_ATTR_GID;
const int fuse_set_attr_size = FUSE_SET_ATTR_SIZE;
const int fuse_set_attr_atime = FUSE_SET_ATTR_ATIME;
const int fuse_set_attr_mtime = FUSE_SET_ATTR_MTIME;

