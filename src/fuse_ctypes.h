/* Necessary to prevent gccxml from complaining about
 * an undefined type. Settings this to a random type should
 * be safe as long as the FUSE API does not start using
 * variable numbers of arguments. */
#define __builtin_va_arg_pack_len int
#define __builtin_va_arg_pack int

#define FUSE_USE_VERSION 28
#include <fuse_lowlevel.h>
#include <attr/xattr.h>
#include <errno.h>

