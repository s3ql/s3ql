#ifndef LIBLZMA_FILEOBJ_H
#define LIBLZMA_FILEOBJ_H 1

#include "liblzma.h"
#include "liblzma_file.h"
#include <structmember.h>

/* Our very own off_t-like type, 64-bit if possible */
/* copied from Objects/fileobject.c */
#if !defined(HAVE_LARGEFILE_SUPPORT)
typedef off_t Py_off_t;
#elif SIZEOF_OFF_T >= 8
typedef off_t Py_off_t;
#elif SIZEOF_FPOS_T >= 8
typedef fpos_t Py_off_t;
#else
#error "Large file support, but neither off_t nor fpos_t is large enough."
#endif

#define BUF(v) PyString_AS_STRING((PyStringObject *)v)

typedef enum file_mode_e {
	MODE_CLOSED = 0,
	MODE_READ = 1,
	MODE_READ_EOF = 2,
	MODE_WRITE = 3
} file_mode;

#define LZMAFileObject_Check(v)  (Py_TYPE(v) == &LZMAFile_Type)

typedef struct {
	PyObject_HEAD
	PyObject *file;

	char* f_buf;		/* Allocated readahead buffer */
	char* f_bufend;		/* Points after last occupied position */
	char* f_bufptr;		/* Current buffer position */

	int f_softspace;	/* Flag used by 'print' command */

	bool f_univ_newline;	/* Handle any newline convention */
	int f_newlinetypes;	/* Types of newlines seen */
	bool f_skipnextlf;	/* Skip next \n */

	lzma_FILE *fp;
	lzma_options_lzma options;
	lzma_filter filters[LZMA_FILTERS_MAX + 2];
	uint64_t memlimit;

	file_mode mode;
	Py_off_t pos;
	Py_off_t size;
#ifdef WITH_THREAD
	PyThread_type_lock lock;
#endif
} LZMAFileObject;

extern PyTypeObject LZMAFile_Type;

#endif /* LIBLZMA_FILEOBJ_H */
