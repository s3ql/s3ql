#ifndef LIBLZMA_DECOMPRESSOBJ_H
#define LIBLZMA_DECOMPRESSOBJ_H 1

#include "liblzma.h"
#include <structmember.h>

typedef struct
{
	PyObject_HEAD
	lzma_stream lzus;
	PyObject *unused_data;
	PyObject *unconsumed_tail;
	Py_ssize_t max_length;
	bool is_initialised, running;
	uint64_t memlimit;
#ifdef WITH_THREAD
	PyThread_type_lock lock;
#endif
} LZMADecompObject;

extern PyTypeObject LZMADecomp_Type;

#endif /* LIBLZMA_DECOMPRESSOBJ_H */
