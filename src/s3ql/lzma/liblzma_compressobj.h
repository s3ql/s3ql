#ifndef LIBLZMA_COMPRESSOBJ_H
#define LIBLZMA_COMPRESSOBJ_H 1

#include "liblzma.h"
#include <structmember.h>

typedef struct
{
	PyObject_HEAD
	lzma_stream lzus;
	lzma_options_lzma options;
	lzma_filter filters[LZMA_FILTERS_MAX + 2];
	bool is_initialised, running;
	PyObject *lzma_options;
#ifdef WITH_THREAD
	PyThread_type_lock lock;
#endif
} LZMACompObject;

extern PyTypeObject LZMAComp_Type;

#endif /* LIBLZMA_COMPRESSOBJ_H */
