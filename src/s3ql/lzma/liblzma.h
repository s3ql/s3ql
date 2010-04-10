#ifndef LIBLZMA_H
#define LIBLZMA_H 1

/* To handle length as ssize_t in stead of int, otherwise we'd either have to
 * use the internal _PyArg_ParseTuple_SizeT function to avoid screwups
 */
#define PY_SSIZE_T_CLEAN 1
#include <Python.h>
#include <stdio.h>
#include <stdlib.h>
#if defined (__APPLE__) || defined(__FreeBSD__) || \
    defined(__OpenBSD__) || defined(__NetBSD__) || \
    defined (__sun) || defined (__svr4__)
#include <stdlib.h>
#else
#include <malloc.h>
#endif
#include <string.h>
#include <inttypes.h>
#if !defined(linux) && !defined(__sun) && !defined(__svr4__)
typedef unsigned long ulong;
#endif
#include <sys/types.h>
#include <lzma.h>

#ifdef WITH_THREAD
#include <pythread.h>
#define ACQUIRE_LOCK(obj) do { \
	if (!PyThread_acquire_lock(obj->lock, 0)) { \
		Py_BEGIN_ALLOW_THREADS \
		PyThread_acquire_lock(obj->lock, 1); \
		Py_END_ALLOW_THREADS \
	} } while(0)
#define RELEASE_LOCK(obj) PyThread_release_lock(obj->lock)
#else
#define ACQUIRE_LOCK(obj)
#define RELEASE_LOCK(obj)
#endif

#ifdef __STDC_VERSION__
#if __STDC_VERSION__ >= 199901L
#include <stdbool.h>
#endif
#elif defined(__C99FEATURES__)
#include <stdbool.h>
#else
#define bool    uint8_t
#define true    1
#define false   0
#ifndef inline
#define inline __inline__
#endif
#endif

#define INITCHECK if (!self->is_initialised) {	PyErr_Format(PyExc_RuntimeError, "%s object not initialised!", self->ob_type->tp_name);	return NULL; }

#endif /* LIBLZMA_H */
