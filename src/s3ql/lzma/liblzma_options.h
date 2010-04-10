#ifndef LIBLZMA_OPTIONS_H
#define LIBLZMA_OPTIONS_H 1

#include "liblzma.h"
#include <structmember.h>

#define CHECK_RANGE(x, a, b, msg) if (check_range(x, a, b)) { PyErr_Format(PyExc_ValueError, msg, a, b, (int32_t)x); ret = false; goto end; }
static inline bool check_range(uint32_t x, uint32_t a, uint32_t b){
	return (x < a || x > b);
}
#define MEMBER_DESCRIPTOR(name, type, variable, text) (PyMemberDef){name, type, offsetof(LZMAOptionsObject, variable), RO, PyString_AsString(PyString_Format(PyString_FromString((char*)text), (PyObject*)self->variable))}

#define	LZMA_BEST_SPEED			0
#define	LZMA_BEST_COMPRESSION		9
#define	LZMA_MODE_DEFAULT		LZMA_MODE_NORMAL
#define LZMA_MODE_INVALID		-1
#define LZMA_MF_INVALID			-1

#define	LZMA_MF_DEFAULT			LZMA_MF_BT4
#define	LZMA_MF_CYCLES_DEFAULT		0
#define LZMA_DICT_SIZE_MAX 		(UINT32_C(1) << 30) + (UINT32_C(1) << 29)
#define LZMA_NICE_LEN_MIN		5
#define LZMA_NICE_LEN_MAX		273
#define LZMA_NICE_LEN_DEFAULT		128

typedef struct
{
	PyObject_HEAD
	PyObject *format,
		 *check,
		 *level,
		 *dict_size,
		 *lc,
		 *lp,
		 *pb,
		 *mode_dict,
		 *mode,
		 *nice_len,
		 *mf_dict,
		 *mf,
		 *depth;
} LZMAOptionsObject;

extern PyTypeObject LZMAOptions_Type;

bool init_lzma_options(const char *funcName, PyObject *kwargs, lzma_filter *filters);
PyObject *LZMA_options_get(lzma_filter filter);

#define DEFAULT_OPTIONS_STRING "options={'format':'xz', 'check':'crc32', 'level':6, 'extreme':False,\n\
'dict_size':23, 'lc':3 'lp':0, 'pb':2, 'mode':2,\n\
'nice_len':128, 'mf':'bt4', 'depth':0"


#endif /* LIBLZMA_OPTIONS_H */
