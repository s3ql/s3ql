#include "liblzma_decompressobj.h"
#include "liblzma_util.h"

PyDoc_STRVAR(LZMADecomp_decompress__doc__,
"decompress(data[, max_length=0]) -> string\n\
\n\
Return a string containing the decompressed version of the data.\n\
\n\
After calling this function, some of the input data may still be stored in\n\
internal buffers for later processing.\n\
Call the flush() method to clear these buffers.\n\
If the max_length parameter is specified then the return value will be\n\
no longer than max_length. Unconsumed input data will be stored in\n\
the unconsumed_tail data descriptor.");

static PyObject *
LZMADecomp_decompress(LZMADecompObject *self, PyObject *args, PyObject *kwargs)
{
	Py_buffer pdata;
	Py_ssize_t datasize, oldbufsize, bufsize = SMALLCHUNK;
    	uint8_t *data;
    	uint64_t start_total_out;
	PyObject *ret = NULL;
	lzma_stream *lzus = &self->lzus;
	lzma_ret lzuerror;
	static char *kwlist[] = {"data", "max_length", NULL};
   
	INITCHECK
	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s*|l:decompress", kwlist,
			  &pdata, &self->max_length))
		return NULL;
	data = pdata.buf;
	datasize = pdata.len;

	ACQUIRE_LOCK(self);
	if (!self->running) {
		PyErr_SetString(PyExc_EOFError,
				"end of stream was already found");
		goto error;
	}

    	if (self->max_length < 0) {
		PyErr_SetString(PyExc_ValueError,
				"max_length must be greater than zero");
		goto error;
    	}

	/* limit amount of data allocated to max_length */
	if (self->max_length && bufsize > self->max_length)
		bufsize = self->max_length;

	if(!(ret = PyString_FromStringAndSize(NULL, bufsize)))
		goto error;

	start_total_out = lzus->total_out;
	lzus->avail_in = (size_t)datasize;
	lzus->next_in = data;
	lzus->avail_out = (size_t)bufsize;
	lzus->next_out = (uint8_t *)PyString_AS_STRING(ret);

	for (;;) {
		Py_BEGIN_ALLOW_THREADS
		lzuerror = lzma_code(lzus, LZMA_RUN);
		Py_END_ALLOW_THREADS

		if (lzus->avail_in == 0 || lzus->avail_out != 0)
			break; /* no more input data */

		/* If max_length set, don't continue decompressing if we've already
		 * reached the limit.
		 */
		if (self->max_length && bufsize >= self->max_length)
			break;

		/* otherwise, ... */
		oldbufsize= bufsize;
		bufsize = bufsize << 1;
		if (self->max_length && bufsize > self->max_length)
			bufsize = self->max_length;
		
		if (_PyString_Resize(&ret, bufsize) < 0)
			goto error;
		lzus->next_out = (uint8_t *)PyString_AS_STRING(ret) + oldbufsize;
		lzus->avail_out = (size_t)bufsize - (size_t)oldbufsize;
		if(!Util_CatchLZMAError(lzuerror, lzus, false))
			goto error;
	}

    	/* Not all of the compressed data could be accommodated in the output
	 * buffer of specified size. Return the unconsumed tail in an attribute.
	 */
    	if(self->max_length) {
		Py_DECREF(self->unconsumed_tail);
		self->unconsumed_tail = PyString_FromStringAndSize((const char *)lzus->next_in,
				(Py_ssize_t)lzus->avail_in);
		if(!self->unconsumed_tail) {
			goto error;
		}
    	}

    	/* The end of the compressed data has been reached, so set the
	 * unused_data attribute to a string containing the remainder of the
	 * data in the string.  Note that this is also a logical place to call
	 * lzma_end, but the old behaviour of only calling it on flush() is
	 * preserved.
	 */
    	if (lzuerror == LZMA_STREAM_END) {
		Py_XDECREF(self->unused_data);  /* Free original empty string */
		self->unused_data = PyString_FromStringAndSize(
				(const char *)lzus->next_in, (Py_ssize_t)lzus->avail_in);
		if (self->unused_data == NULL) {
			goto error;
		}
		/* We will only get LZMA_BUF_ERROR if the output buffer was full
		 * but there wasn't more output when we tried again, so it is
		 * not an error condition.
		 */
	} else if(!Util_CatchLZMAError(lzuerror, lzus, false))
		goto error;

	_PyString_Resize(&ret, (Py_ssize_t)lzus->total_out - (Py_ssize_t)start_total_out);

	RELEASE_LOCK(self);
	PyBuffer_Release(&pdata);
	return ret;

 error:
	RELEASE_LOCK(self);
	PyBuffer_Release(&pdata);
	Py_XDECREF(ret);
	return NULL;
}

PyDoc_STRVAR(LZMADecomp_flush__doc__,
"flush( [flushmode=LZMA_FINISH, bufsize] ) -> string\n\
\n\
Return a string containing any remaining decompressed data.\n\
'bufsize', if given, is the initial size of the output buffer.\n\
\n\
The decompressor object can only be used again after this call\n\
if reset() is called afterwards.");

static PyObject * LZMADecomp_flush(LZMADecompObject *self, PyObject *args, PyObject *kwargs)
{
	Py_ssize_t bufsize = SMALLCHUNK;
	
	PyObject *ret = NULL;
	lzma_action flushmode = LZMA_FINISH;
	uint64_t start_total_out;
	lzma_stream *lzus = &self->lzus;
	lzma_ret lzuerror;

	static char *kwlist[] = {"flushmode", "bufsize", NULL};
   
	INITCHECK
	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|ii:decompress", kwlist,
			  &flushmode, &bufsize))
		return NULL;

	ACQUIRE_LOCK(self);
	if (!self->running) {
		PyErr_SetString(PyExc_ValueError, "object was already flushed");
		goto error;
	}

	switch(flushmode){
		case(LZMA_SYNC_FLUSH):
		case(LZMA_FULL_FLUSH):
			PyErr_Format(LZMAError, "%d is not supported as flush mode for decoding", flushmode);
			goto error;
		case(LZMA_RUN):
		case(LZMA_FINISH):
			break;
		default:
			PyErr_Format(LZMAError, "Invalid flush mode: %d", flushmode);
			goto error;
	}

	if (!(ret = PyString_FromStringAndSize(NULL, bufsize)))
		goto error;


	start_total_out = lzus->total_out;
	lzus->avail_out = (size_t)bufsize;
	lzus->next_out = (uint8_t *)PyString_AS_STRING(ret);

	for (;;) {
		Py_BEGIN_ALLOW_THREADS
		lzuerror = lzma_code(lzus, flushmode);
		Py_END_ALLOW_THREADS

		if (lzus->avail_in == 0 || lzus->avail_out != 0)
			break; /* no more input data */

		if (_PyString_Resize(&ret, bufsize << 1) < 0)
			goto error;
		lzus->next_out = (uint8_t *)PyString_AS_STRING(ret) + bufsize;
		lzus->avail_out = (size_t)bufsize;
		bufsize = bufsize << 1;

		if(!Util_CatchLZMAError(lzuerror, lzus, false))
			goto error;
	}

	
    	/* If flushmode is LZMA_FINISH, we also have to call lzma_end() to free
	 * various data structures. Note we should only get LZMA_STREAM_END when
	 * flushmode is LZMA_FINISH
	 */
	if (lzuerror == LZMA_STREAM_END) {
		lzma_end(lzus);
		self->running = false;
		if(!Util_CatchLZMAError(lzuerror, lzus, false))
			goto error;
	}
	_PyString_Resize(&ret, (Py_ssize_t)lzus->total_out - (Py_ssize_t)start_total_out);

	RELEASE_LOCK(self);
	return ret;

error:
	RELEASE_LOCK(self);
	Py_XDECREF(ret);
    	return ret;
}

PyDoc_STRVAR(LZMADecomp_reset__doc__,
"reset([maxlength=0, memlimit=-1]) -> None\n\
\n\
Resets the decompression object.");

static PyObject *
LZMADecomp_reset(LZMADecompObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *result=NULL;
	lzma_stream *lzus = &self->lzus;	
	lzma_ret lzuerror;
    	static char *kwlist[] = {"max_length", "memlimit", NULL};

	INITCHECK
	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|lK:reset", kwlist,
				&self->max_length, &self->memlimit))
		return NULL;

	if (self->max_length < 0) {
		PyErr_SetString(PyExc_ValueError,
				"max_length must be greater than zero");
		goto error;
	}
	ACQUIRE_LOCK(self);	
	if (self->running)
		lzma_end(lzus);
	
	Py_CLEAR(self->unused_data);
	Py_CLEAR(self->unconsumed_tail);
	if((self->unused_data = PyString_FromString("")) == NULL)
		goto error;
	if((self->unconsumed_tail = PyString_FromString("")) == NULL)
		goto error;

	lzma_stream tmp = LZMA_STREAM_INIT;                      
 	*lzus = tmp;                                             

	lzuerror = lzma_auto_decoder(lzus, self->memlimit, 0);
	if(!Util_CatchLZMAError(lzuerror, lzus, false))
		goto error;
	self->running = true;

	result = Py_None;
 error:
	RELEASE_LOCK(self);
	Py_XINCREF(result);
	return result;
}

static PyMemberDef LZMADecomp_members[] = {
	{"unused_data", T_OBJECT, offsetof(LZMADecompObject, unused_data),
		RO, NULL},
	{"unconsumed_tail", T_OBJECT, offsetof(LZMADecompObject,
		unconsumed_tail), RO, NULL},
	{NULL, 0, 0, 0, NULL} /* Sentinel */
};

static PyMethodDef LZMADecomp_methods[4] =
{
	{"decompress", (PyCFunction)LZMADecomp_decompress, METH_VARARGS|METH_KEYWORDS,
		LZMADecomp_decompress__doc__},
	{"flush", (PyCFunction)LZMADecomp_flush, METH_VARARGS|METH_KEYWORDS,
		LZMADecomp_flush__doc__},
	{"reset", (PyCFunction)LZMADecomp_reset, METH_VARARGS|METH_KEYWORDS,
		LZMADecomp_reset__doc__},
	{NULL, NULL, 0, NULL} /*sentinel*/
};

static PyObject *
LZMADecompObject_new(PyTypeObject *type, __attribute__((unused)) PyObject *args, __attribute__((unused)) PyObject *kwargs)
{
	LZMADecompObject *self;
	self = (LZMADecompObject *)type->tp_alloc(type, 0);

	if (self != NULL){
		self->is_initialised = false;
		self->running = false;
		self->max_length = 0;
		self->memlimit = -1;
		if((self->unused_data = PyString_FromString("")) == NULL)
				goto error;
		if((self->unconsumed_tail = PyString_FromString("")) == NULL)
			goto error;
		lzma_stream tmp = LZMA_STREAM_INIT;
		self->lzus = tmp;
	}
	else
		return NULL;

	return (PyObject *)self;
 error:
	Py_DECREF(self);
	return NULL;
}

static void
LZMADecomp_dealloc(LZMADecompObject *self)
{
#ifdef WITH_THREAD
	if (self->lock)
		PyThread_free_lock(self->lock);
#endif
	if (self->is_initialised)
		lzma_end(&self->lzus);
	Py_XDECREF(self->unused_data);
	Py_XDECREF(self->unconsumed_tail);
	Py_TYPE(self)->tp_free((PyObject *)self);
}

static int
LZMADecomp_init(LZMADecompObject *self, PyObject *args, PyObject *kwargs)
{
	lzma_stream *lzus = &self->lzus;	
	lzma_ret lzuerror;

	static char *kwlist[] = {"input", "max_length", "memlimit", NULL};

	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|lK:LZMADecompressor", kwlist,
				&self->max_length, &self->memlimit))
		return -1;

#ifdef WITH_THREAD
	self->lock = PyThread_allocate_lock();
	if (!self->lock) {
		PyErr_SetString(PyExc_MemoryError, "unable to allocate lock");
		goto error;
	}
#endif

	if (self->max_length < 0) {
		PyErr_SetString(PyExc_ValueError,
				"max_length must be greater than zero");
		goto error;
	}

	lzuerror = lzma_auto_decoder(lzus, self->memlimit, LZMA_CONCATENATED);
	if(!Util_CatchLZMAError(lzuerror, lzus, false))
		goto error;

	self->is_initialised = true;
	self->running = true;

	return 0;

 error:
#ifdef WITH_THREAD
	if (self->lock) {
		PyThread_free_lock(self->lock);
		self->lock = NULL;
	}
#endif
	free(self);
	return -1;
}

PyDoc_STRVAR(LZMADecomp__doc__,
"LZMADecompressor([max_length=0, memlimit=-1]) -> decompressor object\n\
\n\
Create a new decompressor object. This object may be used to decompress\n\
data sequentially. If you want to decompress data in one shot, use the\n\
decompress() function instead.\n");

PyTypeObject LZMADecomp_Type = {
	PyObject_HEAD_INIT(NULL)
	0,						/*ob_size*/
	"lzma.LZMADecompressor",			/*tp_name*/
	sizeof(LZMADecompObject),			/*tp_basicsize*/
	0,						/*tp_itemsize*/
	(destructor)LZMADecomp_dealloc,			/*tp_dealloc*/
	0,						/*tp_print*/
	0,						/*tp_getattr*/
	0,						/*tp_setattr*/
	0,						/*tp_compare*/
	0,						/*tp_repr*/
	0,						/*tp_as_number*/
	0,						/*tp_as_sequence*/
	0,						/*tp_as_mapping*/
	0,						/*tp_hash*/
	0,						/*tp_call*/
	0,						/*tp_str*/
	PyObject_GenericGetAttr,			/*tp_getattro*/
	PyObject_GenericSetAttr,			/*tp_setattro*/
	0,						/*tp_as_buffer*/
	Py_TPFLAGS_DEFAULT|Py_TPFLAGS_BASETYPE,		/*tp_flags*/
	LZMADecomp__doc__,         			/*tp_doc*/
	0,						/*tp_traverse*/
	0,						/*tp_clear*/
	0,						/*tp_richcompare*/
	0,						/*tp_weaklistoffset*/
	0,						/*tp_iter*/
	0,						/*tp_iternext*/
	LZMADecomp_methods,				/*tp_methods*/
	LZMADecomp_members,				/*tp_members*/
	0,						/*tp_getset*/
	0,						/*tp_base*/
	0,						/*tp_dict*/
	0,						/*tp_descr_get*/
	0,						/*tp_descr_set*/
	0,						/*tp_dictoffset*/
	(initproc)LZMADecomp_init,			/*tp_init*/
	PyType_GenericAlloc,				/*tp_alloc*/
	LZMADecompObject_new,				/*tp_new*/
	_PyObject_Del,					/*tp_free*/
	0,						/*tp_is_gc*/
	0,						/*tp_bases*/
	0,						/*tp_mro*/
	0,						/*tp_cache*/
	0,						/*tp_subclasses*/
	0,						/*tp_weaklist*/
	0,						/*tp_del*/
	0						/*tp_version_tag*/
};
