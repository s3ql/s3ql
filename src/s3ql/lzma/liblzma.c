#include "liblzma.h"
#include "liblzma_compressobj.h"
#include "liblzma_decompressobj.h"
#include "liblzma_options.h"
#include "liblzma_util.h"

static const char __author__[] =
"The lzma python module was written by:\n\
\n\
    Per Øyvind Karlsen <peroyvind@mandriva.org>\n\
";

PyDoc_STRVAR(LZMA_compress__doc__,
"compress(string [, "DEFAULT_OPTIONS_STRING"]) -> string\n\
\n\
Compress data using the given parameters, returning a string\n\
containing the compressed data.");

static PyObject *
LZMA_compress(__attribute__((unused)) PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *ret = NULL, *options_dict = NULL;
	Py_buffer pdata;
	uint8_t *data;
	Py_ssize_t datasize, bufsize;
	lzma_ret lzuerror;
	lzma_stream _lzus;
	lzma_stream *lzus = &_lzus;
	lzma_filter filters[LZMA_FILTERS_MAX + 2];
	lzma_options_lzma options;

    	static char *kwlist[] = {"input", "options", NULL};

	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s*|O:compress", kwlist,
			       	&pdata, &options_dict))
		return NULL;

	filters[0].options = &options;
	if(!init_lzma_options("compress", options_dict, filters))
		return NULL;

	data = pdata.buf;
	datasize = pdata.len;

	lzma_stream tmp = LZMA_STREAM_INIT;
	*lzus = tmp;
	bufsize = lzma_stream_buffer_bound(datasize);
	/* TODO: if(bufsize == 0) goto error; */

	if (!(ret = PyString_FromStringAndSize(NULL, bufsize)))
		return NULL;

	if(filters[0].id == LZMA_FILTER_LZMA2)
	{
		size_t loc = 0;
		Py_BEGIN_ALLOW_THREADS
		lzuerror = lzma_stream_buffer_encode(filters, filters[LZMA_FILTERS_MAX + 1].id,
				NULL, data, (size_t)datasize,
 				(uint8_t *)PyString_AS_STRING(ret), &loc, (size_t)bufsize);
		Py_END_ALLOW_THREADS
		_PyString_Resize(&ret, (Py_ssize_t)loc);

	}
	else if(filters[0].id == LZMA_FILTER_LZMA1)
	{
		lzuerror = lzma_alone_encoder(lzus, filters[0].options);
		
		if(!Util_CatchLZMAError(lzuerror, lzus, true))
			goto error;
		
		lzus->avail_in = (size_t)datasize;
		lzus->next_in = data;
		lzus->next_out = (uint8_t *)PyString_AS_STRING(ret);
		lzus->avail_out = (size_t)bufsize;

		Py_BEGIN_ALLOW_THREADS
		lzuerror = lzma_code(lzus, LZMA_FINISH);
		Py_END_ALLOW_THREADS

		if(!Util_CatchLZMAError(lzuerror, lzus, true))
			goto error;
		
		lzma_end(lzus);
		if (lzuerror == LZMA_STREAM_END)
			_PyString_Resize(&ret, (Py_ssize_t)lzus->total_out);
	}

	PyBuffer_Release(&pdata);
	return ret;

 error:
	if(lzuerror != LZMA_MEM_ERROR && lzuerror != LZMA_PROG_ERROR)
		lzma_end(lzus);
	Py_XDECREF(ret);
	PyBuffer_Release(&pdata);
	return NULL;
}

PyDoc_STRVAR(LZMA_decompress__doc__,
"decompress(string[, bufsize=8192, memlimit=-1]) -> string\n\
\n\
Decompress data in one shot. If you want to decompress data sequentially,\n\
use an instance of LZMADecompressor instead.\n\
\n\
Optional arg 'bufsize' is the initial output buffer size.\n\
Optional arg 'memlimit' is the maximum amount of memory the decoder may use,\n\
-1 means no limit.");

static PyObject *
LZMA_decompress(__attribute__((unused)) PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *ret = NULL;
	Py_buffer pdata;
	uint8_t *data;
	Py_ssize_t datasize, bufsize = SMALLCHUNK;
	uint64_t memlimit = -1;
	lzma_ret lzuerror;
	lzma_stream _lzus;
	lzma_stream *lzus = &_lzus;

	static char *kwlist[] = {"input", "bufsize", "memlimit", NULL};

	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s*|lK:decompress", kwlist,
			  &pdata, &bufsize, &memlimit))
		return NULL;
	data = pdata.buf;
	datasize = pdata.len;

	if (datasize == 0) {
		PyBuffer_Release(&pdata);
		return PyString_FromString("");
	}

	ret = PyString_FromStringAndSize(NULL, bufsize);
	if (!ret) {
		PyBuffer_Release(&pdata);
		return NULL;
	}

	lzma_stream tmp = LZMA_STREAM_INIT;
	*lzus = tmp;

	lzus->avail_in = (size_t)datasize;
	lzus->avail_out = (size_t)bufsize;
	lzus->next_out =  (uint8_t *)PyString_AS_STRING(ret);
	lzus->next_in = (uint8_t *)data;

	lzuerror = lzma_auto_decoder(lzus, memlimit, 0);
	if(!Util_CatchLZMAError(lzuerror, lzus, false))
		goto error;

	while (lzuerror != LZMA_STREAM_END){
		Py_BEGIN_ALLOW_THREADS
		lzuerror=lzma_code(lzus, LZMA_RUN);
		Py_END_ALLOW_THREADS

		if(!Util_CatchLZMAError(lzuerror, lzus, false))
			goto error;
		if(lzuerror == LZMA_STREAM_END)
			break;
		if(lzuerror == LZMA_OK){
			if (_PyString_Resize(&ret, bufsize << 1) < 0) {
				goto error;
			}
		lzus->next_out = (uint8_t *)PyString_AS_STRING(ret) + bufsize;
		lzus->avail_out = (size_t)bufsize;
		bufsize = bufsize << 1;
		}
	} 

	_PyString_Resize(&ret, (Py_ssize_t)lzus->total_out);
	lzma_end(lzus);
	PyBuffer_Release(&pdata);

	return ret;
	
 error:
	if(lzuerror != LZMA_MEM_ERROR && lzuerror != LZMA_PROG_ERROR)
		lzma_end(lzus);	
	Py_XDECREF(ret);
	PyBuffer_Release(&pdata);
	return NULL;
}

PyDoc_STRVAR(LZMA_crc32__doc__,
"crc32(string[, start]) -> int\n\
\n\
Compute a CRC-32 checksum of string.\n\
\n\
An optional starting value 'start' can be specified.");

static PyObject *
LZMA_crc32(__attribute__((unused)) PyObject *self, PyObject *args)
{
	uint32_t crc32val = lzma_crc32(NULL, (size_t)0,  (uint32_t)0);
    	uint8_t *buf;
    	Py_ssize_t size;
    	if (!PyArg_ParseTuple(args, "s#|I:crc32", &buf, &size, &crc32val))
		return NULL;
    	crc32val = lzma_crc32(buf, (size_t)size, crc32val);
    	return PyInt_FromLong((long)crc32val);
}

PyDoc_STRVAR(LZMA_crc64__doc__,
"crc64(string[, start]) -> int\n\
\n\
Compute a CRC-64 checksum of string.\n\
\n\
An optional starting value 'start' can be specified.");

static PyObject *
LZMA_crc64(__attribute__((unused)) PyObject *self, PyObject *args)
{
	uint64_t crc64val = lzma_crc64(NULL, (size_t)0, (uint64_t)0);
    	uint8_t *buf;
    	Py_ssize_t size;
    	if (!PyArg_ParseTuple(args, "s#|K:crc64", &buf, &size, &crc64val))
		return NULL;
    	crc64val = lzma_crc64(buf, (size_t)size, crc64val);
    	return PyLong_FromUnsignedLongLong(crc64val);
}

static PyMethodDef lzma_methods[] = {
	{"compress", (PyCFunction)LZMA_compress,
		METH_VARARGS|METH_KEYWORDS, LZMA_compress__doc__},
    	{"crc32", (PyCFunction)LZMA_crc32,
		METH_VARARGS, LZMA_crc32__doc__},
    	{"crc64", (PyCFunction)LZMA_crc64,
		METH_VARARGS, LZMA_crc64__doc__},
	{"decompress", (PyCFunction)LZMA_decompress,
		METH_VARARGS|METH_KEYWORDS, LZMA_decompress__doc__},
	{0, 0, 0, 0}
};

PyDoc_STRVAR(lzma_module_documentation,
"The python lzma module provides a comprehensive interface for\n\
the lzma compression library. It implements one shot (de)compression\n\
functions, CRC-32 & CRC-64 checksum computations, types for sequential\n\
(de)compression, and advanced options for lzma compression.\n\
");

/* declare function before defining it to avoid compile warnings */
PyMODINIT_FUNC initlzma(void);
PyMODINIT_FUNC
initlzma(void)
{
	PyObject *ver, *optionsSingleton, *module;
	char verstring[10], major, minor[5], revision[5], s[8];
    	Py_TYPE(&LZMAComp_Type) = &PyType_Type;
    	Py_TYPE(&LZMADecomp_Type) = &PyType_Type;
    	Py_TYPE(&LZMAFile_Type) = &PyType_Type;
	
    	module = Py_InitModule3("lzma", lzma_methods,
 			lzma_module_documentation);
    	if (module == NULL)
		return;
	
	optionsSingleton = PyType_GenericNew(&LZMAOptions_Type, NULL, NULL);
	
	if(PyType_Ready(&LZMAOptions_Type) < 0)
		return;

	LZMAError = PyErr_NewException("LZMA.error", NULL, NULL);
    	if (LZMAError != NULL) {
		Py_INCREF(LZMAError);
		PyModule_AddObject(module, "error", LZMAError);
    	}
    
	Py_INCREF(&LZMAOptions_Type);
	PyModule_AddObject(module, "LZMAOptions", (PyObject *)&LZMAOptions_Type);

	Py_INCREF(&LZMAComp_Type);
	PyModule_AddObject(module, "LZMACompressor", (PyObject *)&LZMAComp_Type);

	Py_INCREF(&LZMADecomp_Type);
	PyModule_AddObject(module, "LZMADecompressor", (PyObject *)&LZMADecomp_Type);

	Py_INCREF(&LZMAFile_Type);
	PyModule_AddObject(module, "LZMAFile", (PyObject *)&LZMAFile_Type);

	PyModule_AddObject(module, "options", optionsSingleton);
    	PyModule_AddIntConstant(module, "LZMA_RUN", (ulong)LZMA_RUN);
	PyModule_AddIntConstant(module, "LZMA_SYNC_FLUSH", (ulong)LZMA_SYNC_FLUSH);
	PyModule_AddIntConstant(module, "LZMA_FULL_FLUSH", (ulong)LZMA_FULL_FLUSH);
	PyModule_AddIntConstant(module, "LZMA_FINISH", (ulong)LZMA_FINISH);

	PyModule_AddObject(module, "__author__", PyString_FromString(__author__));

	/* A bit ugly, but what the hell.. */
	snprintf(verstring, 9, "%d", LZMA_VERSION);
	verstring[9] = 0;
	major = verstring[0];
	sprintf(minor, "%c%c%c", verstring[1], verstring[2], verstring[3]);
	sprintf(revision, "%c%c%c", verstring[4], verstring[5], verstring[6]);
	if(verstring[7] == '0')
		sprintf(s, "alpha");
	else if(verstring[7] == '1')
		sprintf(s, "beta");
	else
		sprintf(s, "stable");

	ver = PyString_FromFormat("%c.%d.%d%s", major, atoi(minor), atoi(revision), s);
    	if (ver != NULL)
		PyModule_AddObject(module, "LZMA_VERSION", ver);

	PyModule_AddStringConstant(module, "__version__", VERSION);
}
