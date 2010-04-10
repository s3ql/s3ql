#include "liblzma_compressobj.h"
#include "liblzma_options.h"
#include "liblzma_util.h"

PyDoc_STRVAR(LZMAComp_compress__doc__,
"compress(data) -> string\n\
\n\
Feed the compressor object with data to compress sequently.\n\
This function will return the header for the compressed string for the first\n\
input provided, this header will be needed to concatenate with the rest of\n\
the stream when flushing to have a proper stream able to be decompressed\n\
again.\n");

static PyObject *
LZMAComp_compress(LZMACompObject *self, PyObject *args)
{
        Py_buffer pdata;
        Py_ssize_t datasize, bufsize = SMALLCHUNK;
        uint8_t *data;
        uint64_t total_out;
        PyObject *ret = NULL;
        lzma_stream *lzus = &self->lzus;
        lzma_ret lzuerror;

        INITCHECK
        if (!PyArg_ParseTuple(args, "s*:compress", &pdata))
                return NULL;
        data = pdata.buf;
        datasize = pdata.len;

        ACQUIRE_LOCK(self);
        if (!self->running) {
                PyErr_SetString(PyExc_ValueError,
                                "this object was already flushed");
                goto error;
        }

        if (!(ret = PyString_FromStringAndSize(NULL, bufsize)))
                goto error;

        lzus->avail_in = (size_t)datasize;
        lzus->next_in = data;
        lzus->avail_out = (size_t)bufsize;
        lzus->next_out = (uint8_t *)PyString_AS_STRING(ret);

        total_out = lzus->total_out;

        for (;;) {
                Py_BEGIN_ALLOW_THREADS
                lzuerror = lzma_code(lzus, LZMA_RUN);
                Py_END_ALLOW_THREADS
                if (lzus->avail_in == 0 || lzus->avail_out != 0)
                        break;
                if (_PyString_Resize(&ret, bufsize << 1) < 0)
                        goto error;
                lzus->next_out = (uint8_t *)PyString_AS_STRING(ret) + (lzus->total_out - total_out);
                lzus->avail_out = (size_t)bufsize - (lzus->next_out - (uint8_t *)PyString_AS_STRING(ret));
                bufsize =  bufsize << 1;
                if(!Util_CatchLZMAError(lzuerror, lzus, true))
                        goto error;
        }

        _PyString_Resize(&ret, (Py_ssize_t)lzus->total_out - (Py_ssize_t)total_out);

        RELEASE_LOCK(self);
        PyBuffer_Release(&pdata);
        return ret;

 error:
        RELEASE_LOCK(self);
        PyBuffer_Release(&pdata);
        Py_XDECREF(ret);
        return NULL;
}

PyDoc_STRVAR(LZMAComp_flush__doc__,
"flush( [mode] ) -> string\n\
\n\
Returns a string containing any remaining compressed data.\n\
\n\
'mode' can be one of the constants LZMA_SYNC_FLUSH, LZMA_FULL_FLUSH, LZMA_FINISH; the\n\
default value used when mode is not specified is LZMA_FINISH.\n\
If mode == LZMA_FINISH, the compressor object can no longer be used after\n\
calling the flush() method.  Otherwise, more data can still be compressed.\n");

static PyObject *
LZMAComp_flush(LZMACompObject *self, PyObject *args)
{
        Py_ssize_t bufsize = SMALLCHUNK;
        PyObject *ret = NULL;
        lzma_action flushmode = LZMA_FINISH;
        uint64_t total_out;
        lzma_stream *lzus = &self->lzus;
        lzma_ret lzuerror;

        INITCHECK
        if (!PyArg_ParseTuple(args, "|i:flush", &flushmode))
                return NULL;

        ACQUIRE_LOCK(self);
        if (!self->running) {
                PyErr_SetString(PyExc_ValueError, "object was already flushed");
                goto error;
        }

        switch(flushmode){
                case(LZMA_SYNC_FLUSH):
                case(LZMA_FULL_FLUSH):
                        if(self->filters[0].id == LZMA_FILTER_LZMA1) {
                                PyErr_Format(LZMAError, "%d is not supported as flush mode for LZMA_Alone format", flushmode);
                                goto error;
                        }
                /* Flushing with LZMA_RUN is a no-op, so there's no point in
                 * doing any work at all; just return an empty string.
                 */
                case(LZMA_RUN):
                        ret = PyString_FromStringAndSize(NULL, 0);
                        goto error;
                case(LZMA_FINISH):
                        break;
                default:
                        PyErr_Format(LZMAError, "Invalid flush mode: %d", flushmode);
                        goto error;
        }

        self->running = false;
        if (!(ret = PyString_FromStringAndSize(NULL, bufsize)))
                goto error;

        lzus->avail_in = 0;
        lzus->avail_out = (size_t)bufsize;
        lzus->next_out = (uint8_t *)PyString_AS_STRING(ret);

        total_out = lzus->total_out;

        for (;;) {
                Py_BEGIN_ALLOW_THREADS
                lzuerror = lzma_code(lzus, flushmode);
                Py_END_ALLOW_THREADS
                if(!Util_CatchLZMAError(lzuerror, lzus, true))
                    goto error;
                if(lzuerror == LZMA_STREAM_END)
                    break;
                if (_PyString_Resize(&ret, bufsize << 1) < 0)
                        goto error;
                lzus->next_out = (uint8_t *)PyString_AS_STRING(ret) + (lzus->total_out - total_out);;
                lzus->avail_out = (size_t)bufsize - (lzus->next_out - (uint8_t *)PyString_AS_STRING(ret));
                bufsize = bufsize << 1;
        }

        _PyString_Resize(&ret, (Py_ssize_t)self->lzus.total_out - (Py_ssize_t)total_out);

        RELEASE_LOCK(self);
        return ret;

error:
        RELEASE_LOCK(self);
        Py_XDECREF(ret);
    return ret;
}

PyDoc_STRVAR(LZMAComp_reset__doc__,
"reset(["DEFAULT_OPTIONS_STRING"]) -> None\n\
\n\
Resets the compression object keeping the compression settings.\n\
These existing settings can be overriden by providing\n\
keyword settings.");

static PyObject *
LZMAComp_reset(LZMACompObject *self, PyObject *args, PyObject *kwargs)
{
        PyObject *result = NULL, *options_dict = NULL;
        lzma_stream *lzus = &self->lzus;
        lzma_ret lzuerror = LZMA_OK;

        static char *kwlist[] = {"options", NULL};

        INITCHECK
        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|O:reset", kwlist,
                                &options_dict))
                goto error;

        if(!init_lzma_options("reset", options_dict, self->filters))
                goto error;

        self->lzma_options = LZMA_options_get(self->filters[0]);


        ACQUIRE_LOCK(self);
        if (self->running)
                lzma_end(lzus);

        if(self->filters[0].id == LZMA_FILTER_LZMA2)
                lzuerror = lzma_stream_encoder(lzus, self->filters, self->filters[LZMA_FILTERS_MAX + 1].id);
        else if(self->filters[0].id == LZMA_FILTER_LZMA1)
                lzuerror = lzma_alone_encoder(lzus, self->filters[0].options);

        if(!Util_CatchLZMAError(lzuerror, lzus, true))
                goto error;
        self->running = true;

        result = Py_None;
 error:
        RELEASE_LOCK(self);
        Py_XINCREF(result);
        return result;
}

PyDoc_STRVAR(LZMAComp_lzma_options__doc__,
"Dictionary containing the lzma encoder options.");

static PyMemberDef LZMAComp_members[] = {
        {"lzma_options", T_OBJECT, offsetof(LZMACompObject, lzma_options),
                RO, LZMAComp_lzma_options__doc__},
        {NULL, 0, 0, 0, NULL}	/* Sentinel */
};

static PyMethodDef LZMAComp_methods[] =
{
        {"compress", (PyCFunction)LZMAComp_compress, METH_VARARGS,
                LZMAComp_compress__doc__},
        {"flush", (PyCFunction)LZMAComp_flush, METH_VARARGS,
                LZMAComp_flush__doc__},
        {"reset", (PyCFunction)LZMAComp_reset, METH_VARARGS | METH_KEYWORDS,
                LZMAComp_reset__doc__},
        {0, 0, 0, 0}
};

static int
LZMAComp_init(LZMACompObject *self, PyObject *args, PyObject *kwargs)
{
        PyObject *options_dict = NULL;
        lzma_stream *lzus = &self->lzus;
        lzma_ret lzuerror = LZMA_OK;

        static char *kwlist[] = {"options", NULL};

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|O:LZMACompressor", kwlist,
                                &options_dict))
                return -1;

        if(!init_lzma_options("LZMACompressor", options_dict, self->filters))
                goto error;

        self->lzma_options = LZMA_options_get(self->filters[0]);

#ifdef WITH_THREAD
        self->lock = PyThread_allocate_lock();
        if (!self->lock) {
                PyErr_SetString(PyExc_MemoryError, "unable to allocate lock");
                goto error;
        }
#endif

        if(self->filters[0].id == LZMA_FILTER_LZMA2)
                lzuerror = lzma_stream_encoder(lzus, self->filters, self->filters[LZMA_FILTERS_MAX + 1].id);
        else if(self->filters[0].id == LZMA_FILTER_LZMA1)
                lzuerror = lzma_alone_encoder(lzus, self->filters[0].options);

        if(!Util_CatchLZMAError(lzuerror, lzus, true))
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
        return -1;
}

static PyObject *
LZMACompObject_new(PyTypeObject *type, __attribute__((unused)) PyObject *args, __attribute__((unused)) PyObject *kwargs)
{
        LZMACompObject *self;
        self = (LZMACompObject *)type->tp_alloc(type, 0);
        if (self != NULL){
                self->is_initialised = false;
                self->running = false;
                lzma_stream tmp = LZMA_STREAM_INIT;
                self->lzus = tmp;
                self->filters[0].options = &self->options;
        }
        else
                return NULL;

        return (PyObject *)self;
}

static void
LZMAComp_dealloc(LZMACompObject *self)
{
#ifdef WITH_THREAD
        if (self->lock)
                PyThread_free_lock(self->lock);
#endif
        if (self->is_initialised)
                lzma_end(&self->lzus);
        Py_XDECREF(self->lzma_options);
        Py_TYPE(self)->tp_free((PyObject *)self);
}

PyDoc_STRVAR(LZMAComp__doc__,
"LZMACompressor(["DEFAULT_OPTIONS_STRING"]) -> compressor object\n\
Create a new compressor object. This object may be used to compress\n\
data sequentially. If you want to compress data in one shot, use the\n\
compress() function instead.\n");

PyTypeObject LZMAComp_Type = {
        PyObject_HEAD_INIT(NULL)
        0,						/*ob_size*/
        "lzma.LZMACompressor",				/*tp_name*/
        sizeof(LZMACompObject),				/*tp_basicsize*/
        0,						/*tp_itemsize*/
        (destructor)LZMAComp_dealloc,			/*tp_dealloc*/
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
        LZMAComp__doc__,                                /*tp_doc*/
        0,						/*tp_traverse*/
        0,						/*tp_clear*/
        0,						/*tp_richcompare*/
        0,						/*tp_weaklistoffset*/
        0,						/*tp_iter*/
        0,						/*tp_iternext*/
        LZMAComp_methods,				/*tp_methods*/
        LZMAComp_members,				/*tp_members*/
        0,						/*tp_getset*/
        0,						/*tp_base*/
        0,						/*tp_dict*/
        0,						/*tp_descr_get*/
        0,						/*tp_descr_set*/
        0,						/*tp_dictoffset*/
        (initproc)LZMAComp_init,			/*tp_init*/
        PyType_GenericAlloc,				/*tp_alloc*/
        LZMACompObject_new,				/*tp_new*/
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
