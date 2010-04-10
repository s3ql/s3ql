#include "liblzma_fileobj.h"

#include "liblzma_options.h"
#include "liblzma_util.h"

PyDoc_STRVAR(LZMAFile_read__doc__,
"read([size]) -> string\n\
\n\
Read at most size uncompressed bytes, returned as a string. If the size\n\
argument is negative or omitted, read until EOF is reached.\n\
");

/* This is a hacked version of Python's fileobject.c:file_read(). */
static PyObject *
LZMAFile_read(LZMAFileObject *self, PyObject *args)
{
	long bytesrequested = -1;
	size_t bytesread, buffersize, chunksize;
	lzma_ret lzuerror;
	PyObject *ret = NULL;

	if (!PyArg_ParseTuple(args, "|l:read", &bytesrequested))
		return NULL;

	ACQUIRE_LOCK(self);
	switch (self->mode) {
		case MODE_READ:
			break;
		case MODE_READ_EOF:
			ret = PyString_FromString("");
			goto cleanup;
		case MODE_CLOSED:
			PyErr_SetString(PyExc_ValueError,
					"I/O operation on closed file");
			goto cleanup;
		default:
			PyErr_SetString(PyExc_IOError,
					"file is not ready for reading");
			goto cleanup;
		case MODE_WRITE:
			break;
	}

	if (bytesrequested < 0)
		buffersize = Util_NewBufferSize((size_t)0);
	else
		buffersize = bytesrequested;
	if (buffersize > INT_MAX) {
		PyErr_SetString(PyExc_OverflowError,
				"requested number of bytes is "
				"more than a Python string can hold");
		goto cleanup;
	}
	ret = PyString_FromStringAndSize((char *)NULL, buffersize);
	if (ret == NULL)
		goto cleanup;
	bytesread = 0;

	for (;;) {
		Py_BEGIN_ALLOW_THREADS
		chunksize = Util_UnivNewlineRead(&lzuerror, self->fp,
						 BUF(ret)+bytesread,
						 buffersize-bytesread,
						 self);
		self->pos += chunksize;
		Py_END_ALLOW_THREADS
		bytesread += chunksize;
		if (lzuerror == LZMA_STREAM_END) {
			self->size = self->pos;
			self->mode = MODE_READ_EOF;
			break;
		} else if (lzuerror != LZMA_OK) {
			Util_CatchLZMAError(lzuerror, &self->fp->strm, self->fp->encoding);
			Py_DECREF(ret);
			ret = NULL;
			goto cleanup;
		}
		if (bytesrequested < 0) {
			buffersize = Util_NewBufferSize(buffersize);
			if (_PyString_Resize(&ret, buffersize) < 0)
				goto cleanup;
		} else {
			break;
		}
	}
	if (bytesread != buffersize)
		_PyString_Resize(&ret, bytesread);

cleanup:
	RELEASE_LOCK(self);
	return ret;
}


PyDoc_STRVAR(LZMAFile_readline__doc__,
"readline([size]) -> string\n\
\n\
Return the next line from the file, as a string, retaining newline.\n\
A non-negative size argument will limit the maximum number of bytes to\n\
return (an incomplete line may be returned then). Return an empty\n\
string at EOF.\n\
");

static PyObject *
LZMAFile_readline(LZMAFileObject *self, PyObject *args)
{
	PyObject *ret = NULL;
	int sizehint = -1;

	if (!PyArg_ParseTuple(args, "|i:readline", &sizehint))
		return NULL;

	ACQUIRE_LOCK(self);
	switch (self->mode) {
		case MODE_READ:
			break;
		case MODE_READ_EOF:
			ret = PyString_FromString("");
			goto cleanup;
		case MODE_CLOSED:
			PyErr_SetString(PyExc_ValueError,
					"I/O operation on closed file");
			goto cleanup;
		case MODE_WRITE:
		default:
			PyErr_SetString(PyExc_IOError,
					"file is not ready for reading");
			goto cleanup;
	}

	if (sizehint == 0)
		ret = PyString_FromString("");
	else
		ret = Util_GetLine(self, (sizehint < 0) ? 0 : sizehint);

cleanup:
	RELEASE_LOCK(self);
	return ret;
}

PyDoc_STRVAR(LZMAFile_readlines__doc__,
"readlines([size]) -> list\n\
\n\
Call readline() repeatedly and return a list of lines read.\n\
The optional size argument, if given, is an approximate bound on the\n\
total number of bytes in the lines returned.\n\
");

/* This is a hacked version of Python's fileobject.c:file_readlines(). */
static PyObject *
LZMAFile_readlines(LZMAFileObject *self, PyObject *args)
{
	long sizehint = 0;
	PyObject *list = NULL;
	PyObject *line;
	char small_buffer[SMALLCHUNK];
	char *buffer = small_buffer;
	size_t buffersize = SMALLCHUNK;
	PyObject *big_buffer = NULL;
	size_t nfilled = 0;
	size_t nread;
	size_t totalread = 0;
	char *p, *q, *end;
	int err;
	int shortread = 0;
	lzma_ret lzuerror;

	if (!PyArg_ParseTuple(args, "|l:readlines", &sizehint))
		return NULL;

	ACQUIRE_LOCK(self);
		switch (self->mode) {
		case MODE_READ:
			break;
		case MODE_READ_EOF:
			list = PyList_New(0);
			goto cleanup;
		case MODE_CLOSED:
			PyErr_SetString(PyExc_ValueError,
					"I/O operation on closed file");
			goto cleanup;
		case MODE_WRITE:
		default:
			PyErr_SetString(PyExc_IOError,
					"file is not ready for reading");
			goto cleanup;
	}

	if ((list = PyList_New(0)) == NULL)
		goto cleanup;

	for (;;) {
		Py_BEGIN_ALLOW_THREADS
		nread = Util_UnivNewlineRead(&lzuerror, self->fp,
					     buffer+nfilled,
					     buffersize-nfilled, self);
		self->pos += nread;
		Py_END_ALLOW_THREADS
		if (lzuerror == LZMA_STREAM_END) {
			self->size = self->pos;
			self->mode = MODE_READ_EOF;
			if (nread == 0) {
				sizehint = 0;
				break;
			}
			shortread = 1;
		} else if (lzuerror != LZMA_OK) {
			Util_CatchLZMAError(lzuerror, &self->fp->strm, self->fp->encoding);
		  error:
			Py_DECREF(list);
			list = NULL;
			goto cleanup;
		}
		totalread += nread;
		p = memchr(buffer+nfilled, '\n', nread);
		if (!shortread && p == NULL) {
			/* Need a larger buffer to fit this line */
			nfilled += nread;
			buffersize *= 2;
			if (buffersize > INT_MAX) {
				PyErr_SetString(PyExc_OverflowError,
				"line is longer than a Python string can hold");
				goto error;
			}
			if (big_buffer == NULL) {
				/* Create the big buffer */
				big_buffer = PyString_FromStringAndSize(
					NULL, buffersize);
				if (big_buffer == NULL)
					goto error;
				buffer = PyString_AS_STRING(big_buffer);
				memcpy(buffer, small_buffer, nfilled);
			}
			else {
				/* Grow the big buffer */
				_PyString_Resize(&big_buffer, buffersize);
				buffer = PyString_AS_STRING(big_buffer);
			}
			continue;			
		}
		end = buffer+nfilled+nread;
		q = buffer;
		while (p != NULL) {
			/* Process complete lines */
			p++;
			line = PyString_FromStringAndSize(q, p-q);
			if (line == NULL)
				goto error;
			err = PyList_Append(list, line);
			Py_DECREF(line);
			if (err != 0)
				goto error;
			q = p;
			p = memchr(q, '\n', end-q);
		}
		/* Move the remaining incomplete line to the start */
		nfilled = end-q;
		memmove(buffer, q, nfilled);
		if (sizehint > 0)
			if (totalread >= (size_t)sizehint)
				break;
		if (shortread) {
			sizehint = 0;
			break;
		}
	}
	if (nfilled != 0) {
		/* Partial last line */
		line = PyString_FromStringAndSize(buffer, nfilled);
		if (line == NULL)
			goto error;
		if (sizehint > 0) {
			/* Need to complete the last line */
			PyObject *rest = Util_GetLine(self, 0);
			if (rest == NULL) {
				Py_DECREF(line);
				goto error;
			}
			PyString_Concat(&line, rest);
			Py_DECREF(rest);
			if (line == NULL)
				goto error;
		}
		err = PyList_Append(list, line);
		Py_DECREF(line);
		if (err != 0)
			goto error;
	}

  cleanup:
	RELEASE_LOCK(self);
	if (big_buffer) {
		Py_DECREF(big_buffer);
	}
	return list;
}

PyDoc_STRVAR(LZMAFile_xreadlines__doc__,
"xreadlines() -> self\n\
\n\
For backward compatibility. LZMAFile objects now include the performance\n\
optimizations previously implemented in the xreadlines module.\n\
");

PyDoc_STRVAR(LZMAFile_write__doc__,
"write(data) -> None\n\
\n\
Write the 'data' string to file. Note that due to buffering, close() may\n\
be needed before the file on disk reflects the data written.\n\
");

/* This is a hacked version of Python's fileobject.c:file_write(). */
static PyObject *
LZMAFile_write(LZMAFileObject *self, PyObject *args)
{
	PyObject *ret = NULL;
	Py_buffer pbuf;
	char *buf;
	Py_ssize_t len;
	lzma_ret lzuerror;

	if (!PyArg_ParseTuple(args, "s*:write", &pbuf))
		return NULL;
	buf = pbuf.buf;
	len = pbuf.len;

	ACQUIRE_LOCK(self);
	switch (self->mode) {
		case MODE_WRITE:
			break;

		case MODE_CLOSED:
			PyErr_SetString(PyExc_ValueError,
					"I/O operation on closed file");
			goto cleanup;

		case MODE_READ_EOF:
		case MODE_READ:
		default:
			PyErr_SetString(PyExc_IOError,
					"file is not ready for writing");
			goto cleanup;
	}

	self->f_softspace = 0;

	Py_BEGIN_ALLOW_THREADS
	lzma_write (&lzuerror, self->fp, buf, len);
	self->pos += len;
	Py_END_ALLOW_THREADS

	if (lzuerror != LZMA_OK) {
		Util_CatchLZMAError(lzuerror, &self->fp->strm, self->fp->encoding);
		goto cleanup;
	}

	Py_INCREF(Py_None);
	ret = Py_None;

cleanup:
	PyBuffer_Release(&pbuf);
	RELEASE_LOCK(self);
	return ret;
}

PyDoc_STRVAR(LZMAFile_writelines__doc__,
"writelines(sequence_of_strings) -> None\n\
\n\
Write the sequence of strings to the file. Note that newlines are not\n\
added. The sequence can be any iterable object producing strings. This is\n\
equivalent to calling write() for each string.\n\
");

/* This is a hacked version of Python's fileobject.c:file_writelines(). */
static PyObject *
LZMAFile_writelines(LZMAFileObject *self, PyObject *seq)
{
#define CHUNKSIZE 1000
	PyObject *list = NULL;
	PyObject *iter = NULL;
	PyObject *ret = NULL;
	PyObject *line;
	int i, j, index, len, islist;
	lzma_ret lzuerror;

	ACQUIRE_LOCK(self);
	switch (self->mode) {
		case MODE_WRITE:
			break;

		case MODE_CLOSED:
			PyErr_SetString(PyExc_ValueError,
					"I/O operation on closed file");
			goto error;

		case MODE_READ:
		case MODE_READ_EOF:
		default:
			PyErr_SetString(PyExc_IOError,
					"file is not ready for writing");
			goto error;
	}

	islist = PyList_Check(seq);
	if  (!islist) {
		iter = PyObject_GetIter(seq);
		if (iter == NULL) {
			PyErr_SetString(PyExc_TypeError,
				"writelines() requires an iterable argument");
			goto error;
		}
		list = PyList_New(CHUNKSIZE);
		if (list == NULL)
			goto error;
	}

	/* Strategy: slurp CHUNKSIZE lines into a private list,
	   checking that they are all strings, then write that list
	   without holding the interpreter lock, then come back for more. */
	for (index = 0; ; index += CHUNKSIZE) {
		if (islist) {
			Py_XDECREF(list);
			list = PyList_GetSlice(seq, index, index+CHUNKSIZE);
			if (list == NULL)
				goto error;
			j = PyList_GET_SIZE(list);
		}
		else {
			for (j = 0; j < CHUNKSIZE; j++) {
				line = PyIter_Next(iter);
				if (line == NULL) {
					if (PyErr_Occurred())
						goto error;
					break;
				}
				PyList_SetItem(list, j, line);
			}
		}
		if (j == 0)
			break;

		/* Check that all entries are indeed strings. If not,
		   apply the same rules as for file.write() and
		   convert the rets to strings. This is slow, but
		   seems to be the only way since all conversion APIs
		   could potentially execute Python code. */
		for (i = 0; i < j; i++) {
			PyObject *v = PyList_GET_ITEM(list, i);
			if (!PyString_Check(v)) {
			    	const char *buffer;
			    	Py_ssize_t len;
				if (PyObject_AsCharBuffer(v, &buffer, &len)) {
					PyErr_SetString(PyExc_TypeError,
							"writelines() "
							"argument must be "
							"a sequence of "
							"strings");
					goto error;
				}
				line = PyString_FromStringAndSize(buffer,
								  len);
				if (line == NULL)
					goto error;
				Py_DECREF(v);
				PyList_SET_ITEM(list, i, line);
			}
		}

		self->f_softspace = 0;

		/* Since we are releasing the global lock, the
		   following code may *not* execute Python code. */
		Py_BEGIN_ALLOW_THREADS
		for (i = 0; i < j; i++) {
		    	line = PyList_GET_ITEM(list, i);
			len = PyString_GET_SIZE(line);
			lzma_write (&lzuerror, self->fp,
				     PyString_AS_STRING(line), len);
			if (lzuerror != LZMA_OK) {
				Py_BLOCK_THREADS
				Util_CatchLZMAError(lzuerror, &self->fp->strm, self->fp->encoding);
				goto error;
			}
		}
		Py_END_ALLOW_THREADS

		if (j < CHUNKSIZE)
			break;
	}

	Py_INCREF(Py_None);
	ret = Py_None;

  error:
	RELEASE_LOCK(self);
	Py_XDECREF(list);
  	Py_XDECREF(iter);
	return ret;
#undef CHUNKSIZE
}

PyDoc_STRVAR(LZMAFile_seek__doc__,
"seek(offset [, whence]) -> None\n\
\n\
Move to new file position. Argument offset is a byte count. Optional\n\
argument whence defaults to 0 (offset from start of file, offset\n\
should be >= 0); other values are 1 (move relative to current position,\n\
positive or negative), and 2 (move relative to end of file, usually\n\
negative, although many platforms allow seeking beyond the end of a file).\n\
\n\
Note that seeking of lzma files is emulated, and depending on the parameters\n\
the operation may be extremely slow.\n\
");

static PyObject *
LZMAFile_seek(LZMAFileObject *self, PyObject *args)
{
	int where = 0;
	PyObject *offobj;
	Py_off_t offset;
	char small_buffer[SMALLCHUNK];
	char *buffer = small_buffer;
	Py_ssize_t buffersize = SMALLCHUNK;
	Py_off_t bytesread = 0;
	size_t readsize;
	int chunksize;
	lzma_ret lzuerror;
	PyObject *ret = NULL;

	if (!PyArg_ParseTuple(args, "O|i:seek", &offobj, &where))
		return NULL;
#if !defined(HAVE_LARGEFILE_SUPPORT)
	offset = PyInt_AsLong(offobj);
#else
	offset = PyLong_Check(offobj) ?
		PyLong_AsLongLong(offobj) : PyInt_AsLong(offobj);
#endif
	if (PyErr_Occurred())
		return NULL;

	ACQUIRE_LOCK(self);
	Util_DropReadAhead(self);
	switch (self->mode) {
		case MODE_READ:
		case MODE_READ_EOF:
			break;

		case MODE_CLOSED:
			PyErr_SetString(PyExc_ValueError,
					"I/O operation on closed file");
			goto cleanup;

		case MODE_WRITE:
		default:
			PyErr_SetString(PyExc_IOError,
					"seek works only while reading");
			goto cleanup;
	}	

	if (where == 2) {
		if (self->size == -1) {
			assert(self->mode != MODE_READ_EOF);
			for (;;) {
				Py_BEGIN_ALLOW_THREADS
				chunksize = Util_UnivNewlineRead(
						&lzuerror, self->fp,
						buffer, buffersize,
						self);
				self->pos += chunksize;
				Py_END_ALLOW_THREADS

				bytesread += chunksize;
				if (lzuerror == LZMA_STREAM_END) {
					break;
				} else if (lzuerror != LZMA_OK) {
					Util_CatchLZMAError(lzuerror, &self->fp->strm, self->fp->encoding);
					goto cleanup;
				}
			}
			self->mode = MODE_READ_EOF;
			self->size = self->pos;
			bytesread = 0;
		}
		offset = self->size + offset;
	} else if (where == 1) {
		offset = self->pos + offset;
	}

	/* Before getting here, offset must be the absolute position the file 
	 * pointer should be set to. */

	if (offset >= self->pos) {
		/* we can move forward */
		offset -= self->pos;
	} else {
		/* we cannot move back, so rewind the stream */
		lzma_close_real(&lzuerror, self->fp);
		if (self->fp) {
			PyFile_DecUseCount((PyFileObject *)self->file);
			self->fp = NULL;
		}
		if (lzuerror != LZMA_OK) {
			Util_CatchLZMAError(lzuerror, &self->fp->strm, self->fp->encoding);
			goto cleanup;
		}
		ret = PyObject_CallMethod(self->file, "seek", "(i)", 0);
		if (!ret)
			goto cleanup;
		Py_DECREF(ret);
		ret = NULL;
		self->pos = 0;
		self->fp = lzma_open_real(&lzuerror, self->filters, PyFile_AsFile(self->file), self->memlimit);

		if (self->fp)
			PyFile_IncUseCount((PyFileObject *)self->file);
		if (lzuerror != LZMA_OK) {
			Util_CatchLZMAError(lzuerror, &self->fp->strm, self->fp->encoding);
			goto cleanup;
		}
		self->mode = MODE_READ;
	}

	if (offset <= 0 || self->mode == MODE_READ_EOF )
		goto exit;

	/* Before getting here, offset must be set to the number of bytes
	 * to walk forward. */
	for (;;) {
		if (offset-bytesread > buffersize)
			readsize = buffersize;
		else
			/* offset might be wider that readsize, but the result
			 * of the subtraction is bound by buffersize (see the
			 * condition above). buffersize is 8192. */
			readsize = (size_t)(offset-bytesread);
		Py_BEGIN_ALLOW_THREADS
		chunksize = Util_UnivNewlineRead(&lzuerror, self->fp,
						 buffer, readsize, self);
		self->pos += chunksize;
		Py_END_ALLOW_THREADS
		bytesread += chunksize;
		if (lzuerror == LZMA_STREAM_END) {
			self->size = self->pos;
			self->mode = MODE_READ_EOF;
			break;
		} else if (lzuerror != LZMA_OK) {
			Util_CatchLZMAError(lzuerror, &self->fp->strm, self->fp->encoding);
			goto cleanup;
		}
		if (bytesread == offset)
			break;
	}

exit:
	Py_INCREF(Py_None);
	ret = Py_None;

cleanup:
	RELEASE_LOCK(self);
	return ret;
}

PyDoc_STRVAR(LZMAFile_tell__doc__,
"tell() -> int\n\
\n\
Return the current file position, an integer (may be a long integer).\n\
");

static PyObject *
LZMAFile_tell(LZMAFileObject *self, __attribute__((unused)) PyObject *args)
{
	PyObject *ret = NULL;

	if (self->mode == MODE_CLOSED) {
		PyErr_SetString(PyExc_ValueError,
				"I/O operation on closed file");
		goto cleanup;
	}

#if !defined(HAVE_LARGEFILE_SUPPORT)
	ret = PyInt_FromLong(self->pos);
#else
	ret = PyLong_FromLongLong(self->pos);
#endif

cleanup:
	return ret;
}

PyDoc_STRVAR(LZMAFile_close__doc__,
"close() -> None or (perhaps) an integer\n\
\n\
Close the file. Sets data attribute .closed to true. A closed file\n\
cannot be used for further I/O operations. close() may be called more\n\
than once without error.\n\
");

static PyObject *
LZMAFile_close(LZMAFileObject *self)
{
	PyObject *ret = NULL;
	lzma_ret lzuerror = LZMA_OK;

	ACQUIRE_LOCK(self);
	lzma_close_real(&lzuerror, self->fp);
	if (self->fp) {
		PyFile_DecUseCount((PyFileObject *)self->file);
		self->fp = NULL;
	}
	self->mode = MODE_CLOSED;
	ret = PyObject_CallMethod(self->file, "close", NULL);
	if (lzuerror != LZMA_OK && lzuerror != LZMA_STREAM_END) {
		Util_CatchLZMAError(lzuerror, NULL, self->fp->encoding);
		Py_XDECREF(ret);
		ret = NULL;
	}

	RELEASE_LOCK(self);
	return ret;
}

PyDoc_STRVAR(LZMAFile_enter_doc,
"__enter__() -> self.");

static PyObject *
LZMAFile_enter(LZMAFileObject *self)
{
	if (self->mode == MODE_CLOSED) {
		PyErr_SetString(PyExc_ValueError,
				"I/O operation on closed file");
		return NULL;
	}
	Py_INCREF(self);
	return (PyObject *) self;
}

PyDoc_STRVAR(LZMAFile_exit_doc,
"__exit__(*excinfo) -> None.  Closes the file.");

static PyObject *
LZMAFile_exit(LZMAFileObject *self, __attribute__((unused))PyObject *args)
{
	PyObject *ret = PyObject_CallMethod((PyObject *) self, "close", NULL);
	if (!ret)
		/* If error occurred, pass through */
		return NULL;
	Py_DECREF(ret);
	Py_RETURN_NONE;
}

static PyObject *LZMAFile_getiter(LZMAFileObject *self);

static PyMethodDef LZMAFile_methods[] = {
	{"read", (PyCFunction)LZMAFile_read, METH_VARARGS, LZMAFile_read__doc__},
	{"readline", (PyCFunction)LZMAFile_readline, METH_VARARGS, LZMAFile_readline__doc__},
	{"readlines", (PyCFunction)LZMAFile_readlines, METH_VARARGS, LZMAFile_readlines__doc__},
	{"xreadlines", (PyCFunction)LZMAFile_getiter, METH_VARARGS, LZMAFile_xreadlines__doc__},
	{"write", (PyCFunction)LZMAFile_write, METH_VARARGS, LZMAFile_write__doc__},
	{"writelines", (PyCFunction)LZMAFile_writelines, METH_O, LZMAFile_writelines__doc__},
	{"seek", (PyCFunction)LZMAFile_seek, METH_VARARGS, LZMAFile_seek__doc__},
	{"tell", (PyCFunction)LZMAFile_tell, METH_NOARGS, LZMAFile_tell__doc__},
	{"close", (PyCFunction)LZMAFile_close, METH_NOARGS, LZMAFile_close__doc__},
	{"__enter__", (PyCFunction)LZMAFile_enter, METH_NOARGS, LZMAFile_enter_doc},
	{"__exit__", (PyCFunction)LZMAFile_exit, METH_VARARGS, LZMAFile_exit_doc},
	{NULL, NULL, 0, NULL}		/* sentinel */
};


/* ===================================================================== */
/* Getters and setters of LZMAFile. */

/* This is a hacked version of Python's fileobject.c:get_newlines(). */
static PyObject *
LZMAFile_get_newlines(LZMAFileObject *self, __attribute__((unused)) void *closure)
{
	switch (self->f_newlinetypes) {
	case NEWLINE_UNKNOWN:
		Py_INCREF(Py_None);
		return Py_None;
	case NEWLINE_CR:
		return PyString_FromString("\r");
	case NEWLINE_LF:
		return PyString_FromString("\n");
	case NEWLINE_CR|NEWLINE_LF:
		return Py_BuildValue("(ss)", "\r", "\n");
	case NEWLINE_CRLF:
		return PyString_FromString("\r\n");
	case NEWLINE_CR|NEWLINE_CRLF:
		return Py_BuildValue("(ss)", "\r", "\r\n");
	case NEWLINE_LF|NEWLINE_CRLF:
		return Py_BuildValue("(ss)", "\n", "\r\n");
	case NEWLINE_CR|NEWLINE_LF|NEWLINE_CRLF:
		return Py_BuildValue("(sss)", "\r", "\n", "\r\n");
	default:
		PyErr_Format(PyExc_SystemError, 
			     "Unknown newlines value 0x%x\n", 
			     self->f_newlinetypes);
		return NULL;
	}
}

static PyObject *
LZMAFile_get_closed(LZMAFileObject *self, __attribute__((unused)) void *closure)
{
	return PyInt_FromLong(self->mode == MODE_CLOSED);
}

static PyObject *
LZMAFile_get_mode(LZMAFileObject *self, __attribute__((unused)) void *closure)
{
	return PyObject_GetAttrString(self->file, "mode");
}

static PyObject *
LZMAFile_get_name(LZMAFileObject *self, __attribute__((unused)) void *closure)
{
	return PyObject_GetAttrString(self->file, "name");
}

static PyGetSetDef LZMAFile_getset[] = {
	{"closed", (getter)LZMAFile_get_closed, NULL,
		"True if the file is closed", NULL},
	{"newlines", (getter)LZMAFile_get_newlines, NULL,
		"end-of-line convention used in this file", NULL},
	{"mode", (getter)LZMAFile_get_mode, NULL,
		"file mode ('r', 'w', or 'U')", NULL},
	{"name", (getter)LZMAFile_get_name, NULL,
		"file name", NULL},
	{NULL, NULL, NULL, NULL, NULL}	/* Sentinel */
};


/* ===================================================================== */
/* Members of LZMAFile_Type. */

#undef OFF
#define OFF(x) offsetof(LZMAFileObject, x)

static PyMemberDef LZMAFile_members[] = {
	{"softspace",	T_INT,		OFF(f_softspace), 0,
	 "flag indicating that a space needs to be printed; used by print"},
	{NULL, 0, 0, 0, NULL}	/* Sentinel */
};

/* ===================================================================== */
/* Slot definitions for LZMAFile_Type. */

static int
LZMAFile_init(LZMAFileObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *name = NULL, *options_dict = NULL;
	char *mode = "r";
	int buffering = -1;
	lzma_ret lzuerror;

	static char *kwlist[] = {"name", "mode", "buffering", "memlimit",
		"options", NULL};

	self->filters[0].options = NULL;

	self->size = -1;
	self->memlimit = -1;

	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|siKO:LZMAFile", kwlist,
					&name, &mode, &buffering, &self->memlimit, &options_dict))
		return -1;

	for (;;) {
		bool error = false;
		switch (*mode) {
			case 'w':
				if(!self->filters[0].options)
				{
					self->filters[0].options = &self->options;
					if(!init_lzma_options("LZMAFile", options_dict, self->filters))
						return -1;
				}
				break;

			case 'r':
				if(self->filters[0].options)
					error = true;
			case 'b':
				break;

			case 'U':
#ifdef __VMS
				self->f_univ_newline = false;
#else
				self->f_univ_newline = true;
#endif
				break;

			default:
				error = true;
				break;
		}
		if (error) {
			if(self->filters[0].options)
				free(self->filters[0].options);
			PyErr_Format(PyExc_ValueError,
				     "invalid mode char %c", *mode);
			return -1;
		}
		mode++;
		if (*mode == '\0')
			break;
	}

	mode = self->filters[0].options ? "wb" : "rb";

	self->file = PyObject_CallFunction((PyObject*)&PyFile_Type, "(Osi)",
					   name, mode, buffering);
	if (self->file == NULL)
		return -1;

	/* From now on, we have stuff to dealloc, so jump to error label
	 * instead of returning */

#ifdef WITH_THREAD
	self->lock = PyThread_allocate_lock();
	if (!self->lock) {
		PyErr_SetString(PyExc_MemoryError, "unable to allocate lock");
		goto error;
	}
#endif

	self->fp = lzma_open_real(&lzuerror, self->filters, PyFile_AsFile(self->file), self->memlimit);

	if (lzuerror != LZMA_OK) {
		Util_CatchLZMAError(lzuerror, &self->fp->strm, self->fp->encoding);
		goto error;
	}
	PyFile_IncUseCount((PyFileObject *)self->file);

	self->mode = self->filters[0].options ? MODE_WRITE : MODE_READ;

	return 0;

error:
	Py_CLEAR(self->file);
#ifdef WITH_THREAD
	if (self->lock) {
		PyThread_free_lock(self->lock);
		self->lock = NULL;
	}
#endif
	return -1;
}

static void
LZMAFile_dealloc(LZMAFileObject *self)
{
	lzma_ret lzuerror;
#ifdef WITH_THREAD
	if (self->lock)
		PyThread_free_lock(self->lock);
#endif
	lzma_close_real(&lzuerror, self->fp);
	if (self->fp) {
		PyFile_DecUseCount((PyFileObject *)self->file);
		self->fp = NULL;
	}
	Util_DropReadAhead(self);
	Py_XDECREF(self->file);
	Py_TYPE(self)->tp_free((PyObject *)self);
}

/* This is a hacked version of Python's fileobject.c:file_getiter(). */
static PyObject *
LZMAFile_getiter(LZMAFileObject *self)
{
	if (self->mode == MODE_CLOSED) {
		PyErr_SetString(PyExc_ValueError,
				"I/O operation on closed file");
		return NULL;
	}
	Py_INCREF((PyObject*)self);
	return (PyObject *)self;
}

/* This is a hacked version of Python's fileobject.c:file_iternext(). */
#define READAHEAD_BUFSIZE 8192
static PyObject *
LZMAFile_iternext(LZMAFileObject *self)
{
	PyStringObject* ret;
	ACQUIRE_LOCK(self);
		if (self->mode == MODE_CLOSED) {
                RELEASE_LOCK(self);
		PyErr_SetString(PyExc_ValueError,
				"I/O operation on closed file");
		return NULL;
	}
	ret = Util_ReadAheadGetLineSkip(self, 0, READAHEAD_BUFSIZE);
	RELEASE_LOCK(self);
	if (ret == NULL || PyString_GET_SIZE(ret) == 0) {
		Py_XDECREF(ret);
		return NULL;
	}
	return (PyObject *)ret;
}

/* ===================================================================== */
/* LZMAFile_Type definition. */

PyDoc_VAR(LZMAFile__doc__) =
PyDoc_STR(
"LZMAFile(name [, mode='r', buffering=0, memlimit=-1,\n"
DEFAULT_OPTIONS_STRING"]) -> file object\n\
\n\
Open a lzma file. The mode can be 'r' or 'w', for reading (default) or\n\
writing. When opened for writing, the file will be created if it doesn't\n\
exist, and truncated otherwise. If the buffering argument is given, 0 means\n\
unbuffered, and larger numbers specify the buffer size.\n\
")
PyDoc_STR(
"\n\
Add a 'U' to mode to open the file for input with universal newline\n\
support. Any line ending in the input file will be seen as a '\\n' in\n\
Python. Also, a file so opened gains the attribute 'newlines'; the value\n\
for this attribute is one of None (no newline read yet), '\\r', '\\n',\n\
'\\r\\n' or a tuple containing all the newline types seen. Universal\n\
newlines are available only when reading.\n\
")
;

PyTypeObject LZMAFile_Type = {
	PyObject_HEAD_INIT(NULL)
	0,					/*ob_size*/
	"lzma.LZMAFile",			/*tp_name*/
	sizeof(LZMAFileObject),			/*tp_basicsize*/
	0,					/*tp_itemsize*/
	(destructor)LZMAFile_dealloc,		/*tp_dealloc*/
	0,					/*tp_print*/
	0,					/*tp_getattr*/
	0,					/*tp_setattr*/
	0,					/*tp_compare*/
	0,					/*tp_repr*/
	0,					/*tp_as_number*/
	0,					/*tp_as_sequence*/
	0,					/*tp_as_mapping*/
	0,					/*tp_hash*/
        0,					/*tp_call*/
        0,					/*tp_str*/
        PyObject_GenericGetAttr,		/*tp_getattro*/
        PyObject_GenericSetAttr,		/*tp_setattro*/
        0,					/*tp_as_buffer*/
        Py_TPFLAGS_DEFAULT|Py_TPFLAGS_BASETYPE,	/*tp_flags*/
        LZMAFile__doc__,			/*tp_doc*/
        0,					/*tp_traverse*/
        0,					/*tp_clear*/
        0,					/*tp_richcompare*/
        0,					/*tp_weaklistoffset*/
        (getiterfunc)LZMAFile_getiter,		/*tp_iter*/
        (iternextfunc)LZMAFile_iternext,	/*tp_iternext*/
        LZMAFile_methods,			/*tp_methods*/
        LZMAFile_members,			/*tp_members*/
        LZMAFile_getset,			/*tp_getset*/
        0,                      		/*tp_base*/
        0,                      		/*tp_dict*/
        0,                      		/*tp_descr_get*/
        0,                      		/*tp_descr_set*/
        0,                      		/*tp_dictoffset*/
        (initproc)LZMAFile_init,		/*tp_init*/
        PyType_GenericAlloc,    		/*tp_alloc*/
        PyType_GenericNew,      		/*tp_new*/
      	_PyObject_Del,          		/*tp_free*/
        0,                      		/*tp_is_gc*/
	0,					/*tp_bases*/
	0,					/*tp_mro*/
	0,					/*tp_cache*/
	0,					/*tp_subclasses*/
	0,					/*tp_weaklist*/
	0,					/*tp_del*/
	0					/*tp_version_tag*/
};
