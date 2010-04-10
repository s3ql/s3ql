#include "liblzma_util.h"
#include "liblzma_fileobj.h"

PyObject *LZMAError = NULL;

bool
Util_CatchLZMAError(lzma_ret lzuerror, lzma_stream *lzus, bool encoding)
{
	bool ret = true;
	switch(lzuerror) {
		case LZMA_OK:
		case LZMA_STREAM_END:
			break;

		case LZMA_NO_CHECK:
			PyErr_WarnEx(LZMAError, "stream has no integrity check", 1);
			break;

		case LZMA_UNSUPPORTED_CHECK:
			if(encoding)
			{
				PyErr_SetString(LZMAError, "Cannot calculate the integrity check");
				ret = false;
			}
			else if(!encoding)
			{
				char warning[50];
				sprintf(warning, "check type '%d' is unsupported, check will not be validated",
						lzma_get_check(lzus));
				PyErr_SetString(LZMAError, warning);
			}
			break;

		case LZMA_GET_CHECK:
			//TODO: ?
			break;

		case LZMA_MEM_ERROR:
			PyErr_SetString(PyExc_MemoryError, "cannot allocate memory");
			ret = false;
			break;

		case LZMA_MEMLIMIT_ERROR:
			PyErr_SetString(PyExc_MemoryError, "memory usage limit was reached");
			ret = false;
			break;

		case LZMA_FORMAT_ERROR:
			PyErr_SetString(LZMAError, "unknown file format");
			ret = false;
			break;

		case LZMA_OPTIONS_ERROR:
			PyErr_SetString(LZMAError, "invalid or unsupported options");
			ret = false;
			break;

		case LZMA_DATA_ERROR:
			PyErr_SetString(PyExc_IOError, "invalid data stream");
			ret = false;
			break;

		case LZMA_BUF_ERROR:
			if (lzus != NULL && lzus->avail_out > 0) {
				PyErr_SetString(PyExc_IOError, "unknown BUF error");
				ret = false;
			}
			break;

		/*case LZMA_HEADER_ERROR:
			PyErr_SetString(PyExc_RuntimeError, "invalid or unsupported header");
			ret = false;
			break;*/

		case LZMA_PROG_ERROR:
			//FIXME: fix more accurate error message..
			PyErr_SetString(PyExc_ValueError,
					"the lzma library has received wrong "
					"options");
			ret = false;
			break;		

		default:
			ret = false;
			PyErr_SetString(LZMAError, "unknown error!");
			break;

	}
	return ret;
}

/* This is a hacked version of Python's fileobject.c:new_buffersize(). */
size_t
Util_NewBufferSize(size_t currentsize)
{
	if (currentsize > SMALLCHUNK) {
		/* Keep doubling until we reach BIGCHUNK;
		   then keep adding BIGCHUNK. */
		if (currentsize <= BIGCHUNK)
			return currentsize + currentsize;
		else
			return currentsize + BIGCHUNK;
	}
	return currentsize + SMALLCHUNK;
}

/* This is a hacked version of Python's fileobject.c:get_line(). */
PyObject *
Util_GetLine(LZMAFileObject *f, int n)
{
	char c;
	char *buf, *end;
	size_t total_v_size;	/* total # of slots in buffer */
	size_t used_v_size;	/* # used slots in buffer */
	size_t increment;       /* amount to increment the buffer */
	PyObject *v;
	lzma_ret lzuerror;
	int bytes_read;
	int newlinetypes = f->f_newlinetypes;
	bool skipnextlf = f->f_skipnextlf;
	bool univ_newline = f->f_univ_newline;

	total_v_size = n > 0 ? n : 100;
	v = PyString_FromStringAndSize((char *)NULL, total_v_size);
	if (v == NULL)
		return NULL;

	buf = BUF(v);
	end = buf + total_v_size;

	for (;;) {
		Py_BEGIN_ALLOW_THREADS
		while (buf != end) {
			bytes_read = lzma_read(&lzuerror, f->fp, &c, 1);
			f->pos++;
			if (bytes_read == 0) break;
			if (univ_newline) {
				if (skipnextlf) {
					skipnextlf = false;
					if (c == '\n') {
						/* Seeing a \n here with skipnextlf true means we
						 * saw a \r before.
						 */
						newlinetypes |= NEWLINE_CRLF;
						if (lzuerror != LZMA_OK) break;
						bytes_read = lzma_read(&lzuerror, f->fp, &c, 1);
						f->pos++;
						if (bytes_read == 0) break;
					} else {
						newlinetypes |= NEWLINE_CR;
					}
				}
				if (c == '\r') {
					skipnextlf = true;
					c = '\n';
				} else if (c == '\n')
					newlinetypes |= NEWLINE_LF;
			}
			*buf++ = c;
			if (lzuerror != LZMA_OK || c == '\n') break;
		}
		if (univ_newline && lzuerror == LZMA_STREAM_END && skipnextlf)
			newlinetypes |= NEWLINE_CR;
		Py_END_ALLOW_THREADS
		f->f_newlinetypes = newlinetypes;
		f->f_skipnextlf = skipnextlf;
		if (lzuerror == LZMA_STREAM_END) {
			f->size = f->pos;
			break;
		} else if (lzuerror != LZMA_OK) {
			Util_CatchLZMAError(lzuerror, &f->fp->strm, f->fp->encoding);
			Py_DECREF(v);
			return NULL;
		}
		if (c == '\n')
			break;
		/* Must be because buf == end */
		if (n > 0)
			break;
		used_v_size = total_v_size;
		increment = total_v_size >> 2; /* mild exponential growth */
		total_v_size += increment;
		if (total_v_size > INT_MAX) {
			PyErr_SetString(PyExc_OverflowError,
			    "line is longer than a Python string can hold");
			Py_DECREF(v);
			return NULL;
		}
		if (_PyString_Resize(&v, total_v_size) < 0)
			return NULL;
		buf = BUF(v) + used_v_size;
		end = BUF(v) + total_v_size;
	}

	used_v_size = buf - BUF(v);
	if (used_v_size != total_v_size)
		_PyString_Resize(&v, used_v_size);
	return v;
}

/* This is a hacked version of Python's
 * fileobject.c:Py_UniversalNewlineFread(). */
size_t
Util_UnivNewlineRead(lzma_ret *lzuerror, lzma_FILE *stream,
		     char* buf, size_t n, LZMAFileObject *f)
{
	char *dst = buf;
	int newlinetypes, skipnextlf;

	assert(buf != NULL);
	assert(stream != NULL);

	if (!f->f_univ_newline)
		return lzma_read(lzuerror, stream, buf, n);

	newlinetypes = f->f_newlinetypes;
	skipnextlf = f->f_skipnextlf;

	/* Invariant:  n is the number of bytes remaining to be filled
	 * in the buffer.
	 */
	while (n) {
		size_t nread;
		int shortread;
		char *src = dst;

		nread = lzma_read(lzuerror, stream, dst, n);
		assert(nread <= n);
		n -= nread; /* assuming 1 byte out for each in; will adjust */
		shortread = n != 0;	/* true iff EOF or error */
		while (nread--) {
			char c = *src++;
			if (c == '\r') {
				/* Save as LF and set flag to skip next LF. */
				*dst++ = '\n';
				skipnextlf = true;
			}
			else if (skipnextlf && c == '\n') {
				/* Skip LF, and remember we saw CR LF. */
				skipnextlf = false;
				newlinetypes |= NEWLINE_CRLF;
				++n;
			}
			else {
				/* Normal char to be stored in buffer.  Also
				 * update the newlinetypes flag if either this
				 * is an LF or the previous char was a CR.
				 */
				if (c == '\n')
					newlinetypes |= NEWLINE_LF;
				else if (skipnextlf)
					newlinetypes |= NEWLINE_CR;
				*dst++ = c;
				skipnextlf = false;
			}
		}
		if (shortread) {
			/* If this is EOF, update type flags. */
			if (skipnextlf && *lzuerror == LZMA_STREAM_END)
				newlinetypes |= NEWLINE_CR;
			break;
		}
	}
	f->f_newlinetypes = newlinetypes;
	f->f_skipnextlf = skipnextlf;
	return dst - buf;
}

/* This is a hacked version of Python's fileobject.c:drop_readahead(). */
void
Util_DropReadAhead(LZMAFileObject *f)
{
	if (f->f_buf != NULL) {
		PyMem_Free(f->f_buf);
		f->f_buf = NULL;
	}
}

/* This is a hacked version of Python's fileobject.c:readahead(). */
int
Util_ReadAhead(LZMAFileObject *f, int bufsize)
{
	int chunksize;
	lzma_ret lzuerror;

	if (f->f_buf != NULL) {
		if((f->f_bufend - f->f_bufptr) >= 1)
			return 0;
		else
			Util_DropReadAhead(f);
	}
	if (f->fp->eof) {
		f->f_bufptr = f->f_buf;
		f->f_bufend = f->f_buf;
		return 0;
	}
	if ((f->f_buf = PyMem_Malloc(bufsize)) == NULL) {
		PyErr_NoMemory();
		return -1;
	}
	Py_BEGIN_ALLOW_THREADS
	chunksize = Util_UnivNewlineRead(&lzuerror, f->fp, f->f_buf,
					 bufsize, f);
	Py_END_ALLOW_THREADS
	f->pos += chunksize;
	if (lzuerror == LZMA_STREAM_END) {
		f->size = f->pos;
	} else if (lzuerror != LZMA_OK) {
		Util_CatchLZMAError(lzuerror, &f->fp->strm, f->fp->encoding);
		Util_DropReadAhead(f);
		return -1;
	}
	f->f_bufptr = f->f_buf;
	f->f_bufend = f->f_buf + chunksize;
	return 0;
}

/* This is a hacked version of Python's
 * fileobject.c:readahead_get_line_skip(). */
PyStringObject *
Util_ReadAheadGetLineSkip(LZMAFileObject *f, int skip, int bufsize)
{
	PyStringObject* s;
	char *bufptr;
	char *buf;
	int len;

	if (f->f_buf == NULL)
		if (Util_ReadAhead(f, bufsize) < 0)
			return NULL;

	len = f->f_bufend - f->f_bufptr;
	if (len == 0)
		return (PyStringObject *)
			PyString_FromStringAndSize(NULL, skip);
	bufptr = memchr(f->f_bufptr, '\n', len);
	if (bufptr != NULL) {
		bufptr++;			/* Count the '\n' */
		len = bufptr - f->f_bufptr;
		s = (PyStringObject *)
			PyString_FromStringAndSize(NULL, skip+len);
		if (s == NULL)
			return NULL;
		memcpy(PyString_AS_STRING(s)+skip, f->f_bufptr, len);
		f->f_bufptr = bufptr;
		if (bufptr == f->f_bufend)
			Util_DropReadAhead(f);
	} else {
		bufptr = f->f_bufptr;
		buf = f->f_buf;
		f->f_buf = NULL; 	/* Force new readahead buffer */
                s = Util_ReadAheadGetLineSkip(f, skip+len,
					      bufsize + (bufsize>>2));
		if (s == NULL) {
		        PyMem_Free(buf);
			return NULL;
		}
		memcpy(PyString_AS_STRING(s)+skip, bufptr, len);
		PyMem_Free(buf);
	}
	return s;
}
