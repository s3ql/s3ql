#ifndef LIBLZMA_UTIL_H
#define LIBLZMA_UTIL_H 1

#include "liblzma.h"
#include "liblzma_fileobj.h"

#if BUFSIZ <= 1024
#define SMALLCHUNK 8192
#else
#define	SMALLCHUNK BUFSIZ
#endif

#if SIZEOF_INT < 4
#define BIGCHUNK  (512 * 32)
#else
#define BIGCHUNK  (512 * 1024)
#endif

/* Bits in f_newlinetypes */
#define NEWLINE_UNKNOWN	0	/* No newline seen, yet */
#define NEWLINE_CR 1		/* \r newline seen */
#define NEWLINE_LF 2		/* \n newline seen */
#define NEWLINE_CRLF 4		/* \r\n newline seen */

extern PyObject *LZMAError;

bool Util_CatchLZMAError(lzma_ret lzuerror, lzma_stream *lzus, bool encoding);

size_t Util_NewBufferSize(size_t currentsize);

PyObject *Util_GetLine(LZMAFileObject *f, int n);

size_t Util_UnivNewlineRead(lzma_ret *lzuerror, lzma_FILE *stream,
		char* buf, size_t n, LZMAFileObject *f);

void Util_DropReadAhead(LZMAFileObject *f);

int Util_ReadAhead(LZMAFileObject *f, int bufsize);

PyStringObject *Util_ReadAheadGetLineSkip(LZMAFileObject *f, int skip, int bufsize);

#endif /* LIBLZMA_UTIL_H */
