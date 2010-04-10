#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <sys/types.h>

#include <lzma.h>

#include "liblzma_file.h"

lzma_FILE *lzma_open_real(lzma_ret *lzma_error, lzma_filter *filters, FILE *fp, uint64_t memlimit)
{
	lzma_ret *ret = lzma_error;
	bool encoding = filters[0].options ? true : false;
	lzma_FILE *lzma_file;
    
	if (!fp)
		return NULL;

	lzma_file = calloc(1, sizeof(*lzma_file));

	if (!lzma_file) {
		(void) fclose(fp);
		return NULL;
	}

	lzma_file->fp = fp;
	lzma_file->encoding = encoding;
	lzma_file->eof = false;
	lzma_stream tmp = LZMA_STREAM_INIT;
	lzma_file->strm = tmp;

	if (encoding) {
		if(filters[0].id == LZMA_FILTER_LZMA1)
			*ret = lzma_alone_encoder(&lzma_file->strm, filters[0].options);
		else
			*ret = lzma_stream_encoder(&lzma_file->strm, filters, filters[LZMA_FILTERS_MAX + 1].id);
	} else
		*ret = lzma_auto_decoder(&lzma_file->strm, memlimit, 0);

	if (*ret != LZMA_OK) {
		(void) fclose(fp);
		memset(lzma_file, 0, sizeof(*lzma_file));
		free(lzma_file);
		return NULL;
	}
	return lzma_file;
}

int lzma_flush(lzma_FILE *lzma_file)
{
	return fflush(lzma_file->fp);
}

int lzma_close_real(lzma_ret *lzma_error, lzma_FILE *lzma_file)
{
	lzma_ret *ret = lzma_error;	
	int retval = 0;
	size_t n;

	if (!lzma_file)
		return -1;
	if (lzma_file->encoding) {
		for (;;) {
			lzma_file->strm.avail_out = kBufferSize;
			lzma_file->strm.next_out = (uint8_t *)lzma_file->buf;
			*ret = lzma_code(&lzma_file->strm, LZMA_FINISH);
			if (*ret != LZMA_OK && *ret != LZMA_STREAM_END)
			{
				retval = -1;
				break;
			}
			n = kBufferSize - lzma_file->strm.avail_out;
			if (n && fwrite(lzma_file->buf, 1, n, lzma_file->fp) != n)
			{
				retval = -1;
				break;
			}
			if (*ret == LZMA_STREAM_END)
				break;
		}
	} else
		*ret = LZMA_OK;

	lzma_end(&lzma_file->strm);
	return retval;
}

int lzma_close(lzma_ret *lzma_error, lzma_FILE *lzma_file)
{
	int rc;
	rc = lzma_close_real(lzma_error, lzma_file);
	if(rc)
		return rc;
	rc = fclose(lzma_file->fp);
	return rc;
}

ssize_t lzma_read(lzma_ret *lzma_error, lzma_FILE *lzma_file, void *buf, size_t len)
{
	lzma_ret *ret = lzma_error;
	bool eof = false;
    
	if (!lzma_file || lzma_file->encoding)
		return -1;
	if (lzma_file->eof)
		return 0;

	lzma_file->strm.next_out = buf;
	lzma_file->strm.avail_out = len;
	for (;;) {
		if (!lzma_file->strm.avail_in) {
			lzma_file->strm.next_in = (uint8_t *)lzma_file->buf;
			lzma_file->strm.avail_in = fread(lzma_file->buf, 1, kBufferSize, lzma_file->fp);
			if (!lzma_file->strm.avail_in)
				eof = true;
		}
		*ret = lzma_code(&lzma_file->strm, LZMA_RUN);
		if (*ret == LZMA_STREAM_END) {
			lzma_file->eof = true;
			return len - lzma_file->strm.avail_out;
		}
		if (*ret != LZMA_OK)
			return -1;
		if (!lzma_file->strm.avail_out)
			return len;
		if (eof)
			return -1;
	}
}

ssize_t lzma_write(lzma_ret *lzma_error, lzma_FILE *lzma_file, void *buf, size_t len)
{
	lzma_ret *ret = lzma_error;
	size_t n;

	if (!lzma_file || !lzma_file->encoding)
		return -1;
	if (!len)
		return 0;

	lzma_file->strm.next_in = buf;
	lzma_file->strm.avail_in = len;
	for (;;) {
		lzma_file->strm.next_out = (uint8_t *)lzma_file->buf;
		lzma_file->strm.avail_out = kBufferSize;
		*ret = lzma_code(&lzma_file->strm, LZMA_RUN);
		if (*ret != LZMA_OK)
			return -1;
		n = kBufferSize - lzma_file->strm.avail_out;
		if (n && fwrite(lzma_file->buf, 1, n, lzma_file->fp) != n)
			return -1;
		if (!lzma_file->strm.avail_in)
			return len;
	}
}
