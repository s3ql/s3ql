#include <sys/types.h>
#include <inttypes.h>
#include <lzma.h>
#include <stdio.h>
#include <stdlib.h>
#include <malloc.h>
#include <string.h>
#include <fcntl.h>

#define CHUNKSIZE_IN 4096
#define CHUNKSIZE_OUT (1024*512)

int main(int argc, char **argv)
{
	int ret;
	lzma_stream strm;
	size_t write_size;
	FILE *file_in = stdin;
	FILE *file_out = stdout;
	unsigned char *buffer_in = malloc (CHUNKSIZE_IN);
	unsigned char *buffer_out = malloc (CHUNKSIZE_OUT);
	/* Check the command line arguments. */
	if (argc > 1 && 0 == strcmp (argv[1], "--help")) {
		printf ("\nLZMAdec - a small LZMA decoder\n\nUsage: %s [--help]\n\nThe compressed data is read from stdin and uncompressed to stdout.\n\n",	argv[0]);
		return 0;
	}

	if (buffer_in == NULL || buffer_out == NULL) {
		fprintf (stderr, "%s: Not enough memory.\n", argv[0]);
        return 5;
    }
	strm.avail_in = 0;
	strm.next_in = NULL;
    strm = LZMA_STREAM_INIT_VAR;
	lzma_auto_decoder(&strm, 0, 0);

    strm.next_out = buffer_out;
    strm.avail_out = CHUNKSIZE_OUT;
    for (;;) {
	if (!strm.avail_in) {
	    strm.next_in = buffer_in;
	    strm.avail_in = fread (buffer_in, sizeof (unsigned char),
					CHUNKSIZE_IN, file_in);
	}
	ret = lzma_code(&strm, LZMA_RUN);
	if (ret == LZMA_STREAM_END) {
		write_size = CHUNKSIZE_OUT - strm.avail_out;
		if (write_size != (fwrite (buffer_out, sizeof (unsigned char),
				write_size, file_out)))
			lzma_end (&strm);
			return 0;		
	}
	if (ret != LZMA_OK && ret != LZMA_STREAM_END)
		return 1;
	}
}
