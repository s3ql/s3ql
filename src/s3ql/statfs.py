'''
statfs.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

from .logging import logging, setup_logging
from .common import assert_fs_owner, pretty_print_size
from .parse_args import ArgumentParser
from prometheus_client import start_http_server, Metric, REGISTRY
import time
import llfuse
import struct
import sys

log = logging.getLogger(__name__)

def parse_args(args):
    '''Parse command line'''

    parser = ArgumentParser(
        description="Print file system statistics.")

    parser.add_debug()
    parser.add_quiet()
    parser.add_version()

    parser.add_argument("mountpoint", metavar='<mountpoint>',
                        type=(lambda x: x.rstrip('/')),
                        help='Mount point of the file system to examine')

    parser.add_argument("--raw", action="store_true", default=False,
                          help="Do not pretty-print numbers")
    parser.add_argument("--prometheus_exporter", action="store_true", default=False,
                          help="Run prometheus web service exporter")
    parser.add_argument("--prometheus_port", action="store",type=int,default=7301,
                          help="Prometheus exporter listen port")

    return parser.parse_args(args)

def main(args=None):
    '''Print file system statistics to sys.stdout'''

    if args is None:
        args = sys.argv[1:]

    options = parse_args(args)
    setup_logging(options)

    if options.raw:
        pprint = lambda x: '%d bytes' % x
    else:
        pprint = pretty_print_size

    ctrlfile = assert_fs_owner(options.mountpoint, mountpoint=True)
    if options.prometheus_exporter:        
        start_http_server(options.prometheus_port)
        REGISTRY.register(S3QLStatsCollector(ctrlfile))
        while True: time.sleep(1)
    else:
        buf = llfuse.getxattr(ctrlfile, 's3qlstat', size_guess=256)

        (entries, blocks, inodes, fs_size, dedup_size,
        compr_size, db_size, cache_cnt, cache_size, dirty_cnt,
        dirty_size, removal_cnt) = struct.unpack('QQQQQQQQQQQQ', buf)
        p_dedup = dedup_size * 100 / fs_size if fs_size else 0
        p_compr_1 = compr_size * 100 / fs_size if fs_size else 0
        p_compr_2 = compr_size * 100 / dedup_size if dedup_size else 0
        print ('Directory entries:    %d' % entries,
            'Inodes:               %d' % inodes,
            'Data blocks:          %d' % blocks,
            'Total data size:      %s' % pprint(fs_size),
            'After de-duplication: %s (%.2f%% of total)'
                % (pprint(dedup_size), p_dedup),
            'After compression:    %s (%.2f%% of total, %.2f%% of de-duplicated)'
                % (pprint(compr_size), p_compr_1, p_compr_2),
            'Database size:        %s (uncompressed)' % pprint(db_size),
            'Cache size:           %s, %d entries' % (pprint(cache_size), cache_cnt),
            'Cache size (dirty):   %s, %d entries' % (pprint(dirty_size), dirty_cnt),
            'Queued object removals: %d' % (removal_cnt,),
            sep='\n')
    


if __name__ == '__main__':
    main(sys.argv[1:])

class S3QLStatsCollector(object):
    def __init__(self, ctrlfile):
        self.ctrlfile=ctrlfile

    def collect(self):
        # Use a decent sized buffer, otherwise the statistics have to be
        # calculated three(!) times because we need to invoke getxattr
        # three times.
        buf = llfuse.getxattr(self.ctrlfile, 's3qlstat', size_guess=256)

        (entries, blocks, inodes, fs_size, dedup_size,
        compr_size, db_size, cache_cnt, cache_size, dirty_cnt,
        dirty_size, removal_cnt) = struct.unpack('QQQQQQQQQQQQ', buf)
        p_dedup = dedup_size * 100 / fs_size if fs_size else 0
        p_compr_1 = compr_size * 100 / fs_size if fs_size else 0
        p_compr_2 = compr_size * 100 / dedup_size if dedup_size else 0

        metric = Metric('svc_directory_entires','Directory entries', 'summary')
        metric.add_sample('svc_directory_entries_count',value=entries, labels={})
        yield metric

        metric = Metric('svc_inodes','Inodes', 'summary')
        metric.add_sample('svc_inodes_count',value=inodes, labels={})
        yield metric

        metric = Metric('svc_data_blocks','Data blocks', 'summary')
        metric.add_sample('svc_data_blocks_count',value=blocks, labels={})
        yield metric

        metric = Metric('svc_data_size','Data size', 'summary')
        metric.add_sample('svc_data_size_total',value=fs_size, labels={})
        metric.add_sample('svc_data_size_after_dedup',value=dedup_size, labels={})
        metric.add_sample('svc_data_size_after_dedup_percent',value=p_dedup, labels={})
        metric.add_sample('svc_data_size_after_compress',value=compr_size, labels={})
        metric.add_sample('svc_data_size_after_compress_percent_total',value=p_compr_1, labels={})
        metric.add_sample('svc_data_size_after_compress_percent_dedup',value=p_compr_2, labels={})
        yield metric


        metric = Metric('svc_database_size','Database size', 'summary')
        metric.add_sample('svc_database_size_count_uncompress',value=db_size, labels={})
        yield metric

        metric = Metric('svc_cache_size','Cache size', 'summary')
        metric.add_sample('svc_cache_size_count',value=cache_size, labels={})
        metric.add_sample('svc_cache_size_count_entries',value=cache_cnt, labels={})
        metric.add_sample('svc_cache_size_dirty_count',value=dirty_size, labels={})
        metric.add_sample('svc_cache_size_dirty_count_entries',value=dirty_cnt, labels={})
        yield metric

        metric = Metric('svc_queue_removal','Cache size', 'summary')
        metric.add_sample('svc_queue_removal_count',value=removal_cnt, labels={})
        yield metric
        