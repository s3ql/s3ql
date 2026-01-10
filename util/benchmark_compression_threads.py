#!/usr/bin/env python3
"""
Benchmark: Thread dispatch overhead vs compression/decompression time

This script determines the data size thresholds at which:
- Compression+encryption takes longer than dispatching work to a separate thread
- Decryption+decompression takes longer than dispatching work to a separate thread

Approach:
1. Measure thread dispatch overhead with adaptive iterations until stable
2. Exponential growth to find upper bound where operation exceeds overhead
3. Binary search to narrow down the crossover point
"""

import argparse
import bz2
import io
import logging
import lzma
import math
import os
import statistics
import time
import zlib
from collections.abc import Awaitable, Callable

import trio

from s3ql.backends.comprenc import (
    compress_fh,
    decompress_fh,
    decrypt_fh,
    encrypt_fh,
    sha256,
)
from s3ql.types import CompressorProtocol, DecompressorProtocol

log = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='Find data size thresholds for threaded compression/decompression'
    )
    parser.add_argument(
        '--algorithm',
        '-a',
        choices=['zlib', 'bzip2', 'lzma'],
        default='zlib',
        help='Compression algorithm (default: zlib)',
    )
    parser.add_argument(
        '--level',
        '-l',
        type=int,
        choices=range(10),
        default=6,
        metavar='0-9',
        help='Compression level (default: 6)',
    )
    parser.add_argument(
        '--sem-threshold',
        '-s',
        type=float,
        default=0.20,
        metavar='FRACTION',
        help='Target relative standard error of mean for measurements (default: 0.20)',
    )
    parser.add_argument(
        '--size-threshold',
        '-t',
        type=float,
        default=0.20,
        metavar='FRACTION',
        help='Binary search stops when size interval < this fraction (default: 0.20)',
    )
    parser.add_argument(
        '--verbose',
        '-v',
        action='store_true',
        help='Enable verbose output showing measurement progress',
    )
    return parser.parse_args()


def make_compressor(algorithm: str, level: int) -> CompressorProtocol:
    if algorithm == 'zlib':
        return zlib.compressobj(level)
    elif algorithm == 'bzip2':
        return bz2.BZ2Compressor(level)
    elif algorithm == 'lzma':
        return lzma.LZMACompressor(preset=level)
    raise ValueError(f'Unknown algorithm: {algorithm}')


def make_decompressor(algorithm: str) -> DecompressorProtocol:
    if algorithm == 'zlib':
        return zlib.decompressobj()
    elif algorithm == 'bzip2':
        return bz2.BZ2Decompressor()
    elif algorithm == 'lzma':
        return lzma.LZMADecompressor()
    raise ValueError(f'Unknown algorithm: {algorithm}')


def compress_and_encrypt(data: bytes, algorithm: str, level: int, key: bytes) -> bytes:
    """Compress then encrypt using s3ql's methods."""
    compr = make_compressor(algorithm, level)
    ifh = io.BytesIO(data)
    compressed_buf = io.BytesIO()
    compress_fh(ifh, compressed_buf, compr)

    compressed_buf.seek(0)
    encrypted_buf = io.BytesIO()
    data_key = sha256(key + b'benchmark')
    encrypt_fh(compressed_buf, encrypted_buf, data_key)

    return encrypted_buf.getvalue()


def decrypt_and_decompress(encrypted_data: bytes, algorithm: str, key: bytes) -> bytes:
    """Decrypt then decompress using s3ql's methods."""
    encrypted_buf = io.BytesIO(encrypted_data)
    decrypted_buf = io.BytesIO()
    data_key = sha256(key + b'benchmark')
    decrypt_fh(encrypted_buf, decrypted_buf, data_key)

    decrypted_buf.seek(0)
    decomp = make_decompressor(algorithm)
    output_buf = io.BytesIO()
    decompress_fh(decrypted_buf, output_buf, decomp)

    return output_buf.getvalue()


def generate_test_data(size: int) -> bytes:
    """Generate compressible test data with mix of repeated and random content."""
    pattern = b"The quick brown fox jumps over the lazy dog. " * 100
    repeated = (pattern * (size // len(pattern) + 1))[: size // 2]
    random_part = os.urandom(size - len(repeated))
    return repeated + random_part


def format_time(seconds: float) -> str:
    """Format time in appropriate units."""
    if seconds < 1e-6:
        return f"{seconds * 1e9:.1f}ns"
    elif seconds < 1e-3:
        return f"{seconds * 1e6:.1f}us"
    elif seconds < 1:
        return f"{seconds * 1e3:.2f}ms"
    else:
        return f"{seconds:.3f}s"


def format_time_with_error(mean: float, stdev: float) -> str:
    """Format time as mean +/- stdev in appropriate units."""
    if mean < 1e-6:
        return f"{mean * 1e9:.1f} +/- {stdev * 1e9:.1f}ns"
    elif mean < 1e-3:
        return f"{mean * 1e6:.1f} +/- {stdev * 1e6:.1f}us"
    elif mean < 1:
        return f"{mean * 1e3:.2f} +/- {stdev * 1e3:.2f}ms"
    else:
        return f"{mean:.3f} +/- {stdev:.3f}s"


def format_size(bytes_count: int) -> str:
    """Format byte size."""
    if bytes_count < 1024:
        return f"{bytes_count}B"
    elif bytes_count < 1024 * 1024:
        return f"{bytes_count / 1024:.1f}KB"
    else:
        return f"{bytes_count / (1024 * 1024):.1f}MB"


async def measure_until_stable(
    operation: Callable[[], Awaitable[None]],
    target_rel_sem: float,
    verbose: bool = False,
    min_iterations: int = 50,
    max_time: float = 5.0,
) -> tuple[float, float, int, bool]:
    """
    Measure operation time until relative standard error of mean is below threshold.

    Uses Standard Error of Mean (SEM = stdev / sqrt(n)) instead of raw stdev,
    since SEM measures precision of the mean estimate and decreases with more samples.

    Returns (mean, SEM, iteration_count, converged).
    """
    times: list[float] = []
    iteration = 0

    total_time = 0.0
    while total_time < max_time:
        iteration += 1
        op_start = time.perf_counter()
        await operation()
        elapsed = time.perf_counter() - op_start
        times.append(elapsed)
        total_time += elapsed

        if len(times) >= min_iterations:
            mean = statistics.mean(times)
            stdev = statistics.stdev(times)
            sem = stdev / math.sqrt(len(times))
            rel_sem = sem / mean if mean > 0 else float('inf')

            if verbose and iteration % 100 == 0:
                log.debug(
                    'Iteration %d: mean=%s, rel_SEM=%.1f%%',
                    iteration,
                    format_time(mean),
                    rel_sem * 100,
                )

            if rel_sem <= target_rel_sem:
                return mean, sem, len(times), True

    mean = statistics.mean(times)
    stdev = statistics.stdev(times) if len(times) > 1 else 0
    sem = stdev / math.sqrt(len(times))
    return mean, sem, len(times), False


async def measure_thread_overhead(
    target_rel_sem: float, verbose: bool = False
) -> tuple[float, float, int, bool]:
    """Measure the overhead of dispatching a trivial function to a thread."""

    def trivial() -> None:
        pass

    async def run_threaded() -> None:
        await trio.to_thread.run_sync(trivial)

    return await measure_until_stable(
        run_threaded,
        target_rel_sem,
        verbose=verbose,
    )


async def measure_compression_time(
    data: bytes,
    algorithm: str,
    level: int,
    key: bytes,
    target_rel_sem: float,
    verbose: bool = False,
) -> tuple[float, float, int, bool]:
    """Measure compression+encryption time for given data."""

    async def run_compression() -> None:
        compress_and_encrypt(data, algorithm, level, key)

    return await measure_until_stable(
        run_compression,
        target_rel_sem,
        verbose=verbose,
    )


async def measure_decompression_time(
    encrypted_data: bytes,
    algorithm: str,
    key: bytes,
    target_rel_sem: float,
    verbose: bool = False,
) -> tuple[float, float, int, bool]:
    """Measure decryption+decompression time for given encrypted data."""

    async def run_decompression() -> None:
        decrypt_and_decompress(encrypted_data, algorithm, key)

    return await measure_until_stable(
        run_decompression,
        target_rel_sem,
        verbose=verbose,
    )


async def find_crossover_size(
    thread_overhead: float,
    algorithm: str,
    level: int,
    key: bytes,
    target_rel_sem: float,
    size_threshold: float,
    verbose: bool = False,
) -> tuple[int, int, tuple[float, float], tuple[float, float]]:
    """
    Find the data size crossover point using exponential growth + binary search.

    Returns:
        (size_below, size_above, (time_below, stdev_below), (time_above, stdev_above))
    """
    # Phase A: Exponential growth to find upper bound
    size = 100
    prev_size = size
    prev_time = 0.0
    prev_stdev = 0.0

    log.info('Phase A: Exponential growth to find upper bound...')

    while True:
        data = generate_test_data(size)
        comp_time, comp_stdev, iterations, _ = await measure_compression_time(
            data, algorithm, level, key, target_rel_sem, verbose
        )

        if verbose:
            log.debug(
                '  Size %s: %s (%d iterations)',
                format_size(size),
                format_time_with_error(comp_time, comp_stdev),
                iterations,
            )

        if comp_time >= thread_overhead:
            # Found upper bound
            low_size = prev_size
            high_size = size
            low_time, low_stdev = prev_time, prev_stdev
            high_time, high_stdev = comp_time, comp_stdev
            break

        prev_size = size
        prev_time = comp_time
        prev_stdev = comp_stdev
        size *= 2

    log.info(
        'Found bounds: %s (below) to %s (above)',
        format_size(low_size),
        format_size(high_size),
    )

    # Phase B: Binary search to narrow down
    log.info('Phase B: Binary search to narrow down...')

    while (high_size - low_size) / ((low_size + high_size) / 2) > size_threshold:
        mid_size = (low_size + high_size) // 2
        data = generate_test_data(mid_size)
        mid_time, mid_stdev, iterations, _ = await measure_compression_time(
            data, algorithm, level, key, target_rel_sem, verbose
        )

        if verbose:
            log.debug(
                '  Size %s: %s (%d iterations)',
                format_size(mid_size),
                format_time_with_error(mid_time, mid_stdev),
                iterations,
            )

        if mid_time < thread_overhead:
            low_size = mid_size
            low_time, low_stdev = mid_time, mid_stdev
        else:
            high_size = mid_size
            high_time, high_stdev = mid_time, mid_stdev

    return low_size, high_size, (low_time, low_stdev), (high_time, high_stdev)


async def find_decompression_crossover_size(
    thread_overhead: float,
    algorithm: str,
    level: int,
    key: bytes,
    target_rel_sem: float,
    size_threshold: float,
    verbose: bool = False,
) -> tuple[int, int, tuple[float, float], tuple[float, float]]:
    """
    Find the data size crossover point for decompression using exponential growth + binary search.

    Reports sizes in terms of original (uncompressed) data size for consistency with compression.

    Returns:
        (size_below, size_above, (time_below, stdev_below), (time_above, stdev_above))
    """
    # Phase A: Exponential growth to find upper bound
    size = 100
    prev_size = size
    prev_time = 0.0
    prev_stdev = 0.0

    log.info('Phase A: Exponential growth to find upper bound...')

    while True:
        data = generate_test_data(size)
        encrypted_data = compress_and_encrypt(data, algorithm, level, key)
        decomp_time, decomp_stdev, iterations, _ = await measure_decompression_time(
            encrypted_data, algorithm, key, target_rel_sem, verbose
        )

        if verbose:
            log.debug(
                '  Size %s: %s (%d iterations)',
                format_size(size),
                format_time_with_error(decomp_time, decomp_stdev),
                iterations,
            )

        if decomp_time >= thread_overhead:
            low_size = prev_size
            high_size = size
            low_time, low_stdev = prev_time, prev_stdev
            high_time, high_stdev = decomp_time, decomp_stdev
            break

        prev_size = size
        prev_time = decomp_time
        prev_stdev = decomp_stdev
        size *= 2

    log.info(
        'Found bounds: %s (below) to %s (above)',
        format_size(low_size),
        format_size(high_size),
    )

    # Phase B: Binary search to narrow down
    log.info('Phase B: Binary search to narrow down...')

    while (high_size - low_size) / ((low_size + high_size) / 2) > size_threshold:
        mid_size = (low_size + high_size) // 2
        data = generate_test_data(mid_size)
        encrypted_data = compress_and_encrypt(data, algorithm, level, key)
        mid_time, mid_stdev, iterations, _ = await measure_decompression_time(
            encrypted_data, algorithm, key, target_rel_sem, verbose
        )

        if verbose:
            log.debug(
                '  Size %s: %s (%d iterations)',
                format_size(mid_size),
                format_time_with_error(mid_time, mid_stdev),
                iterations,
            )

        if mid_time < thread_overhead:
            low_size = mid_size
            low_time, low_stdev = mid_time, mid_stdev
        else:
            high_size = mid_size
            high_time, high_stdev = mid_time, mid_stdev

    return low_size, high_size, (low_time, low_stdev), (high_time, high_stdev)


async def run_benchmark(args: argparse.Namespace) -> None:
    algorithm = args.algorithm
    level = args.level
    key = os.urandom(32)

    print("=" * 70)
    print("Thread Dispatch Overhead vs Compression/Decompression Time")
    print("=" * 70)
    print(f"\nAlgorithm: {algorithm.upper()}, Level: {level}")
    print(f"SEM threshold: {args.sem_threshold * 100:.0f}%")
    print(f"Size threshold: {args.size_threshold * 100:.0f}%")

    # Step 1: Measure thread dispatch overhead
    print("\n" + "-" * 70)
    print("Measuring thread dispatch overhead...")
    print("-" * 70)

    result = await measure_thread_overhead(args.sem_threshold, verbose=args.verbose)
    overhead_mean, overhead_stdev, overhead_iterations, converged = result

    print(f"Thread overhead: {format_time_with_error(overhead_mean, overhead_stdev)}")
    print(f"Iterations: {overhead_iterations}")

    if not converged:
        sem = overhead_stdev / math.sqrt(overhead_iterations)
        rel_sem = sem / overhead_mean if overhead_mean > 0 else float('inf')
        print()
        print("!" * 70)
        print("WARNING: Thread overhead measurement did not converge!")
        target = args.sem_threshold * 100
        print(f"         Relative SEM: {rel_sem * 100:.1f}% (target: {target:.0f}%)")
        print("         Results may be less accurate than requested.")
        print("!" * 70)

    # Step 2: Find compression crossover size
    print("\n" + "-" * 70)
    print("Finding compression crossover data size...")
    print("-" * 70)

    comp_size_below, comp_size_above, comp_time_below, comp_time_above = await find_crossover_size(
        overhead_mean,
        algorithm,
        level,
        key,
        args.sem_threshold,
        args.size_threshold,
        verbose=args.verbose,
    )

    # Step 3: Find decompression crossover size
    print("\n" + "-" * 70)
    print("Finding decompression crossover data size...")
    print("-" * 70)

    (
        decomp_size_below,
        decomp_size_above,
        decomp_time_below,
        decomp_time_above,
    ) = await find_decompression_crossover_size(
        overhead_mean,
        algorithm,
        level,
        key,
        args.sem_threshold,
        args.size_threshold,
        verbose=args.verbose,
    )

    # Step 4: Print results
    print("\n" + "=" * 70)
    print("RESULTS")
    print("=" * 70)

    print(f"\nThread dispatch overhead: {format_time_with_error(overhead_mean, overhead_stdev)}")

    print("\nCompression + Encryption:")
    comp_below_str = format_time_with_error(*comp_time_below)
    comp_above_str = format_time_with_error(*comp_time_above)
    comp_mean = (comp_size_below + comp_size_above) // 2
    print(f"  Below threshold: {format_size(comp_size_below):>10} -> {comp_below_str}")
    print(f"  Above threshold: {format_size(comp_size_above):>10} -> {comp_above_str}")
    print(f"  Avg threshold:   {format_size(comp_mean):>10}")

    print("\nDecryption + Decompression:")
    decomp_below_str = format_time_with_error(*decomp_time_below)
    decomp_above_str = format_time_with_error(*decomp_time_above)
    decomp_mean = (decomp_size_below + decomp_size_above) // 2
    print(f"  Below threshold: {format_size(decomp_size_below):>10} -> {decomp_below_str}")
    print(f"  Above threshold: {format_size(decomp_size_above):>10} -> {decomp_above_str}")
    print(f"  Avg threshold:   {format_size(decomp_mean):>10}")

    print("\n" + "-" * 70)
    print("RECOMMENDATION")
    print("-" * 70)
    print(f"\nFor {algorithm.upper()} level {level}:")
    print("\n  Compression + Encryption:")
    print(f"    Use threaded execution for data >= {format_size(comp_mean)}")
    print(f"    Use direct (in-thread) execution for data < {format_size(comp_mean)}")
    print("\n  Decryption + Decompression:")
    print(f"    Use threaded execution for data >= {format_size(decomp_mean)}")
    print(f"    Use direct (in-thread) execution for data < {format_size(decomp_mean)}")


def main() -> None:
    args = parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG, format='%(message)s')
    else:
        logging.basicConfig(level=logging.INFO, format='%(message)s')

    trio.run(run_benchmark, args)


if __name__ == "__main__":
    main()
