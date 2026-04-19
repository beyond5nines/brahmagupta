#!/usr/bin/env python3
"""
scan.py — Spark / AWS Glue shuffle file analyzer.

Inspects binary shuffle .data files (and optional companion .index files)
produced by Spark's sort-based and hash-based shuffle writers.  Works with
any dataset — no assumptions are made about the content inside the file.

Reports on shuffle file structure: compression format, partition map,
partition size distribution, byte-level redundancy (token frequency),
and compression codec comparison.  Includes Spark tuning recommendations
based solely on structural observations.

Supported compression wrappers: LZ4Block, Snappy, ZSTD, uncompressed.

Usage:
    python3 scan.py <path/to/shuffle_N_M_0.data> [options]

    Options:
        --max-bytes INT         Maximum bytes to read from the .data file
                                (default: unlimited).  Use this for very
                                large files to avoid OOM.
        --json                  Emit a machine-readable JSON report to stdout
                                instead of the human-readable format.
        --no-recommendations    Skip the Spark tuning recommendations section.
        --verbose               Print extra diagnostic information.
        --version               Show version and exit.

The companion .index file (if present) is auto-detected from the same
directory by substituting the .data extension with .index.
"""

__version__ = "1.0.0"

import argparse
import collections
import gzip
import json
import re
import struct
import sys
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

try:
    import lz4.block as _lz4_block

    _LZ4_AVAILABLE = True
except ImportError:
    _LZ4_AVAILABLE = False

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

LZ4_BLOCK_MAGIC = b"LZ4Block"
SNAPPY_MAGIC = b"\xff\x06\x00\x00sNaPpY"
ZSTD_MAGIC = b"\x28\xb5\x2f\xfd"

BYTES_PER_INDEX_OFFSET = 8
MIN_PRINTABLE_RUN = 5
COMPRESSION_SAMPLE_SIZE = 200_000  # bytes; cap sample to keep estimates fast

MAX_FILE_SIZE_BYTES = 2 * 1024 * 1024 * 1024  # 2 GB hard ceiling

SEPARATOR = "=" * 68
THIN_SEPARATOR = "-" * 68


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class Partition:
    partition_id: int
    offset: int
    size: int
    content_preview: str = ""


@dataclass
class CompressionEstimate:
    codec_name: str
    compressed_size: int
    original_size: int

    @property
    def ratio_percent(self) -> float:
        if self.original_size == 0:
            return 0.0
        return self.compressed_size / self.original_size * 100


@dataclass
class ShuffleReport:
    data_path: Path
    index_path: Optional[Path]
    file_size_bytes: int
    bytes_read: int
    detected_format: str
    partitions: list[Partition]
    total_partition_count: int
    printable_string_count: int
    token_frequency: dict[str, int]
    compression_estimates: list[CompressionEstimate]
    lz4_header_count: int


# ---------------------------------------------------------------------------
# CLI argument parsing
# ---------------------------------------------------------------------------


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="scan",
        description=(
            "Inspect Spark / AWS Glue shuffle .data files. "
            "Detects compression format, maps partitions, measures byte-level "
            "redundancy, and recommends Spark tuning."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "data_file",
        metavar="SHUFFLE_DATA_FILE",
        help="Path to the shuffle .data file to analyze.",
    )
    parser.add_argument(
        "--max-bytes",
        type=_positive_int,
        default=None,
        metavar="N",
        help=(
            "Read at most N bytes from the .data file. "
            "Useful for large files to avoid out-of-memory errors."
        ),
    )
    parser.add_argument(
        "--json",
        action="store_true",
        default=False,
        help="Emit a machine-readable JSON report instead of human-readable output.",
    )
    parser.add_argument(
        "--no-recommendations",
        action="store_true",
        default=False,
        help="Omit the Spark tuning recommendations section.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        default=False,
        help="Print additional diagnostic information.",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )
    return parser


def _positive_int(value: str) -> int:
    integer = int(value)
    if integer <= 0:
        raise argparse.ArgumentTypeError(f"must be a positive integer, got {value}")
    return integer


# ---------------------------------------------------------------------------
# File loading
# ---------------------------------------------------------------------------


def load_data_file(
    data_path: Path, max_bytes: Optional[int] = None
) -> tuple[bytes, int]:
    """
    Read the shuffle .data file up to max_bytes.

    Returns (content, total_file_size).
    Raises ValueError if the file exceeds the hard 2 GB ceiling or is empty.
    """
    total_size = data_path.stat().st_size

    if total_size == 0:
        raise ValueError(f"File is empty: {data_path}")

    if total_size > MAX_FILE_SIZE_BYTES:
        raise ValueError(
            f"File size {total_size:,} bytes exceeds the safety limit of "
            f"{MAX_FILE_SIZE_BYTES:,} bytes (2 GB).  Use --max-bytes to read "
            "a partial slice."
        )

    read_limit = min(total_size, max_bytes) if max_bytes else total_size

    with open(data_path, "rb") as file_handle:
        content = file_handle.read(read_limit)

    return content, total_size


def resolve_index_path(data_path: Path) -> Optional[Path]:
    candidate = data_path.with_suffix(".index")
    return candidate if candidate.exists() else None


# ---------------------------------------------------------------------------
# Format detection
# ---------------------------------------------------------------------------


def detect_format(raw: bytes) -> str:
    if raw[:8] == LZ4_BLOCK_MAGIC:
        return "LZ4Block"
    if raw[:10] == SNAPPY_MAGIC:
        return "Snappy"
    if raw[:4] == ZSTD_MAGIC:
        return "ZSTD"
    return "Unknown/Raw"


# ---------------------------------------------------------------------------
# Index parsing
# ---------------------------------------------------------------------------


def parse_index_offsets(index_path: Path) -> list[int]:
    """
    Parse a Spark shuffle index file into a list of big-endian int64 offsets.

    Raises ValueError if the file is empty or its size is not a multiple of 8.
    """
    raw_index = index_path.read_bytes()

    if len(raw_index) == 0:
        raise ValueError(f"Index file is empty: {index_path}")

    if len(raw_index) % BYTES_PER_INDEX_OFFSET != 0:
        raise ValueError(
            f"Index file size {len(raw_index)} is not a multiple of "
            f"{BYTES_PER_INDEX_OFFSET}; file may be corrupt: {index_path}"
        )

    count = len(raw_index) // BYTES_PER_INDEX_OFFSET
    return [
        struct.unpack_from(">q", raw_index, i * BYTES_PER_INDEX_OFFSET)[0]
        for i in range(count)
    ]


def build_partitions(offsets: list[int], raw: bytes) -> list[Partition]:
    partitions = []
    for partition_id in range(len(offsets) - 1):
        size = offsets[partition_id + 1] - offsets[partition_id]
        if size <= 0:
            continue
        offset = offsets[partition_id]
        block = raw[offset : offset + size]
        preview = _extract_content_preview(block)
        partitions.append(Partition(partition_id, offset, size, preview))
    return partitions


def build_single_partition(raw: bytes) -> list[Partition]:
    preview = _extract_content_preview(raw[:512])
    return [Partition(partition_id=0, offset=0, size=len(raw), content_preview=preview)]


def _extract_content_preview(block: bytes) -> str:
    readable = re.findall(rb"[\x20-\x7e]{5,}", block)
    fragments = [s.decode("utf-8", errors="replace") for s in readable[:3]]
    return " | ".join(fragments)[:80]


# ---------------------------------------------------------------------------
# String extraction
# ---------------------------------------------------------------------------


def extract_printable_strings(raw: bytes) -> list[str]:
    pattern = rb"[\x20-\x7e]{" + str(MIN_PRINTABLE_RUN).encode() + rb",}"
    return [s.decode("utf-8", errors="replace") for s in re.findall(pattern, raw)]


# ---------------------------------------------------------------------------
# Token frequency
# ---------------------------------------------------------------------------


def compute_token_frequency(strings: list[str], top_n: int = 20) -> dict[str, int]:
    """
    Count word-token occurrences across all extracted strings.
    Returns the top_n tokens sorted by frequency descending.
    """
    counter = collections.Counter(re.findall(r"\b\w{4,}\b", " ".join(strings)))
    return dict(counter.most_common(top_n))


# ---------------------------------------------------------------------------
# Compression comparison
# ---------------------------------------------------------------------------


def estimate_compression(raw_sample: bytes) -> list[CompressionEstimate]:
    """
    Measure actual compression ratios for gzip and zlib on the provided
    sample bytes, and (if lz4 is installed) for LZ4 as well.

    The sample is capped at COMPRESSION_SAMPLE_SIZE bytes so this stays fast
    even on large files.
    """
    sample = raw_sample[:COMPRESSION_SAMPLE_SIZE]
    original = len(sample)

    if original == 0:
        return []

    estimates: list[CompressionEstimate] = []

    gzip_size = len(gzip.compress(sample, compresslevel=6))
    estimates.append(CompressionEstimate("Gzip level-6", gzip_size, original))

    zlib_size = len(zlib.compress(sample, level=9))
    estimates.append(CompressionEstimate("Zlib level-9 (≈ZSTD)", zlib_size, original))

    if _LZ4_AVAILABLE:
        try:
            lz4_size = len(_lz4_block.compress(sample))
            estimates.append(
                CompressionEstimate("LZ4 block (measured)", lz4_size, original)
            )
        except Exception:
            pass  # lz4 import succeeded but compress failed — skip silently

    return estimates


# ---------------------------------------------------------------------------
# Report printing
# ---------------------------------------------------------------------------


def print_report(report: ShuffleReport, show_recommendations: bool = True) -> None:
    _print_file_summary(report)
    _print_partition_map(report)
    _print_content_analysis(report)
    _print_redundancy_analysis(report)
    _print_compression_comparison(report)
    if show_recommendations:
        _print_recommendations(report)


def _print_file_summary(report: ShuffleReport) -> None:
    total = report.total_partition_count
    non_empty = len(report.partitions)
    empty = total - non_empty
    empty_pct = (empty / total * 100) if total else 0.0
    truncated = report.bytes_read < report.file_size_bytes

    print(SEPARATOR)
    print(f"SHUFFLE FILE ANALYSIS: {report.data_path.name}")
    print(SEPARATOR)
    print(
        f"\n  .data   : {report.file_size_bytes:>14,} bytes  ({report.file_size_bytes / 1024:.2f} KB)"
    )
    if truncated:
        print(
            f"  (read)  : {report.bytes_read:>14,} bytes  (--max-bytes limit applied)"
        )
    if report.index_path:
        index_size = report.index_path.stat().st_size
        print(f"  .index  : {index_size:>14,} bytes  ({index_size / 1024:.2f} KB)")
    print(f"  Format  : {report.detected_format}")
    if not _LZ4_AVAILABLE and report.detected_format == "LZ4Block":
        print(f"  WARNING : lz4 package not installed — install with: pip install lz4")
    print(f"\n  Total partitions  : {total}")
    print(f"  Non-empty         : {non_empty}")
    print(f"  Empty             : {empty}  ({empty_pct:.0f}% wasted slots)")


def _print_partition_map(report: ShuffleReport) -> None:
    if not report.index_path:
        return
    print(f"\n\n  {'Part':<6} {'Offset':>12} {'Size (B)':>12}  Preview")
    print(f"  {'-' * 72}")
    for partition in report.partitions:
        print(
            f"  {partition.partition_id:<6} {partition.offset:>12,} "
            f"{partition.size:>12,}  {partition.content_preview}"
        )
    sizes = [p.size for p in report.partitions]
    if sizes:
        print(f"\n  Min size : {min(sizes):,} bytes")
        print(f"  Max size : {max(sizes):,} bytes")
        print(f"  Avg size : {sum(sizes) / len(sizes):,.0f} bytes")


def _print_content_analysis(report: ShuffleReport) -> None:
    print(f"\n\n{SEPARATOR}")
    print("CONTENT ANALYSIS")
    print(SEPARATOR)
    print(
        f"\n  Printable strings (≥{MIN_PRINTABLE_RUN} chars) : {report.printable_string_count:,}"
    )
    print(
        f"\n  Note: string count reflects uncompressed readable bytes in the raw file.\n"
        f"        Use token frequency below to understand byte-level redundancy."
    )


def _print_redundancy_analysis(report: ShuffleReport) -> None:
    lz4_overhead = report.lz4_header_count * len(LZ4_BLOCK_MAGIC)

    print(f"\n\n{SEPARATOR}")
    print("REDUNDANCY ANALYSIS")
    print(SEPARATOR)

    if report.lz4_header_count:
        print(
            f"\n  LZ4Block headers  : {report.lz4_header_count}"
            f"  (×{len(LZ4_BLOCK_MAGIC)} bytes = {lz4_overhead:,} bytes overhead)"
        )

    if report.token_frequency:
        print(f"\n  Most repeated tokens (word length ≥ 4):")
        for token, count in report.token_frequency.items():
            print(f"    '{token}' × {count:,}  (~{len(token) * count:,} raw bytes)")


def _print_compression_comparison(report: ShuffleReport) -> None:
    estimates = report.compression_estimates
    if not estimates:
        return
    original = estimates[0].original_size
    print(f"\n\n{SEPARATOR}")
    print(f"COMPRESSION COMPARISON  (sample: {original:,} bytes)")
    print(SEPARATOR)
    print(f"\n  {'Codec':<28} {'Compressed':>12} {'Ratio':>8}")
    print(f"  {'-' * 52}")
    for est in estimates:
        print(
            f"  {est.codec_name:<28} {est.compressed_size:>12,}   {est.ratio_percent:>6.1f}%"
        )
    if not _LZ4_AVAILABLE:
        print(f"\n  Note: install lz4 (pip install lz4) to include LZ4 in comparison.")


def _print_recommendations(report: ShuffleReport) -> None:
    total = report.total_partition_count
    non_empty = len(report.partitions)
    empty = total - non_empty
    empty_pct = (empty / total * 100) if total else 0.0
    file_mb = report.file_size_bytes / 1024 / 1024

    print(f"\n\n{SEPARATOR}")
    print("RECOMMENDATIONS")
    print(SEPARATOR)

    if empty_pct > 20:
        print(
            f"\n  CRITICAL: {empty}/{total} partitions are empty ({empty_pct:.0f}% waste)."
        )
        print("  Enable AQE + lower spark.sql.shuffle.partitions to eliminate this.\n")

    fixes = _build_fix_list(total, non_empty, empty_pct)
    print(f"  {'#':<3} {'Priority':<8} {'Config / Action':<52} Notes")
    print(f"  {'-' * 112}")
    for num, priority, action, note in fixes:
        print(f"  {num:<3} {priority:<8} {action:<52} {note}")

    print(f"\n\n  Recommended Spark config block:")
    print(f"  {'─' * 60}")
    print("""
  spark.conf.set("spark.sql.shuffle.partitions",                  "20")
  spark.conf.set("spark.sql.adaptive.enabled",                    "true")
  spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
  spark.conf.set("spark.io.compression.codec",                    "zstd")
  spark.conf.set("spark.shuffle.spill.compress",                  "true")
""")

    print(f"  Impact estimate:")
    print(f"  {'─' * 60}")
    size_label = (
        f"{file_mb:.2f} MB"
        if file_mb >= 1
        else f"{report.file_size_bytes / 1024:.2f} KB"
    )
    after_part_mb = (file_mb * non_empty / total) if total else file_mb
    after_zstd_mb = after_part_mb * 0.45
    print(f"  Current size             : {size_label}")
    _print_size_line("After partition fix      :", after_part_mb)
    _print_size_line("After ZSTD + partition   :", after_zstd_mb)
    print(f"  S3 spill                 : eliminated (if data fits in executor memory)")


def _print_size_line(label: str, size_mb: float) -> None:
    if size_mb >= 1:
        print(f"  {label} ~{size_mb:.2f} MB")
    else:
        print(f"  {label} ~{size_mb * 1024:.2f} KB")


def _build_fix_list(total_parts: int, non_empty: int, empty_pct: float) -> list:
    return [
        (
            "1",
            "HIGH",
            "spark.sql.shuffle.partitions = 20",
            f"Reduce from {total_parts} → 20; tune to actual dataset size",
        ),
        (
            "2",
            "HIGH",
            "spark.sql.adaptive.enabled = true",
            "AQE auto-coalesces empty/tiny partitions at runtime",
        ),
        (
            "3",
            "HIGH",
            "spark.sql.adaptive.coalescePartitions = true",
            "Merges tiny partitions; eliminates empty slots",
        ),
        (
            "4",
            "MEDIUM",
            "spark.io.compression.codec = zstd",
            "ZSTD: better ratio + speed than LZ4 on mixed data",
        ),
        (
            "5",
            "MEDIUM",
            "spark.shuffle.spill.compress = true",
            "Ensure compression applies to sort-based shuffle writer",
        ),
        (
            "6",
            "MEDIUM",
            "df.coalesce(N) before wide transforms",
            "Fallback if runtime does not support AQE",
        ),
        (
            "7",
            "MEDIUM",
            "Push filters earlier in DAG",
            "Reduce rows before shuffle stage",
        ),
        (
            "8",
            "LOW",
            "Increase executor memory",
            "Higher spill threshold reduces S3 write frequency",
        ),
        (
            "9",
            "LOW",
            "Confirm Spark 3.x (required for AQE)",
            "Spark 2.x does not support Adaptive Query Execution",
        ),
    ]


# ---------------------------------------------------------------------------
# JSON output
# ---------------------------------------------------------------------------


def build_json_report(report: ShuffleReport) -> str:
    data = {
        "version": __version__,
        "data_file": str(report.data_path),
        "index_file": str(report.index_path) if report.index_path else None,
        "file_size_bytes": report.file_size_bytes,
        "bytes_read": report.bytes_read,
        "detected_format": report.detected_format,
        "lz4_available": _LZ4_AVAILABLE,
        "printable_string_count": report.printable_string_count,
        "partitions": {
            "total": report.total_partition_count,
            "non_empty": len(report.partitions),
            "empty": report.total_partition_count - len(report.partitions),
            "detail": [
                {
                    "id": p.partition_id,
                    "offset": p.offset,
                    "size": p.size,
                    "preview": p.content_preview,
                }
                for p in report.partitions
            ],
        },
        "top_tokens": report.token_frequency,
        "compression_estimates": [
            {
                "codec": e.codec_name,
                "original_bytes": e.original_size,
                "compressed_bytes": e.compressed_size,
                "ratio_percent": round(e.ratio_percent, 2),
            }
            for e in report.compression_estimates
        ],
        "lz4_header_count": report.lz4_header_count,
    }
    return json.dumps(data, indent=2)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def analyze(data_path: Path, max_bytes: Optional[int] = None) -> ShuffleReport:
    raw, total_size = load_data_file(data_path, max_bytes)
    index_path = resolve_index_path(data_path)

    detected_format = detect_format(raw)

    if index_path:
        try:
            offsets = parse_index_offsets(index_path)
            partitions = build_partitions(offsets, raw)
            total_partition_count = len(offsets) - 1
        except ValueError as error:
            # Index is corrupt or unreadable — fall back to single-partition mode
            sys.stderr.write(
                f"Warning: could not parse index file ({error}); "
                "treating file as single partition.\n"
            )
            partitions = build_single_partition(raw)
            total_partition_count = 1
            index_path = None
    else:
        partitions = build_single_partition(raw)
        total_partition_count = 1

    strings = extract_printable_strings(raw)
    token_frequency = compute_token_frequency(strings)
    compression_estimates = estimate_compression(raw)
    lz4_header_count = raw.count(LZ4_BLOCK_MAGIC)

    return ShuffleReport(
        data_path=data_path,
        index_path=index_path,
        file_size_bytes=total_size,
        bytes_read=len(raw),
        detected_format=detected_format,
        partitions=partitions,
        total_partition_count=total_partition_count,
        printable_string_count=len(strings),
        token_frequency=token_frequency,
        compression_estimates=compression_estimates,
        lz4_header_count=lz4_header_count,
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    parser = build_argument_parser()
    args = parser.parse_args()

    data_path = Path(args.data_file)

    if not data_path.exists():
        parser.error(f"File not found: {data_path}")

    if not data_path.is_file():
        parser.error(f"Path is not a file: {data_path}")

    if data_path.suffix != ".data":
        sys.stderr.write(
            f"Warning: expected a .data file, got '{data_path.suffix}' — proceeding anyway.\n"
        )

    if args.verbose:
        sys.stderr.write(f"Analyzing: {data_path.resolve()}\n")
        if args.max_bytes:
            sys.stderr.write(f"  --max-bytes: {args.max_bytes:,}\n")

    try:
        report = analyze(data_path, max_bytes=args.max_bytes)
    except ValueError as error:
        sys.stderr.write(f"Error: {error}\n")
        sys.exit(1)

    if args.json:
        print(build_json_report(report))
    else:
        print_report(report, show_recommendations=not args.no_recommendations)


if __name__ == "__main__":
    main()
