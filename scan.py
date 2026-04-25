#!/usr/bin/env python3
"""scan.py — analyze AWS Glue shuffle files.

Reads a shuffle .data file (plus optional .index) and reports on
partition layout, compression, and structural tuning opportunities.

Only AWS Glue's LZ4Block framing is decompressed. Other formats
are reported as Unknown/Raw.

See README.md for details.
"""

__version__ = "0.1.0"

import argparse
import json
import struct
import sys
import zlib
from dataclasses import dataclass
from pathlib import Path

try:
    import lz4.block as _lz4_block
    _LZ4_AVAILABLE = True
except ImportError:
    _LZ4_AVAILABLE = False


LZ4_BLOCK_MAGIC = b"LZ4Block"
COMPRESSION_SAMPLE_SIZE = 200_000  # cap sample to keep estimates fast

MAX_FILE_SIZE_BYTES = 2 * 1024 * 1024 * 1024  # 2 GB
# 4 GB cap on decompressed output — defends against LZ4 zip bombs.
# A tiny file can legitimately decompress to TBs otherwise.
MAX_DECOMPRESSED_BYTES = 4 * 1024 * 1024 * 1024

SEPARATOR = "=" * 68


@dataclass
class Partition:
    partition_id: int
    offset: int
    size: int


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
    index_path: Path | None
    file_size_bytes: int
    detected_format: str
    partitions: list[Partition]
    total_partition_count: int
    compression_estimates: list[CompressionEstimate]
    decompressed_for_comparison: bool = False


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="scan",
        description=(
            "Inspect AWS Glue shuffle .data files. "
            "Detects compression, maps partitions, measures codec ratios, "
            "and recommends structural Spark tuning."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("data_file", metavar="SHUFFLE_DATA_FILE",
                        help="shuffle .data file to analyze")
    parser.add_argument("--json", action="store_true",
                        help="emit JSON instead of human-readable output")
    parser.add_argument("--version", action="version",
                        version=f"%(prog)s {__version__}")
    return parser


def load_data_file(data_path: Path) -> tuple[bytes, int]:
    """Read the full shuffle .data file into memory.

    Partial reads aren't supported: when a .index file is present it
    references absolute byte offsets in the full file, so truncation
    would silently corrupt the partition map.
    """
    total_size = data_path.stat().st_size
    if total_size == 0:
        raise ValueError(f"empty file: {data_path}")
    if total_size > MAX_FILE_SIZE_BYTES:
        raise ValueError(
            f"file size {total_size:,} exceeds {MAX_FILE_SIZE_BYTES:,} byte cap"
        )
    return data_path.read_bytes(), total_size


def detect_format(raw: bytes) -> str:
    if raw[:8] == LZ4_BLOCK_MAGIC:
        return "LZ4Block"
    return "Unknown/Raw"


def parse_index_offsets(index_path: Path) -> list[int]:
    """Parse shuffle .index into a list of big-endian int64 offsets.

    Validates that the first offset is 0 and offsets are non-decreasing.
    Raises ValueError on empty file, wrong byte count, or broken invariants.
    """
    raw = index_path.read_bytes()
    if not raw:
        raise ValueError(f"empty index: {index_path}")
    if len(raw) % 8:
        raise ValueError(f"bad index size {len(raw)} (not multiple of 8): {index_path}")

    offsets = list(struct.unpack(f">{len(raw) // 8}q", raw))

    if offsets[0] != 0:
        raise ValueError(f"index must start at 0, got {offsets[0]}: {index_path}")
    for i in range(len(offsets) - 1):
        if offsets[i + 1] < offsets[i]:
            raise ValueError(
                f"offsets not monotonic at index {i+1}: "
                f"{offsets[i+1]:,} < {offsets[i]:,}"
            )

    return offsets


def build_partitions(offsets: list[int], raw: bytes) -> list[Partition]:
    if offsets and offsets[-1] > len(raw):
        raise ValueError(
            f"index references byte {offsets[-1]:,} but data file is "
            f"{len(raw):,} bytes (index/data out of sync)"
        )
    partitions = []
    for i in range(len(offsets) - 1):
        size = offsets[i + 1] - offsets[i]
        if size < 0:
            raise ValueError(f"partition {i} has negative size ({size})")
        if size == 0:
            continue
        partitions.append(Partition(i, offsets[i], size))
    return partitions


def decompress_lz4_blocks(
    raw: bytes, max_total_bytes: int = MAX_DECOMPRESSED_BYTES
) -> bytes:
    """Decompress an AWS Glue LZ4Block-framed shuffle payload.

    Returns b"" if lz4 is unavailable or the magic is missing.
    Writes a stderr warning on mid-stream failure and returns whatever
    decoded so far. Raises ValueError if cumulative output exceeds
    `max_total_bytes` — defense against LZ4 zip bombs.
    """
    if not _LZ4_AVAILABLE or not raw.startswith(LZ4_BLOCK_MAGIC):
        return b""

    out = bytearray()
    pos = 16  # 8-byte magic + 4-byte token + 4-byte max-block-size
    stopped = False

    # Spark default LZ4 block size is 64 KB but is configurable
    # (spark.io.compression.lz4.blockSize). Try increasing caps on failure
    # so a tuned job's shuffle still decodes.
    block_caps = (65536, 131072, 262144, 1048576)

    while pos + 4 <= len(raw):
        (sz,) = struct.unpack_from(">i", raw, pos)
        pos += 4
        if sz <= 0 or pos + sz > len(raw):
            stopped = True
            break
        block = None
        for cap in block_caps:
            try:
                block = _lz4_block.decompress(raw[pos:pos + sz], uncompressed_size=cap)
                break
            except Exception:
                continue
        if block is None:
            sys.stderr.write(
                f"warning: lz4 decode failed at offset {pos} "
                f"(tried block sizes up to {block_caps[-1]:,})\n"
            )
            return bytes(out)
        if len(out) + len(block) > max_total_bytes:
            raise ValueError(
                f"decompressed size exceeds {max_total_bytes:,} byte cap (zip bomb?)"
            )
        out.extend(block)
        pos += sz

    if stopped and pos < len(raw):
        sys.stderr.write(f"warning: lz4 stream truncated at offset {pos}\n")

    return bytes(out)


def estimate_compression(raw_sample: bytes) -> list[CompressionEstimate]:
    sample = raw_sample[:COMPRESSION_SAMPLE_SIZE]
    if not sample:
        return []

    estimates = [
        CompressionEstimate(
            "Zlib level-9 (DEFLATE)", len(zlib.compress(sample, level=9)), len(sample)
        )
    ]

    if _LZ4_AVAILABLE:
        try:
            lz4_size = len(_lz4_block.compress(sample))
            estimates.append(CompressionEstimate("LZ4 block (measured)", lz4_size, len(sample)))
        except Exception as e:
            sys.stderr.write(f"warning: LZ4 compression failed: {e}\n")

    return estimates


def _suggest_partitions(non_empty: int) -> int:
    # round non_empty up to nearest 10, floor 10
    return max(10, ((max(non_empty, 0) + 9) // 10) * 10)


def print_report(report: ShuffleReport) -> None:
    _print_file_summary(report)
    _print_partition_map(report)
    _print_compression_comparison(report)
    _print_recommendations(report)


def _print_file_summary(report: ShuffleReport) -> None:
    total = report.total_partition_count
    non_empty = len(report.partitions)
    empty = total - non_empty
    empty_pct = (empty / total * 100) if total else 0.0

    print(SEPARATOR)
    print(f"SHUFFLE FILE ANALYSIS: {report.data_path.name}")
    print(SEPARATOR)
    print(
        f"\n  .data   : {report.file_size_bytes:>14,} bytes"
        f"  ({report.file_size_bytes / 1024:.2f} KB)"
    )
    if report.index_path:
        idx = report.index_path.stat().st_size
        print(f"  .index  : {idx:>14,} bytes  ({idx / 1024:.2f} KB)")
    print(f"  Format  : {report.detected_format}")
    if not _LZ4_AVAILABLE and report.detected_format == "LZ4Block":
        print("  WARNING : lz4 package not installed — pip install lz4")
    print(f"\n  Total partitions  : {total}")
    print(f"  Non-empty         : {non_empty}")
    print(f"  Empty             : {empty}  ({empty_pct:.0f}% wasted slots)")


def _print_partition_map(report: ShuffleReport) -> None:
    if not report.index_path:
        return
    print(f"\n\n  {'Part':<6} {'Offset':>12} {'Size (B)':>12}")
    print(f"  {'-' * 36}")
    for p in report.partitions:
        print(f"  {p.partition_id:<6} {p.offset:>12,} {p.size:>12,}")

    sizes = [p.size for p in report.partitions]
    if sizes:
        print(f"\n  Min size : {min(sizes):,} bytes")
        print(f"  Max size : {max(sizes):,} bytes")
        print(f"  Avg size : {sum(sizes) / len(sizes):,.0f} bytes")


def _print_compression_comparison(report: ShuffleReport) -> None:
    estimates = report.compression_estimates
    if not estimates:
        return
    original = estimates[0].original_size
    source = "decompressed payload" if report.decompressed_for_comparison else "raw file"

    print(f"\n\n{SEPARATOR}")
    print(f"COMPRESSION COMPARISON  (sample: {original:,} bytes from {source})")
    print(SEPARATOR)

    if report.detected_format == "LZ4Block" and not report.decompressed_for_comparison:
        print("\n  Note: ratios below are on raw compressed bytes — "
              "decompression failed or lz4 unavailable.")

    print(f"\n  {'Codec':<28} {'Compressed':>12} {'Ratio':>8}")
    print(f"  {'-' * 52}")
    for est in estimates:
        print(
            f"  {est.codec_name:<28} {est.compressed_size:>12,}   "
            f"{est.ratio_percent:>6.1f}%"
        )


def _print_recommendations(report: ShuffleReport) -> None:
    total = report.total_partition_count
    non_empty = len(report.partitions)
    empty = total - non_empty
    parts = _suggest_partitions(non_empty)

    print(f"\n\n{SEPARATOR}")
    print("RECOMMENDATIONS")
    print(SEPARATOR)

    if parts < total:
        print(f"""
  # Reduce from {total} → {parts} (non-empty = {non_empty}, rounded to nearest 10)
  spark.conf.set("spark.sql.shuffle.partitions",                  "{parts}")

  # AQE auto-coalesces empty/tiny partitions at runtime
  spark.conf.set("spark.sql.adaptive.enabled",                    "true")
  spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
""")
        print(
            f"  Eliminates {empty} empty partition slot(s) — reduces shuffle "
            f"metadata and merge passes; file-size change is negligible.\n"
        )
    else:
        print(
            f"\n  Partitions look balanced ({non_empty}/{total} non-empty) — "
            f"no reduction suggested.\n"
        )

    # Codec hint applies regardless of partition layout
    print(
        '  # Verify against the compression comparison above before switching\n'
        '  spark.conf.set("spark.io.compression.codec", "zstd")\n'
    )


def build_json_report(report: ShuffleReport) -> str:
    data = {
        "version": __version__,
        "data_file": str(report.data_path),
        "index_file": str(report.index_path) if report.index_path else None,
        "file_size_bytes": report.file_size_bytes,
        "detected_format": report.detected_format,
        "lz4_available": _LZ4_AVAILABLE,
        "partitions": {
            "total": report.total_partition_count,
            "non_empty": len(report.partitions),
            "empty": report.total_partition_count - len(report.partitions),
            "detail": [
                {"id": p.partition_id, "offset": p.offset, "size": p.size}
                for p in report.partitions
            ],
        },
        "compression_estimates": [
            {
                "codec": e.codec_name,
                "original_bytes": e.original_size,
                "compressed_bytes": e.compressed_size,
                "ratio_percent": round(e.ratio_percent, 2),
            }
            for e in report.compression_estimates
        ],
        "decompressed_for_comparison": report.decompressed_for_comparison,
        "recommended_shuffle_partitions": _suggest_partitions(len(report.partitions)),
    }
    return json.dumps(data, indent=2)


def analyze(data_path: Path) -> ShuffleReport:
    """Load, parse, and measure a shuffle file. Returns a ShuffleReport."""
    raw, total_size = load_data_file(data_path)

    index_candidate = data_path.with_suffix(".index")
    index_path: Path | None = index_candidate if index_candidate.exists() else None

    detected = detect_format(raw)

    if index_path:
        try:
            offsets = parse_index_offsets(index_path)
            partitions = build_partitions(offsets, raw)
            total_partition_count = len(offsets) - 1
        except ValueError as error:
            sys.stderr.write(
                f"warning: could not parse index ({error}); "
                "treating file as single partition\n"
            )
            partitions = [Partition(0, 0, len(raw))]
            total_partition_count = 1
            index_path = None
    else:
        partitions = [Partition(0, 0, len(raw))]
        total_partition_count = 1

    # For LZ4Block files, decompress before measuring codec ratios — otherwise
    # we'd be re-compressing already-compressed bytes and every codec would
    # report ~1.0x. Fall back to raw bytes if decompression yields nothing.
    sample = raw
    decompressed_for_comparison = False
    if detected == "LZ4Block":
        decompressed = decompress_lz4_blocks(raw)
        if decompressed:
            sample = decompressed
            decompressed_for_comparison = True

    return ShuffleReport(
        data_path=data_path,
        index_path=index_path,
        file_size_bytes=total_size,
        detected_format=detected,
        partitions=partitions,
        total_partition_count=total_partition_count,
        compression_estimates=estimate_compression(sample),
        decompressed_for_comparison=decompressed_for_comparison,
    )


def main() -> None:
    parser = build_argument_parser()
    args = parser.parse_args()
    data_path = Path(args.data_file)

    if not data_path.exists():
        parser.error(f"file not found: {data_path}")
    if not data_path.is_file():
        parser.error(f"not a file: {data_path}")

    try:
        report = analyze(data_path)
    except ValueError as error:
        sys.stderr.write(f"error: {error}\n")
        sys.exit(1)

    if args.json:
        print(build_json_report(report))
    else:
        print_report(report)


if __name__ == "__main__":
    main()
