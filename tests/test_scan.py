"""
Tests for scan.py.

Covers: format detection, index parsing, partition building,
compression estimates, end-to-end analysis, error paths, and JSON output.
"""

import json
import struct
import pytest
from pathlib import Path

from scan import (
    LZ4_BLOCK_MAGIC,
    CompressionEstimate,
    Partition,
    _suggest_partitions,
    analyze,
    build_json_report,
    build_partitions,
    decompress_lz4_blocks,
    detect_format,
    estimate_compression,
    load_data_file,
    parse_index_offsets,
)

LZ4_STREAM_HEADER_SIZE = 16  # 8-byte magic + 4-byte token + 4-byte max-block-size

try:
    import lz4.block as _lz4_block
    _LZ4_AVAILABLE = True
except ImportError:
    _LZ4_AVAILABLE = False

lz4_required = pytest.mark.skipif(
    not _LZ4_AVAILABLE, reason="lz4 package not installed"
)


# ---------------------------------------------------------------------------
# detect_format
# ---------------------------------------------------------------------------


def test_detect_format_lz4(lz4_shuffle):
    raw = lz4_shuffle.read_bytes()
    assert detect_format(raw) == "LZ4Block"


def test_detect_format_raw():
    assert detect_format(b"random bytes that match nothing") == "Unknown/Raw"


def test_detect_format_empty():
    assert detect_format(b"") == "Unknown/Raw"


# ---------------------------------------------------------------------------
# load_data_file
# ---------------------------------------------------------------------------


def test_load_data_file_full(raw_shuffle):
    content, total = load_data_file(raw_shuffle)
    assert total == raw_shuffle.stat().st_size
    assert len(content) == total


def test_load_data_file_empty_raises(empty_file):
    with pytest.raises(ValueError, match="empty"):
        load_data_file(empty_file)


# ---------------------------------------------------------------------------
# resolve_index_path
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# parse_index_offsets
# ---------------------------------------------------------------------------


def test_parse_index_offsets_valid(tmp_path):
    offsets = [0, 100, 200, 300]
    index_file = tmp_path / "shuffle_0_0_0.index"
    index_file.write_bytes(struct.pack(">4q", *offsets))
    result = parse_index_offsets(index_file)
    assert result == offsets


def test_parse_index_offsets_empty_raises(tmp_path):
    index_file = tmp_path / "empty.index"
    index_file.write_bytes(b"")
    with pytest.raises(ValueError, match="empty"):
        parse_index_offsets(index_file)


def test_parse_index_offsets_corrupt_raises(corrupt_index_shuffle):
    index_file = corrupt_index_shuffle.with_suffix(".index")
    with pytest.raises(ValueError, match="multiple of 8"):
        parse_index_offsets(index_file)


# ---------------------------------------------------------------------------
# build_partitions
# ---------------------------------------------------------------------------


def test_build_partitions_counts(raw_shuffle):
    raw = raw_shuffle.read_bytes()
    size = len(raw)
    third = size // 3
    offsets = [0, third, third * 2, size]
    partitions = build_partitions(offsets, raw)
    assert len(partitions) == 3


def test_build_partitions_sizes(raw_shuffle):
    raw = raw_shuffle.read_bytes()
    size = len(raw)
    third = size // 3
    offsets = [0, third, third * 2, size]
    partitions = build_partitions(offsets, raw)
    for p in partitions:
        assert p.size > 0


def test_build_partitions_empty_slots_excluded():
    """Empty partitions (size == 0) must not appear in the result."""
    raw = b"x" * 300
    offsets = [0, 100, 100, 200, 300]   # partition 1 is empty (100→100)
    partitions = build_partitions(offsets, raw)
    non_empty = [p for p in partitions if p.size > 0]
    assert len(non_empty) == len(partitions)


# ---------------------------------------------------------------------------
# estimate_compression
# ---------------------------------------------------------------------------


def test_estimate_compression_returns_results():
    sample = b"The quick brown fox jumps over the lazy dog. " * 1000
    estimates = estimate_compression(sample)
    assert len(estimates) >= 1   # zlib always present (lz4 optional)
    assert any("Zlib" in e.codec_name for e in estimates)


def test_estimate_compression_ratios_valid():
    sample = b"AAAA" * 50_000   # highly compressible
    estimates = estimate_compression(sample)
    for est in estimates:
        assert 0 < est.ratio_percent <= 100
        assert est.compressed_size <= est.original_size


def test_estimate_compression_empty():
    assert estimate_compression(b"") == []


# ---------------------------------------------------------------------------
# analyze — integration tests
# ---------------------------------------------------------------------------


def test_analyze_raw_shuffle(raw_shuffle):
    report = analyze(raw_shuffle)
    assert report.data_path == raw_shuffle
    assert report.file_size_bytes > 0
    assert report.detected_format == "Unknown/Raw"
    assert len(report.partitions) == 3
    assert report.total_partition_count == 3


def test_analyze_lz4_shuffle(lz4_shuffle):
    report = analyze(lz4_shuffle)
    assert report.detected_format == "LZ4Block"


def test_analyze_no_index(no_index_shuffle):
    report = analyze(no_index_shuffle)
    assert report.index_path is None
    assert report.total_partition_count == 1
    assert len(report.partitions) == 1


def test_analyze_corrupt_index_falls_back(corrupt_index_shuffle):
    """Corrupt index should not raise — tool falls back to single-partition mode."""
    report = analyze(corrupt_index_shuffle)
    assert report.index_path is None
    assert report.total_partition_count == 1


def test_analyze_empty_file_raises(empty_file):
    with pytest.raises(ValueError, match="empty"):
        analyze(empty_file)


# ---------------------------------------------------------------------------
# JSON output
# ---------------------------------------------------------------------------


def test_build_json_report_valid_json(raw_shuffle):
    report = analyze(raw_shuffle)
    output = build_json_report(report)
    data = json.loads(output)
    assert data["detected_format"] == "Unknown/Raw"
    assert data["partitions"]["total"] == 3
    assert data["partitions"]["non_empty"] == 3
    assert data["partitions"]["empty"] == 0
    assert isinstance(data["compression_estimates"], list)


def test_build_json_report_schema_keys(raw_shuffle):
    report = analyze(raw_shuffle)
    data = json.loads(build_json_report(report))
    required_keys = {
        "version", "data_file", "index_file", "file_size_bytes",
        "detected_format", "lz4_available", "partitions",
        "compression_estimates", "decompressed_for_comparison",
        "recommended_shuffle_partitions",
    }
    assert required_keys.issubset(data.keys())


def test_build_json_report_partition_detail_keys(raw_shuffle):
    report = analyze(raw_shuffle)
    data = json.loads(build_json_report(report))
    for partition in data["partitions"]["detail"]:
        assert {"id", "offset", "size"} == set(partition.keys())


def test_build_json_report_compression_estimate_keys(raw_shuffle):
    report = analyze(raw_shuffle)
    data = json.loads(build_json_report(report))
    for est in data["compression_estimates"]:
        assert {"codec", "original_bytes", "compressed_bytes", "ratio_percent"}.issubset(
            est.keys()
        )


# ---------------------------------------------------------------------------
# recommended_shuffle_partitions
# ---------------------------------------------------------------------------


def test_suggest_partitions():
    # post shows 39 non-empty → recommends 40
    assert _suggest_partitions(39) == 40
    assert _suggest_partitions(0) == 10        # floor
    assert _suggest_partitions(72) == 80       # scales up


# ---------------------------------------------------------------------------
# decompress_lz4_blocks
# ---------------------------------------------------------------------------


def _build_lz4_stream(payloads: list[bytes]) -> bytes:
    # 16-byte header, then 4-byte BE length + compressed block per payload
    stream = bytearray(LZ4_BLOCK_MAGIC)
    stream.extend(b"\x00" * (LZ4_STREAM_HEADER_SIZE - len(LZ4_BLOCK_MAGIC)))
    for payload in payloads:
        compressed = _lz4_block.compress(payload, store_size=False)
        stream.extend(struct.pack(">i", len(compressed)))
        stream.extend(compressed)
    return bytes(stream)


@lz4_required
def test_decompress_lz4_blocks_roundtrip():
    """Building an LZ4 stream then decompressing it returns the original payload."""
    original = b"The quick brown fox jumps over the lazy dog. " * 200
    stream = _build_lz4_stream([original])
    decompressed = decompress_lz4_blocks(stream)
    assert decompressed == original


@lz4_required
def test_decompress_lz4_blocks_multiple_blocks():
    """Multi-block streams decompress in order and concatenate."""
    payloads = [b"first-block-" * 50, b"second-block-" * 50, b"third-block-" * 50]
    stream = _build_lz4_stream(payloads)
    decompressed = decompress_lz4_blocks(stream)
    assert decompressed == b"".join(payloads)


def test_decompress_lz4_blocks_missing_magic_returns_empty():
    assert decompress_lz4_blocks(b"not an lz4 stream") == b""


def test_decompress_lz4_blocks_empty_input_returns_empty():
    assert decompress_lz4_blocks(b"") == b""


@lz4_required
def test_decompress_lz4_blocks_truncated_stream_returns_partial():
    """A truncated stream returns whatever decoded before the corruption."""
    payloads = [b"clean-block-" * 40]
    stream = _build_lz4_stream(payloads)
    # Chop off the last 10 bytes — header intact, block data corrupted
    truncated = stream[:-10]
    result = decompress_lz4_blocks(truncated)
    # Partial result is acceptable (possibly b""); must not raise
    assert isinstance(result, bytes)


@lz4_required
def test_decompress_lz4_blocks_raises_on_size_cap():
    """A zip-bomb-shaped stream (many blocks past the cap) must raise."""
    payload = b"A" * 60_000
    stream = _build_lz4_stream([payload, payload, payload])
    # Set cap below combined payload size — must raise rather than OOM
    with pytest.raises(ValueError, match="cap"):
        decompress_lz4_blocks(stream, max_total_bytes=60_000)


# ---------------------------------------------------------------------------
# Recommendation text — config keys must be real Spark keys
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Index validation — structural invariants
# ---------------------------------------------------------------------------


def test_parse_index_rejects_nonzero_first_offset(tmp_path):
    index_file = tmp_path / "bad_first.index"
    index_file.write_bytes(struct.pack(">3q", 100, 200, 300))
    with pytest.raises(ValueError, match="must start at 0"):
        parse_index_offsets(index_file)


def test_parse_index_rejects_decreasing_offsets(tmp_path):
    index_file = tmp_path / "bad_order.index"
    index_file.write_bytes(struct.pack(">4q", 0, 200, 100, 300))
    with pytest.raises(ValueError, match="not monotonic"):
        parse_index_offsets(index_file)


def test_parse_index_rejects_negative_offsets(tmp_path):
    index_file = tmp_path / "bad_negative.index"
    index_file.write_bytes(struct.pack(">3q", 0, 100, -50))
    with pytest.raises(ValueError, match="not monotonic|negative"):
        parse_index_offsets(index_file)


def test_build_partitions_rejects_offset_beyond_file():
    raw = b"x" * 100
    offsets = [0, 50, 200]
    with pytest.raises(ValueError, match="out of sync"):
        build_partitions(offsets, raw)


# ---------------------------------------------------------------------------
# Verbose LZ4 diagnostics
# ---------------------------------------------------------------------------


@lz4_required
def test_decompress_lz4_blocks_warns_on_truncation(capsys):
    # truncated stream → stderr warning, partial bytes returned
    stream = _build_lz4_stream([b"clean-block-" * 40])[:-5]
    decompress_lz4_blocks(stream)
    assert "warning" in capsys.readouterr().err


@lz4_required
def test_decompress_lz4_blocks_clean_stream_silent(capsys):
    decompress_lz4_blocks(_build_lz4_stream([b"A" * 5000]))
    assert capsys.readouterr().err == ""


def test_recommendations_use_correct_spark_config_keys(raw_shuffle, capsys):
    # guard against regression of the .enabled suffix
    from scan import print_report
    print_report(analyze(raw_shuffle))
    out = capsys.readouterr().out
    assert "spark.sql.adaptive.coalescePartitions.enabled" in out
    bare = [
        line for line in out.splitlines()
        if "spark.sql.adaptive.coalescePartitions" in line
        and "spark.sql.adaptive.coalescePartitions.enabled" not in line
    ]
    assert not bare, f"bare key without .enabled: {bare}"


# ---------------------------------------------------------------------------
# analyze — LZ4 decompression integration
# ---------------------------------------------------------------------------


@lz4_required
def test_analyze_lz4_shuffle_real_payload_measures_decompressed(tmp_path):
    """
    A real LZ4Block stream wrapping compressible text should cause the
    compression comparison to run on the decompressed payload, producing
    meaningful (<<100%) ratios for gzip/zlib.
    """
    original = b"AAAA " * 10_000  # highly compressible
    stream = _build_lz4_stream([original])

    data_file = tmp_path / "shuffle_real_lz4.data"
    data_file.write_bytes(stream)

    report = analyze(data_file)
    assert report.detected_format == "LZ4Block"
    assert report.decompressed_for_comparison is True
    # Ratios should reflect the decompressed payload, not compressed bytes
    zlib_est = next(e for e in report.compression_estimates if "Zlib" in e.codec_name)
    assert zlib_est.ratio_percent < 50, (
        "zlib should compress decompressed 'AAAA' payload to well under 50%, "
        f"got {zlib_est.ratio_percent:.1f}%"
    )


def test_analyze_lz4_shuffle_fixture_falls_back_to_raw(lz4_shuffle):
    """
    The existing fixture writes LZ4 magic followed by non-LZ4 bytes.
    Decompression should fail silently and decompressed_for_comparison=False.
    """
    report = analyze(lz4_shuffle)
    assert report.detected_format == "LZ4Block"
    assert report.decompressed_for_comparison is False


def test_build_json_report_includes_new_fields(raw_shuffle):
    data = json.loads(build_json_report(analyze(raw_shuffle)))
    assert "decompressed_for_comparison" in data
    assert "recommended_shuffle_partitions" in data
    assert isinstance(data["recommended_shuffle_partitions"], int)
