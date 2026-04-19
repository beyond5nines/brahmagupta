"""
Tests for analyze_shuffle.py.

Covers: format detection, index parsing, partition building,
string extraction, token frequency, compression estimates,
end-to-end analysis, error paths, and JSON output.
"""

import json
import struct
import pytest
from pathlib import Path

from analyze_shuffle import (
    LZ4_BLOCK_MAGIC,
    SNAPPY_MAGIC,
    ZSTD_MAGIC,
    CompressionEstimate,
    Partition,
    analyze,
    build_json_report,
    build_partitions,
    build_single_partition,
    compute_token_frequency,
    detect_format,
    estimate_compression,
    extract_printable_strings,
    load_data_file,
    parse_index_offsets,
    resolve_index_path,
)


# ---------------------------------------------------------------------------
# detect_format
# ---------------------------------------------------------------------------


def test_detect_format_lz4(lz4_shuffle):
    raw = lz4_shuffle.read_bytes()
    assert detect_format(raw) == "LZ4Block"


def test_detect_format_snappy(snappy_shuffle):
    raw = snappy_shuffle.read_bytes()
    assert detect_format(raw) == "Snappy"


def test_detect_format_zstd(zstd_shuffle):
    raw = zstd_shuffle.read_bytes()
    assert detect_format(raw) == "ZSTD"


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


def test_load_data_file_max_bytes(raw_shuffle):
    content, total = load_data_file(raw_shuffle, max_bytes=50)
    assert len(content) == 50
    assert total == raw_shuffle.stat().st_size


def test_load_data_file_empty_raises(empty_file):
    with pytest.raises(ValueError, match="empty"):
        load_data_file(empty_file)


def test_load_data_file_max_bytes_larger_than_file(raw_shuffle):
    total_size = raw_shuffle.stat().st_size
    content, _ = load_data_file(raw_shuffle, max_bytes=total_size * 10)
    assert len(content) == total_size


# ---------------------------------------------------------------------------
# resolve_index_path
# ---------------------------------------------------------------------------


def test_resolve_index_path_found(raw_shuffle):
    index = resolve_index_path(raw_shuffle)
    assert index is not None
    assert index.suffix == ".index"
    assert index.exists()


def test_resolve_index_path_missing(no_index_shuffle):
    assert resolve_index_path(no_index_shuffle) is None


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


def test_build_single_partition(no_index_shuffle):
    raw = no_index_shuffle.read_bytes()
    partitions = build_single_partition(raw)
    assert len(partitions) == 1
    assert partitions[0].offset == 0
    assert partitions[0].size == len(raw)


# ---------------------------------------------------------------------------
# extract_printable_strings
# ---------------------------------------------------------------------------


def test_extract_printable_strings_finds_known_tokens():
    raw = b"Hello World this is a test " * 5
    strings = extract_printable_strings(raw)
    joined = " ".join(strings)
    assert "Hello" in joined
    assert "World" in joined


def test_extract_printable_strings_empty():
    assert extract_printable_strings(b"") == []


def test_extract_printable_strings_binary_noise():
    raw = b"\x00\x01\x02\x03\x04"
    assert extract_printable_strings(raw) == []


# ---------------------------------------------------------------------------
# compute_token_frequency
# ---------------------------------------------------------------------------


def test_compute_token_frequency_counts():
    strings = ["Virtual Active Virtual Session Virtual"]
    freq = compute_token_frequency(strings)
    assert freq["Virtual"] == 3
    assert freq["Active"] == 1
    assert freq["Session"] == 1


def test_compute_token_frequency_top_n():
    strings = [f"token{i} " * (20 - i) for i in range(25)]
    freq = compute_token_frequency(strings, top_n=5)
    assert len(freq) <= 5


def test_compute_token_frequency_empty():
    assert compute_token_frequency([]) == {}


# ---------------------------------------------------------------------------
# estimate_compression
# ---------------------------------------------------------------------------


def test_estimate_compression_returns_results():
    sample = b"The quick brown fox jumps over the lazy dog. " * 1000
    estimates = estimate_compression(sample)
    assert len(estimates) >= 2   # gzip + zlib always present


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
    assert report.printable_string_count > 0


def test_analyze_lz4_shuffle(lz4_shuffle):
    report = analyze(lz4_shuffle)
    assert report.detected_format == "LZ4Block"
    assert report.lz4_header_count > 0


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


def test_analyze_max_bytes(raw_shuffle):
    full_report = analyze(raw_shuffle)
    partial_report = analyze(raw_shuffle, max_bytes=50)
    assert partial_report.bytes_read == 50
    assert partial_report.file_size_bytes == full_report.file_size_bytes


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
    assert isinstance(data["top_tokens"], dict)


def test_build_json_report_schema_keys(raw_shuffle):
    report = analyze(raw_shuffle)
    data = json.loads(build_json_report(report))
    required_keys = {
        "version", "data_file", "index_file", "file_size_bytes",
        "bytes_read", "detected_format", "lz4_available",
        "printable_string_count", "partitions", "top_tokens",
        "compression_estimates", "lz4_header_count",
    }
    assert required_keys.issubset(data.keys())


def test_build_json_report_partition_detail_keys(raw_shuffle):
    report = analyze(raw_shuffle)
    data = json.loads(build_json_report(report))
    for partition in data["partitions"]["detail"]:
        assert {"id", "offset", "size", "preview"}.issubset(partition.keys())


def test_build_json_report_compression_estimate_keys(raw_shuffle):
    report = analyze(raw_shuffle)
    data = json.loads(build_json_report(report))
    for est in data["compression_estimates"]:
        assert {"codec", "original_bytes", "compressed_bytes", "ratio_percent"}.issubset(
            est.keys()
        )
