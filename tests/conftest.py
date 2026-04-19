"""
Pytest fixtures that generate synthetic Spark shuffle file pairs.

All fixtures use tmp_path so no binary files need to be committed to the repo.
"""

import struct
import pytest


def _write_index(path, offsets):
    """Write a list of int64 big-endian offsets to a .index file."""
    path.write_bytes(struct.pack(f">{len(offsets)}q", *offsets))


@pytest.fixture
def raw_shuffle(tmp_path):
    """
    Minimal raw (uncompressed) shuffle pair — 3 non-empty partitions.

    Data is human-readable ASCII so printable-string and token-frequency
    tests can make deterministic assertions.
    """
    chunk = b"Virtual Active Session Connection Interface " * 30
    data = chunk * 3                       # ~3 equal segments
    third = len(data) // 3
    offsets = [0, third, third * 2, len(data)]

    data_file = tmp_path / "shuffle_0_0_0.data"
    data_file.write_bytes(data)
    _write_index(tmp_path / "shuffle_0_0_0.index", offsets)

    return data_file


@pytest.fixture
def lz4_shuffle(tmp_path):
    """
    Shuffle file whose first 8 bytes are the LZ4Block magic.
    Used to verify format detection; content is not real LZ4.
    """
    data = b"LZ4Block" * 10 + b"padding bytes " * 100
    data_file = tmp_path / "shuffle_1_0_0.data"
    data_file.write_bytes(data)
    return data_file


@pytest.fixture
def snappy_shuffle(tmp_path):
    """Shuffle file starting with Snappy magic bytes."""
    data = b"\xff\x06\x00\x00sNaPpY" + b"x" * 200
    data_file = tmp_path / "shuffle_2_0_0.data"
    data_file.write_bytes(data)
    return data_file


@pytest.fixture
def zstd_shuffle(tmp_path):
    """Shuffle file starting with ZSTD magic bytes."""
    data = b"\x28\xb5\x2f\xfd" + b"z" * 200
    data_file = tmp_path / "shuffle_3_0_0.data"
    data_file.write_bytes(data)
    return data_file


@pytest.fixture
def no_index_shuffle(tmp_path):
    """Shuffle .data file with no companion .index file."""
    data = b"standalone data " * 50
    data_file = tmp_path / "shuffle_4_0_0.data"
    data_file.write_bytes(data)
    return data_file


@pytest.fixture
def corrupt_index_shuffle(tmp_path):
    """Shuffle pair where the .index file has a non-multiple-of-8 byte count."""
    data = b"some data " * 100
    data_file = tmp_path / "shuffle_5_0_0.data"
    data_file.write_bytes(data)
    index_file = tmp_path / "shuffle_5_0_0.index"
    index_file.write_bytes(b"\x00" * 7)   # 7 bytes — not a multiple of 8
    return data_file


@pytest.fixture
def empty_file(tmp_path):
    """Zero-byte .data file for error-path testing."""
    f = tmp_path / "empty.data"
    f.write_bytes(b"")
    return f
