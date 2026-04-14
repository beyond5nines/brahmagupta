# glue-busters

A collection of tools for debugging and optimizing AWS Glue and Spark jobs.

## Tools

### analyze_shuffle.py

A command-line tool for inspecting Spark and AWS Glue shuffle `.data` files.

Shuffle files are the intermediate binary files Spark writes to disk (or spills
to S3 in Glue) during wide transformations like `groupBy`, `join`, and
`repartition`. When a job spills unexpectedly or runs slower than expected,
these files are the first place to look — but they are binary and not human
readable without a tool.

`analyze_shuffle.py` tells you:

- What compression wrapper Spark used (LZ4Block, Snappy, ZSTD, or raw)
- How many partitions the file contains and how many are empty
- The byte offset and size of every non-empty partition
- Which tokens repeat most in the raw bytes (redundancy signal)
- How much gzip, zlib, and LZ4 would compress the data compared to what is
  currently on disk
- Concrete Spark config changes to eliminate S3 spill and over-partitioning

No assumptions are made about the content of the data. The tool works on any
Spark shuffle file regardless of what dataset produced it.

---

## Requirements

- Python 3.10 or later
- `lz4` (optional — enables LZ4 measurement in the compression comparison)

All other dependencies (`gzip`, `zlib`, `struct`, `argparse`, etc.) are Python
standard library.

---

## Installation

### Run directly (no install)

```bash
python3 analyze_shuffle.py shuffle_N_M_0.data
```

### Install as a package

```bash
pip install .
```

This installs the `analyze-shuffle` command so you can run it from anywhere:

```bash
analyze-shuffle shuffle_N_M_0.data
```

### Install with LZ4 support

```bash
pip install ".[lz4]"
```

---

## Usage

```
analyze-shuffle SHUFFLE_DATA_FILE [options]
```

The companion `.index` file is auto-detected from the same directory by
substituting `.data` with `.index`. You never need to pass it manually.

### Options

| Flag | Description |
|------|-------------|
| `--max-bytes N` | Read at most N bytes. Use this for very large files to avoid OOM. |
| `--json` | Emit machine-readable JSON instead of human-readable output. |
| `--no-recommendations` | Omit the Spark tuning recommendations section. |
| `--verbose` | Print the resolved file path and read limits to stderr before analysis. |
| `--version` | Show version and exit. |
| `--help` | Show help and exit. |

---

## Examples

```bash
# Basic analysis
python3 analyze_shuffle.py shuffle_98_37726_0.data

# Cap memory use on a large file
python3 analyze_shuffle.py shuffle_53_21062_0.data --max-bytes 500000

# Machine-readable JSON
python3 analyze_shuffle.py shuffle_98_37726_0.data --json

# Save JSON report to file
python3 analyze_shuffle.py shuffle_98_37726_0.data --json > report.json

# Query JSON with jq
python3 analyze_shuffle.py shuffle_98_37726_0.data --json | jq '.partitions.empty'

# Skip recommendations (useful in CI pipelines)
python3 analyze_shuffle.py shuffle_98_37726_0.data --no-recommendations

# Verbose mode with a read limit
python3 analyze_shuffle.py shuffle_53_21062_0.data --verbose --max-bytes 100000
```

---

## Output sections

### File summary
File size, detected compression format, and partition counts. The empty
partition percentage is the primary signal for over-partitioning.

```
====================================================================
SHUFFLE FILE ANALYSIS: shuffle_98_37726_0.data
====================================================================

  .data   :         28,114 bytes  (27.46 KB)
  .index  :            584 bytes  (0.57 KB)
  Format  : LZ4Block

  Total partitions  : 72
  Non-empty         : 39
  Empty             : 33  (46% wasted slots)
```

### Partition map
Per-partition byte offset, size, and a short content preview. Only shown when
a `.index` file is present. Use this to spot skewed partition sizes.

```
  Part         Offset     Size (B)  Preview
  ------------------------------------------------------------------------
  0                 0          502  LZ4Block% | ...
  1               502          505  LZ4Block% | ...
  ...

  Min size : 464 bytes
  Max size : 1,564 bytes
  Avg size : 721 bytes
```

### Content analysis
Count of printable ASCII strings (≥ 5 chars) extracted from the raw bytes.
A high count on a file that claims to be LZ4-compressed means the compression
is not actually reducing the data — the bytes are stored raw inside the wrapper.

### Redundancy analysis
The top 20 most-repeated word tokens across all printable strings, with a raw
byte cost estimate for each. High repetition of the same token is the clearest
signal that a dictionary codec like ZSTD would outperform LZ4 on this data.

```
  Most repeated tokens (word length ≥ 4):
    'Virtual'  × 375  (~2,625 raw bytes)
    'Active'   × 323  (~1,938 raw bytes)
    ...
```

### Compression comparison
Actual compression ratios measured against a 200 KB sample of the file.
Uses `gzip` (level 6), `zlib` (level 9, used as a ZSTD proxy), and `lz4`
if installed. All values are measured — none are estimates.

```
  Codec                        Compressed     Ratio
  ----------------------------------------------------
  Gzip level-6                    142,048    71.0%
  Zlib level-9 (≈ZSTD)            141,182    70.6%
  LZ4 block (measured)            158,001    79.0%
```

### Recommendations
Prioritised list of Spark config changes with a before/after size estimate.
A `CRITICAL` banner is shown when more than 20% of partitions are empty.

---

## JSON output schema

```json
{
  "version": "1.0.0",
  "data_file": "shuffle_98_37726_0.data",
  "index_file": "shuffle_98_37726_0.index",
  "file_size_bytes": 28114,
  "bytes_read": 28114,
  "detected_format": "LZ4Block",
  "lz4_available": false,
  "printable_string_count": 707,
  "partitions": {
    "total": 72,
    "non_empty": 39,
    "empty": 33,
    "detail": [
      { "id": 0, "offset": 0, "size": 502, "preview": "..." }
    ]
  },
  "top_tokens": { "token": 42 },
  "compression_estimates": [
    {
      "codec": "Gzip level-6",
      "original_bytes": 200000,
      "compressed_bytes": 142048,
      "ratio_percent": 71.02
    }
  ],
  "lz4_header_count": 78
}
```

---

## Background: what is a Spark shuffle file?

When Spark executes a wide transformation (anything that moves data between
executors — `groupBy`, `join`, `distinct`, `repartition`), it writes
intermediate data to local disk before sending it across the network. Each
executor produces one `.data` file per shuffle stage containing all output
partitions concatenated together, and one `.index` file listing the byte
offset of each partition within the data file.

In AWS Glue, executors use S3 as the spill target when local disk is
exhausted. Analysing the shuffle files tells you why spill is happening and
what configuration changes will stop it.

---

## Contributing

Contributions are welcome. Please open an issue before submitting a pull
request for anything beyond a small bug fix, so the change can be discussed
first.

Areas where contributions are particularly useful:

- Support for additional compression formats (Snappy decompression, ZSTD)
- Additional output formats (CSV, Markdown)
- Tests — this is the project's biggest current gap
- Support for Spark on platforms other than AWS Glue (Databricks, HDInsight, etc.)

### Development setup

```bash
git clone https://github.com/beyond5nines/glue-busters
cd glue-busters
pip install -e ".[dev]"
```

---

## License

Apache License 2.0 — see [LICENSE](LICENSE).
