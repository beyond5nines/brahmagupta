# brahmagupta

A collection of tools for debugging and optimizing AWS Glue and Spark jobs.

---

## About the name

[**Brahmagupta**](https://en.wikipedia.org/wiki/Brahmagupta) (c. 598–668 CE) was an Indian mathematician and astronomer whose *Brāhmasphuṭasiddhānta* was the first text to treat zero as a number with defined arithmetic rules — a concept that underpins every computation in a modern data pipeline.

This project is named after him because it does what he did: looks past the surface abstraction, measures the actual structure underneath, and says precisely what needs to change.

---

## Tools

### scan.py

A command-line tool for inspecting AWS Glue shuffle `.data` files.

Shuffle files are the intermediate binary files Glue writes during wide
transformations like `groupBy`, `join`, and `repartition`. When a job spills
unexpectedly or runs slower than expected, these files are the first place
to look — but they are binary and not human readable without a tool.

`scan.py` tells you:

- What compression wrapper was used (LZ4Block, Snappy, ZSTD, or raw)
- How many partitions the file contains and how many are empty
- The byte offset and size of every non-empty partition
- How much zlib and LZ4 would compress the data compared to what is
  currently on disk
- Concrete Spark config changes derived from the observed file structure

No assumptions are made about the content of the data. The tool works on any
AWS Glue shuffle file regardless of what dataset produced it.

> **Scope note:** verified on AWS Glue 3.0/4.0/5.0 shuffle files. Apache
> Spark shuffle files (jpountz `LZ4BlockOutputStream` framing), Databricks
> Runtime, and EMR may use different byte layouts and are not currently
> supported. Contributions adding tested support for those targets are
> welcome.

> **Background reading:** this tool was built during the investigation
> documented in [Look Ma, No Servers! — The "No Space Left on Device" Trap](https://beyond5nines.com/look-ma-no-servers-03/).

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
python3 scan.py shuffle_N_M_0.data
```

### Install as a package

```bash
pip install .
```

This installs the `scan` command so you can run it from anywhere:

```bash
scan shuffle_N_M_0.data
```

### Install with LZ4 support

```bash
pip install ".[lz4]"
```

---

## Usage

```
scan SHUFFLE_DATA_FILE [options]
```

The companion `.index` file is auto-detected from the same directory by
substituting `.data` with `.index`. You never need to pass it manually.

### Options

| Flag | Description |
|------|-------------|
| `--json` | Emit machine-readable JSON instead of human-readable output. |
| `--no-recommendations` | Omit the Spark tuning recommendations section. |
| `--verbose` | Print resolved file path and LZ4 decode diagnostics to stderr. |
| `--version` | Show version and exit. |
| `--help` | Show help and exit. |

---

## Examples

```bash
# Basic analysis
python3 scan.py shuffle_98_37726_0.data

# Machine-readable JSON
python3 scan.py shuffle_98_37726_0.data --json

# Save JSON report to file
python3 scan.py shuffle_98_37726_0.data --json > report.json

# Query JSON with jq
python3 scan.py shuffle_98_37726_0.data --json | jq '.partitions.empty'

# Skip recommendations (useful in CI pipelines)
python3 scan.py shuffle_98_37726_0.data --no-recommendations

# Verbose mode — surfaces LZ4 decode errors and partial-decompression warnings
python3 scan.py shuffle_53_21062_0.data --verbose
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
Per-partition byte offset and size. Only shown when a `.index` file is
present. Use this to spot skewed partition sizes.

```
  Part         Offset     Size (B)
  ------------------------------------
  0                 0          502
  1               502          505
  ...

  Min size : 464 bytes
  Max size : 1,564 bytes
  Avg size : 721 bytes
```

### Compression comparison
Actual compression ratios measured against a 200 KB sample. For LZ4Block
files the sample is the decompressed payload (so ratios reflect real
content compressibility, not re-compression of already-compressed bytes).
Uses `zlib` (level 9, DEFLATE algorithm) and `lz4` if installed. All values
are measured — none are estimates.

```
  Codec                        Compressed     Ratio
  ----------------------------------------------------
  Zlib level-9 (DEFLATE)          141,182    70.6%
  LZ4 block (measured)            158,001    79.0%
```

### Recommendations
Four structural config changes derived from the scanned file — partition
count, AQE, coalesce, and codec. A `CRITICAL` banner is shown when more
than 20% of partitions are empty.

---

## JSON output schema

```json
{
  "version": "1.0.0",
  "data_file": "shuffle_98_37726_0.data",
  "index_file": "shuffle_98_37726_0.index",
  "file_size_bytes": 28114,
  "detected_format": "LZ4Block",
  "lz4_available": true,
  "partitions": {
    "total": 72,
    "non_empty": 39,
    "empty": 33,
    "detail": [
      { "id": 0, "offset": 0, "size": 502 }
    ]
  },
  "compression_estimates": [
    {
      "codec": "Zlib level-9 (DEFLATE)",
      "original_bytes": 200000,
      "compressed_bytes": 141182,
      "ratio_percent": 70.59
    }
  ],
  "decompressed_for_comparison": true,
  "recommended_shuffle_partitions": 40
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
- Support for Spark on platforms other than AWS Glue (Databricks, HDInsight, etc.)

### Development setup

```bash
git clone https://github.com/beyond5nines/brahmagupta
cd brahmagupta
pip install -e ".[dev]"
```

---

## License

Apache License 2.0 — see [LICENSE](LICENSE).
