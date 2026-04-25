# brahmagupta

Tools for debugging AWS Glue jobs.

[**Brahmagupta**](https://en.wikipedia.org/wiki/Brahmagupta) (c. 598–668 CE) — Indian mathematician who first treated zero as a number with arithmetic rules. This project measures the structure underneath the abstraction.

---

## scan.py

Inspect AWS Glue shuffle `.data` files. Reports partition layout, compression, and structural tuning opportunities.

> **Scope:** verified on AWS Glue 3.0 / 4.0 / 5.0. Apache Spark, Databricks, EMR may use different LZ4 framing — not currently supported. PRs welcome.

> **Background:** built during the investigation in [Look Ma, No Servers! — The "No Space Left on Device" Trap](https://beyond5nines.com/look-ma-no-servers-03/).

### Install

```bash
pip install ".[lz4]"   # with LZ4 decompression
# or
pip install .          # without lz4 — codec comparison still works
```

Requires Python 3.10+.

### Usage

```bash
scan shuffle_N_M_0.data
scan shuffle_N_M_0.data --json | jq '.partitions.empty'
```

The companion `.index` file is auto-detected by replacing `.data` with `.index`.

### Example output

Real run on a Glue shuffle file:

```
====================================================================
SHUFFLE FILE ANALYSIS: shuffle_98_37726_0.data
====================================================================

  .data   :         28,114 bytes  (27.46 KB)
  .index  :            584 bytes  (0.57 KB)
  Format  : LZ4Block
  WARNING : lz4 package not installed — pip install lz4

  Total partitions  : 72
  Non-empty         : 39
  Empty             : 33  (46% wasted slots)


  Part         Offset     Size (B)
  ------------------------------------
  0                 0          502
  1               502          505
  2             1,007          465
  5             1,472        1,260
  ...
  71           27,647          467

  Min size : 464 bytes
  Max size : 1,564 bytes
  Avg size : 721 bytes


====================================================================
COMPRESSION COMPARISON  (sample: 28,114 bytes from raw file)
====================================================================

  Note: ratios below are on raw compressed bytes — decompression failed or lz4 unavailable.

  Codec                          Compressed    Ratio
  ----------------------------------------------------
  Zlib level-9 (DEFLATE)             12,952     46.1%


====================================================================
RECOMMENDATIONS
====================================================================

  # Reduce from 72 → 40 (non-empty = 39, rounded to nearest 10)
  spark.conf.set("spark.sql.shuffle.partitions",                  "40")

  # AQE auto-coalesces empty/tiny partitions at runtime
  spark.conf.set("spark.sql.adaptive.enabled",                    "true")
  spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

  Eliminates 33 empty partition slot(s) — reduces shuffle metadata and merge passes; file-size change is negligible.

  # Verify against the compression comparison above before switching
  spark.conf.set("spark.io.compression.codec", "zstd")
```

### JSON output

`--json` emits a machine-readable report:

```json
{
  "version": "0.1.0",
  "data_file": "shuffle_98_37726_0.data",
  "file_size_bytes": 28114,
  "detected_format": "LZ4Block",
  "partitions": {"total": 72, "non_empty": 39, "empty": 33, "detail": [...]},
  "compression_estimates": [...],
  "decompressed_for_comparison": false,
  "recommended_shuffle_partitions": 40
}
```

---

## Contributing

Open an issue before submitting a PR beyond a small fix. Useful areas:

- Snappy / ZSTD decompression
- Spark / Databricks / EMR LZ4 framing variants
- Skew detection, human-readable sizes, glob mode

```bash
git clone https://github.com/beyond5nines/brahmagupta
cd brahmagupta
pip install -e ".[dev]"
pytest
```

---

## License

Apache 2.0 — see [LICENSE](LICENSE).
