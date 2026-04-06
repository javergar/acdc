# ACDC — Alberto's Change Data Capture

Single-binary Rust daemon that performs Change Data Capture (CDC) from MySQL to Apache Iceberg tables on S3.

Reads MySQL binlog events in real-time, converts them to Arrow RecordBatches, writes Parquet files to S3, and commits Iceberg table metadata via a REST catalog (Polaris).

## Architecture

```
MySQL binlog (ROW format)
  → mysql_async BinlogStream
  → per-table in-memory buffers (Arrow column builders)
  → on flush: Arrow RecordBatch → Parquet file on S3
  → Iceberg table commit (append data file, update snapshot)
  → clear buffers
```

- Single process, single replication connection to MySQL
- No Kafka, no Debezium, no JVM
- Append-only changelog: every binlog event becomes a row with CDC metadata
- Consumers materialise current state downstream

## Quick Start

```bash
# Start infrastructure
docker compose up -d
./docker/setup.sh

# Run ACDC
cargo run -- acdc.toml
```

### Docker Services

| Service | Port | Purpose |
|---------|------|---------|
| MySQL 8.0 | 3306 | Source database (binlog ROW format, GTID enabled) |
| RustFS | 9000/9001 | S3-compatible storage (data + metadata) |
| Polaris | 8181/8182 | Iceberg REST catalog |

## Configuration

See `acdc.toml` for all options. Key sections:

```toml
[source]
host = "127.0.0.1"
port = 3306
user = "cdc_user"
password = "cdc_password"
server_id = 100
database = "testdb"
tables = ["orders", "customers"]

[sink]
mode = "iceberg"           # "local", "s3", or "iceberg"
warehouse = "s3://bucket/prefix"
catalog_uri = "http://localhost:8181/api/catalog"
catalog_name = "acdc"
namespace = "cdc"
credential = "root:s3cr3t"  # Polaris OAuth client_id:client_secret

[sink.writer]
batch_size = 10000          # rows per flush
flush_interval_secs = 30    # max seconds between flushes
```

### Observability

```toml
[observability]
log_level = "info"           # trace, debug, info, warn, error
json_logs = false            # true for structured JSON (CloudWatch, Datadog)
health_check_bind = "0.0.0.0:8080"
stats_interval_secs = 30     # how often to log throughput stats
# log_file = "/var/log/acdc/acdc.log"  # optional: daily-rotating file
```

**Logging in production:**

By default, logs go to stderr. This is correct for ECS (use `awslogs` driver), Docker (`json-file` with `max-size`), or systemd (`journald`). Set `json_logs = true` for structured logging.

If running outside a container runtime, set `log_file` to enable daily-rotating file output (logs go to both stderr and the file). Old log files are NOT automatically deleted — use logrotate or a cron job to clean them.

To control log volume in production:
- `log_level = "info"` is the right default (~1-5 KB per flush cycle)
- `log_level = "warn"` for minimal output (only problems)
- `stats_interval_secs = 60` or higher to reduce stats line frequency

### Sink Modes

- **`local`** — writes Parquet files to `output_dir` on the local filesystem
- **`s3`** — writes Parquet files directly to S3 (no Iceberg metadata)
- **`iceberg`** — writes Parquet to S3 and commits to Iceberg REST catalog

## CDC Event Schema

Every row written to Iceberg has the user's table columns plus these CDC metadata columns:

| Column | Type | Description |
|--------|------|-------------|
| `_cdc_op` | Utf8 | `"I"` (insert), `"U"` (update), `"D"` (delete), `"S"` (snapshot) |
| `_cdc_ts` | Timestamp(μs, UTC) | MySQL event timestamp |
| `_cdc_seq` | Int64 | Monotonically increasing counter, unique per event |

Downstream consumers materialise current state with:

```sql
SELECT * FROM table
QUALIFY ROW_NUMBER() OVER (PARTITION BY pk_col ORDER BY _cdc_seq DESC) = 1
WHERE _cdc_op != 'D'
```

## MySQL Type Mapping

All MySQL column types are mapped to Iceberg-compatible Arrow types. The mapping avoids unsigned Arrow types since Iceberg does not support them — unsigned MySQL integers are widened to the next larger signed type.

### Integer Types

| MySQL Type | Arrow Type | Notes |
|-----------|-----------|-------|
| `TINYINT` | Int8 | |
| `TINYINT UNSIGNED` | Int16 | Widened: max 255 fits in Int16 |
| `TINYINT(1)` / `BOOLEAN` | Boolean | Special-cased |
| `SMALLINT` | Int16 | |
| `SMALLINT UNSIGNED` | Int32 | Widened: max 65535 fits in Int32 |
| `MEDIUMINT` | Int32 | |
| `INT` | Int32 | |
| `INT UNSIGNED` | Int64 | Widened: max ~4B fits in Int64 |
| `BIGINT` | Int64 | |
| `BIGINT UNSIGNED` | Int64 | **Overflow possible** — see below |

### Floating Point

| MySQL Type | Arrow Type | Notes |
|-----------|-----------|-------|
| `FLOAT` | Float32 | |
| `DOUBLE` | Float64 | |
| `DECIMAL(p,s)` | Decimal128(p,s) | Precision clamped to 38 (Arrow max) |

### String Types

| MySQL Type | Arrow Type | Notes |
|-----------|-----------|-------|
| `CHAR` / `VARCHAR` / `TEXT` variants | Utf8 | |
| `ENUM` | Utf8 | Binlog sends ordinal number, stored as string |
| `SET` | Utf8 | Binlog sends bitmask, stored as string |
| `JSON` | Utf8 | Serialised as JSON string |

### Binary Types

| MySQL Type | Arrow Type | Notes |
|-----------|-----------|-------|
| `BINARY` / `VARBINARY` / `BLOB` variants | Binary | |
| `BIT(n)` | Binary | Raw bytes |
| Spatial types (`POINT`, `GEOMETRY`, etc.) | Binary | WKB format |

### Date/Time Types

| MySQL Type | Arrow Type | Notes |
|-----------|-----------|-------|
| `DATE` | Date32 | Days since epoch. Zero dates → NULL |
| `DATETIME` / `TIMESTAMP` | Timestamp(μs, UTC) | Microsecond precision preserved |
| `TIME` | Int64 | Signed microseconds (supports negative TIME) |
| `YEAR` | Int16 | |

### Edge Cases and Known Limitations

#### BIGINT UNSIGNED overflow

MySQL `BIGINT UNSIGNED` has a max value of 2^64 - 1 (~18.4 quintillion), but Iceberg has no unsigned 64-bit integer type. Values are stored in Int64:
- Values ≤ `i64::MAX` (9,223,372,036,854,775,807): stored correctly
- Values > `i64::MAX`: stored as the raw bit pattern (appears negative). A warning is logged.

If you need exact `BIGINT UNSIGNED` values, query with: `CAST(col AS HUGEINT) & 0xFFFFFFFFFFFFFFFF` in DuckDB, or use `DECIMAL(20,0)` in MySQL instead.

#### ENUM and SET from binlog

MySQL's ROW-format binlog encodes ENUM values as 1-based ordinal integers and SET values as bitmask integers, not as their string labels. ACDC stores these as their string representation of the integer (e.g., `"2"` for the second ENUM variant).

To resolve ENUM labels downstream, join with the ENUM definition from `INFORMATION_SCHEMA.COLUMNS` or maintain a mapping table.

#### Zero dates

MySQL allows `0000-00-00` as a DATE and `0000-00-00 00:00:00` as a DATETIME, which are invalid in the Iceberg/Arrow type system. These are converted to NULL.

#### Negative TIME

MySQL TIME range is `-838:59:59` to `838:59:59`. Stored as signed microseconds in Int64, so negative values are preserved correctly. To convert back: `col_time / 1000000` gives signed seconds.

#### DECIMAL precision

Arrow Decimal128 supports a maximum precision of 38 digits. MySQL `DECIMAL(65,30)` will be clamped to `Decimal128(38,30)`. Values exceeding the clamped precision are stored with reduced precision. A warning is logged if the declared precision exceeds 38.

#### Integer bounds checking

All narrowing integer conversions (e.g., Int64 → Int8 for TINYINT) include bounds checking. If a value exceeds the target type's range, NULL is written and a warning is logged. This should not happen under normal operation since MySQL enforces the same bounds, but protects against data corruption from malformed binlog events.

#### Non-UTF-8 bytes in string columns

If a VARCHAR/TEXT column contains bytes that are not valid UTF-8 (possible with `latin1` or `binary` collations), the value is stored as Binary rather than Utf8, and a warning is logged.

#### JSON handling

- Native MySQL JSON columns are decoded from MySQL's internal JSONB binary format
- JSON values are serialised to compact JSON strings in the Utf8 column
- `JsonDiff` partial updates (MySQL 8.0.17+) are not yet supported — these are logged as warnings and the column is set to NULL for that row
- Empty objects `{}`, empty arrays `[]`, and `null` are all valid and preserved

## Error Handling

| Error Type | Behavior |
|-----------|----------|
| Per-row conversion failure | Write NULL for the column, log warning, continue pipeline |
| Type mismatch (builder vs value) | Write NULL, log warning with value and expected type |
| Transient S3/network error | Retry with exponential backoff (TODO) |
| Fatal error (auth, config) | Log error, exit non-zero |

## Project Status

| Milestone | Status | Description |
|-----------|--------|-------------|
| 1. Binlog → JSON stdout | Done | MySQL binlog parsing, event decoding |
| 2. Arrow → local Parquet | Done | Type mapping, per-table buffering, Parquet writer |
| 3. Parquet → S3 | Done | object_store S3 writer, tested with RustFS |
| 4. Iceberg catalog | Done | REST catalog via iceberg-rust, table auto-creation, snapshot commits |
| 5. Initial snapshot | Planned | Full SELECT * before CDC |
| 6. Schema evolution | Planned | DDL detection, health check, graceful shutdown |

## Development

### Prerequisites

- Rust 1.80+
- Docker and Docker Compose
- zlib (for building): `apt install zlib1g-dev` or build from source to `~/.local`

### Build

```bash
cargo build          # debug
cargo build --release
```

### Test Environment

```bash
docker compose up -d
./docker/setup.sh    # creates S3 buckets, Polaris catalog, namespace, roles
cargo run -- acdc.toml
```

### Polaris + RustFS Setup Notes

The docker-compose uses RustFS (S3-compatible storage with STS support) and Apache Polaris (Iceberg REST catalog). Key configuration points:

- Polaris requires STS for credential vending — RustFS provides this
- `storageConfigInfo` must have `endpoint`, `endpointInternal`, and `pathStyleAccess` at the top level (not nested in `s3`)
- iceberg-rust 0.4 has an **inverted `s3.path-style-access` bug** — set to `"false"` to get actual path-style access
- Polaris bootstrap credentials are set via `POLARIS_BOOTSTRAP_CREDENTIALS` env var
- OAuth scope must be `PRINCIPAL_ROLE:ALL` for Polaris 1.3+
