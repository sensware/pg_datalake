# On-Premises Object Storage (MinIO & Ceph)

pg_lake works with any S3-compatible object store, not just AWS S3.
MinIO and Ceph RadosGW are the most common on-premises choices.
Both implement the S3 API and work transparently with the `s3://` URL scheme
already used throughout pg_lake.

## How it works

pg_lake delegates storage I/O to **pgduck_server**, which uses DuckDB's S3
file system under the hood.  DuckDB's S3 file system accepts an `ENDPOINT`
parameter that overrides the default `s3.amazonaws.com` host, and a
`URL_STYLE 'path'` parameter that switches from virtual-hosted–style URLs
(required by AWS) to path-style URLs (required by most on-prem systems).

Credentials are stored in pgduck_server as *DuckDB secrets*.  You can create
them two ways:

| Method | When to use |
|--------|-------------|
| `lake_engine.configure_s3_compat()` | Interactive setup, scripted provisioning |
| `--init_file_path` SQL file | Persistent configuration that survives pgduck_server restarts |

## Quick start with `configure_s3_compat()`

Connect to PostgreSQL (not pgduck_server) and call:

```sql
SELECT lake_engine.configure_s3_compat(
    name     => 'my_minio',
    endpoint => 'minio.internal:9000',
    key_id   => 'accesskey',
    secret   => 'secretkey',
    scope    => 's3://warehouse'   -- optional: restrict to one bucket
);
```

After this call you can read and write `s3://warehouse/…` paths through normal
pg_lake operations:

```sql
-- Query a Parquet file stored on MinIO
CREATE FOREIGN TABLE orders ()
SERVER pg_lake
OPTIONS (path 's3://warehouse/orders/*.parquet');

SELECT count(*) FROM orders;

-- Create an Iceberg table on MinIO
SET pg_lake_iceberg.default_location_prefix TO 's3://warehouse/iceberg';

CREATE TABLE events (
    ts  timestamptz,
    msg text
) USING iceberg;

INSERT INTO events VALUES (now(), 'hello from MinIO');
```

## Function reference

```
lake_engine.configure_s3_compat(
    name      text,
    endpoint  text,
    key_id    text,
    secret    text,
    scope     text    DEFAULT NULL,
    use_ssl   boolean DEFAULT false,
    region    text    DEFAULT 'us-east-1',
    url_style text    DEFAULT 'path'
) RETURNS void
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `name` | — | Unique secret identifier. Letters, digits, underscores, hyphens only. |
| `endpoint` | — | `host:port` of the S3-compatible endpoint. |
| `key_id` | — | S3 access key ID. |
| `secret` | — | S3 secret access key. |
| `scope` | `NULL` | `s3://bucket` prefix the secret applies to. `NULL` matches all buckets. Use one secret per bucket when different buckets have different credentials. |
| `use_ssl` | `false` | Set `true` when the endpoint is HTTPS. |
| `region` | `'us-east-1'` | Value embedded in the AWS Signature v4 `Authorization` header. MinIO and Ceph accept any non-empty value; the default works. |
| `url_style` | `'path'` | `'path'` is required for most on-prem systems. `'vhost'` uses virtual-hosted–style URLs like AWS S3. |

`configure_s3_compat()` internally executes
`CREATE OR REPLACE SECRET … (TYPE S3, …)` on the connected pgduck_server.
The secret lives in pgduck_server memory until the process exits.

## Persisting credentials across restarts

To avoid re-running `configure_s3_compat()` every time pgduck_server starts,
add the equivalent `CREATE SECRET` statement to the SQL file you pass to
`--init_file_path`:

```sql
-- /etc/pgduck/secrets.sql
CREATE SECRET my_minio (
    TYPE      S3,
    KEY_ID    'accesskey',
    SECRET    'secretkey',
    ENDPOINT  'minio.internal:9000',
    SCOPE     's3://warehouse',
    URL_STYLE 'path',
    USE_SSL   false,
    REGION    'us-east-1'
);
```

Start pgduck_server with:

```bash
pgduck_server --init_file_path /etc/pgduck/secrets.sql
```

## Multiple buckets with separate credentials

Create one secret per scope:

```sql
-- Warehouse bucket (read/write service account)
SELECT lake_engine.configure_s3_compat(
    name   => 'minio_warehouse',
    endpoint => 'minio.internal:9000',
    key_id => 'warehouse_key',
    secret => 'warehouse_secret',
    scope  => 's3://warehouse'
);

-- Archive bucket (read-only service account)
SELECT lake_engine.configure_s3_compat(
    name   => 'minio_archive',
    endpoint => 'minio.internal:9000',
    key_id => 'archive_readonly_key',
    secret => 'archive_readonly_secret',
    scope  => 's3://archive'
);
```

## Ceph RadosGW

Ceph RadosGW exposes an S3-compatible API.  The configuration is identical
to MinIO — supply the RadosGW endpoint and use path-style URLs:

```sql
SELECT lake_engine.configure_s3_compat(
    name     => 'ceph_rgw',
    endpoint => 'ceph-rgw.internal:7480',
    key_id   => 'rgw_access_key',
    secret   => 'rgw_secret_key',
    scope    => 's3://datalake',
    region   => 'default'          -- Ceph uses 'default' by convention
);
```

## TLS / HTTPS endpoints

For endpoints with HTTPS, set `use_ssl => true` and omit the port if you
are using the standard HTTPS port (443):

```sql
SELECT lake_engine.configure_s3_compat(
    name     => 'minio_tls',
    endpoint => 'minio.internal',
    key_id   => 'accesskey',
    secret   => 'secretkey',
    scope    => 's3://secure-bucket',
    use_ssl  => true
);
```

## Troubleshooting

**Connection refused / timeout**
Verify pgduck_server can reach the endpoint.  The process runs under the
PostgreSQL service account; check firewall rules for that user and host.

**Access denied (403)**
Double-check `key_id` and `secret`.  For Ceph, also verify the user has
`s3:GetObject`, `s3:PutObject`, `s3:DeleteObject`, and `s3:ListBucket`
permissions on the target bucket.

**SignatureDoesNotMatch**
Make sure `region` matches the value configured in the S3-compatible server.
Most MinIO single-node deployments accept any region string; Ceph defaults to
`'default'`.  If the server rejects AWS4 signatures, also check that the
system clock on the pgduck_server host is within five minutes of the storage
server clock.

**Path-style vs virtual-hosted-style**
MinIO and Ceph RadosGW require `url_style => 'path'` (the default).
Virtual-hosted–style requires a wildcard DNS entry; path-style does not.

**Secret does not survive a restart**
Secrets created with `configure_s3_compat()` live only in pgduck_server
memory.  Add the equivalent `CREATE SECRET` block to your
`--init_file_path` SQL file to make them permanent.

## Local development with MinIO

For a fully local development environment that mirrors production:

```bash
# Install and start MinIO (single-node, single-drive)
minio server /tmp/minio-data --console-address ":9001"

# Create a bucket via the MinIO CLI
mc alias set local http://localhost:9000 minioadmin minioadmin
mc mb local/dev-warehouse
```

```sql
-- Register the local MinIO instance
SELECT lake_engine.configure_s3_compat(
    name     => 'local_minio',
    endpoint => 'localhost:9000',
    key_id   => 'minioadmin',
    secret   => 'minioadmin',
    scope    => 's3://dev-warehouse'
);

-- Point Iceberg at the local bucket
SET pg_lake_iceberg.default_location_prefix TO 's3://dev-warehouse/iceberg';
```

See the [building from source](building-from-source.md) guide for full local
development setup instructions.
