"""
Tests for lake_engine.configure_s3_compat() — the PostgreSQL helper that
registers an S3-compatible on-premises object store (MinIO, Ceph, etc.)
with pgduck_server at runtime.

We reuse the existing Moto mock S3 server as a stand-in for a real on-prem
system.  The key difference from other S3 tests is that the bucket used here
has NO pre-configured pgduck_server secret in the startup --init_file_path;
the secret is created only via configure_s3_compat(), which is exactly the
on-prem workflow.
"""

import psycopg2
import pytest
import server_params
from utils_pytest import *


# Bucket created specifically for these tests so there is no pre-configured
# pgduck_server secret for it at startup.
ON_PREM_BUCKET = "on-prem-test-bucket"


def _drop_pgduck_secret(name):
    """Connect directly to pgduck_server and drop a secret by name."""
    try:
        pgduck = psycopg2.connect(
            host=server_params.PGDUCK_UNIX_DOMAIN_PATH,
            port=server_params.PGDUCK_PORT,
        )
        pgduck.autocommit = True
        with pgduck.cursor() as cur:
            cur.execute(f"DROP SECRET IF EXISTS {name}")
        pgduck.close()
    except Exception:
        pass


@pytest.fixture(scope="module")
def on_prem_bucket(s3):
    """Create (and teardown) a dedicated bucket in the Moto mock server."""
    s3.create_bucket(
        Bucket=ON_PREM_BUCKET,
        CreateBucketConfiguration={"LocationConstraint": TEST_AWS_REGION},
    )
    yield
    # best-effort: delete all objects then the bucket
    try:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=ON_PREM_BUCKET):
            for obj in page.get("Contents", []):
                s3.delete_object(Bucket=ON_PREM_BUCKET, Key=obj["Key"])
        s3.delete_bucket(Bucket=ON_PREM_BUCKET)
    except Exception:
        pass


@pytest.fixture(scope="module")
def s3_compat_secret(on_prem_bucket, extension, superuser_conn):
    """
    Register the on-prem bucket with pgduck_server via configure_s3_compat().

    This is the central fixture that exercises the new function: it connects
    to PostgreSQL and calls lake_engine.configure_s3_compat(), which forwards
    a CREATE OR REPLACE SECRET command to pgduck_server.
    """
    run_command(
        f"""
        SELECT lake_engine.configure_s3_compat(
            name     => 'on_prem_test',
            endpoint => 'localhost:{MOTO_PORT}',
            key_id   => '{TEST_AWS_ACCESS_KEY_ID}',
            secret   => '{TEST_AWS_SECRET_ACCESS_KEY}',
            scope    => 's3://{ON_PREM_BUCKET}',
            use_ssl  => false,
            region   => '{TEST_AWS_REGION}'
        )
        """,
        superuser_conn,
    )
    superuser_conn.commit()
    yield
    # Drop the runtime secret directly on pgduck_server so it does not
    # interfere with other test modules.
    _drop_pgduck_secret("on_prem_test")


# ---------------------------------------------------------------------------
# Basic read / write round-trip
# ---------------------------------------------------------------------------


def test_configure_s3_compat_copy_to(pg_conn, s3_compat_secret, s3):
    """Write a Parquet file to the on-prem bucket and confirm the object lands there."""
    url = f"s3://{ON_PREM_BUCKET}/test_configure_s3_compat/data.parquet"

    run_command(
        f"COPY (SELECT generate_series(1, 50) AS n) TO '{url}'",
        pg_conn,
    )
    pg_conn.commit()

    keys = [
        obj["Key"]
        for obj in s3.list_objects_v2(
            Bucket=ON_PREM_BUCKET, Prefix="test_configure_s3_compat/"
        ).get("Contents", [])
    ]
    assert "test_configure_s3_compat/data.parquet" in keys

    pg_conn.rollback()


def test_configure_s3_compat_copy_from(pg_conn, s3_compat_secret):
    """Write then read back a Parquet file through the on-prem bucket."""
    url = f"s3://{ON_PREM_BUCKET}/test_s3_compat_roundtrip/data.parquet"

    run_command(
        f"COPY (SELECT generate_series(1, 100) AS n) TO '{url}'",
        pg_conn,
    )
    pg_conn.commit()

    run_command("CREATE TEMP TABLE tmp_compat (n int)", pg_conn)
    run_command(f"COPY tmp_compat FROM '{url}'", pg_conn)

    result = run_query("SELECT count(*) FROM tmp_compat", pg_conn)
    assert result[0][0] == 100

    pg_conn.rollback()


def test_configure_s3_compat_foreign_table(pg_conn, s3_compat_secret):
    """Query on-prem Parquet data via a foreign table (vectorised path)."""
    url = f"s3://{ON_PREM_BUCKET}/test_s3_compat_fdw/data.parquet"

    run_command(
        f"COPY (SELECT n, n * 2 AS doubled FROM generate_series(1, 10) n) TO '{url}'",
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""
        CREATE FOREIGN TABLE on_prem_fdw (n int, doubled int)
        SERVER pg_lake
        OPTIONS (path '{url}')
        """,
        pg_conn,
    )

    result = run_query("SELECT sum(n), sum(doubled) FROM on_prem_fdw", pg_conn)
    assert result[0][0] == 55   # sum(1..10)
    assert result[0][1] == 110  # sum(2..20 step 2)

    run_command("DROP FOREIGN TABLE on_prem_fdw", pg_conn)
    pg_conn.rollback()


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


def test_configure_s3_compat_invalid_name(superuser_conn):
    """A secret name with illegal characters must be rejected by PostgreSQL."""
    error = run_command(
        f"""
        SELECT lake_engine.configure_s3_compat(
            name     => 'bad name!',
            endpoint => 'localhost:{MOTO_PORT}',
            key_id   => 'k',
            secret   => 's'
        )
        """,
        superuser_conn,
        raise_error=False,
    )
    assert "invalid secret name" in error
    superuser_conn.rollback()


def test_configure_s3_compat_invalid_url_style(superuser_conn):
    """An unrecognised url_style must be rejected before touching pgduck_server."""
    error = run_command(
        f"""
        SELECT lake_engine.configure_s3_compat(
            name      => 'bad_style',
            endpoint  => 'localhost:{MOTO_PORT}',
            key_id    => 'k',
            secret    => 's',
            url_style => 'ftp'
        )
        """,
        superuser_conn,
        raise_error=False,
    )
    assert "url_style must be 'path' or 'vhost'" in error
    superuser_conn.rollback()


def test_configure_s3_compat_null_required_arg(superuser_conn):
    """NULL for a required argument (endpoint) must be rejected."""
    error = run_command(
        """
        SELECT lake_engine.configure_s3_compat(
            name     => 'null_test',
            endpoint => NULL,
            key_id   => 'k',
            secret   => 's'
        )
        """,
        superuser_conn,
        raise_error=False,
    )
    assert "must not be NULL" in error
    superuser_conn.rollback()


# ---------------------------------------------------------------------------
# Idempotency — calling configure_s3_compat() twice must not raise
# ---------------------------------------------------------------------------


def test_configure_s3_compat_idempotent(superuser_conn, on_prem_bucket):
    """CREATE OR REPLACE SECRET is idempotent: a second call must succeed."""
    for _ in range(2):
        run_command(
            f"""
            SELECT lake_engine.configure_s3_compat(
                name     => 'idempotency_test',
                endpoint => 'localhost:{MOTO_PORT}',
                key_id   => '{TEST_AWS_ACCESS_KEY_ID}',
                secret   => '{TEST_AWS_SECRET_ACCESS_KEY}',
                scope    => 's3://{ON_PREM_BUCKET}'
            )
            """,
            superuser_conn,
        )
    superuser_conn.commit()
    _drop_pgduck_secret("idempotency_test")
