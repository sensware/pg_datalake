"""
Tests for data-governance snapshot lifecycle features introduced in 3.4.

Covers:
  - lake_governance.snapshot_policies table CRUD
  - lake_governance.set_snapshot_policy / get_snapshot_policy / drop_snapshot_policy
  - lake_governance.expire_snapshots
  - lake_governance.snapshot_compliance view

Intentionally NOT tested here: lineage, auditing, data-catalog UI,
CDC/streaming, HFT/real-time writes.
"""

import pytest
import psycopg2
from utils_pytest import *


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _create_iceberg_table(conn, table_name, s3_path):
    """Create a minimal Iceberg table in a temp schema."""
    run_command(
        f"""
        CREATE TABLE {table_name} (id bigint, val text)
        WITH (location = '{s3_path}');
        """,
        conn,
    )


def _drop_table_if_exists(conn, table_name):
    run_command(f"DROP TABLE IF EXISTS {table_name}", conn)
    conn.commit()


# ---------------------------------------------------------------------------
# Tests — snapshot_policies table
# ---------------------------------------------------------------------------

def test_snapshot_policies_table_exists(superuser_conn, iceberg_extension):
    """lake_governance.snapshot_policies table should exist after upgrade."""
    result = run_query(
        """
        SELECT count(*)
        FROM   pg_catalog.pg_class c
        JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE  n.nspname = 'lake_governance'
        AND    c.relname = 'snapshot_policies'
        """,
        superuser_conn,
    )
    assert result[0][0] == 1
    superuser_conn.rollback()


def test_set_and_get_snapshot_policy(superuser_conn, iceberg_extension, s3):
    """set_snapshot_policy inserts/updates a row; get_snapshot_policy returns it."""
    table = "public.gov_snap_test"
    s3_path = f"s3://{TEST_BUCKET}/governance/snap_policy_test"

    _drop_table_if_exists(superuser_conn, table)
    _create_iceberg_table(superuser_conn, table, s3_path)
    superuser_conn.commit()

    try:
        # Set a policy
        run_command(
            f"""
            SELECT lake_governance.set_snapshot_policy(
                '{table}'::regclass,
                24,    -- max_age_hours
                2,     -- min_snapshots_to_keep
                10     -- max_snapshots_to_keep
            )
            """,
            superuser_conn,
        )
        superuser_conn.commit()

        rows = run_query(
            f"SELECT * FROM lake_governance.get_snapshot_policy('{table}'::regclass)",
            superuser_conn,
        )
        assert len(rows) == 1
        row = rows[0]
        assert row["max_snapshot_age_hours"] == 24
        assert row["min_snapshots_to_keep"] == 2
        assert row["max_snapshots_to_keep"] == 10
        assert row["policy_source"] == "per-table-policy"

        # Update the same policy
        run_command(
            f"""
            SELECT lake_governance.set_snapshot_policy(
                '{table}'::regclass,
                48,   -- new age
                1,    -- min keep
                NULL  -- no max cap
            )
            """,
            superuser_conn,
        )
        superuser_conn.commit()

        rows = run_query(
            f"SELECT * FROM lake_governance.get_snapshot_policy('{table}'::regclass)",
            superuser_conn,
        )
        assert rows[0]["max_snapshot_age_hours"] == 48
        assert rows[0]["max_snapshots_to_keep"] is None

    finally:
        _drop_table_if_exists(superuser_conn, table)


def test_drop_snapshot_policy(superuser_conn, iceberg_extension, s3):
    """drop_snapshot_policy removes the row; subsequent get returns policy_source='none'."""
    table = "public.gov_drop_policy_test"
    s3_path = f"s3://{TEST_BUCKET}/governance/drop_policy_test"

    _drop_table_if_exists(superuser_conn, table)
    _create_iceberg_table(superuser_conn, table, s3_path)
    superuser_conn.commit()

    try:
        run_command(
            f"SELECT lake_governance.set_snapshot_policy('{table}'::regclass, 72)",
            superuser_conn,
        )
        superuser_conn.commit()

        run_command(
            f"SELECT lake_governance.drop_snapshot_policy('{table}'::regclass)",
            superuser_conn,
        )
        superuser_conn.commit()

        rows = run_query(
            f"SELECT * FROM lake_governance.get_snapshot_policy('{table}'::regclass)",
            superuser_conn,
        )
        # With global GUC = 0 (disabled) and no per-table policy, source = 'none'
        assert rows[0]["policy_source"] in ("none", "global-guc")

    finally:
        _drop_table_if_exists(superuser_conn, table)


def test_snapshot_policy_invalid_min_keep(superuser_conn, iceberg_extension, s3):
    """set_snapshot_policy should reject min_snapshots_to_keep < 1."""
    table = "public.gov_invalid_min_test"
    s3_path = f"s3://{TEST_BUCKET}/governance/invalid_min_test"

    _drop_table_if_exists(superuser_conn, table)
    _create_iceberg_table(superuser_conn, table, s3_path)
    superuser_conn.commit()

    try:
        err = run_command(
            f"""
            SELECT lake_governance.set_snapshot_policy(
                '{table}'::regclass,
                24,
                0   -- invalid: must be >= 1
            )
            """,
            superuser_conn,
            raise_error=False,
        )
        assert "min_snapshots_to_keep must be >= 1" in err

    finally:
        superuser_conn.rollback()
        _drop_table_if_exists(superuser_conn, table)


def test_snapshot_compliance_view_exists(superuser_conn, iceberg_extension):
    """lake_governance.snapshot_compliance view should exist."""
    result = run_query(
        """
        SELECT count(*)
        FROM   pg_catalog.pg_class c
        JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE  n.nspname = 'lake_governance'
        AND    c.relname = 'snapshot_compliance'
        AND    c.relkind = 'v'
        """,
        superuser_conn,
    )
    assert result[0][0] == 1
    superuser_conn.rollback()


def test_snapshot_compliance_returns_rows(superuser_conn, iceberg_extension, s3):
    """snapshot_compliance returns one row per Iceberg table with policy info."""
    table_name_bare = "gov_compliance_test"
    table = f"public.{table_name_bare}"
    s3_path = f"s3://{TEST_BUCKET}/governance/compliance_test"

    _drop_table_if_exists(superuser_conn, table)
    _create_iceberg_table(superuser_conn, table, s3_path)
    superuser_conn.commit()

    # Write a row so there is at least one snapshot
    run_command(
        f"INSERT INTO {table} VALUES (1, 'hello')",
        superuser_conn,
    )
    superuser_conn.commit()

    try:
        run_command(
            f"SELECT lake_governance.set_snapshot_policy('{table}'::regclass, 168, 1, 5)",
            superuser_conn,
        )
        superuser_conn.commit()

        rows = run_query(
            f"""
            SELECT schema_name, table_name, compliance_status,
                   policy_max_age_hours, policy_min_keep, policy_max_keep
            FROM   lake_governance.snapshot_compliance
            WHERE  table_name = '{table_name_bare}'
            """,
            superuser_conn,
        )
        assert len(rows) == 1
        row = rows[0]
        assert row["schema_name"] == "public"
        assert row["table_name"] == table_name_bare
        # A brand-new table with one snapshot should be COMPLIANT
        assert row["compliance_status"] == "COMPLIANT"
        assert row["policy_max_age_hours"] == 168
        assert row["policy_max_keep"] == 5

    finally:
        _drop_table_if_exists(superuser_conn, table)
        run_command(
            f"SELECT lake_governance.drop_snapshot_policy('{table}'::regclass)",
            superuser_conn,
            raise_error=False,
        )
        superuser_conn.commit()


def test_snapshot_compliance_count_violation(superuser_conn, iceberg_extension, s3):
    """snapshot_compliance reports COUNT_VIOLATION when snapshots exceed max_keep."""
    table_name_bare = "gov_count_violation_test"
    table = f"public.{table_name_bare}"
    s3_path = f"s3://{TEST_BUCKET}/governance/count_violation"

    _drop_table_if_exists(superuser_conn, table)
    _create_iceberg_table(superuser_conn, table, s3_path)
    superuser_conn.commit()

    try:
        # Create several snapshots by writing multiple times
        for i in range(3):
            run_command(
                f"INSERT INTO {table} VALUES ({i}, 'row{i}')",
                superuser_conn,
            )
            superuser_conn.commit()

        # Set policy that allows only 1 snapshot
        run_command(
            f"SELECT lake_governance.set_snapshot_policy('{table}'::regclass, NULL, 1, 1)",
            superuser_conn,
        )
        superuser_conn.commit()

        rows = run_query(
            f"""
            SELECT compliance_status, total_snapshots
            FROM   lake_governance.snapshot_compliance
            WHERE  table_name = '{table_name_bare}'
            """,
            superuser_conn,
        )
        assert len(rows) == 1
        # 3 snapshots with max_keep=1 → violation
        if rows[0]["total_snapshots"] > 1:
            assert rows[0]["compliance_status"] == "COUNT_VIOLATION"

    finally:
        _drop_table_if_exists(superuser_conn, table)
        run_command(
            f"SELECT lake_governance.drop_snapshot_policy('{table}'::regclass)",
            superuser_conn,
            raise_error=False,
        )
        superuser_conn.commit()
