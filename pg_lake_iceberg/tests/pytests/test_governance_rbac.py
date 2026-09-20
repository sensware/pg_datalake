"""
Tests for data-governance RBAC helpers and data-consolidation source registry
introduced in 3.4 (pg_lake_engine migration).

Covers:
  - lake_admin role existence
  - lake_governance.grant_table_access / revoke_table_access
  - lake_governance.table_access_summary view
  - lake_governance.external_sources registry (register / deregister)

Intentionally NOT tested: lineage, audit logs, CDC/streaming, HFT.
"""

import pytest
import psycopg2
from utils_pytest import *


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _create_role_if_not_exists(conn, role):
    run_command(
        f"""
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{role}') THEN
                CREATE ROLE {role};
            END IF;
        END
        $$;
        """,
        conn,
    )
    conn.commit()


def _drop_role_if_exists(conn, role):
    run_command(
        f"""
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{role}') THEN
                DROP ROLE {role};
            END IF;
        END
        $$;
        """,
        conn,
    )
    conn.commit()


def _create_iceberg_table(conn, table_name, s3_path):
    run_command(
        f"""
        CREATE TABLE {table_name} (id bigint, val text)
        WITH (location = '{s3_path}');
        """,
        conn,
    )
    conn.commit()


def _drop_table_if_exists(conn, table_name):
    run_command(f"DROP TABLE IF EXISTS {table_name}", conn)
    conn.commit()


# ---------------------------------------------------------------------------
# lake_admin role
# ---------------------------------------------------------------------------

def test_lake_admin_role_exists(superuser_conn, iceberg_extension):
    """lake_admin role must exist after the 3.4 upgrade."""
    result = run_query(
        "SELECT count(*) FROM pg_catalog.pg_roles WHERE rolname = 'lake_admin'",
        superuser_conn,
    )
    assert result[0][0] == 1
    superuser_conn.rollback()


def test_lake_admin_inherits_lake_read_and_write(superuser_conn, iceberg_extension):
    """lake_admin must be a member of both lake_read and lake_write."""
    result = run_query(
        """
        SELECT r.rolname
        FROM   pg_catalog.pg_auth_members m
        JOIN   pg_catalog.pg_roles        r ON r.oid = m.roleid
        WHERE  m.member = (SELECT oid FROM pg_roles WHERE rolname = 'lake_admin')
        ORDER  BY r.rolname
        """,
        superuser_conn,
    )
    role_names = [row[0] for row in result]
    assert "lake_read" in role_names
    assert "lake_write" in role_names
    superuser_conn.rollback()


# ---------------------------------------------------------------------------
# lake_governance schema
# ---------------------------------------------------------------------------

def test_lake_governance_schema_exists(superuser_conn, iceberg_extension):
    result = run_query(
        "SELECT count(*) FROM pg_catalog.pg_namespace WHERE nspname = 'lake_governance'",
        superuser_conn,
    )
    assert result[0][0] == 1
    superuser_conn.rollback()


# ---------------------------------------------------------------------------
# grant_table_access / revoke_table_access
# ---------------------------------------------------------------------------

def test_grant_table_access_read(superuser_conn, iceberg_extension, s3):
    """grant_table_access('read') gives SELECT on the table and lake_read membership."""
    table = "public.gov_rbac_read_test"
    s3_path = f"s3://{TEST_BUCKET}/governance/rbac_read_test"
    test_role = "gov_test_reader"

    _drop_table_if_exists(superuser_conn, table)
    _create_iceberg_table(superuser_conn, table, s3_path)
    _create_role_if_not_exists(superuser_conn, test_role)

    try:
        run_command(
            f"""
            SELECT lake_governance.grant_table_access(
                'public', 'gov_rbac_read_test', '{test_role}', 'read'
            )
            """,
            superuser_conn,
        )
        superuser_conn.commit()

        # Check table-level SELECT privilege
        result = run_query(
            f"""
            SELECT has_table_privilege('{test_role}', '{table}', 'SELECT')
            """,
            superuser_conn,
        )
        assert result[0][0] is True

        # Check lake_read membership
        result = run_query(
            f"""
            SELECT pg_has_role('{test_role}', 'lake_read', 'member')
            """,
            superuser_conn,
        )
        assert result[0][0] is True

    finally:
        run_command(
            f"""
            SELECT lake_governance.revoke_table_access(
                'public', 'gov_rbac_read_test', '{test_role}', 'read'
            )
            """,
            superuser_conn,
            raise_error=False,
        )
        _drop_table_if_exists(superuser_conn, table)
        _drop_role_if_exists(superuser_conn, test_role)


def test_grant_table_access_write(superuser_conn, iceberg_extension, s3):
    """grant_table_access('write') gives INSERT/UPDATE/DELETE and lake_write membership."""
    table = "public.gov_rbac_write_test"
    s3_path = f"s3://{TEST_BUCKET}/governance/rbac_write_test"
    test_role = "gov_test_writer"

    _drop_table_if_exists(superuser_conn, table)
    _create_iceberg_table(superuser_conn, table, s3_path)
    _create_role_if_not_exists(superuser_conn, test_role)

    try:
        run_command(
            f"""
            SELECT lake_governance.grant_table_access(
                'public', 'gov_rbac_write_test', '{test_role}', 'write'
            )
            """,
            superuser_conn,
        )
        superuser_conn.commit()

        for priv in ("INSERT", "UPDATE", "DELETE"):
            result = run_query(
                f"SELECT has_table_privilege('{test_role}', '{table}', '{priv}')",
                superuser_conn,
            )
            assert result[0][0] is True, f"Expected {priv} privilege"

        result = run_query(
            f"SELECT pg_has_role('{test_role}', 'lake_write', 'member')",
            superuser_conn,
        )
        assert result[0][0] is True

    finally:
        run_command(
            f"""
            SELECT lake_governance.revoke_table_access(
                'public', 'gov_rbac_write_test', '{test_role}', 'write'
            )
            """,
            superuser_conn,
            raise_error=False,
        )
        _drop_table_if_exists(superuser_conn, table)
        _drop_role_if_exists(superuser_conn, test_role)


def test_grant_table_access_invalid_type(superuser_conn, iceberg_extension):
    """grant_table_access should reject unknown access types."""
    err = run_command(
        """
        SELECT lake_governance.grant_table_access(
            'public', 'nonexistent', 'someuser', 'superwrite'
        )
        """,
        superuser_conn,
        raise_error=False,
    )
    assert "Invalid access type" in err
    superuser_conn.rollback()


def test_revoke_table_access(superuser_conn, iceberg_extension, s3):
    """revoke_table_access removes SELECT privilege granted earlier."""
    table = "public.gov_rbac_revoke_test"
    s3_path = f"s3://{TEST_BUCKET}/governance/rbac_revoke_test"
    test_role = "gov_test_revoker"

    _drop_table_if_exists(superuser_conn, table)
    _create_iceberg_table(superuser_conn, table, s3_path)
    _create_role_if_not_exists(superuser_conn, test_role)

    try:
        run_command(
            f"SELECT lake_governance.grant_table_access('public','gov_rbac_revoke_test','{test_role}','read')",
            superuser_conn,
        )
        superuser_conn.commit()

        run_command(
            f"SELECT lake_governance.revoke_table_access('public','gov_rbac_revoke_test','{test_role}','read')",
            superuser_conn,
        )
        superuser_conn.commit()

        result = run_query(
            f"SELECT has_table_privilege('{test_role}', '{table}', 'SELECT')",
            superuser_conn,
        )
        assert result[0][0] is False

    finally:
        _drop_table_if_exists(superuser_conn, table)
        _drop_role_if_exists(superuser_conn, test_role)


# ---------------------------------------------------------------------------
# table_access_summary view
# ---------------------------------------------------------------------------

def test_table_access_summary_view_exists(superuser_conn, iceberg_extension):
    result = run_query(
        """
        SELECT count(*)
        FROM   pg_catalog.pg_class c
        JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE  n.nspname = 'lake_governance'
        AND    c.relname = 'table_access_summary'
        AND    c.relkind = 'v'
        """,
        superuser_conn,
    )
    assert result[0][0] == 1
    superuser_conn.rollback()


def test_table_access_summary_shows_grants(superuser_conn, iceberg_extension, s3):
    """table_access_summary should list the SELECT grant we make."""
    table = "public.gov_summary_test"
    s3_path = f"s3://{TEST_BUCKET}/governance/summary_test"
    test_role = "gov_summary_reader"

    _drop_table_if_exists(superuser_conn, table)
    _create_iceberg_table(superuser_conn, table, s3_path)
    _create_role_if_not_exists(superuser_conn, test_role)

    try:
        run_command(
            f"SELECT lake_governance.grant_table_access('public','gov_summary_test','{test_role}','read')",
            superuser_conn,
        )
        superuser_conn.commit()

        rows = run_query(
            f"""
            SELECT schema_name, table_name, grantee, privileges
            FROM   lake_governance.table_access_summary
            WHERE  table_name = 'gov_summary_test'
            AND    grantee    = '{test_role}'
            """,
            superuser_conn,
        )
        assert len(rows) >= 1
        assert "SELECT" in rows[0]["privileges"]

    finally:
        run_command(
            f"SELECT lake_governance.revoke_table_access('public','gov_summary_test','{test_role}','read')",
            superuser_conn,
            raise_error=False,
        )
        _drop_table_if_exists(superuser_conn, table)
        _drop_role_if_exists(superuser_conn, test_role)


# ---------------------------------------------------------------------------
# external_sources registry
# ---------------------------------------------------------------------------

def test_external_sources_table_exists(superuser_conn, iceberg_extension):
    result = run_query(
        """
        SELECT count(*)
        FROM   pg_catalog.pg_class c
        JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE  n.nspname = 'lake_governance'
        AND    c.relname = 'external_sources'
        """,
        superuser_conn,
    )
    assert result[0][0] == 1
    superuser_conn.rollback()


def test_register_external_source(superuser_conn, iceberg_extension):
    """register_external_source inserts a row into external_sources."""
    run_command(
        """
        SELECT lake_governance.register_external_source(
            'test_s3_feed',
            's3',
            '{"bucket": "data-feeds", "prefix": "orders/"}',
            'Daily order feed from legacy ERP'
        )
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    try:
        rows = run_query(
            "SELECT * FROM lake_governance.external_sources WHERE source_name = 'test_s3_feed'",
            superuser_conn,
        )
        assert len(rows) == 1
        row = rows[0]
        assert row["source_type"] == "s3"
        assert row["is_active"] is True
        assert "orders/" in str(row["connection_info"])
    finally:
        run_command(
            "DELETE FROM lake_governance.external_sources WHERE source_name = 'test_s3_feed'",
            superuser_conn,
        )
        superuser_conn.commit()


def test_register_external_source_upsert(superuser_conn, iceberg_extension):
    """Calling register_external_source twice updates the existing row."""
    run_command(
        """
        SELECT lake_governance.register_external_source(
            'upsert_src', 'parquet', NULL, 'original description'
        )
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    run_command(
        """
        SELECT lake_governance.register_external_source(
            'upsert_src', 'csv', NULL, 'updated description'
        )
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    try:
        rows = run_query(
            "SELECT source_type, description FROM lake_governance.external_sources WHERE source_name = 'upsert_src'",
            superuser_conn,
        )
        assert len(rows) == 1
        assert rows[0]["source_type"] == "csv"
        assert rows[0]["description"] == "updated description"
    finally:
        run_command(
            "DELETE FROM lake_governance.external_sources WHERE source_name = 'upsert_src'",
            superuser_conn,
        )
        superuser_conn.commit()


def test_deregister_external_source(superuser_conn, iceberg_extension):
    """deregister_external_source marks the source inactive."""
    run_command(
        "SELECT lake_governance.register_external_source('deactivate_src', 'fdw')",
        superuser_conn,
    )
    superuser_conn.commit()

    run_command(
        "SELECT lake_governance.deregister_external_source('deactivate_src')",
        superuser_conn,
    )
    superuser_conn.commit()

    try:
        rows = run_query(
            "SELECT is_active FROM lake_governance.external_sources WHERE source_name = 'deactivate_src'",
            superuser_conn,
        )
        assert len(rows) == 1
        assert rows[0]["is_active"] is False
    finally:
        run_command(
            "DELETE FROM lake_governance.external_sources WHERE source_name = 'deactivate_src'",
            superuser_conn,
        )
        superuser_conn.commit()


def test_deregister_nonexistent_source_warns(superuser_conn, iceberg_extension):
    """deregister_external_source on an unknown name should raise a WARNING, not ERROR."""
    # psycopg2 won't raise an exception for WARNINGs; just verify the call succeeds
    run_command(
        "SELECT lake_governance.deregister_external_source('no_such_source_xyz')",
        superuser_conn,
    )
    superuser_conn.rollback()


def test_external_sources_readable_by_lake_read(superuser_conn, iceberg_extension):
    """lake_read members should be able to SELECT from external_sources."""
    result = run_query(
        "SELECT has_table_privilege('lake_read', 'lake_governance.external_sources', 'SELECT')",
        superuser_conn,
    )
    assert result[0][0] is True
    superuser_conn.rollback()
