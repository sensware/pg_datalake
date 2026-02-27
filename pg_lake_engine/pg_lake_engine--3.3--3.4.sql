-- Upgrade script for pg_lake_engine from 3.3 to 3.4

/*
 * lake_engine.configure_s3_compat registers an S3-compatible on-premises
 * object store (MinIO, Ceph RadosGW, etc.) with the pgduck_server query
 * engine so that s3:// URLs pointing at that endpoint are resolved
 * transparently, without users needing to connect to pgduck_server directly.
 *
 * The secret is held in pgduck_server memory for its lifetime.  To make it
 * persist across restarts, add the equivalent CREATE SECRET statement to the
 * --init_file_path SQL file passed to pgduck_server on start-up (see the
 * on-prem object storage documentation for the exact syntax).
 *
 * Parameters
 * ----------
 * name      - unique secret identifier (letters, digits, underscores, hyphens)
 * endpoint  - host:port of the S3-compatible endpoint, e.g. 'minio.corp:9000'
 * key_id    - S3 access key ID
 * secret    - S3 secret access key
 * scope     - optional s3://bucket prefix; omit (NULL) to match all buckets
 * use_ssl   - true when the endpoint uses HTTPS (default false for on-prem)
 * region    - value embedded in AWS Signature v4 headers (default 'us-east-1')
 * url_style - 'path' (default, required for MinIO/Ceph) or 'vhost'
 *
 * Example
 * -------
 *   SELECT lake_engine.configure_s3_compat(
 *       name     => 'my_minio',
 *       endpoint => 'minio.internal:9000',
 *       key_id   => 'accesskey',
 *       secret   => 'secretkey',
 *       scope    => 's3://warehouse'
 *   );
 */
CREATE OR REPLACE FUNCTION lake_engine.configure_s3_compat(
    name      text,
    endpoint  text,
    key_id    text,
    secret    text,
    scope     text    DEFAULT NULL,
    use_ssl   boolean DEFAULT false,
    region    text    DEFAULT 'us-east-1',
    url_style text    DEFAULT 'path'
)
RETURNS void
LANGUAGE C
AS 'MODULE_PATHNAME', $function$configure_s3_compat$function$;

COMMENT ON FUNCTION lake_engine.configure_s3_compat(text,text,text,text,text,boolean,text,text) IS
'Register an S3-compatible on-premises object store (MinIO, Ceph RadosGW, etc.)
with the pgduck_server query engine. Credentials are stored as a DuckDB secret
for the lifetime of the pgduck_server process.';

REVOKE ALL ON FUNCTION lake_engine.configure_s3_compat(text,text,text,text,text,boolean,text,text) FROM public;
GRANT  EXECUTE ON FUNCTION lake_engine.configure_s3_compat(text,text,text,text,text,boolean,text,text) TO lake_write;

-- ============================================================
-- Data Governance & Lifecycle — Foundation (pg_lake_engine)
-- ============================================================
--
-- This section implements the minimal data-governance layer for pg_lake:
--   • lake_admin role  — elevated governance and administration
--   • lake_governance schema — home for all governance objects
--   • RBAC helper functions — grant/revoke lake access per table
--   • table_access_summary view — current privilege snapshot
--   • external_sources registry — track data-consolidation origins
--
-- Intentionally out of scope: lineage, auditing, data-catalog UI,
-- CDC/streaming ingestion.
-- ============================================================

/*
 * Create lake_admin role if it does not yet exist.
 * Members of lake_admin can manage governance objects and administer
 * the lake_read / lake_write role hierarchy.
 */
DO LANGUAGE plpgsql $$
BEGIN
	IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'lake_admin') THEN
		CREATE ROLE lake_admin;
	END IF;
	/* lake_admin implies full lake access */
	GRANT lake_read TO lake_admin;
	GRANT lake_write TO lake_admin;
END;
$$;

/*
 * lake_governance schema holds all governance helper objects.
 * Access is restricted: only lake_admin (and superusers) may use it
 * unless a specific object grants broader access explicitly.
 */
CREATE SCHEMA lake_governance;
GRANT USAGE ON SCHEMA lake_governance TO lake_admin;
GRANT USAGE ON SCHEMA lake_governance TO lake_read;

-- ---------------------------------------------------------------
-- RBAC helper: grant_table_access
-- ---------------------------------------------------------------
--
-- Grants SELECT (and optionally INSERT/UPDATE/DELETE) on a data-lake
-- table to a role, and simultaneously ensures that role carries the
-- appropriate global lake_read / lake_write membership so the URL
-- permission checks in the C layer pass.
--
-- p_schema      target table's schema
-- p_table       target table name
-- p_grantee     role that should receive access
-- p_access      'read', 'write', or 'read_write'
--
-- The function runs with SECURITY DEFINER so that a lake_admin user
-- (who may not be a superuser) can execute GRANT statements.
-- ---------------------------------------------------------------
CREATE OR REPLACE FUNCTION lake_governance.grant_table_access(
    p_schema  text,
    p_table   text,
    p_grantee text,
    p_access  text   -- 'read' | 'write' | 'read_write'
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    v_tbl text;
BEGIN
    IF p_access NOT IN ('read', 'write', 'read_write') THEN
        RAISE EXCEPTION
            'Invalid access type ''%''. Must be ''read'', ''write'', or ''read_write''.',
            p_access;
    END IF;

    v_tbl := format('%I.%I', p_schema, p_table);

    IF p_access IN ('read', 'read_write') THEN
        EXECUTE format('GRANT lake_read TO %I', p_grantee);
        EXECUTE format('GRANT SELECT ON %s TO %I', v_tbl, p_grantee);
    END IF;

    IF p_access IN ('write', 'read_write') THEN
        EXECUTE format('GRANT lake_write TO %I', p_grantee);
        EXECUTE format('GRANT INSERT, UPDATE, DELETE ON %s TO %I', v_tbl, p_grantee);
    END IF;
END;
$$;

REVOKE ALL   ON FUNCTION lake_governance.grant_table_access(text,text,text,text) FROM public;
GRANT  EXECUTE ON FUNCTION lake_governance.grant_table_access(text,text,text,text) TO lake_admin;

COMMENT ON FUNCTION lake_governance.grant_table_access(text,text,text,text) IS
'Grant read, write, or read_write access on a data-lake table to a role.
Automatically assigns lake_read / lake_write membership as required.';

-- ---------------------------------------------------------------
-- RBAC helper: revoke_table_access
-- ---------------------------------------------------------------
CREATE OR REPLACE FUNCTION lake_governance.revoke_table_access(
    p_schema  text,
    p_table   text,
    p_grantee text,
    p_access  text   -- 'read' | 'write' | 'read_write'
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    v_tbl text;
BEGIN
    IF p_access NOT IN ('read', 'write', 'read_write') THEN
        RAISE EXCEPTION
            'Invalid access type ''%''. Must be ''read'', ''write'', or ''read_write''.',
            p_access;
    END IF;

    v_tbl := format('%I.%I', p_schema, p_table);

    IF p_access IN ('read', 'read_write') THEN
        EXECUTE format('REVOKE SELECT ON %s FROM %I', v_tbl, p_grantee);
    END IF;

    IF p_access IN ('write', 'read_write') THEN
        EXECUTE format('REVOKE INSERT, UPDATE, DELETE ON %s FROM %I', v_tbl, p_grantee);
    END IF;
END;
$$;

REVOKE ALL   ON FUNCTION lake_governance.revoke_table_access(text,text,text,text) FROM public;
GRANT  EXECUTE ON FUNCTION lake_governance.revoke_table_access(text,text,text,text) TO lake_admin;

COMMENT ON FUNCTION lake_governance.revoke_table_access(text,text,text,text) IS
'Revoke read, write, or read_write access on a data-lake table from a role.';

-- ---------------------------------------------------------------
-- View: table_access_summary
-- Shows current privilege grants on all non-system tables in a
-- user-visible schema, filtered to roles relevant to pg_lake.
-- ---------------------------------------------------------------
CREATE OR REPLACE VIEW lake_governance.table_access_summary AS
SELECT
    rtg.table_schema                                            AS schema_name,
    rtg.table_name                                             AS table_name,
    c.relkind                                                  AS table_type,
    rtg.grantee                                                AS grantee,
    string_agg(rtg.privilege_type, ', ' ORDER BY rtg.privilege_type) AS privileges,
    bool_or(rtg.is_grantable = 'YES')                         AS is_grantable
FROM information_schema.role_table_grants rtg
JOIN pg_catalog.pg_class      c ON c.relname   = rtg.table_name
JOIN pg_catalog.pg_namespace  n ON n.oid        = c.relnamespace
                                AND n.nspname   = rtg.table_schema
WHERE rtg.table_schema NOT IN (
        'pg_catalog', 'information_schema',
        '__lake__internal__nsp__', 'lake_engine',
        'lake_struct', 'pg_temp'
     )
GROUP BY rtg.table_schema, rtg.table_name, c.relkind, rtg.grantee
ORDER BY rtg.table_schema, rtg.table_name, rtg.grantee;

GRANT SELECT ON lake_governance.table_access_summary TO lake_read;

COMMENT ON VIEW lake_governance.table_access_summary IS
'Current privilege grants on data-lake tables (SELECT, INSERT, UPDATE, DELETE).';

-- ---------------------------------------------------------------
-- Data-consolidation source registry
-- ---------------------------------------------------------------
--
-- Stores metadata about external systems that feed data into this
-- lakehouse via COPY or FDW ingestion.  This is a lightweight
-- registry only; no CDC or streaming connectors are implemented.
-- ---------------------------------------------------------------
CREATE TABLE lake_governance.external_sources (
    source_name        text        PRIMARY KEY,
    source_type        text        NOT NULL,      -- e.g. 'parquet', 'csv', 's3', 'fdw', 'copy'
    connection_info    jsonb,                      -- endpoint, bucket, prefix, etc.
    description        text,
    registered_by      text        NOT NULL DEFAULT current_user,
    registered_at      timestamptz NOT NULL DEFAULT now(),
    last_updated_at    timestamptz NOT NULL DEFAULT now(),
    is_active          boolean     NOT NULL DEFAULT true
);
GRANT SELECT            ON lake_governance.external_sources TO lake_read;
GRANT INSERT, UPDATE, DELETE ON lake_governance.external_sources TO lake_write;

COMMENT ON TABLE lake_governance.external_sources IS
'Registry of external data sources (S3, FDW, COPY) used for data consolidation ingestion.';

-- register_external_source: upsert a source entry
CREATE OR REPLACE FUNCTION lake_governance.register_external_source(
    p_name            text,
    p_type            text,
    p_connection_info jsonb   DEFAULT NULL,
    p_description     text    DEFAULT NULL
)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO lake_governance.external_sources
        (source_name, source_type, connection_info, description)
    VALUES
        (p_name, p_type, p_connection_info, p_description)
    ON CONFLICT (source_name) DO UPDATE SET
        source_type        = EXCLUDED.source_type,
        connection_info    = EXCLUDED.connection_info,
        description        = EXCLUDED.description,
        last_updated_at    = now(),
        is_active          = true;
END;
$$;
REVOKE ALL   ON FUNCTION lake_governance.register_external_source(text,text,jsonb,text) FROM public;
GRANT  EXECUTE ON FUNCTION lake_governance.register_external_source(text,text,jsonb,text) TO lake_write;

COMMENT ON FUNCTION lake_governance.register_external_source(text,text,jsonb,text) IS
'Register or update an external data source in the consolidation registry.';

-- deregister_external_source: soft-delete by marking inactive
CREATE OR REPLACE FUNCTION lake_governance.deregister_external_source(
    p_name text
)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE lake_governance.external_sources
    SET    is_active       = false,
           last_updated_at = now()
    WHERE  source_name = p_name;

    IF NOT FOUND THEN
        RAISE WARNING 'External source ''%'' not found.', p_name;
    END IF;
END;
$$;
REVOKE ALL   ON FUNCTION lake_governance.deregister_external_source(text) FROM public;
GRANT  EXECUTE ON FUNCTION lake_governance.deregister_external_source(text) TO lake_write;

COMMENT ON FUNCTION lake_governance.deregister_external_source(text) IS
'Mark an external data source as inactive in the consolidation registry.';
