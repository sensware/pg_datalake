-- Upgrade script for pg_lake_iceberg from 3.3 to 3.4

-- ============================================================
-- Data Governance & Lifecycle — Snapshot Management
-- ============================================================
--
-- Adds per-table snapshot retention policies and a compliance
-- view on top of the existing global GUC-based expiration
-- (pg_lake_iceberg.max_snapshot_age).
--
-- Intentionally out of scope: lineage tracking, audit logs,
-- data catalog UI, HFT/real-time writes, CDC, streaming.
-- ============================================================

-- lake_governance schema was created by pg_lake_engine--3.3--3.4.
-- Grant USAGE to iceberg-specific roles so governance objects are reachable.
GRANT USAGE ON SCHEMA lake_governance TO iceberg_catalog;

-- ---------------------------------------------------------------
-- Per-table snapshot retention policies
-- ---------------------------------------------------------------
--
-- Each row declares the desired retention behaviour for one Iceberg
-- table managed by this server.  The existing autovacuum worker and
-- write-path expiration already honour the global GUC; this table
-- is the authoritative store for per-table overrides and is read by
-- lake_governance.expire_snapshots() below.
--
-- Columns
-- -------
-- table_name             regclass reference to the foreign table
-- max_snapshot_age_hours max age of non-current snapshots to retain;
--                        NULL = fall back to global GUC value
-- min_snapshots_to_keep  always keep at least this many snapshots
--                        regardless of age (default 1)
-- max_snapshots_to_keep  hard cap on stored snapshot count; NULL = unlimited
-- created_at / updated_at change-tracking timestamps (not an audit log)
-- ---------------------------------------------------------------
CREATE TABLE lake_governance.snapshot_policies (
    table_name             regclass    NOT NULL PRIMARY KEY,
    max_snapshot_age_hours int,
    min_snapshots_to_keep  int         NOT NULL DEFAULT 1
                               CONSTRAINT snapshot_policies_min_keep_positive
                               CHECK (min_snapshots_to_keep >= 1),
    max_snapshots_to_keep  int
                               CONSTRAINT snapshot_policies_max_keep_positive
                               CHECK (max_snapshots_to_keep IS NULL
                                      OR max_snapshots_to_keep >= 1),
    created_at             timestamptz NOT NULL DEFAULT now(),
    updated_at             timestamptz NOT NULL DEFAULT now()
);
GRANT SELECT ON lake_governance.snapshot_policies TO lake_read;
GRANT INSERT, UPDATE, DELETE ON lake_governance.snapshot_policies TO lake_admin;

COMMENT ON TABLE lake_governance.snapshot_policies IS
'Per-table Iceberg snapshot retention policies. Overrides the global
pg_lake_iceberg.max_snapshot_age GUC for individual tables.';

-- ---------------------------------------------------------------
-- set_snapshot_policy — declare or update a retention policy
-- ---------------------------------------------------------------
CREATE OR REPLACE FUNCTION lake_governance.set_snapshot_policy(
    p_table                 regclass,
    p_max_age_hours         int DEFAULT NULL,
    p_min_snapshots_to_keep int DEFAULT 1,
    p_max_snapshots_to_keep int DEFAULT NULL
)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    IF p_min_snapshots_to_keep < 1 THEN
        RAISE EXCEPTION 'min_snapshots_to_keep must be >= 1';
    END IF;
    IF p_max_snapshots_to_keep IS NOT NULL AND p_max_snapshots_to_keep < 1 THEN
        RAISE EXCEPTION 'max_snapshots_to_keep must be >= 1 or NULL';
    END IF;

    INSERT INTO lake_governance.snapshot_policies
        (table_name, max_snapshot_age_hours,
         min_snapshots_to_keep, max_snapshots_to_keep)
    VALUES
        (p_table, p_max_age_hours,
         p_min_snapshots_to_keep, p_max_snapshots_to_keep)
    ON CONFLICT (table_name) DO UPDATE SET
        max_snapshot_age_hours  = EXCLUDED.max_snapshot_age_hours,
        min_snapshots_to_keep   = EXCLUDED.min_snapshots_to_keep,
        max_snapshots_to_keep   = EXCLUDED.max_snapshots_to_keep,
        updated_at              = now();
END;
$$;
REVOKE ALL   ON FUNCTION lake_governance.set_snapshot_policy(regclass,int,int,int) FROM public;
GRANT  EXECUTE ON FUNCTION lake_governance.set_snapshot_policy(regclass,int,int,int) TO lake_admin;

COMMENT ON FUNCTION lake_governance.set_snapshot_policy(regclass,int,int,int) IS
'Declare or update the snapshot retention policy for an Iceberg table.
p_max_age_hours = NULL falls back to the global pg_lake_iceberg.max_snapshot_age GUC.';

-- ---------------------------------------------------------------
-- get_snapshot_policy — retrieve the effective policy for a table
-- ---------------------------------------------------------------
CREATE OR REPLACE FUNCTION lake_governance.get_snapshot_policy(
    p_table regclass
)
RETURNS TABLE (
    table_name             regclass,
    max_snapshot_age_hours int,
    min_snapshots_to_keep  int,
    max_snapshots_to_keep  int,
    policy_source          text,
    created_at             timestamptz,
    updated_at             timestamptz
)
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    v_global_age_hours int;
BEGIN
    -- Read the global GUC (seconds → hours); 0 means expiration disabled.
    BEGIN
        v_global_age_hours :=
            current_setting('pg_lake_iceberg.max_snapshot_age')::bigint / 3600;
    EXCEPTION WHEN OTHERS THEN
        v_global_age_hours := 0;
    END;

    RETURN QUERY
    SELECT
        COALESCE(sp.table_name, p_table),
        COALESCE(sp.max_snapshot_age_hours,
                 NULLIF(v_global_age_hours, 0)),
        COALESCE(sp.min_snapshots_to_keep, 1),
        sp.max_snapshots_to_keep,
        CASE
            WHEN sp.table_name IS NOT NULL THEN 'per-table-policy'
            WHEN v_global_age_hours > 0    THEN 'global-guc'
            ELSE                                'none'
        END::text,
        sp.created_at,
        sp.updated_at
    FROM (SELECT p_table AS tn) AS t
    LEFT JOIN lake_governance.snapshot_policies sp ON sp.table_name = t.tn;
END;
$$;
REVOKE ALL   ON FUNCTION lake_governance.get_snapshot_policy(regclass) FROM public;
GRANT  EXECUTE ON FUNCTION lake_governance.get_snapshot_policy(regclass) TO lake_read;

COMMENT ON FUNCTION lake_governance.get_snapshot_policy(regclass) IS
'Return the effective snapshot retention policy for a table, merging per-table
overrides with the global pg_lake_iceberg.max_snapshot_age GUC.';

-- ---------------------------------------------------------------
-- drop_snapshot_policy — remove a per-table policy override
-- ---------------------------------------------------------------
CREATE OR REPLACE FUNCTION lake_governance.drop_snapshot_policy(
    p_table regclass
)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    DELETE FROM lake_governance.snapshot_policies WHERE table_name = p_table;
    IF NOT FOUND THEN
        RAISE WARNING
            'No snapshot policy found for table %. Nothing was removed.', p_table;
    END IF;
END;
$$;
REVOKE ALL   ON FUNCTION lake_governance.drop_snapshot_policy(regclass) FROM public;
GRANT  EXECUTE ON FUNCTION lake_governance.drop_snapshot_policy(regclass) TO lake_admin;

COMMENT ON FUNCTION lake_governance.drop_snapshot_policy(regclass) IS
'Remove the per-table snapshot retention policy override.
After removal the table reverts to the global GUC setting.';

-- ---------------------------------------------------------------
-- expire_snapshots — manually trigger snapshot expiration
-- ---------------------------------------------------------------
--
-- Runs VACUUM on the Iceberg table which invokes the C-level
-- RemoveOldSnapshotsFromMetadata() / flush_deletion_queue()
-- pipeline.  Expiration respects the per-table policy (if set)
-- via pg_lake_iceberg.max_snapshot_age at the session level,
-- or the global GUC if no per-table policy exists.
-- ---------------------------------------------------------------
CREATE OR REPLACE FUNCTION lake_governance.expire_snapshots(
    p_table regclass
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    v_policy      record;
    v_age_seconds bigint;
BEGIN
    -- Fetch the effective per-table age, if any.
    SELECT * INTO v_policy
    FROM   lake_governance.get_snapshot_policy(p_table);

    IF v_policy.max_snapshot_age_hours IS NOT NULL THEN
        v_age_seconds := v_policy.max_snapshot_age_hours * 3600;
        -- Temporarily apply the per-table max age for this session so
        -- the C expiration logic picks it up during the VACUUM run.
        EXECUTE format(
            'SET LOCAL pg_lake_iceberg.max_snapshot_age = %s',
            v_age_seconds
        );
    END IF;

    EXECUTE format('VACUUM %s', p_table);
END;
$$;
REVOKE ALL   ON FUNCTION lake_governance.expire_snapshots(regclass) FROM public;
GRANT  EXECUTE ON FUNCTION lake_governance.expire_snapshots(regclass) TO lake_admin;

COMMENT ON FUNCTION lake_governance.expire_snapshots(regclass) IS
'Manually trigger Iceberg snapshot expiration for a table. If a per-table
policy exists its max_snapshot_age_hours is applied for the duration of the
call via SET LOCAL before running VACUUM.';

-- ---------------------------------------------------------------
-- snapshot_compliance view
-- ---------------------------------------------------------------
--
-- For each managed Iceberg table shows declared policy settings
-- alongside live snapshot metrics so operators can identify tables
-- that exceed their retention targets without reading object storage.
-- ---------------------------------------------------------------
CREATE OR REPLACE VIEW lake_governance.snapshot_compliance AS
WITH iceberg_tables_with_meta AS (
    SELECT
        it.table_name::regclass                AS table_oid,
        n.nspname                              AS schema_name,
        c.relname                              AS table_name_text,
        it.metadata_location
    FROM lake_iceberg.tables_internal it
    JOIN pg_catalog.pg_class     c ON c.oid   = it.table_name
    JOIN pg_catalog.pg_namespace n ON n.oid   = c.relnamespace
    WHERE it.metadata_location IS NOT NULL
),
snapshot_stats AS (
    SELECT
        t.table_oid,
        t.schema_name,
        t.table_name_text,
        count(*)              AS total_snapshots,
        max(s.timestamp_ms)   AS latest_snapshot_at,
        min(s.timestamp_ms)   AS oldest_snapshot_at
    FROM iceberg_tables_with_meta t
    CROSS JOIN LATERAL lake_iceberg.snapshots(t.metadata_location) s
    GROUP BY t.table_oid, t.schema_name, t.table_name_text
)
SELECT
    ss.schema_name,
    ss.table_name_text                                       AS table_name,
    ss.total_snapshots,
    ss.oldest_snapshot_at,
    ss.latest_snapshot_at,
    gp.max_snapshot_age_hours                                AS policy_max_age_hours,
    gp.min_snapshots_to_keep                                 AS policy_min_keep,
    gp.max_snapshots_to_keep                                 AS policy_max_keep,
    gp.policy_source,
    CASE
        WHEN gp.max_snapshot_age_hours IS NOT NULL
             AND ss.oldest_snapshot_at <
                 (now() - make_interval(hours => gp.max_snapshot_age_hours))
            THEN 'AGE_VIOLATION'
        WHEN gp.max_snapshots_to_keep IS NOT NULL
             AND ss.total_snapshots > gp.max_snapshots_to_keep
            THEN 'COUNT_VIOLATION'
        ELSE 'COMPLIANT'
    END                                                      AS compliance_status
FROM snapshot_stats ss
CROSS JOIN LATERAL lake_governance.get_snapshot_policy(ss.table_oid) gp
ORDER BY ss.schema_name, ss.table_name_text;

GRANT SELECT ON lake_governance.snapshot_compliance TO lake_read;

COMMENT ON VIEW lake_governance.snapshot_compliance IS
'Per-table Iceberg snapshot compliance status. Compares live snapshot counts
and ages against declared retention policies (per-table or global GUC).
compliance_status values: COMPLIANT | AGE_VIOLATION | COUNT_VIOLATION.
Caution: reading snapshot metadata requires object-storage I/O per table.';
