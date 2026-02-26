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
