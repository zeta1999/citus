CREATE OR REPLACE FUNCTION citus_internal.invalidate_inactive_shared_connections()
RETURNS trigger
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$invalidate_inactive_shared_connections$$;

COMMENT ON FUNCTION citus_internal.invalidate_inactive_shared_connections()
     IS 'invalidated inactive shared connections';

REVOKE ALL ON FUNCTION citus_internal.invalidate_inactive_shared_connections()
FROM PUBLIC;
