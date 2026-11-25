#!/bin/sh

# The official image runs this file only when initializing an empty data volume.
psql --no-psqlrc --no-password --set=ON_ERROR_STOP=1 \
    --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<'SQL'
\getenv database_name POSTGRES_DB
\getenv app_role MIGRATION_DB_ROLE
\getenv app_password MIGRATION_DB_PASSWORD
\getenv app_access MIGRATION_DB_ACCESS

BEGIN;

CREATE ROLE :"app_role" LOGIN PASSWORD :'app_password';

REVOKE ALL ON DATABASE :"database_name" FROM PUBLIC;
GRANT CONNECT ON DATABASE :"database_name" TO :"app_role";
REVOKE ALL ON SCHEMA public FROM PUBLIC;
GRANT USAGE ON SCHEMA public TO :"app_role";

SELECT :'app_access' = 'write' AS can_write \gset
\if :can_write
  GRANT TEMPORARY ON DATABASE :"database_name" TO :"app_role";
  ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO :"app_role";
  ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT USAGE, SELECT ON SEQUENCES TO :"app_role";
\else
  ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO :"app_role";
\endif

COMMIT;
SQL
