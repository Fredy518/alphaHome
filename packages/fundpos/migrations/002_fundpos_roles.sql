DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'fundpos_reader') THEN
        CREATE ROLE fundpos_reader NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'fundpos_writer') THEN
        CREATE ROLE fundpos_writer NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'fundpos_migrator') THEN
        CREATE ROLE fundpos_migrator NOLOGIN;
    END IF;
END $$;

REVOKE ALL ON SCHEMA fundpos FROM PUBLIC;
GRANT USAGE ON SCHEMA fundpos TO fundpos_reader, fundpos_writer;
GRANT USAGE, CREATE ON SCHEMA fundpos TO fundpos_migrator;

GRANT SELECT ON ALL TABLES IN SCHEMA fundpos TO fundpos_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA fundpos TO fundpos_writer;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA fundpos TO fundpos_migrator;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA fundpos TO fundpos_writer;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA fundpos TO fundpos_migrator;

ALTER DEFAULT PRIVILEGES IN SCHEMA fundpos
    GRANT SELECT ON TABLES TO fundpos_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA fundpos
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO fundpos_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA fundpos
    GRANT ALL PRIVILEGES ON TABLES TO fundpos_migrator;
ALTER DEFAULT PRIVILEGES IN SCHEMA fundpos
    GRANT USAGE, SELECT ON SEQUENCES TO fundpos_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA fundpos
    GRANT ALL PRIVILEGES ON SEQUENCES TO fundpos_migrator;

COMMENT ON ROLE fundpos_reader IS 'NOLOGIN capability role for read-only fundpos consumers';
COMMENT ON ROLE fundpos_writer IS 'NOLOGIN capability role for fundpos ingestion and publication';
COMMENT ON ROLE fundpos_migrator IS 'NOLOGIN capability role for reviewed fundpos migrations';
