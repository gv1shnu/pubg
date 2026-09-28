DO $$ BEGIN CREATE ROLE stream_writer LOGIN; EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN CREATE ROLE transformer LOGIN; EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN CREATE ROLE tableau_reader LOGIN; EXCEPTION WHEN duplicate_object THEN NULL; END $$;
\getenv writer_password WRITER_PASSWORD
\getenv transform_password TRANSFORM_PASSWORD
\getenv tableau_password TABLEAU_PASSWORD
ALTER ROLE stream_writer PASSWORD :'writer_password';
ALTER ROLE transformer PASSWORD :'transform_password';
ALTER ROLE tableau_reader PASSWORD :'tableau_password';
GRANT USAGE ON SCHEMA raw,ops TO stream_writer;
GRANT INSERT,SELECT ON raw.events,ops.rejections,ops.window_observations TO stream_writer;
GRANT UPDATE ON ops.window_observations TO stream_writer;
GRANT USAGE ON SCHEMA raw,curated,mart,ops TO transformer;
GRANT SELECT ON ALL TABLES IN SCHEMA raw,curated,mart,ops TO transformer;
GRANT UPDATE(corrected_at) ON raw.events TO transformer;
GRANT INSERT,UPDATE,DELETE ON ops.metrics,ops.batch_runs TO transformer;
GRANT CREATE ON SCHEMA mart TO transformer;
GRANT USAGE ON SCHEMA mart TO tableau_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA mart TO tableau_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE transformer IN SCHEMA mart GRANT SELECT ON TABLES TO tableau_reader;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
