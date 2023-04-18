CREATE USER doma_idds_r WITH PASSWORD 'test_idds';
GRANT CONNECT ON DATABASE doma_idds TO doma_idds_r;
GRANT USAGE ON SCHEMA doma_idds TO doma_idds_r;
GRANT SELECT ON ALL TABLES IN SCHEMA doma_idds TO doma_idds_r;
ALTER DEFAULT PRIVILEGES IN SCHEMA doma_idds GRANT SELECT ON TABLES TO doma_idds_r;

CREATE USER doma_idds WITH PASSWORD 'test_idds_pass';
create database doma_idds;
\connect doma_idds;
create schema if not exists doma_idds authorization doma_idds;
GRANT CONNECT ON DATABASE doma_idds TO doma_idds;
GRANT USAGE ON SCHEMA doma_idds TO doma_idds;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA doma_idds TO doma_idds;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA doma_idds TO doma_idds;
GRANT ALL PRIVILEGES ON DATABASE doma_idds TO doma_idds;
#ALTER DEFAULT PRIVILEGES IN SCHEMA doma_idds GRANT SELECT ON TABLES TO doma_idds;

set search_path to doma_idds;
