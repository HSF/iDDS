CREATE USER doma_idds_r WITH PASSWORD 'Tiaroa4dr_idds';
GRANT CONNECT ON DATABASE doma_idds TO doma_idds_r;
GRANT USAGE ON SCHEMA doma_idds TO doma_idds_r;
GRANT SELECT ON ALL TABLES IN SCHEMA doma_idds TO doma_idds_r;
ALTER DEFAULT PRIVILEGES IN SCHEMA doma_idds GRANT SELECT ON TABLES TO doma_idds_r;

