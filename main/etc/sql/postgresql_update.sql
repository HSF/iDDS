--2023.04.01
alter table health add column status INTEGER;
alter table contents_update add column content_metadata VARCHAR(100);
alter table contents_update add column fetch_status INTEGER DEFAULT 0;


-- 2023.09.26
-- update slac idds database, without updating the idds models
alter table contents alter column name type varchar(8000);

-- 2023.11.09
-- update slac idds database, without updating the idds models
alter table contents alter column name type varchar(40000);

--2024.01.06
alter table contents alter column content_metadata type VARCHAR(1000);
alter table contents_update alter column content_metadata type VARCHAR(1000);
