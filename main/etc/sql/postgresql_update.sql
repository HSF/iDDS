--2023.04.01
alter table health add column status INTEGER;
alter table contents_update add column content_metadata VARCHAR(100);
alter table contents_update add column fetch_status INTEGER DEFAULT 0;

