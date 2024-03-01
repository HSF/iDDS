-- 2022.08.23
alter table REQUESTS add (oldstatus NUMBER(2));
alter table REQUESTS add (new_retries NUMBER(5) DEFAULT 0);
alter table REQUESTS add (update_retries NUMBER(5) DEFAULT 0);
alter table REQUESTS add (max_new_retries NUMBER(5) DEFAULT 3);
alter table REQUESTS add (max_update_retries NUMBER(5) DEFAULT 0);
# alter table REQUESTS add (new_poll_period NUMBER(10) DEFAULT 10);
# alter table REQUESTS add (update_poll_period NUMBER(10) DEFAULT 10);
# alter table REQUESTS drop column new_poll_period
# alter table REQUESTS drop column update_poll_period
alter table REQUESTS add (new_poll_period INTERVAL DAY TO SECOND DEFAULT '00 00:00:01');
alter table REQUESTS add (update_poll_period INTERVAL DAY TO SECOND DEFAULT '00 00:00:10');

alter table TRANSFORMS add (oldstatus NUMBER(2));
alter table TRANSFORMS add (new_retries NUMBER(5) DEFAULT 0);
alter table TRANSFORMS add (update_retries NUMBER(5) DEFAULT 0);
alter table TRANSFORMS add (max_new_retries NUMBER(5) DEFAULT 3);
alter table TRANSFORMS add (max_update_retries NUMBER(5) DEFAULT 0);
#alter table TRANSFORMS add (new_poll_period NUMBER(10) DEFAULT 10);
#alter table TRANSFORMS add (update_poll_period NUMBER(10) DEFAULT 10);
alter table TRANSFORMS add (errors VARCHAR2(1024));
# alter table TRANSFORMS drop column new_poll_period
# alter table TRANSFORMS drop column update_poll_period
alter table TRANSFORMS add (new_poll_period INTERVAL DAY TO SECOND DEFAULT '00 00:00:01');
alter table TRANSFORMS add (update_poll_period INTERVAL DAY TO SECOND DEFAULT '00 00:00:10');


alter table PROCESSINGS add (oldstatus NUMBER(2));
alter table PROCESSINGS add (new_retries NUMBER(5) DEFAULT 0);
alter table PROCESSINGS add (update_retries NUMBER(5) DEFAULT 0);
alter table PROCESSINGS add (max_new_retries NUMBER(5) DEFAULT 3);
alter table PROCESSINGS add (max_update_retries NUMBER(5) DEFAULT 0);
#alter table PROCESSINGS add (new_poll_period NUMBER(10) DEFAULT 10);
#alter table PROCESSINGS add (update_poll_period NUMBER(10) DEFAULT 10);
alter table PROCESSINGS add (errors VARCHAR2(1024));
# alter table PROCESSINGS drop column new_poll_period
# alter table PROCESSINGS drop column update_poll_period
alter table PROCESSINGS add (new_poll_period INTERVAL DAY TO SECOND DEFAULT '00 00:00:01');
alter table PROCESSINGS add (update_poll_period INTERVAL DAY TO SECOND DEFAULT '00 00:00:10');


alter table MESSAGES add (retries NUMBER(5) DEFAULT 0);

-- oracle 11
CREATE SEQUENCE COMMAND_ID_SEQ MINVALUE 1 INCREMENT BY 1 START WITH 1 NOCACHE NOORDER NOCYCLE;
CREATE TABLE COMMANDS
(
    cmd_id NUMBER(12),
    request_id NUMBER(12),
    workload_id NUMBER(10),
    transform_id NUMBER(12),
    processing_id NUMBER(12),
    cmd_type NUMBER(2),
    status NUMBER(2),
    substatus NUMBER(2),
    locking NUMBER(2),
    username VARCHAR2(20),
    retries NUMBER(5) DEFAULT 0,
    source NUMBER(2),
    destination NUMBER(2),
    num_contents NUMBER(7),
    created_at DATE DEFAULT SYS_EXTRACT_UTC(systimestamp(0)),
    updated_at DATE DEFAULT SYS_EXTRACT_UTC(systimestamp(0)),
    cmd_content CLOB,
    errors VARCHAR2(1024),
    CONSTRAINT COMMANDS_PK PRIMARY KEY (cmd_id) -- USING INDEX LOCAL,
);

CREATE OR REPLACE TRIGGER TRIG_COMMAND_ID
    BEFORE INSERT
    ON COMMANDS
    FOR EACH ROW
    BEGIN
        :NEW.cmd_id := COMMAND_ID_SEQ.NEXTVAL ;
    END;
 /

CREATE INDEX COMMANDS_TYPE_ST_IDX ON COMMANDS (cmd_type, status, destination, request_id);
CREATE INDEX COMMANDS_TYPE_ST_TF_IDX ON COMMANDS (cmd_type, status, destination, transform_id);
CREATE INDEX COMMANDS_TYPE_ST_PR_IDX ON COMMANDS (cmd_type, status, destination, processing_id);

-- oracle 19
CREATE SEQUENCE COMMAND_ID_SEQ MINVALUE 1 INCREMENT BY 1 START WITH 1 NOCACHE ORDER NOCYCLE GLOBAL;
CREATE TABLE COMMANDS
(
    cmd_id NUMBER(12) DEFAULT ON NULL COMMAND_ID_SEQ.NEXTVAL constraint COMMAND_ID_NN NOT NULL,
    request_id NUMBER(12),
    workload_id NUMBER(10),
    transform_id NUMBER(12),
    processing_id NUMBER(12),
    cmd_type NUMBER(2),
    status NUMBER(2),
    substatus NUMBER(2),
    locking NUMBER(2),
    username VARCHAR2(20),
    retries NUMBER(5) DEFAULT 0,
    source NUMBER(2),
    destination NUMBER(2),
    num_contents NUMBER(7),
    created_at DATE DEFAULT SYS_EXTRACT_UTC(systimestamp(0)),
    updated_at DATE DEFAULT SYS_EXTRACT_UTC(systimestamp(0)),
    cmd_content CLOB,
    errors VARCHAR2(1024),
    CONSTRAINT COMMANDS_PK PRIMARY KEY (cmd_id) -- USING INDEX LOCAL,
);

CREATE INDEX COMMANDS_TYPE_ST_IDX ON COMMANDS (cmd_type, status, destination, request_id);
CREATE INDEX COMMANDS_TYPE_ST_TF_IDX ON COMMANDS (cmd_type, status, destination, transform_id);
CREATE INDEX COMMANDS_TYPE_ST_PR_IDX ON COMMANDS (cmd_type, status, destination, processing_id);

-- 2022.11.03
alter table transforms add name VARCHAR2(255);
alter table collections add failed_files NUMBER(10);
alter table collections add missing_files NUMBER(10);


-- 2022.11.30
alter table contents add content_dep_id NUMBER(12);
CREATE INDEX CONTENTS_DEP_IDX ON CONTENTS (request_id, transform_id, content_dep_id) LOCAL;

-- 2022.12.01
alter table requests modify username VARCHAR2(50) default null;

-- 2022.12.06
alter table collections add ext_files NUMBER(10);
alter table collections add processed_ext_files NUMBER(10);
alter table collections add failed_ext_files NUMBER(10);
alter table collections add missing_ext_files NUMBER(10);


-- 2022.12.08
-- oracle 19
CREATE TABLE CONTENTS_ext
(
        content_id NUMBER(12) constraint CONTENT_EXT_PK_NN NOT NULL,
        transform_id NUMBER(12) constraint CONTENT_EXT_TF_ID_NN NOT NULL,
        coll_id NUMBER(14),
        request_id NUMBER(12),
        workload_id NUMBER(10),
        map_id NUMBER(12) DEFAULT 0,
        status NUMBER(2) constraint CONTENT_EXT_STATUS_NN NOT NULL,
	panda_id NUMBER(14),
	job_definition_id NUMBER(12),
	scheduler_id VARCHAR2(128),
        pilot_id VARCHAR2(200),
	creation_time DATE,
	modification_time DATE,
        start_time DATE,
	end_time DATE,
	prod_source_label VARCHAR2(20),
        prod_user_id VARCHAR2(250),
	assigned_priority NUMBER(5),
	current_priority NUMBER(5),
        attempt_nr NUMBER(5),
	max_attempt NUMBER(5),
	max_cpu_count NUMBER(5),
        max_cpu_unit VARCHAR2(32),
	max_disk_count NUMBER(12),
	max_disk_unit VARCHAR2(10),
        min_ram_count NUMBER(12),
	min_ram_unit VARCHAR2(10),
	cpu_consumption_time NUMBER(12),
        cpu_consumption_unit VARCHAR2(128),
	job_status VARCHAR2(10),
	job_name VARCHAR2(255),
        trans_exit_code NUMBER(5),
	pilot_error_code NUMBER(5),
	pilot_error_diag VARCHAR2(500),
        exe_error_code NUMBER(5),
	exe_error_diag VARCHAR2(500),
	sup_error_code NUMBER(5),
        sup_error_diag VARCHAR2(250),
	ddm_error_code NUMBER(5),
	ddm_error_diag VARCHAR2(500),
        brokerage_error_code NUMBER(5),
	brokerage_error_diag VARCHAR2(250),
        job_dispatcher_error_code NUMBER(5),
	job_dispatcher_error_diag VARCHAR2(250),
        task_buffer_error_code NUMBER(5),
	task_buffer_error_diag VARCHAR2(300),
        computing_site VARCHAR2(128),
	computing_element VARCHAR2(128),
        grid VARCHAR2(50),
	cloud VARCHAR2(50),
	cpu_conversion float(20),
	task_id NUMBER(12),
        vo VARCHAR2(16),
	pilot_timing VARCHAR2(100),
	working_group VARCHAR2(20),
        processing_type VARCHAR2(64),
	prod_user_name VARCHAR2(60),
	core_count NUMBER(5),
        n_input_files NUMBER(10),
	req_id NUMBER(12),
	jedi_task_id NUMBER(12),
        actual_core_count NUMBER(5),
	max_rss NUMBER(12),
	max_vmem NUMBER(12),
        max_swap NUMBER(12),
	max_pss NUMBER(12),
	avg_rss NUMBER(12),
	avg_vmem NUMBER(12),
        avg_swap NUMBER(12),
	avg_pss NUMBER(12),
	max_walltime NUMBER(12),
	disk_io NUMBER(12),
        failed_attempt NUMBER(5),
	hs06 NUMBER(12),
	hs06sec NUMBER(12),
        memory_leak VARCHAR2(10),
	memory_leak_x2 VARCHAR2(10),
	job_label VARCHAR2(20),
        CONSTRAINT CONTENT_EXT_PK PRIMARY KEY (content_id),
	CONSTRAINT CONTENT_EXT_TRANSFORM_ID_FK FOREIGN KEY(transform_id) REFERENCES TRANSFORMS(transform_id)
)
PCTFREE 3
PARTITION BY RANGE(TRANSFORM_ID)
INTERVAL ( 100000 )
( PARTITION initial_part VALUES LESS THAN (1) );

CREATE INDEX CONTENTS_EXT_RTF_IDX ON CONTENTS_ext (request_id, transform_id, workload_id, coll_id, content_id, panda_id, status) LOCAL;


-- 2022.12.29
create table contents_update(content_id number(12), substatus number(2))
CREATE TRIGGER update_content_dep_status before delete ON contents_update
     for each row
        BEGIN
           update contents set substatus = :old.substatus where contents.content_dep_id = :old.content_id;
        END;

-- 2023.01.24
alter table CONTENTS_ext modify max_cpu_count NUMBER(12);

-- 2023.01.25
alter table contents_update add (
	request_id NUMBER(12),
	transform_id NUMBER(12),
	workload_id NUMBER(10),
	coll_id NUMBER(14));

-- 2023.02.01
--- update (select c.content_id, c.substatus as c_substatus, t.substatus as t_substatus from  contents c inner join
-- (select content_id, substatus from contents where request_id=486 and transform_id=3027 and content_relation_type =1 and status != substatus) t
--- on c.content_dep_id = t.content_id where c.request_id=486 and c.substatus != t.substatus) set c_substatus = t_substatus;

--- remove 
"""
CREATE OR REPLACE FUNCTION update_contents_to_others(request_id_in IN NUMBER, transform_id_in IN NUMBER)
RETURN NUMBER
IS num_rows NUMBER;
BEGIN
    update (select c.content_id, c.substatus as c_substatus, t.substatus as t_substatus from  contents c inner join
    (select content_id, substatus from contents where request_id = request_id_in and transform_id = transform_id_in and content_relation_type = 1 and status != substatus) t
    on c.content_dep_id = t.content_id where c.request_id = request_id_in and c.substatus != t.substatus) set c_substatus = t_substatus;
    
    num_rows := SQL%rowcount;
    RETURN (num_rows);
END;

CREATE OR REPLACE FUNCTION update_contents_from_others(request_id_in IN NUMBER, transform_id_in IN NUMBER)
RETURN NUMBER
IS num_rows NUMBER;
BEGIN
    update (select c.content_id, c.substatus as c_substatus, t.substatus as t_substatus from  contents c inner join
    (select content_id, substatus from contents where request_id = request_id_in and content_relation_type = 1 and status != 0) t
    on c.content_dep_id = t.content_id where c.request_id = request_id_in and c.transform_id = transform_id_in and c.substatus != t.substatus) set c_substatus = t_substatus;

    num_rows := SQL%rowcount;
    RETURN (num_rows);
END;
"""

CREATE OR REPLACE procedure update_contents_from_others(request_id_in NUMBER, transform_id_in NUMBER) AS
BEGIN
    update (select c.content_id, c.substatus as c_substatus, t.substatus as t_substatus from  contents c inner join
    (select content_id, substatus from contents where request_id = request_id_in and content_relation_type = 1) t
    on c.content_dep_id = t.content_id where c.request_id = request_id_in and c.transform_id = transform_id_in and c.content_relation_type = 3 and c.substatus != t.substatus) set c_substatus = t_substatus;
END;


CREATE OR REPLACE procedure update_contents_to_others(request_id_in NUMBER, transform_id_in NUMBER) AS
BEGIN
    update (select c.content_id, c.substatus as c_substatus, t.substatus as t_substatus from  contents c inner join
    (select content_id, substatus from contents where request_id = request_id_in and transform_id = transform_id_in and content_relation_type = 1) t
    on c.content_dep_id = t.content_id where c.request_id = request_id_in and c.content_relation_type = 3 and c.substatus != t.substatus) set c_substatus = t_substatus;
END;



-- 2023.02.14
drop index CONTENTS_REQ_TF_COLL_IDX
CREATE INDEX CONTENTS_REQ_TF_COLL_IDX ON CONTENTS (request_id, transform_id, workload_id, coll_id, content_relation_type, status, substatus) LOCAL;


-- 2023.02.22

CREATE OR REPLACE procedure update_contents_from_others(request_id_in NUMBER, transform_id_in NUMBER) AS
BEGIN
    update (select c.content_id, c.substatus as c_substatus, t.substatus as t_substatus from  
    (select content_id, substatus, content_dep_id from contents where request_id = request_id_in and transform_id = transform_id_in and content_relation_type = 3) c inner join
    (select content_id, substatus from contents where request_id = request_id_in and content_relation_type = 1) t
    on c.content_dep_id = t.content_id where c.substatus != t.substatus) set c_substatus = t_substatus;
END;


CREATE OR REPLACE procedure update_contents_to_others(request_id_in NUMBER, transform_id_in NUMBER) AS
BEGIN
    update (select c.content_id, c.substatus as c_substatus, t.substatus as t_substatus from  
    (select content_id, substatus, content_dep_id from contents where request_id = request_id_in and content_relation_type = 3) c inner join
    (select content_id, substatus from contents where request_id = request_id_in and transform_id = transform_id_in and content_relation_type = 1) t
    on c.content_dep_id = t.content_id where c.substatus != t.substatus) set c_substatus = t_substatus;
END;



--- 2023.03.06
drop index PROCESSINGS_STATUS_POLL_IDX;
drop index CONTENTS_REL_IDX;
drop index CONTENTS_TF_IDX;
drop index CONTENTS_EXT_RTW_IDX;
drop index CONTENTS_EXT_RTM_IDX;
drop index COMMANDS_STATUS_IDX;
drop index MESSAGES_ST_IDX;
drop index MESSAGES_TYPE_STU_IDX;
drop index REQUESTS_STATUS_POLL_IDX;
drop index TRANSFORMS_REQ_IDX;
drop index TRANSFORMS_STATUS_POLL_IDX;
drop index COLLECTIONS_REQ_IDX;

CREATE INDEX PROCESSINGS_STATUS_POLL_IDX ON PROCESSINGS (status, processing_id, locking, updated_at, new_poll_period, update_poll_period, created_at) COMPRESS 3 LOCAL;

CREATE INDEX CONTENTS_REL_IDX ON CONTENTS  (request_id, content_relation_type, transform_id, substatus) COMPRESS 3 LOCAL;
CREATE INDEX CONTENTS_TF_IDX ON CONTENTS  (transform_id, request_id, coll_id, content_relation_type, map_id) COMPRESS 4 LOCAL;

CREATE INDEX CONTENTS_EXT_RTW_IDX ON contents_ext (request_id, transform_id, workload_id) COMPRESS 3 ;
CREATE INDEX CONTENTS_EXT_RTM_IDX ON contents_ext (request_id, transform_id, map_id) COMPRESS 2;

CREATE INDEX COMMANDS_STATUS_IDX on commands (status, locking, updated_at) COMPRESS 2;

CREATE INDEX MESSAGES_ST_IDX on messages (status, destination, created_at) COMPRESS 2;
CREATE INDEX MESSAGES_TYPE_STU_IDX on messages (msg_type, status, destination, retries, updated_at, created_at) COMPRESS 3;

CREATE INDEX REQUESTS_STATUS_POLL_IDX on REQUESTS (status, request_id, locking, priority, updated_at, new_poll_period, update_poll_period, next_poll_at, created_at) COMPRESS 3 LOCAL;

CREATE INDEX TRANSFORMS_REQ_IDX on transforms (request_id, transform_id) COMPRESS 2;
CREATE INDEX TRANSFORMS_STATUS_POLL_IDX on transforms (status, transform_id, locking, updated_at, new_poll_period, update_poll_period, created_at) COMPRESS 3 LOCAL;

CREATE INDEX COLLECTIONS_REQ_IDX on collections (request_id, transform_id, updated_at) COMPRESS 2;


-- 2023.03.10

CREATE SEQUENCE EVENT_ID_SEQ MINVALUE 1 INCREMENT BY 1 START WITH 1 NOCACHE ORDER NOCYCLE GLOBAL;
CREATE TABLE EVENTS
(
    event_id NUMBER(12) DEFAULT ON NULL EVENT_ID_SEQ.NEXTVAL constraint EVENT_ID_NN NOT NULL,
    event_type NUMBER(12),
    event_actual_id NUMBER(12),
    priority NUMBER(12),
    status NUMBER(2),
    created_at DATE DEFAULT SYS_EXTRACT_UTC(systimestamp(0)),
    processing_at DATE DEFAULT SYS_EXTRACT_UTC(systimestamp(0)),
    processed_at DATE DEFAULT SYS_EXTRACT_UTC(systimestamp(0)),
    content CLOB,
    CONSTRAINT EVENTS_PK PRIMARY KEY (event_id) -- USING INDEX LOCAL,
);

CREATE TABLE EVENTS_ARCHIVE
(
    event_id NUMBER(12),
    event_type NUMBER(12),
    event_actual_id NUMBER(12),
    priority NUMBER(12),
    status NUMBER(2),
    created_at DATE DEFAULT SYS_EXTRACT_UTC(systimestamp(0)),
    processing_at DATE DEFAULT SYS_EXTRACT_UTC(systimestamp(0)),
    processed_at DATE DEFAULT SYS_EXTRACT_UTC(systimestamp(0)),
    content CLOB,
    CONSTRAINT EVENTS_AR_PK PRIMARY KEY (event_id) -- USING INDEX LOCAL,
);

CREATE TABLE EVENTS_PRIORITY
(
    event_type NUMBER(12),
    event_actual_id NUMBER(12),
    priority NUMBER(12),
    last_processed_at DATE DEFAULT SYS_EXTRACT_UTC(systimestamp(0)),
    updated_at DATE DEFAULT SYS_EXTRACT_UTC(systimestamp(0)),
    CONSTRAINT EVENTS_PR_PK PRIMARY KEY (event_type, event_actual_id) -- USING INDEX LOCAL,
);


--- 2023.03.16
alter table HEALTH add (status NUMBER(2));
alter table contents_update add content_metadata CLOB;
alter table health modify payload VARCHAR2(2048);


--- 2023.03.29
alter table contents_update add fetch_status NUMBER(2) DEFAULT 0;


-- 2023.05.18
alter table requests add site VARCHAR2(50);
CREATE INDEX REQUESTS_STATUS_SITE ON requests  (status, site, request_id) COMPRESS 3 LOCAL;

alter table transforms add site VARCHAR2(50);
CREATE INDEX TRANSFORMS_STATUS_SITE ON transforms  (status, site, request_id, transform_id) COMPRESS 3 LOCAL;

alter table processings add site VARCHAR2(50);
CREATE INDEX PROCESSINGS_STATUS_SITE ON processings  (status, site, request_id, transform_id, processing_id) COMPRESS 3 LOCAL;

alter table messages add fetching_id NUMBER(12);


CREATE SEQUENCE THROTTLER_ID_SEQ MINVALUE 1 INCREMENT BY 1 START WITH 1 NOCACHE ORDER NOCYCLE GLOBAL;
CREATE TABLE Throttlers
(
    throttler_id NUMBER(12) DEFAULT ON NULL THROTTLER_ID_SEQ.NEXTVAL constraint THROTTLER_ID_NN NOT NULL,
    site VARCHAR2(50),
    status NUMBER(2),
    num_requests NUMBER(12),
    num_transforms NUMBER(12),
    num_processings NUMBER(12),
    new_contents NUMBER(12),
    queue_contents NUMBER(12),
    created_at DATE DEFAULT SYS_EXTRACT_UTC(systimestamp(0)),
    updated_at DATE DEFAULT SYS_EXTRACT_UTC(systimestamp(0)),
    others CLOB,
    CONSTRAINT THROTTLER_PK PRIMARY KEY (throttler_id), -- USING INDEX LOCAL,
    CONSTRAINT THROTTLER_SITE_UQ UNIQUE (site)
);

alter table Messages add (poll_period INTERVAL DAY TO SECOND DEFAULT '00 00:05:00');


--- 20230626
alter table contents add (external_coll_id NUMBER(12), external_content_id NUMBER(12), external_event_id NUMBER(12), external_event_status NUMBER(2));

alter table contents add (sub_map_id NUMBER(12) default 0);
alter table contents add (dep_sub_map_id NUMBER(12) default 0);
alter table contents drop constraint CONTENT_ID_UQ;
alter table contents add constraint CONTENT_ID_UQ UNIQUE (transform_id, coll_id, map_id, sub_map_id, dep_sub_map_id, content_relation_type, name, min_id, max_id) USING INDEX LOCAL;


--- 20230927
alter table contents add (name_md5 varchar2(33), scope_name_md5 varchar2(33));
update contents set name_md5=standard_hash(name, 'MD5'), scope_name_md5=standard_hash(scope || name, 'MD5');
alter table contents drop constraint CONTENT_ID_UQ;
alter table contents add constraint CONTENT_ID_UQ UNIQUE (transform_id, coll_id, map_id, sub_map_id, dep_sub_map_id, content_relation_type, name_md5, scope_name_md5, min_id, max_id) USING INDEX LOCAL;
drop index CONTENTS_ID_NAME_IDX;
CREATE INDEX CONTENTS_ID_NAME_IDX ON CONTENTS (coll_id, scope, standard_hash(name, 'MD5'), status);


--- 20240111
CREATE SEQUENCE METAINFO_ID_SEQ MINVALUE 1 INCREMENT BY 1 START WITH 1 NOCACHE ORDER NOCYCLE GLOBAL;
CREATE TABLE meta_info
(
    meta_id NUMBER(12) DEFAULT ON NULL METAINFO_ID_SEQ.NEXTVAL constraint METAINFO_ID_NN NOT NULL,
    name VARCHAR2(50),
    status NUMBER(2),
    created_at DATE DEFAULT SYS_EXTRACT_UTC(systimestamp(0)),
    updated_at DATE DEFAULT SYS_EXTRACT_UTC(systimestamp(0)),
    description VARCHAR2(1000),
    meta_info CLOB,
    CONSTRAINT METAINFO_PK PRIMARY KEY (meta_id), -- USING INDEX LOCAL,
    CONSTRAINT METAINFO_NAME_UQ UNIQUE (name)
);

--- 20240219
alter table TRANSFORMS add (parent_transform_id NUMBER(12));
alter table TRANSFORMS add (previous_transform_id NUMBER(12));
alter table TRANSFORMS add (current_processing_id NUMBER(12));
alter table PROCESSINGS add (processing_type NUMBER(2));
