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
