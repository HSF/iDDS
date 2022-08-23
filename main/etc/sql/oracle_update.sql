-- 2022.08.23
alter table REQUESTS add (oldstatus NUMBER(2));
alter table REQUESTS add (new_retries NUMBER(5) DEFAULT 0);
alter table REQUESTS add (update_retries NUMBER(5) DEFAULT 0);
alter table REQUESTS add (max_new_retries NUMBER(5) DEFAULT 3);
alter table REQUESTS add (max_update_retries NUMBER(5) DEFAULT 0);
alter table REQUESTS add (new_poll_period NUMBER(10) DEFAULT 10);
alter table REQUESTS add (update_poll_period NUMBER(10) DEFAULT 10);

alter table TRANSFORMS add (oldstatus NUMBER(2));
alter table TRANSFORMS add (new_retries NUMBER(5) DEFAULT 0);
alter table TRANSFORMS add (update_retries NUMBER(5) DEFAULT 0);
alter table TRANSFORMS add (max_new_retries NUMBER(5) DEFAULT 3);
alter table TRANSFORMS add (max_update_retries NUMBER(5) DEFAULT 0);
alter table TRANSFORMS add (new_poll_period NUMBER(10) DEFAULT 10);
alter table TRANSFORMS add (update_poll_period NUMBER(10) DEFAULT 10);
alter table TRANSFORMS add (errors VARCHAR2(1024));


alter table PROCESSINGS add (oldstatus NUMBER(2));
alter table PROCESSINGS add (new_retries NUMBER(5) DEFAULT 0);
alter table PROCESSINGS add (update_retries NUMBER(5) DEFAULT 0);
alter table PROCESSINGS add (max_new_retries NUMBER(5) DEFAULT 3);
alter table PROCESSINGS add (max_update_retries NUMBER(5) DEFAULT 0);
alter table PROCESSINGS add (new_poll_period NUMBER(10) DEFAULT 10);
alter table PROCESSINGS add (update_poll_period NUMBER(10) DEFAULT 10);
alter table PROCESSINGS add (errors VARCHAR2(1024));

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
    username VARCHAR2(20)
    retries NUMBER(5) DEFAULT 0,
    source NUMBER(2),
    destination NUMBER(2),
    num_contents NUMBER(7),
    created_at DATE DEFAULT SYS_EXTRACT_UTC(systimestamp(0)),
    updated_at DATE DEFAULT SYS_EXTRACT_UTC(systimestamp(0)),
    cmd_content CLOB,
    CONSTRAINT COMMANDS_PK PRIMARY KEY (cmd_id) -- USING INDEX LOCAL,
);

CREATE INDEX COMMANDS_TYPE_ST_IDX ON COMMANDS (cmd_type, status, destination, request_id);
CREATE INDEX COMMANDS_TYPE_ST_TF_IDX ON COMMANDS (cmd_type, status, destination, transform_id);
CREATE INDEX COMMANDS_TYPE_ST_PR_IDX ON COMMANDS (cmd_type, status, destination, processing_id);

