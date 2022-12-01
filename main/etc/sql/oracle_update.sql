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
	PandaID NUMBER(14),
	jobDefinitionID NUMBER(12),
	schedulerID VARCHAR2(128),
        pilotID VARCHAR2(200),
	creationTime DATE,
	modificationTime DATE,
        startTime DATE,
	endTime DATE,
	prodSourceLabel VARCHAR2(20),
        prodUserID VARCHAR2(250),
	assignedPriority NUMBER(5),
	currentPriority NUMBER(5),
        attemptNr NUMBER(5),
	maxAttempt NUMBER(5),
	maxCpuCount NUMBER(5),
        maxCpuUnit VARCHAR2(32),
	maxDiskCount NUMBER(12),
	maxDiskUnit VARCHAR2(10),
        minRamCount NUMBER(12),
	maxRamUnit VARCHAR2(10),
	cpuConsumptionTime NUMBER(12),
        cpuConsumptionUnit VARCHAR2(128),
	jobStatus VARCHAR2(10),
	jobName VARCHAR2(255),
        transExitCode NUMBER(5),
	pilotErrorCode NUMBER(5),
	pilotErrorDiag VARCHAR2(500),
        exeErrorCode NUMBER(5),
	exeErrorDiag VARCHAR2(500),
	supErrorCode NUMBER(5),
        supErrorDiag VARCHAR2(250),
	ddmErrorCode NUMBER(5),
	ddmErrorDiag VARCHAR2(500),
        brokerageErrorCode NUMBER(5),
	brokerageErrorDiag VARCHAR2(250),
        jobDispatcherErrorCode NUMBER(5),
	jobDispatcherErrorDiag VARCHAR2(250),
        taskBufferErrorCode NUMBER(5),
	taskBufferErrorDiag VARCHAR2(300),
        computingSite VARCHAR2(128),
	computingElement VARCHAR2(128),
        grid VARCHAR2(50),
	cloud VARCHAR2(50),
	cpuConversion float(20),
	taskID NUMBER(12),
        vo VARCHAR2(16),
	pilotTiming VARCHAR2(100),
	workingGroup VARCHAR2(20),
        processingType VARCHAR2(64),
	prodUserName VARCHAR2(60),
	coreCount NUMBER(5),
        nInputFiles NUMBER(10),
	reqID NUMBER(12),
	jediTaskID NUMBER(12),
        actualCoreCount NUMBER(5),
	maxRSS NUMBER(12),
	maxVMEM NUMBER(12),
        maxSWAP NUMBER(12),
	maxPSS NUMBER(12),
	avgRSS NUMBER(12),
	avgVMEM NUMBER(12),
        avgSWAP NUMBER(12),
	avgPSS NUMBER(12),
	maxWalltime NUMBER(12),
	diskIO NUMBER(12),
        failedAttempt NUMBER(5),
	hs06 NUMBER(12),
	hs06sec NUMBER(12),
        memory_leak VARCHAR2(10),
	memory_leak_x2 VARCHAR2(10),
	job_label VARCHAR2(20)
        CONSTRAINT CONTENT_EXT_PK PRIMARY KEY (content_id),
	CONSTRAINT CONTENT_EXT_TRANSFORM_ID_FK FOREIGN KEY(transform_id) REFERENCES TRANSFORMS(transform_id),
)
PCTFREE 0
PARTITION BY REFERENCE(CONTENT_EXT_TRANSFORM_ID_FK);

CREATE INDEX CONTENTS_EXT_RTF_IDX ON CONTENTS (request_id, transform_id, workload_id, coll_id, content_id, PandaID, status) LOCAL;
