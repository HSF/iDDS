DROP SEQUENCE MESSAGE_ID_SEQ;
DROP SEQUENCE REQUEST_ID_SEQ;
DROP SEQUENCE WORKPROGRESS_ID_SEQ;
DROP SEQUENCE TRANSFORM_ID_SEQ;
DROP SEQUENCE PROCESSING_ID_SEQ;
DROP SEQUENCE COLLECTION_ID_SEQ;
DROP SEQUENCE CONTENT_ID_SEQ;
DROP SEQUENCE HEALTH_ID_SEQ;

DROP SEQUENCE COMMAND_ID_SEQ;
DROP SEQUENCE EVENT_ID_SEQ;
DROP SEQUENCE THROTTLER_ID_SEQ;
DROP SEQUENCE METAINFO_ID_SEQ;
DROP SEQUENCE CONDITION_ID_SEQ;

delete from HEALTH;
delete from MESSAGES;
delete from CONTENTS;
delete from REQ2WORKLOAD;
delete from REQ2TRANSFORMS;
delete from WP2TRANSFORMS;
delete from WORKPROGRESSES;
delete from PROCESSINGS;
delete from COLLECTIONS;
delete from TRANSFORMS;
delete from REQUESTS;

delete from CONDITIONS;
delete from META_INFO;
delete from THROTTLERS;
delete from EVENTS;
delete from EVENTS_ARCHIVE;
delete from EVENTS_PRIORITY;
delete from COMMANDS;
delete from CONTENTS_EXT;
delete from CONTENTS_update;

Drop table HEALTH purge;
DROP table MESSAGES purge;
DROP table CONTENTS purge;
DROP table REQ2WORKLOAD purge;
DROP table REQ2TRANSFORMS purge;
DROP table WP2TRANSFORMS purge;
DROP table WORKPROGRESSES purge;
DROP table PROCESSINGS purge;
DROP table COLLECTIONS purge;
DROP table TRANSFORMS purge;
DROP table REQUESTS purge;

DROP table CONDITIONS purge;
DROP table META_INFO purge;
DROP table THROTTLERS purge;
DROP table EVENTS purge;
DROP table EVENTS_ARCHIVE purge;
DROP table EVENTS_PRIORITY purge;
DROP table COMMANDS purge;
DROP table CONTENTS_EXT purge;
DROP table CONTENTS_update purge;

--- requests
CREATE SEQUENCE REQUEST_ID_SEQ MINVALUE 1 INCREMENT BY 1 ORDER NOCACHE NOCYCLE GLOBAL;
CREATE TABLE REQUESTS
(
        request_id NUMBER(12) DEFAULT ON NULL REQUEST_ID_SEQ.NEXTVAL constraint REQ_ID_NN NOT NULL,
        scope VARCHAR2(25) constraint REQ_SCOPE_NN NOT NULL,
        name VARCHAR2(255) constraint REQ_NAME_NN NOT NULL,
        requester VARCHAR2(20),
        request_type NUMBER(2) constraint REQ_DATATYPE_NN NOT NULL,
        username VARCHAR2(20) default null,
        userdn VARCHAR2(200) default null,
        transform_tag VARCHAR2(10),
        workload_id NUMBER(10),
        priority NUMBER(7),
        status NUMBER(2) constraint REQ_STATUS_ID_NN NOT NULL,
        substatus NUMBER(2),
	oldstatus NUMBER(2),
        locking NUMBER(2),
        created_at DATE DEFAULT ON NULL SYS_EXTRACT_UTC(systimestamp(0)) constraint REQ_CREATED_NN NOT NULL,
        updated_at DATE DEFAULT ON NULL SYS_EXTRACT_UTC(systimestamp(0)) constraint REQ_UPDATED_NN NOT NULL,
        next_poll_at DATE DEFAULT ON NULL SYS_EXTRACT_UTC(systimestamp(0)) constraint REQ_NEXT_POLL_NN NOT NULL,
        accessed_at DATE,
        expired_at DATE,
	new_retries NUMBER(3),
        update_retries NUMBER(3),
        max_new_retries NUMBER(3),
        max_update_retries NUMBER(3),
        new_poll_period INTERVAL DAY TO SECOND,
        update_poll_period INTERVAL DAY TO SECOND,
        site VARCHAR2(50),
        campaign VARCHAR2(50),
        campaign_group VARCHAR2(250),
        campaign_tag VARCHAR2(20),
        errors VARCHAR2(1024),
        request_metadata CLOB constraint REQ_REQUEST_METADATA_ENSURE_JSON CHECK(request_metadata IS JSON(LAX)),
        processing_metadata CLOB constraint REQ_PROCESSING_METADATA_ENSURE_JSON CHECK(processing_metadata IS JSON(LAX)),
        CONSTRAINT REQUESTS_PK PRIMARY KEY (request_id) USING INDEX LOCAL,
	CONSTRAINT "REQUESTS_STATUS_ID_NN" CHECK (status IS NOT NULL)
        --- CONSTRAINT REQUESTS_NAME_SCOPE_UQ UNIQUE (name, scope, requester, request_type, transform_tag, workload_id) -- USING INDEX LOCAL,
)
PCTFREE 3
PARTITION BY RANGE(REQUEST_ID)
INTERVAL ( 100000 )
( PARTITION initial_part VALUES LESS THAN (1) );


CREATE INDEX REQUESTS_SCOPE_NAME_IDX ON REQUESTS (name, scope, workload_id) LOCAL;
--- drop index REQUESTS_STATUS_PRIORITY_IDX
CREATE INDEX REQUESTS_STATUS_PRIORITY_IDX ON REQUESTS (status, priority, request_id, locking, updated_at, next_poll_at, created_at) LOCAL COMPRESS 1;
CREATE INDEX REQUESTS_STATUS_POLL_IDX ON requests (status, priority, locking, updated_at, new_poll_period, update_poll_period, created_at, request_id) LOCAL;
CREATE INDEX REQUESTS_STATUS_SITE ON requests (status, site, request_id);

-- alter table REQUESTS add (username VARCHAR2(20) default null);
-- alter table REQUESTS add (userdn VARCHAR2(200) default null);

--- workprogress
CREATE SEQUENCE WORKPROGRESS_ID_SEQ MINVALUE 1 INCREMENT BY 1 ORDER NOCACHE NOCYCLE GLOBAL;
CREATE TABLE WORKPROGRESSES
(
        workprogress_id NUMBER(12) DEFAULT ON NULL WORKPROGRESS_ID_SEQ.NEXTVAL constraint WORKPROGRESS_ID_NN NOT NULL,
        request_id NUMBER(12) constraint WORKPROGRESS__REQ_ID_NN NOT NULL,
        workload_id NUMBER(10),
        scope VARCHAR2(25) constraint WORKPROGRESS_SCOPE_NN NOT NULL,
        name VARCHAR2(255) constraint WORKPROGRESS_NAME_NN NOT NULL,
        priority NUMBER(7),
        status NUMBER(2) constraint WORKPROGRESS_STATUS_ID_NN NOT NULL,
        substatus NUMBER(2),
        locking NUMBER(2),
        created_at DATE DEFAULT ON NULL SYS_EXTRACT_UTC(systimestamp(0)) constraint WORKPROGRESS_CREATED_NN NOT NULL,
        updated_at DATE DEFAULT ON NULL SYS_EXTRACT_UTC(systimestamp(0)) constraint WORKPROGRESS_UPDATED_NN NOT NULL,
        next_poll_at DATE DEFAULT ON NULL SYS_EXTRACT_UTC(systimestamp(0)) constraint WORKPROGRESS_NEXT_POLL_NN NOT NULL,
        accessed_at DATE,
        expired_at DATE,
        errors VARCHAR2(1024),
        workprogress_metadata CLOB constraint WORKPROGRESS_REQUEST_METADATA_ENSURE_JSON CHECK(workprogress_metadata IS JSON(LAX)),
        processing_metadata CLOB constraint WORKPROGRESS_PROCESSING_METADATA_ENSURE_JSON CHECK(processing_metadata IS JSON(LAX)),
        CONSTRAINT WORKPROGRESS_PK PRIMARY KEY (workprogress_id) USING INDEX LOCAL,
        CONSTRAINT WORKPROGRESS_REQ_ID_FK FOREIGN KEY(request_id) REFERENCES REQUESTS(request_id)
)
PCTFREE 3
PARTITION BY RANGE(workprogress_id)
INTERVAL ( 100000 )
( PARTITION initial_part VALUES LESS THAN (1) );

CREATE INDEX WORKPROGRESS_SCOPE_NAME_IDX ON WORKPROGRESSES (name, scope, workprogress_id) LOCAL;
--- drop index REQUESTS_STATUS_PRIORITY_IDX
CREATE INDEX WORKPROGRESS_STATUS_PRIORITY_IDX ON WORKPROGRESSES (status, priority, workprogress_id, locking, updated_at, next_poll_at, created_at) LOCAL COMPRESS 1;


--- transforms
CREATE SEQUENCE TRANSFORM_ID_SEQ MINVALUE 1 INCREMENT BY 1 ORDER NOCACHE NOCYCLE GLOBAL;
CREATE TABLE TRANSFORMS
(
        transform_id NUMBER(12) DEFAULT ON NULL TRANSFORM_ID_SEQ.NEXTVAL constraint TRANSFORM_ID_NN NOT NULL,
        request_id NUMBER(12),
        workload_id NUMBER(10),
        transform_type NUMBER(2) constraint TRANSFORM_TYPE_NN NOT NULL,
        transform_tag VARCHAR2(20),
	internal_id VARCHAR2(20),
        priority NUMBER(7),
        safe2get_output_from_input NUMBER(10),
        status NUMBER(2),
        substatus NUMBER(2),
	oldstatus NUMBER(2),
        locking NUMBER(2),
        retries NUMBER(5) DEFAULT 0,
	parent_transform_id NUMBER(12),
        previous_transform_id NUMBER(12),
        current_processing_id NUMBER(12),
        created_at DATE DEFAULT ON NULL SYS_EXTRACT_UTC(systimestamp(0)) constraint TRANSFORM_CREATED_NN NOT NULL,
        updated_at DATE DEFAULT ON NULL SYS_EXTRACT_UTC(systimestamp(0)) constraint TRANSFORM_UPDATED_NN NOT NULL,
        next_poll_at DATE DEFAULT ON NULL SYS_EXTRACT_UTC(systimestamp(0)) constraint TRANSFORM_NEXT_POLL_NN NOT NULL,
        started_at DATE,
        finished_at DATE,
        expired_at DATE,
	new_retries NUMBER(5) DEFAULT 0,
        update_retries NUMBER(5) DEFAULT 0,
        max_new_retries NUMBER(5) DEFAULT 100,
        max_update_retries NUMBER(5) DEFAULT 100,
        new_poll_period INTERVAL DAY TO SECOND,
        update_poll_period INTERVAL DAY TO SECOND,
        site VARCHAR2(50),
	name VARCHAR2(255),
	has_previous_conditions NUMBER(5),
        loop_index NUMBER(5),
        cloned_from NUMBER(12),
        triggered_conditions CLOB,
        untriggered_conditions CLOB,
        errors VARCHAR2(1024 CHAR),
        transform_metadata CLOB constraint TRANSFORM_METADATA_ENSURE_JSON CHECK(transform_metadata IS JSON(LAX)),
        running_metadata CLOB,
        CONSTRAINT TRANSFORMS_PK PRIMARY KEY (transform_id),
	CONSTRAINT "TRANSFORMS_STATUS_ID_NN" CHECK (status IS NOT NULL)
)
PCTFREE 3
PARTITION BY RANGE(TRANSFORM_ID)
INTERVAL ( 100000 )
( PARTITION initial_part VALUES LESS THAN (1) );

--- alter table transforms add next_poll_at DATE DEFAULT ON NULL SYS_EXTRACT_UTC(systimestamp(0)) constraint TRANSFORM_NEXT_POLL_NN NOT NULL;
CREATE INDEX TRANSFORMS_TYPE_TAG_IDX ON TRANSFORMS (transform_type, transform_tag, transform_id) LOCAL;
CREATE INDEX TRANSFORMS_STATUS_UPDATED_AT_IDX ON TRANSFORMS (status, locking, updated_at, next_poll_at, created_at) LOCAL;
CREATE INDEX TRANSFORMS_STATUS_SITE ON transforms (status, site, request_id, transform_id) LOCAL;
CREATE INDEX TRANSFORMS_REQ_IDX ON transforms (request_id, transform_id);
CREATE INDEX TRANSFORMS_STATUS_POLL_IDX ON transforms (status, locking, updated_at, new_poll_period, update_poll_period, created_at, transform_id);


---- processings
CREATE SEQUENCE PROCESSING_ID_SEQ MINVALUE 1 INCREMENT BY 1 ORDER NOCACHE NOCYCLE GLOBAL;
CREATE TABLE PROCESSINGS
(
        processing_id NUMBER(12) DEFAULT ON NULL PROCESSING_ID_SEQ.NEXTVAL constraint PROCESSING_ID_NN NOT NULL,
        transform_id NUMBER(12) constraint PROCESSINGS_TRANSFORM_ID_NN NOT NULL,
        request_id NUMBER(12),
        workload_id NUMBER(10),
	processing_type NUMBER(2) NOT NULL,
        status NUMBER(2) constraint PROCESSINGS_STATUS_ID_NN NOT NULL,
        substatus NUMBER(2),
	oldstatus NUMBER(2),
        locking NUMBER(2),
        submitter VARCHAR2(20),
        submitted_id NUMBER(12),
        granularity NUMBER(10),
        granularity_type NUMBER(2),
        created_at DATE DEFAULT ON NULL SYS_EXTRACT_UTC(systimestamp(0)) constraint PROCESSING_CREATED_NN NOT NULL,
        updated_at DATE DEFAULT ON NULL SYS_EXTRACT_UTC(systimestamp(0)) constraint PROCESSING_UPDATED_NN NOT NULL,
        next_poll_at DATE DEFAULT ON NULL SYS_EXTRACT_UTC(systimestamp(0)) constraint PROCESSING_NEXT_POLL_NN NOT NULL,
        poller_updated_at DATE,
	submitted_at DATE,
        finished_at DATE,
        expired_at DATE,
	new_retries NUMBER(5) DEFAULT 0,
        update_retries NUMBER(5) DEFAULT 0,
        max_new_retries NUMBER(5) DEFAULT 100,
        max_update_retries NUMBER(5) DEFAULT 100,
        new_poll_period INTERVAL DAY TO SECOND,
        update_poll_period INTERVAL DAY TO SECOND,
        site VARCHAR2(50),
        errors VARCHAR2(1024),
        processing_metadata CLOB constraint PROCESSINGS_METADATA_ENSURE_JSON CHECK(processing_metadata IS JSON(LAX)),
        running_metadata CLOB,
        output_metadata CLOB constraint PROCESSINGS_OUTPUT_METADATA_ENSURE_JSON CHECK(output_metadata IS JSON(LAX)),
        CONSTRAINT PROCESSINGS_PK PRIMARY KEY (processing_id),
        CONSTRAINT PROCESSINGS_TRANSFORM_ID_FK FOREIGN KEY(transform_id) REFERENCES TRANSFORMS(transform_id)
)
PCTFREE 0
PARTITION BY REFERENCE(PROCESSINGS_TRANSFORM_ID_FK);

CREATE INDEX PROCESSINGS_STATUS_UPDATED_AT_IDX ON PROCESSINGS (status, locking, updated_at, next_poll_at, created_at) LOCAL;
CREATE INDEX PROCESSINGS_STATUS_POLL_IDX ON processings (status, processing_id, locking, updated_at, new_poll_period, update_poll_period, created_at);
CREATE INDEX PROCESSINGS_STATUS_SITE ON processings (status, site, request_id, transform_id, processing_id);


--- collections
CREATE SEQUENCE COLLECTION_ID_SEQ MINVALUE 1 INCREMENT BY 1 ORDER CACHE 2 NOCYCLE GLOBAL;
CREATE TABLE COLLECTIONS
(
    coll_id NUMBER(14) DEFAULT ON NULL COLLECTION_ID_SEQ.NEXTVAL constraint COLLECTIONS_ID_NN NOT NULL,
    coll_type NUMBER(2),
    transform_id NUMBER(12) constraint COLLECTION_TRANSFORM_ID_NN NOT NULL,
    request_id NUMBER(12),
    workload_id NUMBER(10),
    relation_type NUMBER(2), -- input, output or log of the transform,
    scope VARCHAR2(25) constraint COLLECTION_SCOPE_NN NOT NULL,
    name VARCHAR2(255) constraint COLLECTION_NAME_NN NOT NULL,
    bytes NUMBER(19),
    status NUMBER(2),
    substatus NUMBER(2),
    locking NUMBER(2),
    total_files NUMBER(19),
    storage_id NUMBER(10),
    new_files NUMBER(10),
    processed_files NUMBER(10),
    processing_files NUMBER(10),
    processing_id NUMBER(12),
    failed_files NUMBER(10),
    missing_files NUMBER(10),
    ext_files NUMBER(10),
    processed_ext_files NUMBER(10),
    failed_ext_files NUMBER(10),
    missing_ext_files NUMBER(10),
    retries NUMBER(5) DEFAULT 0,
    created_at DATE DEFAULT ON NULL SYS_EXTRACT_UTC(systimestamp(0)) constraint COLLECTION_CREATED_NN NOT NULL,
    updated_at DATE DEFAULT ON NULL SYS_EXTRACT_UTC(systimestamp(0)) constraint COLLECTION_UPDATED_NN NOT NULL,
    next_poll_at DATE DEFAULT ON NULL SYS_EXTRACT_UTC(systimestamp(0)) constraint COLLECTION_NEXT_POLL_NN NOT NULL,
    accessed_at DATE,
    expired_at DATE,
    coll_metadata CLOB constraint COLLECTION_METADATA_ensure_json CHECK (COLL_METADATA IS JSON (LAX)),
    CONSTRAINT COLLECTION_PK PRIMARY KEY (coll_id), -- USING INDEX LOCAL,
    CONSTRAINT COLLECTION_NAME_SCOPE_UQ UNIQUE (name, scope, transform_id, relation_type), -- USING INDEX LOCAL,
    CONSTRAINT COLLECTION_TRANSFORM_ID_FK FOREIGN KEY(transform_id) REFERENCES TRANSFORMS(transform_id),
    CONSTRAINT "COLLECTIONS_STATUS_ID_NN" CHECK (status IS NOT NULL),
    CONSTRAINT "COLLECTIONS_TRANSFORM_ID_NN" CHECK (transform_id IS NOT NULL)
)
PCTFREE 0
PARTITION BY REFERENCE(COLLECTION_TRANSFORM_ID_FK);

CREATE INDEX COLLECTIONS_STATUS_RELATIONTYPE_IDX ON COLLECTIONS(status, relation_type);
CREATE INDEX COLLECTIONS_TRANSFORM_IDX ON COLLECTIONS(transform_id, coll_id);
CREATE INDEX COLLECTIONS_STATUS_UPDATED_AT_IDX ON COLLECTIONS (status, locking, updated_at, next_poll_at, created_at) LOCAL;
CREATE INDEX COLLECTIONS_REQ_IDX ON collections (request_id, transform_id, updated_at);


--- contents
CREATE SEQUENCE CONTENT_ID_SEQ MINVALUE 1 INCREMENT BY 1 ORDER CACHE 10 NOCYCLE GLOBAL;
CREATE TABLE CONTENTS
(
        content_id NUMBER(12) DEFAULT ON NULL CONTENT_ID_SEQ.NEXTVAL constraint CONTENT_ID_NN NOT NULL,
        transform_id NUMBER(12) constraint CONTENT_TRANSFORM_ID_NN NOT NULL,
        coll_id NUMBER(14) constraint CONTENT_COLL_ID_NN NOT NULL,
        request_id NUMBER(12),
        workload_id NUMBER(10),
        map_id NUMBER(12) DEFAULT 0,
	sub_map_id NUMBER(19),
        dep_sub_map_id NUMBER(19),
        content_dep_id NUMBER(19),
        scope VARCHAR2(25) constraint CONTENT_SCOPE_NN NOT NULL,
        name VARCHAR2(255) constraint CONTENT_NAME_NN NOT NULL,
	name_md5 VARCHAR2(33),
        scope_name_md5 VARCHAR2(33),
        min_id NUMBER(7) default 0,
        max_id NUMBER(7) default 0,
        content_type NUMBER(2) constraint CONTENT_TYPE_NN NOT NULL,
        content_relation_type NUMBER(2) default 0,
        status NUMBER(2) constraint CONTENT_STATUS_NN NOT NULL,
        substatus NUMBER(2),
        locking NUMBER(2),
        bytes NUMBER(12),
        md5 VARCHAR2(32),
        adler32 VARCHAR2(8),
        processing_id NUMBER(12),
        storage_id NUMBER(10),
        retries NUMBER(5) DEFAULT 0,
	external_coll_id NUMBER(19),
        external_content_id NUMBER(19),
        external_event_id NUMBER(19),
        external_event_status NUMBER(5),
        path VARCHAR2(4000),
        created_at DATE DEFAULT ON NULL SYS_EXTRACT_UTC(systimestamp(0)) constraint CONTENT_CREATED_NN NOT NULL,
        updated_at DATE DEFAULT ON NULL SYS_EXTRACT_UTC(systimestamp(0)) constraint CONTENT_UPDATED_NN NOT NULL,
        accessed_at DATE,
        expired_at DATE,
        --- content_metadata CLOB constraint CONTENT_METADATA_ENSURE_JSON CHECK(CONTENT_METADATA IS JSON(LAX)),
        content_metadata VARCHAR2(1000),
        --- CONSTRAINT CONTENT_PK PRIMARY KEY (name, scope, coll_id, content_type, min_id, max_id) USING INDEX LOCAL,
        CONSTRAINT CONTENT_PK PRIMARY KEY (content_id),
        ---- CONSTRAINT CONTENT_SCOPE_NAME_UQ UNIQUE (name, scope, coll_id, content_type, min_id, max_id) USING INDEX LOCAL,
        ---- CONSTRAINT CONTENT_SCOPE_NAME_UQ UNIQUE (name, scope, coll_id, min_id, max_id) USING INDEX LOCAL,
        ---- CONSTRAINT CONTENT_ID_UQ UNIQUE (transform_id, coll_id, map_id, name, min_id, max_id) USING INDEX LOCAL,
        CONSTRAINT CONTENT_ID_UQ UNIQUE (transform_id, coll_id, map_id, sub_map_id, dep_sub_map_id, content_relation_type, name_md5, scope_name_md5, min_id, max_id),
        CONSTRAINT CONTENT_TRANSFORM_ID_FK FOREIGN KEY(transform_id) REFERENCES TRANSFORMS(transform_id),
        CONSTRAINT CONTENT_COLL_ID_FK FOREIGN KEY(coll_id) REFERENCES COLLECTIONS(coll_id),
	CONSTRAINT "CONTENTS_STATUS_ID_NN" CHECK (status IS NOT NULL),
        CONSTRAINT "CONTENTS_COLL_ID_NN" CHECK (coll_id IS NOT NULL)
)
PCTFREE 0
PARTITION BY REFERENCE(CONTENT_TRANSFORM_ID_FK);

CREATE INDEX CONTENTS_STATUS_UPDATED_IDX ON CONTENTS (status, locking, updated_at, created_at) LOCAL;
CREATE INDEX CONTENTS_ID_NAME_IDX ON CONTENTS (coll_id, scope, md5('name'), status) LOCAL;
CREATE INDEX CONTENTS_REQ_TF_COLL_IDX ON CONTENTS (request_id, transform_id, workload_id, coll_id, content_relation_type, status, substatus) LOCAL;
CREATE INDEX CONTENTS_TF_IDX ON contents (transform_id, request_id, coll_id, map_id, content_relation_type);
CREATE INDEX CONTENTS_DEP_IDX ON contents (request_id, transform_id, content_dep_id);
CREATE INDEX CONTENTS_REL_IDX ON contents (request_id, content_relation_type, transform_id, substatus);


--- messages
CREATE SEQUENCE MESSAGE_ID_SEQ MINVALUE 1 INCREMENT BY 1 START WITH 1 NOCACHE ORDER NOCYCLE GLOBAL;
CREATE TABLE MESSAGES
(
    msg_id NUMBER(12) DEFAULT ON NULL MESSAGE_ID_SEQ.NEXTVAL constraint MESSAGE_ID_NN NOT NULL,
    msg_type NUMBER(2),
    status NUMBER(2),
    substatus NUMBER(2),
    locking NUMBER(2),
    source NUMBER(2),
    destination NUMBER(2),
    request_id NUMBER(12),
    workload_id NUMBER(10),
    transform_id NUMBER(12),
    processing_id NUMBER(12),
    internal_id VARCHAR2(20),
    num_contents NUMBER(7),
    retries NUMBER(5) DEFAULT 0,
    fetching_id NUMBER(12),
    poll_period INTERVAL DAY TO SECOND NOT NULL,
    created_at DATE DEFAULT ON NULL SYS_EXTRACT_UTC(systimestamp(0)),
    updated_at DATE DEFAULT ON NULL SYS_EXTRACT_UTC(systimestamp(0)),
    msg_content CLOB constraint MSG_CONTENT_ENSURE_JSON CHECK(msg_content IS JSON(LAX)),
    CONSTRAINT MESSAGES_PK PRIMARY KEY (msg_id) -- USING INDEX LOCAL,
);

CREATE INDEX MESSAGES_TYPE_ST_IDX ON MESSAGES (msg_type, status, destination, request_id);
CREATE INDEX MESSAGES_TYPE_ST_TF_IDX ON MESSAGES (msg_type, status, destination, transform_id);
CREATE INDEX MESSAGES_TYPE_ST_PR_IDX ON MESSAGES (msg_type, status, destination, processing_id);
CREATE INDEX MESSAGES_ST_IDX ON messages (status, destination, created_at);
CREATE INDEX MESSAGES_TYPE_STU_IDX ON messages (msg_type, status, destination, retries, updated_at, created_at);


--- health
CREATE SEQUENCE HEALTH_ID_SEQ MINVALUE 1 INCREMENT BY 1 START WITH 1 NOCACHE ORDER NOCYCLE GLOBAL;
CREATE TABLE HEALTH
(
    health_id NUMBER(12) DEFAULT ON NULL HEALTH_ID_SEQ.NEXTVAL constraint HEALTH_ID_NN NOT NULL,
    agent VARCHAR2(30),
    hostname VARCHAR2(127),
    pid Number(12),
    status Number(2),
    thread_id Number(20),
    thread_name VARCHAR2(255),
    payload VARCHAR2(2048),
    created_at DATE DEFAULT ON NULL SYS_EXTRACT_UTC(systimestamp(0)),
    updated_at DATE DEFAULT ON NULL SYS_EXTRACT_UTC(systimestamp(0)),
    CONSTRAINT HEALTH_PK PRIMARY KEY (health_id), -- USING INDEX LOCAL,
    CONSTRAINT HEALTH_UQ UNIQUE (agent, hostname, pid, thread_id)
);


--- contents_update
CREATE TABLE contents_update (
        content_id NUMBER(19) NOT NULL,
        substatus NUMBER(2),
        request_id NUMBER(19),
        transform_id NUMBER(19),
        workload_id NUMBER(19),
        fetch_status NUMBER(2) NOT NULL,
        coll_id NUMBER(19),
        content_metadata VARCHAR2(1000),
        PRIMARY KEY (content_id)
);


---- contents_ext
CREATE TABLE contents_ext (
        content_id NUMBER(19) NOT NULL,
        transform_id NUMBER(19) NOT NULL,
        coll_id NUMBER(19) NOT NULL,
        request_id NUMBER(19) NOT NULL,
        workload_id NUMBER(19),
        map_id NUMBER(19) NOT NULL,
        status INTEGER NOT NULL,
        panda_id NUMBER(19),
        job_definition_id NUMBER(19),
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
        attempt_nr INTEGER,
        max_attempt INTEGER,
        max_cpu_count INTEGER,
        max_cpu_unit VARCHAR2(32),
        max_disk_count INTEGER,
        max_disk_unit VARCHAR2(10),
        min_ram_count INTEGER,
        min_ram_unit VARCHAR2(10),
        cpu_consumption_time INTEGER,
        cpu_consumption_unit VARCHAR2(128),
        job_status VARCHAR2(10),
        job_name VARCHAR2(255),
        trans_exit_code INTEGER,
        pilot_error_code INTEGER,
        pilot_error_diag VARCHAR2(500),
        exe_error_code INTEGER,
        exe_error_diag VARCHAR2(500),
        sup_error_code INTEGER,
        sup_error_diag VARCHAR2(250),
        ddm_error_code INTEGER,
        ddm_error_diag VARCHAR2(500),
        brokerage_error_code INTEGER,
        brokerage_error_diag VARCHAR2(250),
        job_dispatcher_error_code INTEGER,
        job_dispatcher_error_diag VARCHAR2(250),
        task_buffer_error_code INTEGER,
        task_buffer_error_diag VARCHAR2(300),
        computing_site VARCHAR2(128),
        computing_element VARCHAR2(128),
	grid VARCHAR2(50),
        cloud VARCHAR2(50),
        cpu_conversion FLOAT,
        task_id NUMBER(19),
        vo VARCHAR2(16 CHAR),
        pilot_timing VARCHAR2(100),
        working_group VARCHAR2(20),
        processing_type VARCHAR2(64),
        prod_user_name VARCHAR2(60),
        core_count INTEGER,
        n_input_files INTEGER,
        req_id NUMBER(19),
        jedi_task_id NUMBER(19),
        actual_core_count INTEGER,
        max_rss INTEGER,
        max_vmem INTEGER,
        max_swap INTEGER,
        max_pss INTEGER,
        avg_rss INTEGER,
        avg_vmem INTEGER,
        avg_swap INTEGER,
        avg_pss INTEGER,
        max_walltime INTEGER,
        disk_io INTEGER,
        failed_attempt INTEGER,
        hs06 INTEGER,
        hs06sec INTEGER,
        memory_leak VARCHAR2(10),
        memory_leak_x2 VARCHAR2(10),
        job_label VARCHAR2(20 CHAR),
        CONSTRAINT "CONTENTS_EXT_PK" PRIMARY KEY (content_id)
);

CREATE INDEX CONTENTS_EXT_RTM_IDX ON contents_ext (request_id, transform_id, map_id);

CREATE INDEX CONTENTS_EXT_RTF_IDX ON contents_ext (request_id, transform_id, workload_id, coll_id, content_id, panda_id, status);

CREATE INDEX CONTENTS_EXT_RTW_IDX ON contents_ext (request_id, transform_id, workload_id);


--- commands
CREATE SEQUENCE COMMAND_ID_SEQ MINVALUE 1 INCREMENT BY 1 START WITH 1 NOCACHE ORDER NOCYCLE GLOBAL;

CREATE TABLE commands (
        cmd_id NUMBER(19) DEFAULT ON NULL COMMAND_ID_SEQ.NEXTVAL constraint COMMAND_ID_NN NOT NULL,
        request_id NUMBER(19) NOT NULL,
        workload_id NUMBER(19),
        transform_id NUMBER(19),
        processing_id NUMBER(19),
        cmd_type NUMBER(2),
        status NUMBER(2) NOT NULL,
        substatus INTEGER,
        locking INTEGER NOT NULL,
        username VARCHAR2(50 CHAR),
        retries INTEGER,
        source INTEGER,
        destination INTEGER,
        created_at DATE NOT NULL,
        updated_at DATE NOT NULL,
        cmd_content CLOB,
        errors VARCHAR2(1024 CHAR),
        CONSTRAINT "COMMANDS_PK" PRIMARY KEY (cmd_id)
) PCTFREE 0;

CREATE INDEX COMMANDS_TYPE_ST_PR_IDX ON commands (cmd_type, status, destination, processing_id);

CREATE INDEX COMMANDS_TYPE_ST_IDX ON commands (cmd_type, status, destination, request_id);

CREATE INDEX COMMANDS_STATUS_IDX ON commands (status, locking, updated_at);

CREATE INDEX COMMANDS_TYPE_ST_TF_IDX ON commands (cmd_type, status, destination, transform_id);


--- events
CREATE TABLE events_priority (
        event_type INTEGER NOT NULL,
        event_actual_id INTEGER NOT NULL,
        priority INTEGER NOT NULL,
        last_processed_at DATE NOT NULL,
        updated_at DATE NOT NULL,
        CONSTRAINT "EVENTS_PR_PK" PRIMARY KEY (event_type, event_actual_id)
) ;

CREATE SEQUENCE EVENT_ID_SEQ MINVALUE 1 INCREMENT BY 1 START WITH 1 NOCACHE ORDER NOCYCLE GLOBAL;

CREATE TABLE events (
        event_id NUMBER(19) DEFAULT ON NULL EVENT_ID_SEQ.NEXTVAL constraint EVENT_ID_NN NOT NULL,
        event_type INTEGER NOT NULL,
        event_actual_id INTEGER NOT NULL,
        priority INTEGER,
        status INTEGER NOT NULL,
        created_at DATE NOT NULL,
        processing_at DATE,
        processed_at DATE,
        content CLOB,
        CONSTRAINT "EVENTS_PK" PRIMARY KEY (event_id)
) ;


CREATE TABLE events_archive (
        event_id NUMBER(19) NOT NULL,
        event_type INTEGER NOT NULL,
        event_actual_id INTEGER NOT NULL,
        priority INTEGER,
        status INTEGER NOT NULL,
        created_at DATE NOT NULL,
        processing_at DATE,
        processed_at DATE,
        content CLOB,
        CONSTRAINT "EVENTS_AR_PK" PRIMARY KEY (event_id)
) ;


--- throttler
CREATE SEQUENCE THROTTLER_ID_SEQ MINVALUE 1 INCREMENT BY 1 START WITH 1 NOCACHE ORDER NOCYCLE GLOBAL;

CREATE TABLE throttlers (
        throttler_id NUMBER(19) DEFAULT ON NULL THROTTLER_ID_SEQ.NEXTVAL constraint THROTTLER_ID_NN NOT NULL,
        site VARCHAR2(50 CHAR) NOT NULL,
        status INTEGER NOT NULL,
        num_requests INTEGER,
        num_transforms INTEGER,
        num_processings INTEGER,
        new_contents INTEGER,
        queue_contents INTEGER,
        created_at DATE NOT NULL,
        updated_at DATE NOT NULL,
        others CLOB,
        CONSTRAINT "THROTTLER_PK" PRIMARY KEY (throttler_id),
        CONSTRAINT "THROTTLER_SITE_UQ" UNIQUE (site)
);


--- meta_info
CREATE SEQUENCE METAINFO_ID_SEQ MINVALUE 1 INCREMENT BY 1 START WITH 1 NOCACHE ORDER NOCYCLE GLOBAL;

CREATE TABLE meta_info (
        meta_id NUMBER(19) DEFAULT ON NULL METAINFO_ID_SEQ.NEXTVAL constraint METAINFO_ID_NN NOT NULL,
        name VARCHAR2(50 CHAR) NOT NULL,
        status INTEGER NOT NULL,
        created_at DATE NOT NULL,
        updated_at DATE NOT NULL,
        description VARCHAR2(1000 CHAR),
        meta_info CLOB,
        CONSTRAINT "METAINFO_PK" PRIMARY KEY (meta_id),
        CONSTRAINT "METAINFO_NAME_UQ" UNIQUE (name)
) ;

--- conditions
CREATE SEQUENCE CONDITION_ID_SEQ MINVALUE 1 INCREMENT BY 1 START WITH 1 NOCACHE ORDER NOCYCLE GLOBAL;

CREATE TABLE conditions (
        condition_id NUMBER(19) DEFAULT ON NULL CONDITION_ID_SEQ.NEXTVAL constraint CONDITION_ID_NN NOT NULL,
        request_id NUMBER(19) NOT NULL,
        internal_id VARCHAR2(20 CHAR),
        name VARCHAR2(250 CHAR),
        status INTEGER NOT NULL,
        substatus INTEGER,
        is_loop INTEGER,
        loop_index INTEGER,
        cloned_from NUMBER(19),
        created_at DATE NOT NULL,
        updated_at DATE NOT NULL,
        evaluate_result VARCHAR2(1000 CHAR),
        previous_transforms CLOB,
        following_transforms CLOB,
        condition CLOB,
        CONSTRAINT "CONDITION_PK" PRIMARY KEY (condition_id),
        CONSTRAINT "CONDITION_ID_UQ" UNIQUE (request_id, internal_id)
) ;
