DROP SEQUENCE MESSAGE_ID_SEQ;
DROP SEQUENCE REQUEST_ID_SEQ;
DROP SEQUENCE WORKPROGRESS_ID_SEQ;
DROP SEQUENCE TRANSFORM_ID_SEQ;
DROP SEQUENCE PROCESSING_ID_SEQ;
DROP SEQUENCE COLLECTION_ID_SEQ;
DROP SEQUENCE CONTENT_ID_SEQ;
DROP SEQUENCE HEALTH_ID_SEQ

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

--- requests
CREATE SEQUENCE REQUEST_ID_SEQ MINVALUE 1 INCREMENT BY 1 ORDER NOCACHE NOCYCLE GLOBAL;
CREATE TABLE REQUESTS
(
        request_id NUMBER(12) DEFAULT ON NULL REQUEST_ID_SEQ.NEXTVAL constraint REQ_ID_NN NOT NULL,
        scope VARCHAR2(25) constraint REQ_SCOPE_NN NOT NULL,
        name VARCHAR2(255) constraint REQ_NAME_NN NOT NULL,
        requester VARCHAR2(20),
        request_type NUMBER(2) constraint REQ_DATATYPE_NN NOT NULL,
        transform_tag VARCHAR2(10),
        workload_id NUMBER(10),
        priority NUMBER(7),
        status NUMBER(2) constraint REQ_STATUS_ID_NN NOT NULL,
        substatus NUMBER(2),
        locking NUMBER(2),
        created_at DATE DEFAULT ON NULL SYS_EXTRACT_UTC(systimestamp(0)) constraint REQ_CREATED_NN NOT NULL,
        updated_at DATE DEFAULT ON NULL SYS_EXTRACT_UTC(systimestamp(0)) constraint REQ_UPDATED_NN NOT NULL,
        next_poll_at DATE DEFAULT ON NULL SYS_EXTRACT_UTC(systimestamp(0)) constraint REQ_NEXT_POLL_NN NOT NULL,
        accessed_at DATE,
        expired_at DATE,
        errors VARCHAR2(1024),
        request_metadata CLOB constraint REQ_REQUEST_METADATA_ENSURE_JSON CHECK(request_metadata IS JSON(LAX)),
        processing_metadata CLOB constraint REQ_PROCESSING_METADATA_ENSURE_JSON CHECK(processing_metadata IS JSON(LAX)),
        CONSTRAINT REQUESTS_PK PRIMARY KEY (request_id) USING INDEX LOCAL
        --- CONSTRAINT REQUESTS_NAME_SCOPE_UQ UNIQUE (name, scope, requester, request_type, transform_tag, workload_id) -- USING INDEX LOCAL,
)
PCTFREE 3
PARTITION BY RANGE(REQUEST_ID)
INTERVAL ( 100000 )
( PARTITION initial_part VALUES LESS THAN (1) );

CREATE INDEX REQUESTS_SCOPE_NAME_IDX ON REQUESTS (name, scope, workload_id) LOCAL;
--- drop index REQUESTS_STATUS_PRIORITY_IDX
CREATE INDEX REQUESTS_STATUS_PRIORITY_IDX ON REQUESTS (status, priority, request_id, locking, updated_at, next_poll_at, created_at) LOCAL COMPRESS 1;


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
        --- CONSTRAINT REQUESTS_NAME_SCOPE_UQ UNIQUE (name, scope, requester, request_type, transform_tag, workload_id) -- USING INDEX LOCAL,
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
        priority NUMBER(7),
        safe2get_output_from_input NUMBER(10),
        status NUMBER(2),
        substatus NUMBER(2),
        locking NUMBER(2),
        retries NUMBER(5) DEFAULT 0,
        created_at DATE DEFAULT ON NULL SYS_EXTRACT_UTC(systimestamp(0)) constraint TRANSFORM_CREATED_NN NOT NULL,
        updated_at DATE DEFAULT ON NULL SYS_EXTRACT_UTC(systimestamp(0)) constraint TRANSFORM_UPDATED_NN NOT NULL,
        next_poll_at DATE DEFAULT ON NULL SYS_EXTRACT_UTC(systimestamp(0)) constraint TRANSFORM_NEXT_POLL_NN NOT NULL,
        started_at DATE,
        finished_at DATE,
        expired_at DATE,
        transform_metadata CLOB constraint TRANSFORM_METADATA_ENSURE_JSON CHECK(transform_metadata IS JSON(LAX)),
        running_metadata CLOB,
        CONSTRAINT TRANSFORMS_PK PRIMARY KEY (transform_id)  
)
PCTFREE 3
PARTITION BY RANGE(TRANSFORM_ID)
INTERVAL ( 100000 )
( PARTITION initial_part VALUES LESS THAN (1) );

--- alter table transforms add next_poll_at DATE DEFAULT ON NULL SYS_EXTRACT_UTC(systimestamp(0)) constraint TRANSFORM_NEXT_POLL_NN NOT NULL;
CREATE INDEX TRANSFORMS_TYPE_TAG_IDX ON TRANSFORMS (transform_type, transform_tag, transform_id) LOCAL;
CREATE INDEX TRANSFORMS_STATUS_UPDATED_AT_IDX ON TRANSFORMS (status, locking, updated_at, next_poll_at, created_at) LOCAL;

--- req2transforms
CREATE TABLE REQ2TRANSFORMS
(
        request_id NUMBER(12) constraint REQ2TRANSFORM_REQ_ID_NN NOT NULL,
        transform_id NUMBER(12) constraint REQ2TRANSFORM_TASK_ID_NN NOT NULL,
        CONSTRAINT REQ2TRANSFORM_PK PRIMARY KEY (request_id, transform_id),
        CONSTRAINT REQ2TRANSFORM_REQ_ID_FK FOREIGN KEY(request_id) REFERENCES REQUESTS(request_id),
        CONSTRAINT REQ2TRANSFORM_TRANS_ID_FK FOREIGN KEY(transform_id) REFERENCES TRANSFORMS(transform_id)
)
PCTFREE 0
COMPRESS FOR OLTP;

--- req2workload
CREATE TABLE REQ2WORKLOAD
(
        request_id NUMBER(12) constraint REQ2TASKS_REQ_ID_NN NOT NULL,
        workload_id NUMBER(12) constraint REQ2TASKS_TASK_ID_NN NOT NULL,
        CONSTRAINT REQ2WORKLOAD_REQ_ID_FK FOREIGN KEY(request_id) REFERENCES REQUESTS(request_id)
)
PCTFREE 0
COMPRESS FOR OLTP
PARTITION BY REFERENCE(REQ2WORKLOAD_REQ_ID_FK);


--- workprogress2transform
CREATE TABLE WP2TRANSFORMS
(
        workprogress_id NUMBER(12) constraint WP2TRANSFORM_WP_ID_NN NOT NULL,
        transform_id NUMBER(12) constraint WP2TRANSFORM_TRANS_ID_NN NOT NULL,
        CONSTRAINT WP2TRANSFORM_PK PRIMARY KEY (workprogress_id, transform_id),
        CONSTRAINT WP2TRANSFORM_WORK_ID_FK FOREIGN KEY(workprogress_id) REFERENCES WORKPROGRESSES(workprogress_id),
        CONSTRAINT WP2TRANSFORM_TRANS_ID_FK FOREIGN KEY(transform_id) REFERENCES TRANSFORMS(transform_id)
)
PCTFREE 0
COMPRESS FOR OLTP;


---- processings
CREATE SEQUENCE PROCESSING_ID_SEQ MINVALUE 1 INCREMENT BY 1 ORDER NOCACHE NOCYCLE GLOBAL;
CREATE TABLE PROCESSINGS
(
        processing_id NUMBER(12) DEFAULT ON NULL PROCESSING_ID_SEQ.NEXTVAL constraint PROCESSING_ID_NN NOT NULL,
        transform_id NUMBER(12) constraint PROCESSINGS_TRANSFORM_ID_NN NOT NULL,
        request_id NUMBER(12),
        workload_id NUMBER(10),
        status NUMBER(2) constraint PROCESSINGS_STATUS_ID_NN NOT NULL,
        substatus NUMBER(2),
        locking NUMBER(2),
        submitter VARCHAR2(20),
        submitted_id NUMBER(12),
        granularity NUMBER(10),
        granularity_type NUMBER(2),
        created_at DATE DEFAULT ON NULL SYS_EXTRACT_UTC(systimestamp(0)) constraint PROCESSING_CREATED_NN NOT NULL,
        updated_at DATE DEFAULT ON NULL SYS_EXTRACT_UTC(systimestamp(0)) constraint PROCESSING_UPDATED_NN NOT NULL,
        next_poll_at DATE DEFAULT ON NULL SYS_EXTRACT_UTC(systimestamp(0)) constraint PROCESSING_NEXT_POLL_NN NOT NULL,
        submitted_at DATE,
        finished_at DATE,
        expired_at DATE,
        processing_metadata CLOB constraint PROCESSINGS_METADATA_ENSURE_JSON CHECK(processing_metadata IS JSON(LAX)),
        running_metadata CLOB,
        output_metadata CLOB constraint PROCESSINGS_OUTPUT_METADATA_ENSURE_JSON CHECK(output_metadata IS JSON(LAX)),
        CONSTRAINT PROCESSINGS_PK PRIMARY KEY (processing_id),
        CONSTRAINT PROCESSINGS_TRANSFORM_ID_FK FOREIGN KEY(transform_id) REFERENCES TRANSFORMS(transform_id)
)
PCTFREE 0
PARTITION BY REFERENCE(PROCESSINGS_TRANSFORM_ID_FK);

CREATE INDEX PROCESSINGS_STATUS_UPDATED_AT_IDX ON PROCESSINGS (status, locking, updated_at, next_poll_at, created_at) LOCAL;


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
    retries NUMBER(5) DEFAULT 0,
    created_at DATE DEFAULT ON NULL SYS_EXTRACT_UTC(systimestamp(0)) constraint COLLECTION_CREATED_NN NOT NULL,
    updated_at DATE DEFAULT ON NULL SYS_EXTRACT_UTC(systimestamp(0)) constraint COLLECTION_UPDATED_NN NOT NULL,
    next_poll_at DATE DEFAULT ON NULL SYS_EXTRACT_UTC(systimestamp(0)) constraint COLLECTION_NEXT_POLL_NN NOT NULL,
    accessed_at DATE,
    expired_at DATE,
    coll_metadata CLOB constraint COLLECTION_METADATA_ensure_json CHECK (COLL_METADATA IS JSON (LAX)),
    CONSTRAINT COLLECTION_PK PRIMARY KEY (coll_id), -- USING INDEX LOCAL,  
    CONSTRAINT COLLECTION_NAME_SCOPE_UQ UNIQUE (name, scope, transform_id, relation_type), -- USING INDEX LOCAL,
    CONSTRAINT COLLECTION_TRANSFORM_ID_FK FOREIGN KEY(transform_id) REFERENCES TRANSFORMS(transform_id)
)
PCTFREE 0
PARTITION BY REFERENCE(COLLECTION_TRANSFORM_ID_FK);

CREATE INDEX COLLECTIONS_STATUS_RELATIONTYPE_IDX ON COLLECTIONS(status, relation_type);
CREATE INDEX COLLECTIONS_TRANSFORM_IDX ON COLLECTIONS(transform_id, coll_id);
CREATE INDEX COLLECTIONS_STATUS_UPDATED_AT_IDX ON COLLECTIONS (status, locking, updated_at, next_poll_at, created_at) LOCAL;


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
        scope VARCHAR2(25) constraint CONTENT_SCOPE_NN NOT NULL,
        name VARCHAR2(255) constraint CONTENT_NAME_NN NOT NULL,
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
        path VARCHAR2(4000),
        created_at DATE DEFAULT ON NULL SYS_EXTRACT_UTC(systimestamp(0)) constraint CONTENT_CREATED_NN NOT NULL,
        updated_at DATE DEFAULT ON NULL SYS_EXTRACT_UTC(systimestamp(0)) constraint CONTENT_UPDATED_NN NOT NULL,
        accessed_at DATE,
        expired_at DATE,
        --- content_metadata CLOB constraint CONTENT_METADATA_ENSURE_JSON CHECK(CONTENT_METADATA IS JSON(LAX)),
        content_metadata VARCHAR2(100),
        --- CONSTRAINT CONTENT_PK PRIMARY KEY (name, scope, coll_id, content_type, min_id, max_id) USING INDEX LOCAL,
        CONSTRAINT CONTENT_PK PRIMARY KEY (content_id),
        ---- CONSTRAINT CONTENT_SCOPE_NAME_UQ UNIQUE (name, scope, coll_id, content_type, min_id, max_id) USING INDEX LOCAL,
        ---- CONSTRAINT CONTENT_SCOPE_NAME_UQ UNIQUE (name, scope, coll_id, min_id, max_id) USING INDEX LOCAL,
        CONSTRAINT CONTENT_ID_UQ UNIQUE (transform_id, coll_id, map_id, name, min_id, max_id) USING INDEX LOCAL,  
        CONSTRAINT CONTENT_TRANSFORM_ID_FK FOREIGN KEY(transform_id) REFERENCES TRANSFORMS(transform_id),
        CONSTRAINT CONTENT_COLL_ID_FK FOREIGN KEY(coll_id) REFERENCES COLLECTIONS(coll_id)
)
PCTFREE 0
PARTITION BY REFERENCE(CONTENT_TRANSFORM_ID_FK);

CREATE INDEX CONTENTS_STATUS_UPDATED_IDX ON CONTENTS (status, locking, updated_at, created_at) LOCAL;
CREATE INDEX CONTENTS_ID_NAME_IDX ON CONTENTS (coll_id, scope, name, status) LOCAL;
CREATE INDEX CONTENTS_REQ_TF_COLL_IDX ON CONTENTS (request_id, transform_id, coll_id, status) LOCAL;

alter table contents modify (min_id NUMBER(7) default 0)
alter table contents modify (max_id NUMBER(7) default 0)
alter table contents add content_relation_type NUMBER(2) default 0

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
    request_id NUMBER(12),
    workload_id NUMBER(10),
    transform_id NUMBER(12),
    num_contents NUMBER(7),
    created_at DATE DEFAULT ON NULL SYS_EXTRACT_UTC(systimestamp(0)),
    updated_at DATE DEFAULT ON NULL SYS_EXTRACT_UTC(systimestamp(0)),
    msg_content CLOB constraint MSG_CONTENT_ENSURE_JSON CHECK(msg_content IS JSON(LAX)),
    CONSTRAINT MESSAGES_PK PRIMARY KEY (msg_id) -- USING INDEX LOCAL,  
);


--- health
CREATE SEQUENCE HEALTH_ID_SEQ MINVALUE 1 INCREMENT BY 1 START WITH 1 NOCACHE ORDER NOCYCLE GLOBAL;
CREATE TABLE HEALTH
(
    health_id NUMBER(12) DEFAULT ON NULL HEALTH_ID_SEQ.NEXTVAL constraint HEALTH_ID_NN NOT NULL,
    agent VARCHAR2(30),
    hostname VARCHAR2(127),
    pid Number(12),
    thread_id Number(20),
    thread_name VARCHAR2(255),
    payload VARCHAR2(255),
    created_at DATE DEFAULT ON NULL SYS_EXTRACT_UTC(systimestamp(0)),
    updated_at DATE DEFAULT ON NULL SYS_EXTRACT_UTC(systimestamp(0)),
    CONSTRAINT HEALTH_PK PRIMARY KEY (health_id), -- USING INDEX LOCAL,  
    CONSTRAINT HEALTH_UQ UNIQUE (agent, hostname, pid, thread_id)
);
