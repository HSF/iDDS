DROP SEQUENCE MESSAGE_ID_SEQ;
DROP SEQUENCE REQUEST_ID_SEQ;
DROP SEQUENCE WORKPROGRESS_ID_SEQ;
DROP SEQUENCE TRANSFORM_ID_SEQ;
DROP SEQUENCE PROCESSING_ID_SEQ;
DROP SEQUENCE COLLECTION_ID_SEQ;
DROP SEQUENCE CONTENT_ID_SEQ;


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
CREATE SEQUENCE REQUEST_ID_SEQ MINVALUE 1 INCREMENT BY 1 ORDER NOCACHE;
CREATE TABLE REQUESTS
(
        request_id NUMBER(12),
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
        created_at DATE DEFAULT SYS_EXTRACT_UTC(systimestamp(0)) constraint REQ_CREATED_NN NOT NULL,
        updated_at DATE DEFAULT SYS_EXTRACT_UTC(systimestamp(0)) constraint REQ_UPDATED_NN NOT NULL,
        next_poll_at DATE DEFAULT SYS_EXTRACT_UTC(systimestamp(0)) constraint REQ_NEXT_POLL_NN NOT NULL,
        accessed_at DATE,
        expired_at DATE,
        errors VARCHAR2(1024),
        request_metadata CLOB,
        processing_metadata CLOB,
        CONSTRAINT REQUESTS_PK PRIMARY KEY (request_id) USING INDEX LOCAL
        --- CONSTRAINT REQUESTS_NAME_SCOPE_UQ UNIQUE (name, scope, requester, request_type, transform_tag, workload_id) -- USING INDEX LOCAL,
)
PCTFREE 3
PARTITION BY RANGE(REQUEST_ID)
INTERVAL ( 100000 )
( PARTITION initial_part VALUES LESS THAN (1) );

CREATE OR REPLACE TRIGGER TRIG_REQUEST_ID
    BEFORE INSERT
    ON REQUESTS
    FOR EACH ROW
    BEGIN
        :NEW.request_id := REQUEST_ID_SEQ.NEXTVAL ;
    END;
 /

CREATE INDEX REQUESTS_SCOPE_NAME_IDX ON REQUESTS (name, scope, workload_id) LOCAL;
--- drop index REQUESTS_STATUS_PRIORITY_IDX
CREATE INDEX REQUESTS_STATUS_PRIORITY_IDX ON REQUESTS (status, priority, request_id, locking, updated_at, next_poll_at, created_at) LOCAL COMPRESS 1;


--- workprogress
CREATE SEQUENCE WORKPROGRESS_ID_SEQ MINVALUE 1 INCREMENT BY 1 ORDER NOCACHE;
CREATE TABLE WORKPROGRESSES
(
        workprogress_id NUMBER(12),
        request_id NUMBER(12) constraint WORKPROGRESS__REQ_ID_NN NOT NULL,
        workload_id NUMBER(10),
        scope VARCHAR2(25) constraint WORKPROGRESS_SCOPE_NN NOT NULL,
        name VARCHAR2(255) constraint WORKPROGRESS_NAME_NN NOT NULL,
        priority NUMBER(7),
        status NUMBER(2) constraint WORKPROGRESS_STATUS_ID_NN NOT NULL,
        substatus NUMBER(2),
        locking NUMBER(2),
        created_at DATE DEFAULT SYS_EXTRACT_UTC(systimestamp(0)) constraint WORKPROGRESS_CREATED_NN NOT NULL,
        updated_at DATE DEFAULT SYS_EXTRACT_UTC(systimestamp(0)) constraint WORKPROGRESS_UPDATED_NN NOT NULL,
        next_poll_at DATE DEFAULT SYS_EXTRACT_UTC(systimestamp(0)) constraint WORKPROGRESS_NEXT_POLL_NN NOT NULL,
        accessed_at DATE,
        expired_at DATE,
        errors VARCHAR2(1024),
        workprogress_metadata CLOB,
        processing_metadata CLOB,
        CONSTRAINT WORKPROGRESS_PK PRIMARY KEY (workprogress_id), --- USING INDEX LOCAL,
        CONSTRAINT WORKPROGRESS_REQ_ID_FK FOREIGN KEY(request_id) REFERENCES REQUESTS(request_id),
        --- CONSTRAINT REQUESTS_NAME_SCOPE_UQ UNIQUE (name, scope, requester, request_type, transform_tag, workload_id) -- USING INDEX LOCAL,
)
PCTFREE 3
PARTITION BY RANGE(REQUEST_ID)
INTERVAL ( 100000 )
( PARTITION initial_part VALUES LESS THAN (1) );

CREATE OR REPLACE TRIGGER TRIG_WORKPROGRESS_ID
    BEFORE INSERT
    ON WORKPROGRESSES
    FOR EACH ROW
    BEGIN
        :NEW.workprogress_id := WORKPROGRESS_ID_SEQ.NEXTVAL ;
    END;
 /

CREATE INDEX WORKPROGRESS_SCOPE_NAME_IDX ON WORKPROGRESSES (name, scope, workprogress_id) LOCAL;
--- drop index REQUESTS_STATUS_PRIORITY_IDX
CREATE INDEX WORKPROGRESS_STATUS_PRI_IDX ON WORKPROGRESSES (status, priority, workprogress_id, locking, updated_at, next_poll_at, created_at) LOCAL COMPRESS 1;


--- transforms
-- CREATE SEQUENCE TRANSFORM_ID_SEQ MINVALUE 1 INCREMENT BY 1 NOORDER CACHE 3 NOCYCLE;
CREATE SEQUENCE TRANSFORM_ID_SEQ MINVALUE 1 INCREMENT BY 1 NOCACHE;
CREATE TABLE TRANSFORMS
(
        transform_id NUMBER(12),
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
        created_at DATE DEFAULT SYS_EXTRACT_UTC(systimestamp(0)) constraint TRANSFORM_CREATED_NN NOT NULL,
        updated_at DATE DEFAULT SYS_EXTRACT_UTC(systimestamp(0)) constraint TRANSFORM_UPDATED_NN NOT NULL,
        next_poll_at DATE DEFAULT SYS_EXTRACT_UTC(systimestamp(0)) constraint TRANSFORM_NEXT_POLL_NN NOT NULL,
        started_at DATE,
        finished_at DATE,
        expired_at DATE,
        transform_metadata CLOB,
        CONSTRAINT TRANSFORMS_PK PRIMARY KEY (transform_id)  
)
PCTFREE 3
PARTITION BY RANGE(TRANSFORM_ID)
INTERVAL ( 100000 )
( PARTITION initial_part VALUES LESS THAN (1) );

CREATE OR REPLACE TRIGGER TRIG_TRANSFORM_ID
    BEFORE INSERT
    ON TRANSFORMS
    FOR EACH ROW
    BEGIN
        :NEW.transform_id := TRANSFORM_ID_SEQ.NEXTVAL ;
    END;
 /


CREATE INDEX TRANSFORMS_TYPE_TAG_IDX ON TRANSFORMS (transform_type, transform_tag, transform_id) LOCAL;
CREATE INDEX TRANSFORMS_STATUS_UPDATED_IDX ON TRANSFORMS (status, locking, updated_at, next_poll_at, created_at) LOCAL;

--- req2transforms
CREATE TABLE REQ2TRANSFORMS
(
        request_id NUMBER(12) constraint REQ2TRANSFORM_REQ_ID_NN NOT NULL,
        transform_id NUMBER(12) constraint REQ2TRANSFORM_TASK_ID_NN NOT NULL,
        CONSTRAINT REQ2TRANSFORM_PK PRIMARY KEY (request_id, transform_id),
        CONSTRAINT REQ2TRANSFORM_REQ_ID_FK FOREIGN KEY(request_id) REFERENCES REQUESTS(request_id),
        CONSTRAINT REQ2TRANSFORM_TRANS_ID_FK FOREIGN KEY(transform_id) REFERENCES TRANSFORMS(transform_id)
)
PCTFREE 3
PARTITION BY RANGE(REQUEST_ID)
INTERVAL ( 100000 )
( PARTITION initial_part VALUES LESS THAN (1) );


--- req2workload
CREATE TABLE REQ2WORKLOAD
(
        request_id NUMBER(12) constraint REQ2TASKS_REQ_ID_NN NOT NULL,
        workload_id NUMBER(12) constraint REQ2TASKS_TASK_ID_NN NOT NULL,
        CONSTRAINT REQ2WORKLOAD_REQ_ID_FK FOREIGN KEY(request_id) REFERENCES REQUESTS(request_id)
)
PCTFREE 3
PARTITION BY RANGE(REQUEST_ID)
INTERVAL ( 100000 )
( PARTITION initial_part VALUES LESS THAN (1) );
-- PCTFREE 0
-- COMPRESS FOR OLTP
-- PARTITION BY REFERENCE(REQ2WORKLOAD_REQ_ID_FK);


--- workprogress2transform
CREATE TABLE WP2TRANSFORMS
(
        workprogress_id NUMBER(12) constraint WP2TRANSFORM_WP_ID_NN NOT NULL,
        transform_id NUMBER(12) constraint WP2TRANSFORM_TRANS_ID_NN NOT NULL,
        CONSTRAINT WP2TRANSFORM_PK PRIMARY KEY (workprogress_id, transform_id),
        CONSTRAINT WP2TRANSFORM_WORK_ID_FK FOREIGN KEY(workprogress_id) REFERENCES WORKPROGRESSES(workprogress_id),
        CONSTRAINT WP2TRANSFORM_TRANS_ID_FK FOREIGN KEY(transform_id) REFERENCES TRANSFORMS(transform_id)
)
PCTFREE 3
PARTITION BY RANGE(workprogress_id)
INTERVAL ( 100000 )
( PARTITION initial_part VALUES LESS THAN (1) );


---- processings
-- CREATE SEQUENCE PROCESSING_ID_SEQ MINVALUE 1 INCREMENT BY 1 NOORDER CACHE 3 NOCYCLE;
CREATE SEQUENCE PROCESSING_ID_SEQ MINVALUE 1 INCREMENT BY 1  NOCACHE;
CREATE TABLE PROCESSINGS
(
        processing_id NUMBER(12),
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
        created_at DATE DEFAULT SYS_EXTRACT_UTC(systimestamp(0)) constraint PROCESSING_CREATED_NN NOT NULL,
        updated_at DATE DEFAULT SYS_EXTRACT_UTC(systimestamp(0)) constraint PROCESSING_UPDATED_NN NOT NULL,
        next_poll_at DATE DEFAULT SYS_EXTRACT_UTC(systimestamp(0)) constraint PROCESSING_NEXT_POLL_NN NOT NULL,
        submitted_at DATE,
        finished_at DATE,
        expired_at DATE,
        processing_metadata CLOB,
        output_metadata CLOB,
        CONSTRAINT PROCESSINGS_PK PRIMARY KEY (processing_id),
        CONSTRAINT PROCESSINGS_TRANSFORM_ID_FK FOREIGN KEY(transform_id) REFERENCES TRANSFORMS(transform_id)
)
PCTFREE 3
PARTITION BY RANGE(TRANSFORM_ID)
INTERVAL ( 100000 )
( PARTITION initial_part VALUES LESS THAN (1) );

--- PCTFREE 0
-- COMPRESS FOR OLTP
--- PARTITION BY REFERENCE(PROCESSINGS_TRANSFORM_ID_FK);

CREATE OR REPLACE TRIGGER TRIG_PROCESSING_ID
    BEFORE INSERT
    ON PROCESSINGS
    FOR EACH ROW
    BEGIN
        :NEW.processing_id := PROCESSING_ID_SEQ.NEXTVAL ;
    END;
 /

CREATE INDEX PROCESSINGS_STATUS_UPDATED_IDX ON PROCESSINGS (status, locking, updated_at, next_poll_at, created_at) LOCAL;


--- collections
-- CREATE SEQUENCE COLLECTION_ID_SEQ MINVALUE 1 INCREMENT BY 1 NOORDER CACHE 2 NOCYCLE;
CREATE SEQUENCE COLLECTION_ID_SEQ MINVALUE 1 INCREMENT BY 1 NOCACHE;
CREATE TABLE COLLECTIONS
(
    coll_id NUMBER(14),
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
    created_at DATE DEFAULT SYS_EXTRACT_UTC(systimestamp(0)) constraint COLLECTION_CREATED_NN NOT NULL,
    updated_at DATE DEFAULT SYS_EXTRACT_UTC(systimestamp(0)) constraint COLLECTION_UPDATED_NN NOT NULL,
    next_poll_at DATE DEFAULT SYS_EXTRACT_UTC(systimestamp(0)) constraint COLLECTION_NEXT_POLL_NN NOT NULL,
    accessed_at DATE,
    expired_at DATE,
    coll_metadata CLOB,
    CONSTRAINT COLLECTION_PK PRIMARY KEY (coll_id), -- USING INDEX LOCAL,  
    CONSTRAINT COLLECTION_NAME_SCOPE_UQ UNIQUE (name, scope, transform_id, relation_type), -- USING INDEX LOCAL,
    CONSTRAINT COLLECTION_TRANSFORM_ID_FK FOREIGN KEY(transform_id) REFERENCES TRANSFORMS(transform_id)
)
PCTFREE 3
PARTITION BY RANGE(TRANSFORM_ID)
INTERVAL ( 100000 )
( PARTITION initial_part VALUES LESS THAN (1) );

---PCTFREE 0
---COMPRESS FOR OLTP
---PARTITION BY REFERENCE(COLLECTION_TRANSFORM_ID_FK);

CREATE OR REPLACE TRIGGER TRIG_COLLECTION_ID
    BEFORE INSERT
    ON COLLECTIONS
    FOR EACH ROW
    BEGIN
        :NEW.coll_id := COLLECTION_ID_SEQ.NEXTVAL ;
    END;
 /

CREATE INDEX COLLECTIONS_STATUS_RELAT_IDX ON COLLECTIONS(status, relation_type);
CREATE INDEX COLLECTIONS_TRANSFORM_IDX ON COLLECTIONS(transform_id, coll_id);
CREATE INDEX COLLECTIONS_STATUS_UPDATED_IDX ON COLLECTIONS (status, locking, updated_at, next_poll_at, created_at) LOCAL;


--- contents
CREATE SEQUENCE CONTENT_ID_SEQ MINVALUE 1 INCREMENT BY 1 NOORDER CACHE 10 NOCYCLE;
CREATE TABLE CONTENTS
(
        content_id NUMBER(12),    
        transform_id NUMBER(12) constraint CONTENT_TRANSFORM_ID_NN NOT NULL,
        coll_id NUMBER(14) constraint CONTENT_COLL_ID_NN NOT NULL,
        request_id NUMBER(12),
        workload_id NUMBER(10),
        map_id NUMBER(12) DEFAULT 0,
        scope VARCHAR2(25) constraint CONTENT_SCOPE_NN NOT NULL,
        name VARCHAR2(255) constraint CONTENT_NAME_NN NOT NULL,
        min_id NUMBER(7) constraint CONTENT_MIN_ID_NN NOT NULL,
        max_id NUMBER(7) constraint CONTENT_MAX_ID_NN NOT NULL,
        content_type NUMBER(2) constraint CONTENT_TYPE_NN NOT NULL,
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
        created_at DATE DEFAULT SYS_EXTRACT_UTC(systimestamp(0)) constraint CONTENT_CREATED_NN NOT NULL,
        updated_at DATE DEFAULT SYS_EXTRACT_UTC(systimestamp(0)) constraint CONTENT_UPDATED_NN NOT NULL,
        accessed_at DATE,
        expired_at DATE,
        content_metadata CLOB,
        ---- CONSTRAINT CONTENT_PK PRIMARY KEY (name, scope, coll_id, content_type, min_id, max_id) USING INDEX LOCAL,
        CONSTRAINT CONTENT_PK PRIMARY KEY (content_id),
        --- CONSTRAINT CONTENT_SCOPE_NAME_UQ UNIQUE (name, scope, coll_id, content_type, min_id, max_id) USING INDEX LOCAL,
        --- CONSTRAINT CONTENT_SCOPE_NAME_UQ UNIQUE (name, scope, coll_id, min_id, max_id) USING INDEX LOCAL,
        CONSTRAINT CONTENT_ID_UQ UNIQUE (transform_id, coll_id, map_id) USING INDEX LOCAL,
        CONSTRAINT CONTENT_TRANSFORM_ID_FK FOREIGN KEY(transform_id) REFERENCES TRANSFORMS(transform_id),
        CONSTRAINT CONTENT_COLL_ID_FK FOREIGN KEY(coll_id) REFERENCES COLLECTIONS(coll_id)
)
PCTFREE 3
PARTITION BY RANGE(TRANSFORM_ID)
INTERVAL ( 10000 )
( PARTITION initial_part VALUES LESS THAN (1) );

---PCTFREE 0
---COMPRESS FOR OLTP
---PARTITION BY REFERENCE(CONTENT_COLL_ID_FK);

CREATE OR REPLACE TRIGGER TRIG_CONTENT_ID
    BEFORE INSERT
    ON CONTENTS
    FOR EACH ROW
    BEGIN
        :NEW.content_id := CONTENT_ID_SEQ.NEXTVAL ;
    END;
 /


CREATE INDEX CONTENTS_STATUS_UPDATED_IDX ON CONTENTS (status, locking, updated_at, created_at) LOCAL;


--- messages
CREATE SEQUENCE MESSAGE_ID_SEQ MINVALUE 1 INCREMENT BY 1 START WITH 1 NOCACHE NOORDER NOCYCLE;
CREATE TABLE MESSAGES
(
    msg_id NUMBER(12),
    msg_type NUMBER(2),
    status NUMBER(2),
    substatus NUMBER(2),
    locking NUMBER(2),
    source NUMBER(2),
    transform_id NUMBER(12),
    num_contents NUMBER(7),
    created_at DATE DEFAULT SYS_EXTRACT_UTC(systimestamp(0)),
    updated_at DATE DEFAULT SYS_EXTRACT_UTC(systimestamp(0)),
    msg_content CLOB,
    CONSTRAINT MESSAGES_PK PRIMARY KEY (msg_id) -- USING INDEX LOCAL,  
);

CREATE OR REPLACE TRIGGER TRIG_MESSAGE_ID
    BEFORE INSERT
    ON MESSAGES
    FOR EACH ROW
    BEGIN
        :NEW.msg_id := MESSAGE_ID_SEQ.NEXTVAL ;
    END;
 /




select r.request_id, r.scope, r.name, r.status, tr.transform_id, tr.transform_status, tr.in_status, tr.in_total_files, tr.in_processed_files, tr.out_status, tr.out_total_files, tr.out_processed_files
from requests r
 full outer join req2transforms rt on (r.request_id=rt.request_id)
 full outer join (
    select t.transform_id, t.status transform_status, in_coll.status in_status, in_coll.total_files in_total_files, in_coll.processed_files in_processed_files,
    out_coll.status out_status, out_coll.total_files out_total_files, out_coll.processed_files out_processed_files
    from transforms t
    full outer join (select coll_id , transform_id, status, total_files, processed_files from collections where relation_type = 0) in_coll on (t.transform_id = in_coll.transform_id)
    full outer join (select coll_id , transform_id, status, total_files, processed_files from collections where relation_type = 1) out_coll on (t.transform_id = out_coll.transform_id)
    ) tr on (rt.transform_id=tr.transform_id)

