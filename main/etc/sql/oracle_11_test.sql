DROP SEQUENCE REQUEST_ID_SEQ;

DROP table REQUESTS purge;

--- requests
CREATE SEQUENCE REQUEST_ID_SEQ MINVALUE 1 INCREMENT BY 1 ORDER CACHE 3 NOCYCLE;
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

