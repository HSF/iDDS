
DROP TABLE "ATLAS_IDDS".contents

DROP TABLE "ATLAS_IDDS".wp2transforms

DROP TABLE "ATLAS_IDDS".collections

DROP TABLE "ATLAS_IDDS".processings

DROP TABLE "ATLAS_IDDS".workprogresses

DROP TABLE "ATLAS_IDDS".conditions

DROP TABLE "ATLAS_IDDS".meta_info

DROP TABLE "ATLAS_IDDS".throttlers

DROP TABLE "ATLAS_IDDS".events_archive

DROP TABLE "ATLAS_IDDS".events

DROP TABLE "ATLAS_IDDS".events_priority

DROP TABLE "ATLAS_IDDS".commands

DROP TABLE "ATLAS_IDDS".messages

DROP TABLE "ATLAS_IDDS".health

DROP TABLE "ATLAS_IDDS".contents_ext

DROP TABLE "ATLAS_IDDS".contents_update

DROP TABLE "ATLAS_IDDS".transforms

DROP TABLE "ATLAS_IDDS".requests

DROP SEQUENCE "ATLAS_IDDS"."REQUEST_ID_SEQ"
DROP SEQUENCE "ATLAS_IDDS"."WORKPROGRESS_ID_SEQ"
DROP SEQUENCE "ATLAS_IDDS"."TRANSFORM_ID_SEQ"
DROP SEQUENCE "ATLAS_IDDS"."PROCESSING_ID_SEQ"
DROP SEQUENCE "ATLAS_IDDS"."COLLECTION_ID_SEQ"
DROP SEQUENCE "ATLAS_IDDS"."CONTENT_ID_SEQ"
DROP SEQUENCE "ATLAS_IDDS"."HEALTH_ID_SEQ"
DROP SEQUENCE "ATLAS_IDDS"."MESSAGE_ID_SEQ"
DROP SEQUENCE "ATLAS_IDDS"."COMMAND_ID_SEQ"
DROP SEQUENCE "ATLAS_IDDS"."EVENT_ID_SEQ"
DROP SEQUENCE "ATLAS_IDDS"."THROTTLER_ID_SEQ"
DROP SEQUENCE "ATLAS_IDDS"."METAINFO_ID_SEQ"
DROP SEQUENCE "ATLAS_IDDS"."CONDITION_ID_SEQ"


CREATE SEQUENCE "ATLAS_IDDS"."REQUEST_ID_SEQ"

CREATE TABLE "ATLAS_IDDS".requests (
	request_id NUMBER(19) NOT NULL, 
	scope VARCHAR2(25 CHAR), 
	name VARCHAR2(255 CHAR), 
	requester VARCHAR2(20 CHAR), 
	request_type INTEGER NOT NULL, 
	username VARCHAR2(20 CHAR), 
	userdn VARCHAR2(200 CHAR), 
	transform_tag VARCHAR2(20 CHAR), 
	workload_id INTEGER, 
	priority INTEGER, 
	status INTEGER NOT NULL, 
	substatus INTEGER, 
	oldstatus INTEGER, 
	locking INTEGER NOT NULL, 
	created_at DATE NOT NULL, 
	updated_at DATE NOT NULL, 
	next_poll_at DATE, 
	accessed_at DATE, 
	expired_at DATE, 
	new_retries INTEGER, 
	update_retries INTEGER, 
	max_new_retries INTEGER, 
	max_update_retries INTEGER, 
	new_poll_period INTERVAL DAY TO SECOND, 
	update_poll_period INTERVAL DAY TO SECOND, 
	site VARCHAR2(50 CHAR), 
	campaign VARCHAR2(50 CHAR), 
	campaign_group VARCHAR2(250 CHAR), 
	campaign_tag VARCHAR2(20 CHAR), 
	errors VARCHAR2(1024 CHAR), 
	request_metadata CLOB, 
	processing_metadata CLOB, 
	CONSTRAINT "REQUESTS_PK" PRIMARY KEY (request_id), 
	CONSTRAINT "REQUESTS_STATUS_ID_NN" CHECK (status IS NOT NULL)
) PCTFREE 0;

CREATE INDEX "ATLAS_IDDS"."REQUESTS_SCOPE_NAME_IDX" ON "ATLAS_IDDS".requests (name, scope, workload_id);

CREATE INDEX "ATLAS_IDDS"."REQUESTS_STATUS_POLL_IDX" ON "ATLAS_IDDS".requests (status, priority, locking, updated_at, new_poll_period, update_poll_period, created_at, request_id);

CREATE INDEX "ATLAS_IDDS"."REQUESTS_STATUS_PRIO_IDX" ON "ATLAS_IDDS".requests (status, priority, request_id, locking, updated_at, next_poll_at, created_at);

CREATE INDEX "ATLAS_IDDS"."REQUESTS_STATUS_SITE" ON "ATLAS_IDDS".requests (status, site, request_id);

CREATE SEQUENCE "ATLAS_IDDS"."TRANSFORM_ID_SEQ"

CREATE TABLE "ATLAS_IDDS".transforms (
	transform_id NUMBER(19) NOT NULL, 
	request_id NUMBER(19) NOT NULL, 
	workload_id INTEGER, 
	transform_type INTEGER NOT NULL, 
	transform_tag VARCHAR2(20 CHAR), 
	internal_id VARCHAR2(20 CHAR), 
	priority INTEGER, 
	safe2get_output_from_input INTEGER, 
	status INTEGER NOT NULL, 
	substatus INTEGER, 
	oldstatus INTEGER, 
	locking INTEGER NOT NULL, 
	retries INTEGER, 
	parent_transform_id NUMBER(19), 
	previous_transform_id NUMBER(19), 
	current_processing_id NUMBER(19), 
	created_at DATE NOT NULL, 
	updated_at DATE NOT NULL, 
	next_poll_at DATE, 
	started_at DATE, 
	finished_at DATE, 
	expired_at DATE, 
	new_retries INTEGER, 
	update_retries INTEGER, 
	max_new_retries INTEGER, 
	max_update_retries INTEGER, 
	new_poll_period INTERVAL DAY TO SECOND, 
	update_poll_period INTERVAL DAY TO SECOND, 
	site VARCHAR2(50 CHAR), 
	name VARCHAR2(255 CHAR), 
	has_previous_conditions INTEGER, 
	loop_index INTEGER, 
	cloned_from NUMBER(19), 
	triggered_conditions CLOB, 
	untriggered_conditions CLOB, 
	errors VARCHAR2(1024 CHAR), 
	transform_metadata CLOB, 
	running_metadata CLOB, 
	CONSTRAINT "TRANSFORMS_PK" PRIMARY KEY (transform_id), 
	CONSTRAINT "TRANSFORMS_STATUS_ID_NN" CHECK (status IS NOT NULL)
) PCTFREE 0;

CREATE INDEX "ATLAS_IDDS"."TRANSFORMS_TYPE_TAG_IDX" ON "ATLAS_IDDS".transforms (transform_type, transform_tag, transform_id);

CREATE INDEX "ATLAS_IDDS"."TRANSFORMS_STATUS_SITE" ON "ATLAS_IDDS".transforms (status, site, request_id, transform_id);

CREATE INDEX "ATLAS_IDDS"."TRANSFORMS_REQ_IDX" ON "ATLAS_IDDS".transforms (request_id, transform_id);

CREATE INDEX "ATLAS_IDDS"."TRANSFORMS_STATUS_POLL_IDX" ON "ATLAS_IDDS".transforms (status, locking, updated_at, new_poll_period, update_poll_period, created_at, transform_id);

CREATE INDEX "ATLAS_IDDS"."TRANSFORMS_STATUS_UPDATED_AT_IDX" ON "ATLAS_IDDS".transforms (status, locking, updated_at, next_poll_at, created_at);


CREATE TABLE "ATLAS_IDDS".contents_update (
	content_id NUMBER(19) NOT NULL, 
	substatus INTEGER, 
	request_id NUMBER(19), 
	transform_id NUMBER(19), 
	workload_id INTEGER, 
	fetch_status INTEGER NOT NULL, 
	coll_id NUMBER(19), 
	content_metadata VARCHAR2(100 CHAR), 
	PRIMARY KEY (content_id)
) PCTFREE 0;


CREATE TABLE "ATLAS_IDDS".contents_ext (
	content_id NUMBER(19) NOT NULL, 
	transform_id NUMBER(19) NOT NULL, 
	coll_id NUMBER(19) NOT NULL, 
	request_id NUMBER(19) NOT NULL, 
	workload_id INTEGER, 
	map_id NUMBER(19) NOT NULL, 
	status INTEGER NOT NULL, 
	panda_id NUMBER(19), 
	job_definition_id NUMBER(19), 
	scheduler_id VARCHAR2(128 CHAR), 
	pilot_id VARCHAR2(200 CHAR), 
	creation_time DATE, 
	modification_time DATE, 
	start_time DATE, 
	end_time DATE, 
	prod_source_label VARCHAR2(20 CHAR), 
	prod_user_id VARCHAR2(250 CHAR), 
	assigned_priority INTEGER, 
	current_priority INTEGER, 
	attempt_nr INTEGER, 
	max_attempt INTEGER, 
	max_cpu_count INTEGER, 
	max_cpu_unit VARCHAR2(32 CHAR), 
	max_disk_count INTEGER, 
	max_disk_unit VARCHAR2(10 CHAR), 
	min_ram_count INTEGER, 
	min_ram_unit VARCHAR2(10 CHAR), 
	cpu_consumption_time INTEGER, 
	cpu_consumption_unit VARCHAR2(128 CHAR), 
	job_status VARCHAR2(10 CHAR), 
	job_name VARCHAR2(255 CHAR), 
	trans_exit_code INTEGER, 
	pilot_error_code INTEGER, 
	pilot_error_diag VARCHAR2(500 CHAR), 
	exe_error_code INTEGER, 
	exe_error_diag VARCHAR2(500 CHAR), 
	sup_error_code INTEGER, 
	sup_error_diag VARCHAR2(250 CHAR), 
	ddm_error_code INTEGER, 
	ddm_error_diag VARCHAR2(500 CHAR), 
	brokerage_error_code INTEGER, 
	brokerage_error_diag VARCHAR2(250 CHAR), 
	job_dispatcher_error_code INTEGER, 
	job_dispatcher_error_diag VARCHAR2(250 CHAR), 
	task_buffer_error_code INTEGER, 
	task_buffer_error_diag VARCHAR2(300 CHAR), 
	computing_site VARCHAR2(128 CHAR), 
	computing_element VARCHAR2(128 CHAR), 
	grid VARCHAR2(50 CHAR), 
	cloud VARCHAR2(50 CHAR), 
	cpu_conversion FLOAT, 
	task_id NUMBER(19), 
	vo VARCHAR2(16 CHAR), 
	pilot_timing VARCHAR2(100 CHAR), 
	working_group VARCHAR2(20 CHAR), 
	processing_type VARCHAR2(64 CHAR), 
	prod_user_name VARCHAR2(60 CHAR), 
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
	memory_leak VARCHAR2(10 CHAR), 
	memory_leak_x2 VARCHAR2(10 CHAR), 
	job_label VARCHAR2(20 CHAR), 
	CONSTRAINT "CONTENTS_EXT_PK" PRIMARY KEY (content_id)
) PCTFREE 0;

CREATE INDEX "ATLAS_IDDS"."CONTENTS_EXT_RTM_IDX" ON "ATLAS_IDDS".contents_ext (request_id, transform_id, map_id);

CREATE INDEX "ATLAS_IDDS"."CONTENTS_EXT_RTF_IDX" ON "ATLAS_IDDS".contents_ext (request_id, transform_id, workload_id, coll_id, content_id, panda_id, status);

CREATE INDEX "ATLAS_IDDS"."CONTENTS_EXT_RTW_IDX" ON "ATLAS_IDDS".contents_ext (request_id, transform_id, workload_id);

CREATE SEQUENCE "ATLAS_IDDS"."HEALTH_ID_SEQ"

CREATE TABLE "ATLAS_IDDS".health (
	health_id NUMBER(19) NOT NULL, 
	agent VARCHAR2(30 CHAR), 
	hostname VARCHAR2(500 CHAR), 
	pid INTEGER, 
	status INTEGER NOT NULL, 
	thread_id NUMBER(19), 
	thread_name VARCHAR2(255 CHAR), 
	payload VARCHAR2(2048 CHAR), 
	created_at DATE, 
	updated_at DATE, 
	CONSTRAINT "HEALTH_PK" PRIMARY KEY (health_id), 
	CONSTRAINT "HEALTH_UK" UNIQUE (agent, hostname, pid, thread_id)
) PCTFREE 0;

CREATE SEQUENCE "ATLAS_IDDS"."MESSAGE_ID_SEQ"

CREATE TABLE "ATLAS_IDDS".messages (
	msg_id NUMBER(19) NOT NULL, 
	msg_type INTEGER NOT NULL, 
	status INTEGER NOT NULL, 
	substatus INTEGER, 
	locking INTEGER NOT NULL, 
	source INTEGER NOT NULL, 
	destination INTEGER NOT NULL, 
	request_id NUMBER(19), 
	workload_id INTEGER, 
	transform_id INTEGER, 
	processing_id INTEGER, 
	internal_id VARCHAR2(20 CHAR), 
	num_contents INTEGER, 
	retries INTEGER, 
	fetching_id INTEGER, 
	created_at DATE NOT NULL, 
	updated_at DATE NOT NULL, 
	poll_period INTERVAL DAY TO SECOND NOT NULL, 
	msg_content CLOB, 
	CONSTRAINT "MESSAGES_PK" PRIMARY KEY (msg_id)
) PCTFREE 0;

CREATE INDEX "ATLAS_IDDS"."MESSAGES_TYPE_STU_IDX" ON "ATLAS_IDDS".messages (msg_type, status, destination, retries, updated_at, created_at);

CREATE INDEX "ATLAS_IDDS"."MESSAGES_ST_IDX" ON "ATLAS_IDDS".messages (status, destination, created_at);

CREATE INDEX "ATLAS_IDDS"."MESSAGES_TYPE_ST_IDX" ON "ATLAS_IDDS".messages (msg_type, status, destination, request_id);

CREATE INDEX "ATLAS_IDDS"."MESSAGES_TYPE_ST_TF_IDX" ON "ATLAS_IDDS".messages (msg_type, status, destination, transform_id);

CREATE INDEX "ATLAS_IDDS"."MESSAGES_TYPE_ST_PR_IDX" ON "ATLAS_IDDS".messages (msg_type, status, destination, processing_id);

CREATE SEQUENCE "ATLAS_IDDS"."COMMAND_ID_SEQ"

CREATE TABLE "ATLAS_IDDS".commands (
	cmd_id NUMBER(19) NOT NULL, 
	request_id NUMBER(19) NOT NULL, 
	workload_id INTEGER, 
	transform_id INTEGER, 
	processing_id INTEGER, 
	cmd_type INTEGER, 
	status INTEGER NOT NULL, 
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

CREATE INDEX "ATLAS_IDDS"."COMMANDS_TYPE_ST_PR_IDX" ON "ATLAS_IDDS".commands (cmd_type, status, destination, processing_id);

CREATE INDEX "ATLAS_IDDS"."COMMANDS_TYPE_ST_IDX" ON "ATLAS_IDDS".commands (cmd_type, status, destination, request_id);

CREATE INDEX "ATLAS_IDDS"."COMMANDS_STATUS_IDX" ON "ATLAS_IDDS".commands (status, locking, updated_at);

CREATE INDEX "ATLAS_IDDS"."COMMANDS_TYPE_ST_TF_IDX" ON "ATLAS_IDDS".commands (cmd_type, status, destination, transform_id);


CREATE TABLE "ATLAS_IDDS".events_priority (
	event_type INTEGER NOT NULL, 
	event_actual_id INTEGER NOT NULL, 
	priority INTEGER NOT NULL, 
	last_processed_at DATE NOT NULL, 
	updated_at DATE NOT NULL, 
	CONSTRAINT "EVENTS_PR_PK" PRIMARY KEY (event_type, event_actual_id)
) PCTFREE 0;

CREATE SEQUENCE "ATLAS_IDDS"."EVENT_ID_SEQ"

CREATE TABLE "ATLAS_IDDS".events (
	event_id NUMBER(19) NOT NULL, 
	event_type INTEGER NOT NULL, 
	event_actual_id INTEGER NOT NULL, 
	priority INTEGER, 
	status INTEGER NOT NULL, 
	created_at DATE NOT NULL, 
	processing_at DATE, 
	processed_at DATE, 
	content CLOB, 
	CONSTRAINT "EVENTS_PK" PRIMARY KEY (event_id)
) PCTFREE 0;


CREATE TABLE "ATLAS_IDDS".events_archive (
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
) PCTFREE 0;

CREATE SEQUENCE "ATLAS_IDDS"."THROTTLER_ID_SEQ"

CREATE TABLE "ATLAS_IDDS".throttlers (
	throttler_id NUMBER(19) NOT NULL, 
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
) PCTFREE 0;

CREATE SEQUENCE "ATLAS_IDDS"."METAINFO_ID_SEQ"

CREATE TABLE "ATLAS_IDDS".meta_info (
	meta_id NUMBER(19) NOT NULL, 
	name VARCHAR2(50 CHAR) NOT NULL, 
	status INTEGER NOT NULL, 
	created_at DATE NOT NULL, 
	updated_at DATE NOT NULL, 
	description VARCHAR2(1000 CHAR), 
	meta_info CLOB, 
	CONSTRAINT "METAINFO_PK" PRIMARY KEY (meta_id), 
	CONSTRAINT "METAINFO_NAME_UQ" UNIQUE (name)
) PCTFREE 0;

CREATE SEQUENCE "ATLAS_IDDS"."CONDITION_ID_SEQ"

CREATE TABLE "ATLAS_IDDS".conditions (
	condition_id NUMBER(19) NOT NULL, 
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
) PCTFREE 0;

CREATE SEQUENCE "ATLAS_IDDS"."WORKPROGRESS_ID_SEQ"

CREATE TABLE "ATLAS_IDDS".workprogresses (
	workprogress_id NUMBER(19) NOT NULL, 
	request_id NUMBER(19), 
	workload_id INTEGER, 
	scope VARCHAR2(25 CHAR), 
	name VARCHAR2(255 CHAR), 
	priority INTEGER, 
	status INTEGER, 
	substatus INTEGER, 
	locking INTEGER, 
	created_at DATE, 
	updated_at DATE, 
	next_poll_at DATE, 
	accessed_at DATE, 
	expired_at DATE, 
	errors VARCHAR2(1024 CHAR), 
	workprogress_metadata CLOB, 
	processing_metadata CLOB, 
	CONSTRAINT "WORKPROGRESS_PK" PRIMARY KEY (workprogress_id), 
	CONSTRAINT "REQ2WORKPROGRESS_REQ_ID_FK" FOREIGN KEY(request_id) REFERENCES "ATLAS_IDDS".requests (request_id), 
	CONSTRAINT "WORKPROGRESS_STATUS_ID_NN" CHECK (status IS NOT NULL)
) PCTFREE 0;

CREATE INDEX "ATLAS_IDDS"."WORKPROGRESS_SCOPE_NAME_IDX" ON "ATLAS_IDDS".workprogresses (name, scope, workprogress_id);

CREATE INDEX "ATLAS_IDDS"."WORKPROGRESS_STATUS_PRIO_IDX" ON "ATLAS_IDDS".workprogresses (status, priority, workprogress_id, locking, updated_at, next_poll_at, created_at);

CREATE SEQUENCE "ATLAS_IDDS"."PROCESSING_ID_SEQ"

CREATE TABLE "ATLAS_IDDS".processings (
	processing_id NUMBER(19) NOT NULL, 
	transform_id NUMBER(19) NOT NULL, 
	request_id NUMBER(19) NOT NULL, 
	workload_id INTEGER, 
	processing_type INTEGER NOT NULL, 
	status INTEGER NOT NULL, 
	substatus INTEGER, 
	oldstatus INTEGER, 
	locking INTEGER NOT NULL, 
	submitter VARCHAR2(20 CHAR), 
	submitted_id INTEGER, 
	granularity INTEGER, 
	granularity_type INTEGER, 
	created_at DATE NOT NULL, 
	updated_at DATE NOT NULL, 
	next_poll_at DATE, 
	poller_updated_at DATE, 
	submitted_at DATE, 
	finished_at DATE, 
	expired_at DATE, 
	new_retries INTEGER, 
	update_retries INTEGER, 
	max_new_retries INTEGER, 
	max_update_retries INTEGER, 
	new_poll_period INTERVAL DAY TO SECOND, 
	update_poll_period INTERVAL DAY TO SECOND, 
	site VARCHAR2(50 CHAR), 
	errors VARCHAR2(1024 CHAR), 
	processing_metadata CLOB, 
	running_metadata CLOB, 
	output_metadata CLOB, 
	CONSTRAINT "PROCESSINGS_PK" PRIMARY KEY (processing_id), 
	CONSTRAINT "PROCESSINGS_TRANSFORM_ID_FK" FOREIGN KEY(transform_id) REFERENCES "ATLAS_IDDS".transforms (transform_id), 
	CONSTRAINT "PROCESSINGS_STATUS_ID_NN" CHECK (status IS NOT NULL), 
	CONSTRAINT "PROCESSINGS_TRANSFORM_ID_NN" CHECK (transform_id IS NOT NULL)
) PCTFREE 0;

CREATE INDEX "ATLAS_IDDS"."PROCESSINGS_STATUS_POLL_IDX" ON "ATLAS_IDDS".processings (status, processing_id, locking, updated_at, new_poll_period, update_poll_period, created_at);

CREATE INDEX "ATLAS_IDDS"."PROCESSINGS_STATUS_SITE" ON "ATLAS_IDDS".processings (status, site, request_id, transform_id, processing_id);

CREATE INDEX "ATLAS_IDDS"."PROCESSINGS_STATUS_UPDATED_IDX" ON "ATLAS_IDDS".processings (status, locking, updated_at, next_poll_at, created_at);

CREATE SEQUENCE "ATLAS_IDDS"."COLLECTION_ID_SEQ"

CREATE TABLE "ATLAS_IDDS".collections (
	coll_id NUMBER(19) NOT NULL, 
	request_id NUMBER(19) NOT NULL, 
	workload_id INTEGER, 
	transform_id NUMBER(19) NOT NULL, 
	coll_type INTEGER NOT NULL, 
	relation_type INTEGER NOT NULL, 
	scope VARCHAR2(25 CHAR), 
	name VARCHAR2(255 CHAR), 
	bytes INTEGER, 
	status INTEGER NOT NULL, 
	substatus INTEGER, 
	locking INTEGER NOT NULL, 
	total_files INTEGER, 
	storage_id INTEGER, 
	new_files INTEGER, 
	processed_files INTEGER, 
	processing_files INTEGER, 
	failed_files INTEGER, 
	missing_files INTEGER, 
	ext_files INTEGER, 
	processed_ext_files INTEGER, 
	failed_ext_files INTEGER, 
	missing_ext_files INTEGER, 
	processing_id INTEGER, 
	retries INTEGER, 
	created_at DATE NOT NULL, 
	updated_at DATE NOT NULL, 
	next_poll_at DATE, 
	accessed_at DATE, 
	expired_at DATE, 
	coll_metadata CLOB, 
	CONSTRAINT "COLLECTIONS_PK" PRIMARY KEY (coll_id), 
	CONSTRAINT "COLLECTIONS_NAME_SCOPE_UQ" UNIQUE (name, scope, transform_id, relation_type), 
	CONSTRAINT "COLLECTIONS_TRANSFORM_ID_FK" FOREIGN KEY(transform_id) REFERENCES "ATLAS_IDDS".transforms (transform_id), 
	CONSTRAINT "COLLECTIONS_STATUS_ID_NN" CHECK (status IS NOT NULL), 
	CONSTRAINT "COLLECTIONS_TRANSFORM_ID_NN" CHECK (transform_id IS NOT NULL)
) PCTFREE 0;

CREATE INDEX "ATLAS_IDDS"."COLLECTIONS_TRANSFORM_IDX" ON "ATLAS_IDDS".collections (transform_id, coll_id);

CREATE INDEX "ATLAS_IDDS"."COLLECTIONS_STATUS_RELAT_IDX" ON "ATLAS_IDDS".collections (status, relation_type);

CREATE INDEX "ATLAS_IDDS"."COLLECTIONS_REQ_IDX" ON "ATLAS_IDDS".collections (request_id, transform_id, updated_at);

CREATE INDEX "ATLAS_IDDS"."COLLECTIONS_STATUS_UPDATED_IDX" ON "ATLAS_IDDS".collections (status, locking, updated_at, next_poll_at, created_at);


CREATE TABLE "ATLAS_IDDS".wp2transforms (
	workprogress_id NUMBER(19) NOT NULL, 
	transform_id NUMBER(19) NOT NULL, 
	CONSTRAINT "WP2TRANSFORM_PK" PRIMARY KEY (workprogress_id, transform_id), 
	CONSTRAINT "WP2TRANSFORM_WORK_ID_FK" FOREIGN KEY(workprogress_id) REFERENCES "ATLAS_IDDS".workprogresses (workprogress_id), 
	CONSTRAINT "WP2TRANSFORM_TRANS_ID_FK" FOREIGN KEY(transform_id) REFERENCES "ATLAS_IDDS".transforms (transform_id)
) PCTFREE 0;

CREATE SEQUENCE "ATLAS_IDDS"."CONTENT_ID_SEQ"

CREATE TABLE "ATLAS_IDDS".contents (
	content_id NUMBER(19) NOT NULL, 
	transform_id NUMBER(19) NOT NULL, 
	coll_id NUMBER(19) NOT NULL, 
	request_id NUMBER(19) NOT NULL, 
	workload_id INTEGER, 
	map_id NUMBER(19) NOT NULL, 
	sub_map_id NUMBER(19), 
	dep_sub_map_id NUMBER(19), 
	content_dep_id NUMBER(19), 
	scope VARCHAR2(25 CHAR), 
	name VARCHAR2(4000 CHAR), 
	name_md5 VARCHAR2(33 CHAR), 
	scope_name_md5 VARCHAR2(33 CHAR), 
	min_id INTEGER, 
	max_id INTEGER, 
	content_type INTEGER NOT NULL, 
	content_relation_type INTEGER NOT NULL, 
	status INTEGER NOT NULL, 
	substatus INTEGER, 
	locking INTEGER NOT NULL, 
	bytes INTEGER, 
	md5 VARCHAR2(32 CHAR), 
	adler32 VARCHAR2(8 CHAR), 
	processing_id INTEGER, 
	storage_id INTEGER, 
	retries INTEGER, 
	external_coll_id NUMBER(19), 
	external_content_id NUMBER(19), 
	external_event_id NUMBER(19), 
	external_event_status INTEGER, 
	path VARCHAR2(4000 CHAR), 
	created_at DATE, 
	updated_at DATE, 
	accessed_at DATE, 
	expired_at DATE, 
	content_metadata VARCHAR2(1000 CHAR), 
	CONSTRAINT "CONTENTS_PK" PRIMARY KEY (content_id), 
	CONSTRAINT "CONTENT_ID_UQ" UNIQUE (transform_id, coll_id, map_id, sub_map_id, dep_sub_map_id, content_relation_type, name_md5, scope_name_md5, min_id, max_id), 
	CONSTRAINT "CONTENTS_TRANSFORM_ID_FK" FOREIGN KEY(transform_id) REFERENCES "ATLAS_IDDS".transforms (transform_id), 
	CONSTRAINT "CONTENTS_COLL_ID_FK" FOREIGN KEY(coll_id) REFERENCES "ATLAS_IDDS".collections (coll_id), 
	CONSTRAINT "CONTENTS_STATUS_ID_NN" CHECK (status IS NOT NULL), 
	CONSTRAINT "CONTENTS_COLL_ID_NN" CHECK (coll_id IS NOT NULL)
) PCTFREE 0;

CREATE INDEX "ATLAS_IDDS"."CONTENTS_TF_IDX" ON "ATLAS_IDDS".contents (transform_id, request_id, coll_id, map_id, content_relation_type);

CREATE INDEX "ATLAS_IDDS"."CONTENTS_DEP_IDX" ON "ATLAS_IDDS".contents (request_id, transform_id, content_dep_id);

CREATE INDEX "ATLAS_IDDS"."CONTENTS_REQ_TF_COLL_IDX" ON "ATLAS_IDDS".contents (request_id, transform_id, workload_id, coll_id, content_relation_type, status, substatus);

CREATE INDEX "ATLAS_IDDS"."CONTENTS_ID_NAME_IDX" ON "ATLAS_IDDS".contents (coll_id, scope, md5('name');
, status);

CREATE INDEX "ATLAS_IDDS"."CONTENTS_STATUS_UPDATED_IDX" ON "ATLAS_IDDS".contents (status, locking, updated_at, created_at);

CREATE INDEX "ATLAS_IDDS"."CONTENTS_REL_IDX" ON "ATLAS_IDDS".contents (request_id, content_relation_type, transform_id, substatus);

