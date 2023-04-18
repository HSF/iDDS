--- with schema doma_idds
CREATE SEQUENCE doma_idds."REQUEST_ID_SEQ" START WITH 1

CREATE TABLE doma_idds.requests (
	request_id BIGINT NOT NULL, 
	scope VARCHAR(25), 
	name VARCHAR(255), 
	requester VARCHAR(20), 
	request_type INTEGER NOT NULL, 
	username VARCHAR(20), 
	userdn VARCHAR(200), 
	transform_tag VARCHAR(20), 
	workload_id INTEGER, 
	priority INTEGER, 
	status INTEGER NOT NULL, 
	substatus INTEGER, 
	oldstatus INTEGER, 
	locking INTEGER NOT NULL, 
	created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL, 
	updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL, 
	next_poll_at TIMESTAMP WITHOUT TIME ZONE, 
	accessed_at TIMESTAMP WITHOUT TIME ZONE, 
	expired_at TIMESTAMP WITHOUT TIME ZONE, 
	new_retries INTEGER, 
	update_retries INTEGER, 
	max_new_retries INTEGER, 
	max_update_retries INTEGER, 
	new_poll_period INTERVAL, 
	update_poll_period INTERVAL, 
	errors VARCHAR(1024), 
	request_metadata JSONB, 
	processing_metadata JSONB, 
	PRIMARY KEY (request_id)
);

CREATE SEQUENCE doma_idds."WORKPROGRESS_ID_SEQ" START WITH 1

CREATE TABLE doma_idds.workprogresses (
	workprogress_id BIGINT NOT NULL, 
	request_id BIGINT, 
	workload_id INTEGER, 
	scope VARCHAR(25), 
	name VARCHAR(255), 
	priority INTEGER, 
	status INTEGER, 
	substatus INTEGER, 
	locking INTEGER, 
	created_at TIMESTAMP WITHOUT TIME ZONE, 
	updated_at TIMESTAMP WITHOUT TIME ZONE, 
	next_poll_at TIMESTAMP WITHOUT TIME ZONE, 
	accessed_at TIMESTAMP WITHOUT TIME ZONE, 
	expired_at TIMESTAMP WITHOUT TIME ZONE, 
	errors VARCHAR(1024), 
	workprogress_metadata JSONB, 
	processing_metadata JSONB, 
	PRIMARY KEY (workprogress_id)
);

CREATE SEQUENCE doma_idds."TRANSFORM_ID_SEQ" START WITH 1

CREATE TABLE doma_idds.transforms (
	transform_id BIGINT NOT NULL, 
	request_id BIGINT NOT NULL, 
	workload_id INTEGER, 
	transform_type INTEGER NOT NULL, 
	transform_tag VARCHAR(20), 
	priority INTEGER, 
	safe2get_output_from_input INTEGER, 
	status INTEGER NOT NULL, 
	substatus INTEGER, 
	oldstatus INTEGER, 
	locking INTEGER NOT NULL, 
	retries INTEGER, 
	created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL, 
	updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL, 
	next_poll_at TIMESTAMP WITHOUT TIME ZONE, 
	started_at TIMESTAMP WITHOUT TIME ZONE, 
	finished_at TIMESTAMP WITHOUT TIME ZONE, 
	expired_at TIMESTAMP WITHOUT TIME ZONE, 
	new_retries INTEGER, 
	update_retries INTEGER, 
	max_new_retries INTEGER, 
	max_update_retries INTEGER, 
	new_poll_period INTERVAL, 
	update_poll_period INTERVAL, 
	name VARCHAR(255), 
	errors VARCHAR(1024), 
	transform_metadata JSONB, 
	running_metadata JSONB, 
	PRIMARY KEY (transform_id)
);


CREATE TABLE doma_idds.wp2transforms (
	workprogress_id BIGINT NOT NULL, 
	transform_id BIGINT NOT NULL, 
	PRIMARY KEY (workprogress_id, transform_id)
);

CREATE SEQUENCE doma_idds."PROCESSING_ID_SEQ" START WITH 1

CREATE TABLE doma_idds.processings (
	processing_id BIGINT NOT NULL, 
	transform_id BIGINT NOT NULL, 
	request_id BIGINT NOT NULL, 
	workload_id INTEGER, 
	status INTEGER NOT NULL, 
	substatus INTEGER, 
	oldstatus INTEGER, 
	locking INTEGER NOT NULL, 
	submitter VARCHAR(20), 
	submitted_id INTEGER, 
	granularity INTEGER, 
	granularity_type INTEGER, 
	created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL, 
	updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL, 
	next_poll_at TIMESTAMP WITHOUT TIME ZONE, 
	poller_updated_at TIMESTAMP WITHOUT TIME ZONE, 
	submitted_at TIMESTAMP WITHOUT TIME ZONE, 
	finished_at TIMESTAMP WITHOUT TIME ZONE, 
	expired_at TIMESTAMP WITHOUT TIME ZONE, 
	new_retries INTEGER, 
	update_retries INTEGER, 
	max_new_retries INTEGER, 
	max_update_retries INTEGER, 
	new_poll_period INTERVAL, 
	update_poll_period INTERVAL, 
	errors VARCHAR(1024), 
	processing_metadata JSONB, 
	running_metadata JSONB, 
	output_metadata JSONB, 
	PRIMARY KEY (processing_id)
);

CREATE SEQUENCE doma_idds."COLLECTION_ID_SEQ" START WITH 1

CREATE TABLE doma_idds.collections (
	coll_id BIGINT NOT NULL, 
	request_id BIGINT NOT NULL, 
	workload_id INTEGER, 
	transform_id BIGINT NOT NULL, 
	coll_type INTEGER NOT NULL, 
	relation_type INTEGER NOT NULL, 
	scope VARCHAR(25), 
	name VARCHAR(255), 
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
	created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL, 
	updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL, 
	next_poll_at TIMESTAMP WITHOUT TIME ZONE, 
	accessed_at TIMESTAMP WITHOUT TIME ZONE, 
	expired_at TIMESTAMP WITHOUT TIME ZONE, 
	coll_metadata JSONB, 
	PRIMARY KEY (coll_id)
);

CREATE SEQUENCE doma_idds."CONTENT_ID_SEQ" START WITH 1

CREATE TABLE doma_idds.contents (
	content_id BIGINT NOT NULL, 
	transform_id BIGINT NOT NULL, 
	coll_id BIGINT NOT NULL, 
	request_id BIGINT NOT NULL, 
	workload_id INTEGER, 
	map_id BIGINT NOT NULL, 
	content_dep_id BIGINT, 
	scope VARCHAR(25), 
	name VARCHAR(4000), 
	min_id INTEGER, 
	max_id INTEGER, 
	content_type INTEGER NOT NULL, 
	content_relation_type INTEGER NOT NULL, 
	status INTEGER NOT NULL, 
	substatus INTEGER, 
	locking INTEGER NOT NULL, 
	bytes INTEGER, 
	md5 VARCHAR(32), 
	adler32 VARCHAR(8), 
	processing_id INTEGER, 
	storage_id INTEGER, 
	retries INTEGER, 
	path VARCHAR(4000), 
	created_at TIMESTAMP WITHOUT TIME ZONE, 
	updated_at TIMESTAMP WITHOUT TIME ZONE, 
	accessed_at TIMESTAMP WITHOUT TIME ZONE, 
	expired_at TIMESTAMP WITHOUT TIME ZONE, 
	content_metadata VARCHAR(100), 
	PRIMARY KEY (content_id)
);


        SET search_path TO doma_idds;
        CREATE OR REPLACE PROCEDURE update_contents_to_others(request_id_in int, transform_id_in int)
        AS $$
        BEGIN
            UPDATE doma_idds.contents set substatus = d.substatus from
            (select content_id, content_dep_id, substatus from doma_idds.contents where request_id = request_id_in and transform_id = transform_id_in and content_relation_type = 1 and status != 0) d
            where doma_idds.contents.request_id = request_id_in and doma_idds.contents.content_relation_type = 3 and doma_idds.contents.substatus != d.substatus and d.content_id = doma_idds.contents.content_dep_id;
        END;
        $$ LANGUAGE PLPGSQL
    

        SET search_path TO doma_idds;
        CREATE OR REPLACE PROCEDURE update_contents_from_others(request_id_in int, transform_id_in int)
        AS $$
        BEGIN

            UPDATE doma_idds.contents set substatus = d.substatus from
            (select content_id, content_dep_id, substatus from doma_idds.contents where request_id = request_id_in and content_relation_type = 1 and status != 0) d
            where doma_idds.contents.request_id = request_id_in and doma_idds.contents.transform_id = transform_id_in and doma_idds.contents.content_relation_type = 3 and doma_idds.contents.substatus != d.substatus and d.content_id = doma_idds.contents.content_dep_id;
        END;
        $$ LANGUAGE PLPGSQL
    

CREATE TABLE doma_idds.contents_update (
	content_id BIGSERIAL NOT NULL, 
	substatus INTEGER, 
	request_id BIGINT, 
	transform_id BIGINT, 
	workload_id INTEGER, 
	coll_id BIGINT, 
	content_metadata VARCHAR(100), 
	PRIMARY KEY (content_id)
);


CREATE TABLE doma_idds.contents_ext (
	content_id BIGSERIAL NOT NULL, 
	transform_id BIGINT NOT NULL, 
	coll_id BIGINT NOT NULL, 
	request_id BIGINT NOT NULL, 
	workload_id INTEGER, 
	map_id BIGINT NOT NULL, 
	status INTEGER NOT NULL, 
	panda_id BIGINT, 
	job_definition_id BIGINT, 
	scheduler_id VARCHAR(128), 
	pilot_id VARCHAR(200), 
	creation_time TIMESTAMP WITHOUT TIME ZONE, 
	modification_time TIMESTAMP WITHOUT TIME ZONE, 
	start_time TIMESTAMP WITHOUT TIME ZONE, 
	end_time TIMESTAMP WITHOUT TIME ZONE, 
	prod_source_label VARCHAR(20), 
	prod_user_id VARCHAR(250), 
	assigned_priority INTEGER, 
	current_priority INTEGER, 
	attempt_nr INTEGER, 
	max_attempt INTEGER, 
	max_cpu_count INTEGER, 
	max_cpu_unit VARCHAR(32), 
	max_disk_count INTEGER, 
	max_disk_unit VARCHAR(10), 
	min_ram_count INTEGER, 
	min_ram_unit VARCHAR(10), 
	cpu_consumption_time INTEGER, 
	cpu_consumption_unit VARCHAR(128), 
	job_status VARCHAR(10), 
	job_name VARCHAR(255), 
	trans_exit_code INTEGER, 
	pilot_error_code INTEGER, 
	pilot_error_diag VARCHAR(500), 
	exe_error_code INTEGER, 
	exe_error_diag VARCHAR(500), 
	sup_error_code INTEGER, 
	sup_error_diag VARCHAR(250), 
	ddm_error_code INTEGER, 
	ddm_error_diag VARCHAR(500), 
	brokerage_error_code INTEGER, 
	brokerage_error_diag VARCHAR(250), 
	job_dispatcher_error_code INTEGER, 
	job_dispatcher_error_diag VARCHAR(250), 
	task_buffer_error_code INTEGER, 
	task_buffer_error_diag VARCHAR(300), 
	computing_site VARCHAR(128), 
	computing_element VARCHAR(128), 
	grid VARCHAR(50), 
	cloud VARCHAR(50), 
	cpu_conversion FLOAT, 
	task_id BIGINT, 
	vo VARCHAR(16), 
	pilot_timing VARCHAR(100), 
	working_group VARCHAR(20), 
	processing_type VARCHAR(64), 
	prod_user_name VARCHAR(60), 
	core_count INTEGER, 
	n_input_files INTEGER, 
	req_id BIGINT, 
	jedi_task_id BIGINT, 
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
	memory_leak VARCHAR(10), 
	memory_leak_x2 VARCHAR(10), 
	job_label VARCHAR(20), 
	PRIMARY KEY (content_id)
);

CREATE SEQUENCE doma_idds."HEALTH_ID_SEQ" START WITH 1

CREATE TABLE doma_idds.health (
	health_id BIGINT NOT NULL, 
	agent VARCHAR(30), 
	hostname VARCHAR(127), 
	pid INTEGER, 
	status INTEGER NOT NULL, 
	thread_id BIGINT, 
	thread_name VARCHAR(255), 
	created_at TIMESTAMP WITHOUT TIME ZONE, 
	updated_at TIMESTAMP WITHOUT TIME ZONE, 
	payload VARCHAR(2048), 
	PRIMARY KEY (health_id)
);

CREATE SEQUENCE doma_idds."MESSAGE_ID_SEQ" START WITH 1

CREATE TABLE doma_idds.messages (
	msg_id BIGINT NOT NULL, 
	msg_type INTEGER NOT NULL, 
	status INTEGER NOT NULL, 
	substatus INTEGER, 
	locking INTEGER NOT NULL, 
	source INTEGER NOT NULL, 
	destination INTEGER NOT NULL, 
	request_id BIGINT NOT NULL, 
	workload_id INTEGER, 
	transform_id INTEGER NOT NULL, 
	processing_id INTEGER NOT NULL, 
	num_contents INTEGER, 
	retries INTEGER, 
	created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL, 
	updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL, 
	msg_content JSONB, 
	PRIMARY KEY (msg_id)
);

CREATE SEQUENCE doma_idds."COMMAND_ID_SEQ" START WITH 1

CREATE TABLE doma_idds.commands (
	cmd_id BIGINT NOT NULL, 
	request_id BIGINT NOT NULL, 
	workload_id INTEGER, 
	transform_id INTEGER, 
	processing_id INTEGER, 
	cmd_type INTEGER, 
	status INTEGER NOT NULL, 
	substatus INTEGER, 
	locking INTEGER NOT NULL, 
	username VARCHAR(50), 
	retries INTEGER, 
	source INTEGER, 
	destination INTEGER, 
	created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL, 
	updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL, 
	cmd_content JSONB, 
	errors VARCHAR(1024), 
	PRIMARY KEY (cmd_id)
);


CREATE TABLE doma_idds.events_priority (
	event_type INTEGER NOT NULL, 
	event_actual_id INTEGER NOT NULL, 
	priority INTEGER NOT NULL, 
	last_processed_at TIMESTAMP WITHOUT TIME ZONE NOT NULL, 
	updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL, 
	PRIMARY KEY (event_type, event_actual_id)
);

CREATE SEQUENCE doma_idds."EVENT_ID_SEQ" START WITH 1

CREATE TABLE doma_idds.events (
	event_id BIGINT NOT NULL, 
	event_type INTEGER NOT NULL, 
	event_actual_id INTEGER NOT NULL, 
	priority INTEGER, 
	status INTEGER NOT NULL, 
	created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL, 
	processing_at TIMESTAMP WITHOUT TIME ZONE, 
	processed_at TIMESTAMP WITHOUT TIME ZONE, 
	content JSONB, 
	PRIMARY KEY (event_id)
);


CREATE TABLE doma_idds.events_archive (
	event_id BIGSERIAL NOT NULL, 
	event_type INTEGER NOT NULL, 
	event_actual_id INTEGER NOT NULL, 
	priority INTEGER, 
	status INTEGER NOT NULL, 
	created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL, 
	processing_at TIMESTAMP WITHOUT TIME ZONE, 
	processed_at TIMESTAMP WITHOUT TIME ZONE, 
	content JSONB, 
	PRIMARY KEY (event_id)
);

