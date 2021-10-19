#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2021


"""
performance test to insert contents.
"""
import cx_Oracle


from idds.common.config import config_get
# from idds.core.contents import add_content


def get_archive_sql(schema):
    sql = """
        BEGIN
            FOR i in (SELECT request_id, scope, name, requester, request_type, transform_tag,
                             workload_id, priority, status, substatus, locking, created_at,
                             updated_at, next_poll_at, accessed_at, expired_at, errors,
                             request_metadata, processing_metadata
                      FROM   {schema}.requests
                      WHERE  status in (3, 5, 9, 17) and created_at < sysdate - interval '3' month
                      order by request_id asc)
            LOOP
                --- archive records
                insert into {schema}.requests_archive(request_id, scope, name, requester,
                            request_type, transform_tag, workload_id, priority, status,
                            substatus, locking, created_at, updated_at, next_poll_at,
                            accessed_at, expired_at, errors, request_metadata,
                            processing_metadata)
                    values(i.request_id, i.scope, i.name, i.requester, i.request_type, i.transform_tag,
                           i.workload_id, i.priority, i.status, i.substatus, i.locking, i.created_at,
                           i.updated_at, i.next_poll_at, i.accessed_at, i.expired_at, i.errors,
                           i.request_metadata, i.processing_metadata);

                insert into {schema}.transforms_archive(transform_id, request_id, workload_id,
                                                        transform_type, transform_tag, priority,
                                                        safe2get_output_from_input, status,
                                                        substatus, locking, retries, created_at,
                                                        updated_at, next_poll_at, started_at,
                                                        finished_at, expired_at, transform_metadata,
                                                        running_metadata)
                    select transform_id, request_id, workload_id, transform_type, transform_tag,
                           priority, safe2get_output_from_input, status, substatus, locking,
                           retries, created_at, updated_at, next_poll_at, started_at, finished_at,
                           expired_at, transform_metadata, running_metadata
                    from {schema}.transforms where request_id=i.request_id;

                insert into {schema}.processings_archive(processing_id, transform_id, request_id,
                           workload_id, status, substatus, locking, submitter, submitted_id,
                           granularity, granularity_type, created_at, updated_at, next_poll_at,
                           submitted_at, finished_at, expired_at, processing_metadata,
                           running_metadata, output_metadata)
                    select processing_id, transform_id, request_id, workload_id, status, substatus,
                           locking, submitter, submitted_id, granularity, granularity_type,
                           created_at, updated_at, next_poll_at, submitted_at, finished_at, expired_at,
                           processing_metadata, running_metadata, output_metadata
                    from {schema}.processings where request_id=i.request_id;

                insert into {schema}.collections_archive(coll_id, coll_type, transform_id, request_id,
                           workload_id, relation_type, scope, name, bytes, status, substatus, locking,
                           total_files, storage_id, new_files, processed_files, processing_files,
                           processing_id, retries, created_at, updated_at, next_poll_at, accessed_at,
                           expired_at, coll_metadata)
                    select coll_id, coll_type, transform_id, request_id, workload_id, relation_type,
                           scope, name, bytes, status, substatus, locking, total_files, storage_id,
                           new_files, processed_files, processing_files, processing_id, retries,
                           created_at, updated_at, next_poll_at, accessed_at, expired_at,
                           coll_metadata
                    from {schema}.collections where request_id=i.request_id;

                insert into {schema}.contents_archive(content_id, transform_id, coll_id, request_id,
                           workload_id, map_id, scope, name, min_id, max_id, content_type,
                           content_relation_type, status, substatus, locking, bytes, md5, adler32,
                           processing_id, storage_id, retries, path, created_at, updated_at,
                           accessed_at, expired_at, content_metadata)
                    select content_id, transform_id, coll_id, request_id, workload_id, map_id,
                           scope, name, min_id, max_id, content_type, content_relation_type,
                           status, substatus, locking, bytes, md5, adler32, processing_id,
                           storage_id, retries, path, created_at, updated_at, accessed_at,
                           expired_at, content_metadata
                    from {schema}.contents where request_id=i.request_id;

                insert into {schema}.messages_archive(msg_id, msg_type, status, substatus, locking,
                           source, destination, request_id, workload_id, transform_id, processing_id,
                           num_contents, created_at, updated_at, msg_content)
                    select msg_id, msg_type, status, substatus, locking, source, destination,
                           request_id, workload_id, transform_id, processing_id, num_contents,
                           created_at, updated_at, msg_content
                    from {schema}.messages where request_id=i.request_id;


                -- clean records
                delete from {schema}.messages where request_id = i.request_id;
                delete from {schema}.contents where request_id = i.request_id;
                delete from {schema}.collections where request_id = i.request_id;
                delete from {schema}.processings where request_id = i.request_id;
                delete from {schema}.transforms where request_id = i.request_id;
                delete from {schema}.requests where request_id = i.request_id;
            END LOOP;
            COMMIT;
    END;
    """
    sql = sql.format(schema=schema)
    return sql


def run_archive_sql(db_pool, schema):
    connection = db_pool.acquire()

    sql = get_archive_sql(schema)
    # print(sql)
    cursor = connection.cursor()
    cursor.execute(sql)
    cursor.close()

    connection.commit()
    db_pool.release(connection)


def get_session_pool():
    sql_connection = config_get('database', 'default')
    sql_connection = sql_connection.replace("oracle://", "")
    user_pass, tns = sql_connection.split('@')
    user, passwd = user_pass.split(':')
    db_pool = cx_Oracle.SessionPool(user, passwd, tns, min=12, max=20, increment=1)

    schema = config_get('database', 'schema')
    return db_pool, schema


def run_archive():
    pool, schema = get_session_pool()
    run_archive_sql(pool, schema)


if __name__ == '__main__':
    run_archive()
