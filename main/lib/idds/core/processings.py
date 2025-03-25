#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019 - 2024


"""
operations related to Processings.
"""

import datetime

from idds.orm.base.session import read_session, transactional_session
from idds.common.constants import ProcessingLocking, ProcessingStatus, ProcessingType, GranularityType, ContentRelationType
from idds.common.utils import get_list_chunks, get_process_thread_info
from idds.orm import (processings as orm_processings,
                      collections as orm_collections,
                      contents as orm_contents,
                      messages as orm_messages,
                      transforms as orm_transforms)


@transactional_session
def add_processing(request_id, workload_id, transform_id, status, submitter=None,
                   substatus=ProcessingStatus.New, granularity=None,
                   granularity_type=GranularityType.File,
                   processing_type=ProcessingType.Workflow,
                   new_poll_period=1, update_poll_period=10,
                   new_retries=0, update_retries=0, max_new_retries=3, max_update_retries=0,
                   expired_at=None, processing_metadata=None, session=None):
    """
    Add a processing.

    :param request_id: The request id.
    :param workload_id: The workload id.
    :param transform_id: Transform id.
    :param status: processing status.
    :param submitter: submitter name.
    :param granularity: Granularity size.
    :param granularity_type: Granularity type.
    :param expired_at: The datetime when it expires.
    :param processing_metadata: The metadata as json.

    :raises DuplicatedObject: If a processing with the same name exists.
    :raises DatabaseException: If there is a database error.

    :returns: processing id.
    """
    return orm_processings.add_processing(request_id=request_id, workload_id=workload_id, transform_id=transform_id,
                                          status=status, substatus=substatus, submitter=submitter,
                                          granularity=granularity, granularity_type=granularity_type,
                                          new_poll_period=new_poll_period,
                                          update_poll_period=update_poll_period,
                                          new_retries=new_retries, update_retries=update_retries,
                                          max_new_retries=max_new_retries,
                                          max_update_retries=max_update_retries,
                                          processing_type=processing_type,
                                          expired_at=expired_at, processing_metadata=processing_metadata,
                                          session=session)


@read_session
def get_processing(processing_id=None, request_id=None, transform_id=None, to_json=False, session=None):
    """
    Get processing or raise a NoObject exception.

    :param processing_id: Processing id.
    :param to_json: return json format.
    :param session: The database session in use.

    :raises NoObject: If no processing is founded.

    :returns: Processing.
    """
    return orm_processings.get_processing(processing_id=processing_id, request_id=request_id,
                                          transform_id=transform_id, to_json=to_json, session=session)


@read_session
def get_processings(request_id=None, workload_id=None, transform_id=None, to_json=False, session=None):
    """
    Get processing or raise a NoObject exception.

    :param processing_id: Processing id.
    :param to_json: return json format.
    :param session: The database session in use.

    :raises NoObject: If no processing is founded.

    :returns: Processing.
    """
    return orm_processings.get_processings(request_id=request_id, workload_id=workload_id,
                                           transform_id=transform_id, to_json=to_json, session=session)


@read_session
def get_processings_by_transform_id(transform_id=None, to_json=False, session=None):
    """
    Get processings or raise a NoObject exception.

    :param tranform_id: Transform id.
    :param to_json: return json format.
    :param session: The database session in use.

    :raises NoObject: If no processing is founded.

    :returns: Processings.
    """
    return orm_processings.get_processings_by_transform_id(transform_id=transform_id, to_json=to_json, session=session)


@transactional_session
def get_processing_by_id_status(processing_id, status=None, locking=False, lock_period=None, session=None):
    # pr = orm_processings.get_processing_by_id_status(processing_id=processing_id, status=status, locking=locking, session=session)
    pr = orm_processings.get_processing_by_id_status(processing_id=processing_id, status=status, session=session)
    if pr is None:
        return pr

    if locking:
        if pr['locking'] in [ProcessingLocking.Locking]:
            if lock_period and pr['updated_at'] < datetime.datetime.utcnow() - datetime.timedelta(seconds=lock_period):
                parameters = {}
                parameters['locking'] = ProcessingLocking.Locking
                parameters['updated_at'] = datetime.datetime.utcnow()
                hostname, pid, thread_id, thread_name = get_process_thread_info()
                parameters['locking_hostname'] = hostname
                parameters['locking_pid'] = pid
                parameters['locking_thread_id'] = thread_id
                parameters['locking_thread_name'] = thread_name
                num_rows = orm_processings.update_processing(processing_id=pr['processing_id'], parameters=parameters, locking=True, session=session)
                if num_rows > 0:
                    return pr
                else:
                    return None
            else:
                return None
        else:
            parameters = {}
            parameters['locking'] = ProcessingLocking.Locking
            parameters['updated_at'] = datetime.datetime.utcnow()
            hostname, pid, thread_id, thread_name = get_process_thread_info()
            parameters['locking_hostname'] = hostname
            parameters['locking_pid'] = pid
            parameters['locking_thread_id'] = thread_id
            parameters['locking_thread_name'] = thread_name
            num_rows = orm_processings.update_processing(processing_id=pr['processing_id'], parameters=parameters, locking=True, session=session)
            if num_rows > 0:
                return pr
            else:
                return None
    return pr


@transactional_session
def get_processings_by_status(status, time_period=None, locking=False, bulk_size=None, to_json=False, by_substatus=False,
                              not_lock=False, next_poll_at=None, for_poller=False, only_return_id=False,
                              min_request_id=None, locking_for_update=False, new_poll=False, update_poll=False, session=None):
    """
    Get processing or raise a NoObject exception.

    :param status: Processing status of list of processing status.
    :param time_period: Time period in seconds.
    :param locking: Whether to retrieve only unlocked items and lock them.
    :param to_json: return json format.
    :param session: The database session in use.

    :raises NoObject: If no processing is founded.

    :returns: Processings.
    """
    if locking:
        if not only_return_id and bulk_size:
            # order by cannot work together with locking. So first select 2 * bulk_size without locking with order by.
            # then select with locking.
            proc_ids = orm_processings.get_processings_by_status(status=status, period=time_period, locking=locking,
                                                                 bulk_size=bulk_size * 2, to_json=False, locking_for_update=False,
                                                                 by_substatus=by_substatus, only_return_id=True,
                                                                 for_poller=for_poller, new_poll=new_poll,
                                                                 min_request_id=min_request_id,
                                                                 update_poll=update_poll, session=session)
            if proc_ids:
                processing2s = orm_processings.get_processings_by_status(status=status, period=time_period, locking=locking,
                                                                         processing_ids=proc_ids,
                                                                         min_request_id=min_request_id,
                                                                         bulk_size=None, to_json=to_json,
                                                                         locking_for_update=locking_for_update,
                                                                         by_substatus=by_substatus, only_return_id=only_return_id,
                                                                         new_poll=new_poll, update_poll=update_poll,
                                                                         for_poller=for_poller, session=session)
                if processing2s:
                    # reqs = req2s[:bulk_size]
                    # order requests
                    processings = []
                    for proc_id in proc_ids:
                        if len(processings) >= bulk_size:
                            break
                        for p in processing2s:
                            if p['processing_id'] == proc_id:
                                processings.append(p)
                                break
                    # processings = processings[:bulk_size]
                else:
                    processings = []
            else:
                processings = []
        else:
            processings = orm_processings.get_processings_by_status(status=status, period=time_period, locking=locking,
                                                                    bulk_size=bulk_size, to_json=to_json,
                                                                    locking_for_update=locking_for_update,
                                                                    new_poll=new_poll, update_poll=update_poll,
                                                                    only_return_id=only_return_id,
                                                                    min_request_id=min_request_id,
                                                                    by_substatus=by_substatus, for_poller=for_poller, session=session)

        parameters = {}
        if not not_lock:
            parameters['locking'] = ProcessingLocking.Locking
        if next_poll_at:
            parameters['next_poll_at'] = next_poll_at
        parameters['updated_at'] = datetime.datetime.utcnow()
        if parameters:
            for processing in processings:
                if type(processing) in [dict]:
                    orm_processings.update_processing(processing['processing_id'], parameters=parameters, session=session)
                else:
                    orm_processings.update_processing(processing, parameters=parameters, session=session)
    else:
        processings = orm_processings.get_processings_by_status(status=status, period=time_period, locking=locking,
                                                                bulk_size=bulk_size, to_json=to_json,
                                                                new_poll=new_poll, update_poll=update_poll,
                                                                only_return_id=only_return_id,
                                                                min_request_id=min_request_id,
                                                                by_substatus=by_substatus, for_poller=for_poller, session=session)
    return processings


@transactional_session
def update_processing(processing_id, parameters, session=None):
    """
    update a processing.

    :param processing_id: the transform id.
    :param parameters: A dictionary of parameters.
    :param session: The database session in use.

    :raises NoObject: If no content is founded.
    :raises DatabaseException: If there is a database error.

    """
    return orm_processings.update_processing(processing_id=processing_id, parameters=parameters, session=session)


@transactional_session
def delete_processing(processing_id=None, session=None):
    """
    delete a processing.

    :param processing_id: The id of the processing.
    :param session: The database session in use.

    :raises NoObject: If no processing is founded.
    :raises DatabaseException: If there is a database error.
    """
    return orm_processings.delete_processing(processing_id=processing_id, session=session)


@transactional_session
def update_processing_with_collection_contents(updated_processing, new_processing=None, updated_collection=None,
                                               updated_files=None, new_files=None,
                                               coll_msg_content=None, file_msg_content=None, transform_updates=None,
                                               message_bulk_size=1000, session=None):
    """
    Update processing with collection, contents, file messages and collection messages.

    :param updated_processing: dict with processing id and parameters.
    :param updated_collection: dict with collection id and parameters.
    :param updated_files: list of content files.
    :param coll_msg_content: message with collection info.
    :param file_msg_content: message with files info.
    """
    if updated_files:
        orm_contents.update_contents(updated_files, session=session)
    if new_files:
        orm_contents.add_contents(contents=new_files, session=session)
    if file_msg_content:
        if not type(file_msg_content) in [list, tuple]:
            file_msg_content = [file_msg_content]
        for file_msg_con in file_msg_content:
            orm_messages.add_message(msg_type=file_msg_con['msg_type'],
                                     status=file_msg_con['status'],
                                     source=file_msg_con['source'],
                                     transform_id=file_msg_con['transform_id'],
                                     num_contents=file_msg_con['num_contents'],
                                     msg_content=file_msg_con['msg_content'],
                                     bulk_size=message_bulk_size,
                                     session=session)
    if updated_collection:
        orm_collections.update_collection(coll_id=updated_collection['coll_id'],
                                          parameters=updated_collection['parameters'],
                                          session=session)
    if coll_msg_content:
        orm_messages.add_message(msg_type=coll_msg_content['msg_type'],
                                 status=coll_msg_content['status'],
                                 source=coll_msg_content['source'],
                                 transform_id=coll_msg_content['transform_id'],
                                 num_contents=coll_msg_content['num_contents'],
                                 msg_content=coll_msg_content['msg_content'],
                                 session=session)
    if updated_processing:
        orm_processings.update_processing(processing_id=updated_processing['processing_id'],
                                          parameters=updated_processing['parameters'],
                                          session=session)
    if new_processing:
        orm_processings.add_processing(**new_processing, session=session)
    if transform_updates:
        orm_transforms.update_transform(transform_id=transform_updates['transform_id'],
                                        parameters=transform_updates['parameters'],
                                        session=session)


def resolve_input_dependency_id(new_input_dependency_contents, request_id=None, session=None):
    coll_ids = []
    for content in new_input_dependency_contents:
        if content['coll_id'] not in coll_ids:
            coll_ids.append(content['coll_id'])
    contents = orm_contents.get_contents(coll_id=coll_ids, request_id=request_id, relation_type=ContentRelationType.Output, session=session)
    content_name_id_map = {}
    for content in contents:
        if content['coll_id'] not in content_name_id_map:
            content_name_id_map[content['coll_id']] = {}
        if content['name'] not in content_name_id_map[content['coll_id']]:
            content_name_id_map[content['coll_id']][content['name']] = {}
        # if content['map_id'] not in content_name_id_map[content['coll_id']][content['name']]:
        #     content_name_id_map[content['coll_id']][content['name']][content['map_id']] = {}
        # content_name_id_map[content['coll_id']][content['name']][content['sub_map_id']] = content['content_id']
        content_name_id_map[content['coll_id']][content['name']] = content['content_id']

    for content in new_input_dependency_contents:
        if 'sub_map_id' not in content or content['sub_map_id'] is None:
            content['sub_map_id'] = 0
        # dep_sub_map_id = content.get("dep_sub_map_id", 0)
        # if dep_sub_map_id is None:
        #     dep_sub_map_id = 0
        # content_dep_id = content_name_id_map[content['coll_id']][content['name']][dep_sub_map_id]
        content_dep_id = content_name_id_map[content['coll_id']][content['name']]
        content['content_dep_id'] = content_dep_id
    return new_input_dependency_contents


@transactional_session
def update_processing_contents(update_processing, update_contents=None, update_messages=None, new_contents=None,
                               update_dep_contents=None, update_collections=None, messages=None,
                               new_update_contents=None, new_input_dependency_contents=None,
                               new_contents_ext=None, update_contents_ext=None,
                               request_id=None, transform_id=None, use_bulk_update_mappings=True,
                               message_bulk_size=2000, session=None):
    """
    Update processing with contents.

    :param update_processing: dict with processing id and parameters.
    :param update_contents: list of content files.
    """
    if update_collections:
        orm_collections.update_collections(update_collections, session=session)
    if update_contents:
        chunks = get_list_chunks(update_contents)
        for chunk in chunks:
            orm_contents.update_contents(chunk, request_id=request_id, transform_id=transform_id,
                                         use_bulk_update_mappings=use_bulk_update_mappings, session=session)
    if new_update_contents:
        # first add and then delete, to trigger the trigger 'update_content_dep_status'.
        # too slow
        chunks = get_list_chunks(new_update_contents)
        for chunk in chunks:
            orm_contents.add_contents_update(chunk, session=session)
        # orm_contents.delete_contents_update(session=session)
        pass
    if new_contents:
        chunks = get_list_chunks(new_contents)
        for chunk in chunks:
            orm_contents.add_contents(chunk, session=session)
    if new_contents_ext:
        chunks = get_list_chunks(new_contents_ext)
        for chunk in chunks:
            orm_contents.add_contents_ext(chunk, session=session)
    if update_contents_ext:
        chunks = get_list_chunks(update_contents_ext)
        for chunk in chunks:
            orm_contents.update_contents_ext(chunk, request_id=request_id, transform_id=transform_id,
                                             use_bulk_update_mappings=use_bulk_update_mappings, session=session)
    if new_input_dependency_contents:
        new_input_dependency_contents = resolve_input_dependency_id(new_input_dependency_contents, request_id=request_id, session=session)
        chunks = get_list_chunks(new_input_dependency_contents)
        for chunk in chunks:
            orm_contents.add_contents(chunk, session=session)
    if update_dep_contents:
        request_id, update_dep_contents_status_name, update_dep_contents_status = update_dep_contents
        for status_name in update_dep_contents_status_name:
            status = update_dep_contents_status_name[status_name]
            status_content_ids = update_dep_contents_status[status_name]
            if status_content_ids:
                chunks = get_list_chunks(status_content_ids)
                for chunk in chunks:
                    orm_contents.update_dep_contents(request_id, chunk, status, session=session)
    if update_processing:
        orm_processings.update_processing(processing_id=update_processing['processing_id'],
                                          parameters=update_processing['parameters'],
                                          session=session)
    if update_messages:
        chunks = get_list_chunks(update_messages)
        for chunk in chunks:
            orm_messages.update_messages(chunk, bulk_size=message_bulk_size, request_id=request_id, transform_id=transform_id,
                                         use_bulk_update_mappings=use_bulk_update_mappings, session=session)
    if messages:
        if not type(messages) in [list, tuple]:
            messages = [messages]
        orm_messages.add_messages(messages, bulk_size=message_bulk_size, session=session)


@transactional_session
def clean_locking(time_period=3600, min_request_id=None, health_items=[], session=None):
    """
    Clearn locking which is older than time period.

    :param time_period in seconds
    """
    orm_processings.clean_locking(time_period=time_period, min_request_id=min_request_id, health_items=health_items, session=session)


@transactional_session
def clean_next_poll_at(status, session=None):
    """
    Clearn next_poll_at.

    :param status: status of the processing
    """
    orm_processings.clean_next_poll_at(status=status, session=session)


@read_session
def get_num_active_processings(active_status=None, session=None):
    return orm_processings.get_num_active_processings(active_status=active_status, session=session)


@read_session
def get_active_processings(active_status=None, session=None):
    return orm_processings.get_active_processings(active_status=active_status, session=session)
