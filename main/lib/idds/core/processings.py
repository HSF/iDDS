#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019 - 2020


"""
operations related to Processings.
"""


from idds.orm.base.session import read_session, transactional_session
from idds.common.constants import ProcessingLocking, ProcessingStatus, GranularityType
from idds.orm import (processings as orm_processings,
                      collections as orm_collections,
                      contents as orm_contents,
                      messages as orm_messages,
                      transforms as orm_transforms)
from idds.core import messages as core_messages


@transactional_session
def add_processing(request_id, workload_id, transform_id, status, submitter=None,
                   substatus=ProcessingStatus.New, granularity=None,
                   granularity_type=GranularityType.File,
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
                                          expired_at=expired_at, processing_metadata=processing_metadata,
                                          session=session)


@read_session
def get_processing(processing_id=None, to_json=False, session=None):
    """
    Get processing or raise a NoObject exception.

    :param processing_id: Processing id.
    :param to_json: return json format.
    :param session: The database session in use.

    :raises NoObject: If no processing is founded.

    :returns: Processing.
    """
    return orm_processings.get_processing(processing_id=processing_id, to_json=to_json, session=session)


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
def get_processings_with_messaging(locking=False, bulk_size=None, session=None):
    msgs = core_messages.retrieve_processing_messages(processing_id=None, bulk_size=bulk_size, session=session)
    if msgs:
        pr_ids = [msg['processing_id'] for msg in msgs]
        if locking:
            pr2s = orm_processings.get_processings_by_status(status=None, processing_ids=pr_ids,
                                                             locking=locking, locking_for_update=True,
                                                             bulk_size=None, session=session)
            if pr2s:
                prs = []
                for pr_id in pr_ids:
                    if len(prs) >= bulk_size:
                        break
                    for pr in pr2s:
                        if pr['processing_id'] == pr_id:
                            prs.append(pr)
                            break
            else:
                prs = []

            parameters = {'locking': ProcessingLocking.Locking}
            for pr in prs:
                orm_processings.update_processing(processing_id=pr['processing_id'], parameters=parameters, session=session)
            return prs
        else:
            prs = orm_processings.get_processings_by_status(status=None, processing_ids=pr_ids, locking=locking,
                                                            locking_for_update=locking,
                                                            bulk_size=bulk_size, session=session)
            return prs
    else:
        return []


@transactional_session
def get_processings_by_status(status, time_period=None, locking=False, bulk_size=None, to_json=False, by_substatus=False, with_messaging=False, session=None):
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
    if with_messaging:
        prs = get_processings_with_messaging(locking=locking, bulk_size=bulk_size, session=session)
        if prs:
            return prs

    if locking:
        if bulk_size:
            # order by cannot work together with locking. So first select 2 * bulk_size without locking with order by.
            # then select with locking.
            proc_ids = orm_processings.get_processings_by_status(status=status, period=time_period, locking=locking,
                                                                 bulk_size=bulk_size * 2, to_json=False, locking_for_update=False,
                                                                 by_substatus=by_substatus, only_return_id=True, session=session)
            if proc_ids:
                processing2s = orm_processings.get_processings_by_status(status=status, period=time_period, locking=locking,
                                                                         processing_ids=proc_ids,
                                                                         bulk_size=None, to_json=to_json, locking_for_update=True,
                                                                         by_substatus=by_substatus, session=session)
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
                                                                    bulk_size=bulk_size, to_json=to_json, locking_for_update=locking,
                                                                    by_substatus=by_substatus, session=session)

        parameters = {'locking': ProcessingLocking.Locking}
        for processing in processings:
            orm_processings.update_processing(processing['processing_id'], parameters=parameters, session=session)
    else:
        processings = orm_processings.get_processings_by_status(status=status, period=time_period, locking=locking,
                                                                bulk_size=bulk_size, to_json=to_json,
                                                                by_substatus=by_substatus, session=session)
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


@transactional_session
def update_processing_contents(processing_update, content_updates, update_messages=None, session=None):
    """
    Update processing with contents.

    :param processing_update: dict with processing id and parameters.
    :param content_updates: list of content files.
    """
    if content_updates:
        orm_contents.update_contents(content_updates, session=session)
    if processing_update:
        orm_processings.update_processing(processing_id=processing_update['processing_id'],
                                          parameters=processing_update['parameters'],
                                          session=session)
    if update_messages:
        orm_messages.update_messages(update_messages, session=session)


@transactional_session
def clean_locking(time_period=3600, session=None):
    """
    Clearn locking which is older than time period.

    :param time_period in seconds
    """
    orm_processings.clean_locking(time_period=time_period, session=session)


@transactional_session
def clean_next_poll_at(status, session=None):
    """
    Clearn next_poll_at.

    :param status: status of the processing
    """
    orm_processings.clean_next_poll_at(status=status, session=session)
