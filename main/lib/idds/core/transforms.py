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
operations related to Transform.
"""

import datetime
import logging

# from idds.common import exceptions

from idds.common.constants import (TransformStatus, ContentRelationType, ContentStatus,
                                   TransformLocking, CollectionRelationType)
from idds.orm.base.session import read_session, transactional_session
from idds.orm import (transforms as orm_transforms,
                      collections as orm_collections,
                      contents as orm_contents,
                      messages as orm_messages,
                      processings as orm_processings)


@transactional_session
def add_transform(request_id, workload_id, transform_type, transform_tag=None, priority=0, name=None,
                  status=TransformStatus.New, substatus=TransformStatus.New, locking=TransformLocking.Idle,
                  new_poll_period=1, update_poll_period=10, retries=0, expired_at=None, transform_metadata=None,
                  new_retries=0, update_retries=0, max_new_retries=3, max_update_retries=0,
                  parent_transform_id=None, previous_transform_id=None, current_processing_id=None,
                  workprogress_id=None, session=None):
    """
    Add a transform.

    :param request_id: The request id.
    :param workload_id: The workload id.
    :param transform_type: Transform type.
    :param transform_tag: Transform tag.
    :param priority: priority.
    :param status: Transform status.
    :param locking: Transform locking.
    :param retries: The number of retries.
    :param expired_at: The datetime when it expires.
    :param transform_metadata: The metadata as json.

    :raises DuplicatedObject: If a transform with the same name exists.
    :raises DatabaseException: If there is a database error.

    :returns: transform id.
    """
    transform_id = orm_transforms.add_transform(request_id=request_id, workload_id=workload_id,
                                                transform_type=transform_type, transform_tag=transform_tag,
                                                priority=priority, status=status, substatus=substatus,
                                                locking=locking, retries=retries, name=name,
                                                new_poll_period=new_poll_period,
                                                update_poll_period=update_poll_period,
                                                new_retries=new_retries, update_retries=update_retries,
                                                max_new_retries=max_new_retries,
                                                max_update_retries=max_update_retries,
                                                parent_transform_id=parent_transform_id,
                                                previous_transform_id=previous_transform_id,
                                                current_processing_id=current_processing_id,
                                                expired_at=expired_at, transform_metadata=transform_metadata,
                                                workprogress_id=workprogress_id, session=session)
    return transform_id


@read_session
def get_transform(transform_id, request_id=None, to_json=False, session=None):
    """
    Get transform or raise a NoObject exception.

    :param transform_id: Transform id.
    :param to_json: return json format.
    :param session: The database session in use.

    :raises NoObject: If no transform is founded.

    :returns: Transform.
    """
    return orm_transforms.get_transform(transform_id=transform_id, request_id=request_id, to_json=to_json, session=session)


@transactional_session
def get_transform_by_id_status(transform_id, status=None, locking=False, session=None):
    tf = orm_transforms.get_transform_by_id_status(transform_id=transform_id, status=status, locking=locking, session=session)
    if tf is not None and locking:
        parameters = {}
        parameters['locking'] = TransformLocking.Locking
        parameters['updated_at'] = datetime.datetime.utcnow()
        orm_transforms.update_transform(transform_id=tf['transform_id'], parameters=parameters, session=session)
    return tf


@read_session
def get_transforms_with_input_collection(transform_type, transform_tag, coll_scope, coll_name, to_json=False, session=None):
    """
    Get transform or raise a NoObject exception.

    :param transform_type: Transform type.
    :param transform_tag: Transform tag.
    :param coll_scope: The collection scope.
    :param coll_name: The collection name.
    :param to_json: return json format.
    :param session: The database session in use.

    :raises NoObject: If no transform is founded.

    :returns: Transforms.
    """
    return orm_transforms.get_transforms_with_input_collection(transform_type, transform_tag, coll_scope,
                                                               coll_name, to_json=to_json, session=session)


@read_session
def get_transform_ids(workprogress_id, request_id=None, workload_id=None, transform_id=None, session=None):
    """
    Get transform ids or raise a NoObject exception.

    :param workprogress_id: Workprogress id.
    :param session: The database session in use.

    :raises NoObject: If no transform is founded.

    :returns: list of transform ids.
    """
    return orm_transforms.get_transform_ids(workprogress_id=workprogress_id, request_id=request_id,
                                            workload_id=workload_id, transform_id=transform_id, session=session)


@read_session
def get_transforms(request_id=None, workload_id=None, transform_id=None, to_json=False, session=None):
    """
    Get transforms or raise a NoObject exception.

    :param workprogress_id: Workprogress id.
    :param to_json: return json format.
    :param session: The database session in use.

    :raises NoObject: If no transform is founded.

    :returns: list of transform.
    """
    return orm_transforms.get_transforms(request_id=request_id,
                                         workload_id=workload_id,
                                         transform_id=transform_id,
                                         to_json=to_json, session=session)


@transactional_session
def get_transforms_by_status(status, period=None, locking=False, bulk_size=None, to_json=False, by_substatus=False,
                             new_poll=False, update_poll=False, only_return_id=False, min_request_id=None,
                             not_lock=False, next_poll_at=None, session=None):
    """
    Get transforms or raise a NoObject exception.

    :param status: Transform status or list of transform status.
    :param session: The database session in use.
    :param locking: Whether to lock retrieved items.
    :param to_json: return json format.

    :raises NoObject: If no transform is founded.

    :returns: list of transform.
    """
    if locking:
        if not only_return_id and bulk_size:
            # order by cannot work together with locking. So first select 2 * bulk_size without locking with order by.
            # then select with locking.
            tf_ids = orm_transforms.get_transforms_by_status(status=status, period=period, locking=locking,
                                                             bulk_size=bulk_size * 2, locking_for_update=False,
                                                             to_json=False, only_return_id=True,
                                                             min_request_id=min_request_id,
                                                             new_poll=new_poll, update_poll=update_poll,
                                                             by_substatus=by_substatus, session=session)
            if tf_ids:
                transform2s = orm_transforms.get_transforms_by_status(status=status, period=period, locking=locking,
                                                                      bulk_size=None, locking_for_update=False,
                                                                      to_json=to_json, transform_ids=tf_ids,
                                                                      new_poll=new_poll, update_poll=update_poll,
                                                                      min_request_id=min_request_id,
                                                                      by_substatus=by_substatus, session=session)
                if transform2s:
                    # reqs = req2s[:bulk_size]
                    # order requests
                    transforms = []
                    for tf_id in tf_ids:
                        if len(transforms) >= bulk_size:
                            break
                        for tf in transform2s:
                            if tf['transform_id'] == tf_id:
                                transforms.append(tf)
                                break
                    # transforms = transforms[:bulk_size]
                else:
                    transforms = []
            else:
                transforms = []
        else:
            transforms = orm_transforms.get_transforms_by_status(status=status, period=period, locking=locking,
                                                                 locking_for_update=False,
                                                                 bulk_size=bulk_size, to_json=to_json,
                                                                 new_poll=new_poll, update_poll=update_poll,
                                                                 only_return_id=only_return_id,
                                                                 min_request_id=min_request_id,
                                                                 by_substatus=by_substatus, session=session)

        parameters = {}
        if not not_lock:
            parameters['locking'] = TransformLocking.Locking
        if next_poll_at:
            parameters['next_poll_at'] = next_poll_at
        parameters['updated_at'] = datetime.datetime.utcnow()
        if parameters:
            for transform in transforms:
                if type(transform) in [dict]:
                    orm_transforms.update_transform(transform_id=transform['transform_id'], parameters=parameters, session=session)
                else:
                    orm_transforms.update_transform(transform_id=transform, parameters=parameters, session=session)
    else:
        transforms = orm_transforms.get_transforms_by_status(status=status, period=period, locking=locking,
                                                             bulk_size=bulk_size, to_json=to_json,
                                                             new_poll=new_poll, update_poll=update_poll,
                                                             only_return_id=only_return_id,
                                                             min_request_id=min_request_id,
                                                             by_substatus=by_substatus, session=session)
    return transforms


@transactional_session
def update_transform(transform_id, parameters, session=None):
    """
    update a transform.

    :param transform_id: the transform id.
    :param parameters: A dictionary of parameters.
    :param session: The database session in use.

    :raises NoObject: If no content is founded.
    :raises DatabaseException: If there is a database error.

    """
    orm_transforms.update_transform(transform_id=transform_id, parameters=parameters, session=session)


@transactional_session
def add_transform_outputs(transform, transform_parameters, input_collections=None, output_collections=None, log_collections=None,
                          update_input_collections=None, update_output_collections=None, update_log_collections=None,
                          new_contents=None, update_contents=None, new_processing=None, update_processing=None,
                          messages=None, update_messages=None, message_bulk_size=10000, session=None):
    """
    For input contents, add corresponding output contents.

    :param transform: the transform.
    :param input_collections: The new input collections.
    :param output_collections: The new output collections.
    :param log_collections: The new log collections.
    :param update_input_collections: The updated input collections.
    :param update_output_collections: The updated output collections.
    :param update_log_collections: The updated log collections.
    :param new_contents: The new contents.
    :param update_contents: The updated contents.
    :param new_processing: The new processing.
    :param messages: Messages.
    :param message_bulk_size: The message bulk size.
    :param session: The database session in use.

    :raises DatabaseException: If there is a database error.
    """
    work = transform['transform_metadata']['work']

    new_pr_ids, update_pr_ids = [], []

    if input_collections:
        for coll in input_collections:
            collection = None
            if 'collection' in coll:
                collection = coll['collection']
                del coll['collection']
            coll_id = orm_collections.add_collection(**coll, session=session)
            if collection:
                # work.set_collection_id(coll, coll_id)
                collection.coll_id = coll_id
    if output_collections:
        for coll in output_collections:
            collection = None
            if 'collection' in coll:
                collection = coll['collection']
                del coll['collection']
            coll_id = orm_collections.add_collection(**coll, session=session)
            if collection:
                # work.set_collection_id(coll, coll_id)
                collection.coll_id = coll_id
    if log_collections:
        for coll in log_collections:
            collection = None
            if 'collection' in coll:
                collection = coll['collection']
                del coll['collection']
            coll_id = orm_collections.add_collection(**coll, session=session)
            if collection:
                # work.set_collection_id(coll, coll_id)
                collection.coll_id = coll_id

    if update_input_collections:
        update_input_colls = [coll.collection for coll in update_input_collections]
        orm_collections.update_collections(update_input_colls, session=session)
    if update_output_collections:
        update_output_colls = [coll.collection for coll in update_output_collections]
        orm_collections.update_collections(update_output_colls, session=session)
    if update_log_collections:
        update_log_colls = [coll.collection for coll in update_log_collections]
        orm_collections.update_collections(update_log_colls, session=session)

    if new_contents:
        orm_contents.add_contents(new_contents, session=session)
    if update_contents:
        orm_contents.update_contents(update_contents, session=session)

    processing_id = None
    if new_processing:
        # print(new_processing)
        processing_id = orm_processings.add_processing(**new_processing, session=session)
        new_pr_ids.append(processing_id)
        transform_parameters['current_processing_id'] = processing_id
    if update_processing:
        for proc_id in update_processing:
            orm_processings.update_processing(processing_id=proc_id, parameters=update_processing[proc_id], session=session)
            update_pr_ids.append(proc_id)

    if messages:
        if not type(messages) in [list, tuple]:
            messages = [messages]
        # for message in messages:
        #     orm_messages.add_message(msg_type=message['msg_type'],
        #                              status=message['status'],
        #                              source=message['source'],
        #                              request_id=message['request_id'],
        #                              workload_id=message['workload_id'],
        #                              transform_id=message['transform_id'],
        #                              num_contents=message['num_contents'],
        #                              msg_content=message['msg_content'],
        #                              bulk_size=message_bulk_size,
        #                              session=session)
        logging.debug("message_bulk_size: %s" % str(message_bulk_size))
        orm_messages.add_messages(messages, bulk_size=message_bulk_size, session=session)
    if update_messages:
        orm_messages.update_messages(update_messages, bulk_size=message_bulk_size, session=session)

    if transform:
        if processing_id:
            # work.set_processing_id(new_processing, processing_id)
            if hasattr(work, 'set_processing_id'):
                work.set_processing_id(new_processing['processing_metadata']['processing'], processing_id)
        if hasattr(work, 'refresh_work'):
            work.refresh_work()
        orm_transforms.update_transform(transform_id=transform['transform_id'],
                                        parameters=transform_parameters,
                                        session=session)
    return new_pr_ids, update_pr_ids


@transactional_session
def delete_transform(transform_id=None, session=None):
    """
    delete a transform.

    :param transform_id: The id of the transform.
    :param session: The database session in use.

    :raises NoObject: If no content is founded.
    :raises DatabaseException: If there is a database error.
    """
    orm_transforms.delete_transform(transform_id=transform_id, session=session)


@transactional_session
def clean_locking(time_period=3600, session=None):
    """
    Clearn locking which is older than time period.

    :param time_period in seconds
    """
    orm_transforms.clean_locking(time_period=time_period, session=session)


@transactional_session
def clean_next_poll_at(status, session=None):
    """
    Clearn next_poll_at.

    :param status: status of the transform
    """
    orm_transforms.clean_next_poll_at(status=status, session=session)


@read_session
def get_transform_input_output_maps(transform_id, input_coll_ids, output_coll_ids, log_coll_ids=[], with_sub_map_id=False, is_es=False, session=None):
    """
    Get transform input output maps.

    :param transform_id: transform id.
    """
    contents = orm_contents.get_contents_by_request_transform(transform_id=transform_id, session=session)
    ret = {}
    for content in contents:
        map_id = content['map_id']
        sub_map_id = content['sub_map_id']
        if not with_sub_map_id:
            if is_es:
                sub_map_id = content['sub_map_id']
                path = content['path']
                if map_id not in ret:
                    ret[map_id] = {'inputs_dependency': [], 'inputs': [], 'outputs': [], 'logs': [], 'others': [],
                                   'es_name': path, 'sub_maps': {}}
            elif map_id not in ret:
                ret[map_id] = {'inputs_dependency': [], 'inputs': [], 'outputs': [], 'logs': [], 'others': []}
        else:
            sub_map_id = content['sub_map_id']
            if map_id not in ret:
                ret[map_id] = {}
            if sub_map_id not in ret[map_id]:
                ret[map_id][sub_map_id] = {'inputs_dependency': [], 'inputs': [], 'outputs': [], 'logs': [], 'others': []}
        """
        if content['coll_id'] in input_coll_ids:
            ret[map_id]['inputs'].append(content)
        elif content['coll_id'] in output_coll_ids:
            ret[map_id]['outputs'].append(content)
        elif content['coll_id'] in log_coll_ids:
            ret[map_id]['logs'].append(content)
        else:
            ret[map_id]['others'].append(content)
        """
        if not with_sub_map_id:
            if content['content_relation_type'] == ContentRelationType.Input:
                ret[map_id]['inputs'].append(content)
            elif content['content_relation_type'] == ContentRelationType.InputDependency:
                ret[map_id]['inputs_dependency'].append(content)
            elif content['content_relation_type'] == ContentRelationType.Output:
                ret[map_id]['outputs'].append(content)

                if is_es:
                    sub_map_id = content['sub_map_id']
                    if sub_map_id not in ret[map_id]['sub_maps'][sub_map_id]:
                        ret[map_id]['sub_maps'][sub_map_id] = []
                    ret[map_id]['sub_maps'][sub_map_id].append(content)
            elif content['content_relation_type'] == ContentRelationType.Log:
                ret[map_id]['logs'].append(content)
            else:
                ret[map_id]['others'].append(content)
        else:
            if content['content_relation_type'] == ContentRelationType.Input:
                ret[map_id][sub_map_id]['inputs'].append(content)
            elif content['content_relation_type'] == ContentRelationType.InputDependency:
                ret[map_id][sub_map_id]['inputs_dependency'].append(content)
            elif content['content_relation_type'] == ContentRelationType.Output:
                ret[map_id][sub_map_id]['outputs'].append(content)
            elif content['content_relation_type'] == ContentRelationType.Log:
                ret[map_id][sub_map_id]['logs'].append(content)
            else:
                ret[map_id][sub_map_id]['others'].append(content)
    return ret


def release_inputs(to_release_inputs):
    update_contents = []
    for to_release in to_release_inputs:
        contents = orm_contents.get_input_contents(request_id=to_release['request_id'],
                                                   coll_id=to_release['coll_id'],
                                                   name=to_release['name'])
        for content in contents:
            if content['content_relation_type'] == ContentRelationType.InputDependency:
                update_content = {'content_id': content['content_id'],
                                  'substatus': to_release['substatus'],
                                  'status': to_release['status']}
                update_contents.append(update_content)
    return update_contents


def release_inputs_by_collection_old(to_release_inputs):
    update_contents = []
    for coll_id in to_release_inputs:
        to_release_contents = to_release_inputs[coll_id]
        if to_release_contents:
            to_release = to_release_contents[0]
            to_release_names_available = []
            to_release_names_fake_available = []
            to_release_names_final_failed = []
            to_release_names_missing = []
            for to_release_content in to_release_contents:
                if (to_release_content['status'] in [ContentStatus.Available]            # noqa: W503
                   or to_release_content['substatus'] in [ContentStatus.Available]):    # noqa: W503
                    to_release_names_available.append(to_release_content['name'])
                elif (to_release_content['status'] in [ContentStatus.FakeAvailable]            # noqa: W503
                     or to_release_content['substatus'] in [ContentStatus.FakeAvailable]):    # noqa: W503, E128
                    to_release_names_fake_available.append(to_release_content['name'])
                elif (to_release_content['status'] in [ContentStatus.FinalFailed]            # noqa: W503
                     or to_release_content['substatus'] in [ContentStatus.FinalFailed]):    # noqa: W503, E128
                    to_release_names_final_failed.append(to_release_content['name'])
                elif (to_release_content['status'] in [ContentStatus.Missing]            # noqa: W503
                      or to_release_content['substatus'] in [ContentStatus.Missing]):    # noqa: W503
                    to_release_names_missing.append(to_release_content['name'])
            contents = orm_contents.get_input_contents(request_id=to_release['request_id'],
                                                       coll_id=to_release['coll_id'],
                                                       name=None)

            for content in contents:
                if (content['content_relation_type'] == ContentRelationType.InputDependency):    # noqa: W503
                    if (content['status'] not in [ContentStatus.Available]                       # noqa: W503
                       and content['name'] in to_release_names_available):                          # noqa: W503
                        update_content = {'content_id': content['content_id'],
                                          'substatus': ContentStatus.Available,
                                          'status': ContentStatus.Available}
                        update_contents.append(update_content)
                    elif (content['status'] not in [ContentStatus.FakeAvailable]                     # noqa: W503
                          and content['name'] in to_release_names_fake_available):                        # noqa: W503
                        update_content = {'content_id': content['content_id'],
                                          'substatus': ContentStatus.FakeAvailable,
                                          'status': ContentStatus.FakeAvailable}
                        update_contents.append(update_content)
                    elif (content['status'] not in [ContentStatus.FinalFailed]                     # noqa: W503
                          and content['name'] in to_release_names_final_failed):                        # noqa: W503
                        update_content = {'content_id': content['content_id'],
                                          'substatus': ContentStatus.FinalFailed,
                                          'status': ContentStatus.FinalFailed}
                        update_contents.append(update_content)
                    elif (content['status'] not in [ContentStatus.Missing]                     # noqa: W503
                          and content['name'] in to_release_names_missing):                        # noqa: W503
                        update_content = {'content_id': content['content_id'],
                                          'substatus': ContentStatus.Missing,
                                          'status': ContentStatus.Missing}
                        update_contents.append(update_content)
    return update_contents


def release_inputs_by_collection(to_release_inputs, final=False):
    update_contents = []
    status_to_check = [ContentStatus.Available, ContentStatus.FakeAvailable, ContentStatus.FinalFailed, ContentStatus.Missing]
    for coll_id in to_release_inputs:
        to_release_contents = to_release_inputs[coll_id]
        if to_release_contents:
            to_release_status = {}
            for to_release_content in to_release_contents:
                if (to_release_content['status'] in status_to_check):
                    to_release_status[to_release_content['name']] = to_release_content['status']
                elif (to_release_content['substatus'] in status_to_check):
                    to_release_status[to_release_content['name']] = to_release_content['substatus']

            # print("to_release_status: %s" % str(to_release_status))

            contents = orm_contents.get_input_contents(request_id=to_release_contents[0]['request_id'],
                                                       coll_id=to_release_contents[0]['coll_id'],
                                                       name=None)
            # print("contents: %s" % str(contents))

            unfinished_contents_dict = {}
            for content in contents:
                if (content['content_relation_type'] == ContentRelationType.InputDependency):    # noqa: W503
                    if content['status'] not in status_to_check:
                        if content['name'] not in unfinished_contents_dict:
                            unfinished_contents_dict[content['name']] = []
                        content_short = {'content_id': content['content_id'], 'status': content['status']}
                        unfinished_contents_dict[content['name']].append(content_short)

            intersection_keys = to_release_status.keys() & unfinished_contents_dict.keys()
            intersection_keys = list(intersection_keys)
            logging.debug("release_inputs_by_collection(coll_id: %s): intersection_keys[:10]: %s" % (coll_id, str(intersection_keys[:10])))

            for name in intersection_keys:
                matched_content_status = to_release_status[name]
                matched_contents = unfinished_contents_dict[name]
                for matched_content in matched_contents:
                    if (matched_content['status'] != matched_content_status):
                        update_content = {'content_id': matched_content['content_id'],
                                          'substatus': matched_content_status,
                                          'status': matched_content_status}
                        update_contents.append(update_content)

    return update_contents


def poll_inputs_dependency_by_collection(unfinished_inputs):
    update_contents = []
    status_to_check = [ContentStatus.Available, ContentStatus.FakeAvailable, ContentStatus.FinalFailed, ContentStatus.Missing]
    for coll_id in unfinished_inputs:
        unfinished_contents = unfinished_inputs[coll_id]
        contents = orm_contents.get_input_contents(request_id=unfinished_contents[0]['request_id'],
                                                   coll_id=unfinished_contents[0]['coll_id'],
                                                   name=None)

        logging.debug("poll_inputs_dependency_by_collection(coll_id: %s): unfinished_contents[:10]: %s" % (coll_id, str(unfinished_contents[:10])))

        to_release_status = {}
        for content in contents:
            if (content['content_relation_type'] == ContentRelationType.Output):    # noqa: W503
                if content['status'] in status_to_check:
                    to_release_status[content['name']] = content['status']
                elif content['substatus'] in status_to_check:
                    to_release_status[content['name']] = content['substatus']

        unfinished_contents_dict = {}
        for content in unfinished_contents:
            if content['name'] not in unfinished_contents_dict:
                unfinished_contents_dict[content['name']] = []
            content_short = {'content_id': content['content_id'], 'status': content['status']}
            unfinished_contents_dict[content['name']].append(content_short)

        intersection_keys = to_release_status.keys() & unfinished_contents_dict.keys()
        intersection_keys = list(intersection_keys)
        logging.debug("poll_inputs_dependency_by_collection(coll_id: %s): intersection_keys[:10]: %s" % (coll_id, str(intersection_keys[:10])))

        for name in intersection_keys:
            matched_content_status = to_release_status[name]
            matched_contents = unfinished_contents_dict[name]
            for matched_content in matched_contents:
                if (matched_content['status'] != matched_content_status):
                    update_content = {'content_id': matched_content['content_id'],
                                      'substatus': matched_content_status,
                                      'status': matched_content_status}
                    update_contents.append(update_content)

        # if len(unfinished_contents_dict.keys()) < len(to_release_status.keys()):
        #     for name, content in unfinished_contents_dict.items():
        #         if name in to_release_status:
        #             matched_content_status = to_release_status[name]
        #             if (content['status'] != matched_content_status):
        #                 update_content = {'content_id': content['content_id'],
        #                                   'substatus': matched_content_status,
        #                                   'status': matched_content_status}
        #                 update_contents.append(update_content)
        # else:
        #     for name, status in to_release_status.items():
        #         if name in unfinished_contents_dict:
        #             matched_content = unfinished_contents_dict[name]
        #             if (matched_content['status'] != status):
        #                 update_content = {'content_id': matched_content['content_id'],
        #                                   'substatus': status,
        #                                   'status': status}
        #                 update_contents.append(update_content)

        # for content in unfinished_contents:
        #     if content['name'] in to_release_status:
        #         matched_content_status = to_release_status[content['name']]
        #         if (content['status'] != matched_content_status):
        #             update_content = {'content_id': content['content_id'],
        #                               'substatus': matched_content_status,
        #                               'status': matched_content_status}
        #             update_contents.append(update_content)

    return update_contents


def get_work_name_to_coll_map(request_id):
    tfs = orm_transforms.get_transforms(request_id=request_id)
    colls = orm_collections.get_collections(request_id=request_id)
    work_name_to_coll_map = {}
    for tf in tfs:
        if ('transform_metadata' in tf and tf['transform_metadata']
           and 'work_name' in tf['transform_metadata'] and tf['transform_metadata']['work_name']):  # noqa: W503
            work_name = tf['transform_metadata']['work_name']
            transform_id = tf['transform_id']
            if work_name not in work_name_to_coll_map:
                work_name_to_coll_map[work_name] = {'inputs': [], 'outputs': []}
            for coll in colls:
                if coll['transform_id'] == transform_id:
                    if coll['relation_type'] == CollectionRelationType.Input:
                        work_name_to_coll_map[work_name]['inputs'].append({'coll_id': coll['coll_id'], 'transform_id': coll['transform_id'],
                                                                           'workload_id': coll['workload_id'],
                                                                           'scope': coll['scope'], 'name': coll['name']})
                    elif coll['relation_type'] == CollectionRelationType.Output:
                        work_name_to_coll_map[work_name]['outputs'].append({'coll_id': coll['coll_id'], 'transform_id': coll['transform_id'],
                                                                            'workload_id': coll['workload_id'],
                                                                            'scope': coll['scope'], 'name': coll['name']})
    return work_name_to_coll_map


@read_session
def get_num_active_transforms(active_status=None, session=None):
    return orm_transforms.get_num_active_transforms(active_status=active_status, session=session)


@read_session
def get_active_transforms(active_status=None, session=None):
    return orm_transforms.get_active_transforms(active_status=active_status, session=session)
