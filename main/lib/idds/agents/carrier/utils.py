#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2022

import json
import logging


from idds.common.constants import (ProcessingStatus,
                                   CollectionStatus,
                                   ContentStatus, ContentType,
                                   ContentRelationType,
                                   WorkStatus,
                                   TransformType2MessageTypeMap,
                                   MessageStatus, MessageSource,
                                   MessageDestination)
from idds.common.utils import setup_logging
from idds.core import (transforms as core_transforms,
                       processings as core_processings,
                       catalog as core_catalog)
from idds.agents.common.cache.redis import get_redis_cache


setup_logging(__name__)


def get_logger(logger=None):
    if logger:
        return logger
    logger = logging.getLogger(__name__)
    return logger


def get_new_content(request_id, transform_id, workload_id, map_id, input_content, content_relation_type=ContentRelationType.Input):
    content = {'transform_id': transform_id,
               'coll_id': input_content['coll_id'],
               'request_id': request_id,
               'workload_id': workload_id,
               'map_id': map_id,
               'scope': input_content['scope'],
               'name': input_content['name'],
               'min_id': input_content['min_id'] if 'min_id' in input_content else 0,
               'max_id': input_content['max_id'] if 'max_id' in input_content else 0,
               'status': input_content['status'] if 'status' in input_content and input_content['status'] is not None else ContentStatus.New,
               'substatus': input_content['substatus'] if 'substatus' in input_content and input_content['substatus'] is not None else ContentStatus.New,
               'path': input_content['path'] if 'path' in input_content else None,
               'content_type': input_content['content_type'] if 'content_type' in input_content else ContentType.File,
               'content_relation_type': content_relation_type,
               'bytes': input_content['bytes'],
               'adler32': input_content['adler32'],
               'content_metadata': input_content['content_metadata']}
    if content['min_id'] is None:
        content['min_id'] = 0
    if content['max_id'] is None:
        content['max_id'] = 0
    return content


def is_process_terminated(processing_status):
    if processing_status in [ProcessingStatus.Finished, ProcessingStatus.Failed,
                             ProcessingStatus.SubFinished, ProcessingStatus.Cancelled,
                             ProcessingStatus.Suspended, ProcessingStatus.Expired,
                             ProcessingStatus.Broken, ProcessingStatus.FinishedOnStep,
                             ProcessingStatus.FinishedOnExec, ProcessingStatus.FinishedTerm]:
        return True
    return False


def is_all_inputs_dependency_available(inputs_dependency):
    for content in inputs_dependency:
        if type(content) is dict:
            if content['status'] not in [ContentStatus.Available, ContentStatus.FakeAvailable]:
                return False
        else:
            # list of content_id, status,
            if content[1] not in [ContentStatus.Available, ContentStatus.FakeAvailable]:
                return False
    return True


def is_all_inputs_dependency_terminated(inputs_dependency):
    for content in inputs_dependency:
        if type(content) is dict:
            if content['status'] not in [ContentStatus.Available, ContentStatus.FakeAvailable,
                                         ContentStatus.FinalFailed, ContentStatus.Missing]:
                return False
        else:
            if content[1] not in [ContentStatus.Available, ContentStatus.FakeAvailable,
                                  ContentStatus.FinalFailed, ContentStatus.Missing]:
                return False
    return True


def is_input_dependency_terminated(input_dependency):
    if type(input_dependency) is dict:
        if input_dependency['status'] in [ContentStatus.Available, ContentStatus.FakeAvailable,
                                          ContentStatus.FinalFailed, ContentStatus.Missing]:
            return True
    else:
        if input_dependency[1] in [ContentStatus.Available, ContentStatus.FakeAvailable,
                                   ContentStatus.FinalFailed, ContentStatus.Missing]:
            return True
    return False


def get_collection_ids(collections):
    coll_ids = [coll.coll_id for coll in collections]
    return coll_ids


def get_input_output_maps(transform_id, work):
    # link collections
    input_collections = work.get_input_collections()
    output_collections = work.get_output_collections()
    log_collections = work.get_log_collections()

    # for coll in input_collections + output_collections + log_collections:
    #     coll_model = core_catalog.get_collection(coll_id=coll.coll_id)
    #     coll.collection = coll_model

    input_coll_ids = get_collection_ids(input_collections)
    output_coll_ids = get_collection_ids(output_collections)
    log_coll_ids = get_collection_ids(log_collections)

    mapped_input_output_maps = core_transforms.get_transform_input_output_maps(transform_id,
                                                                               input_coll_ids=input_coll_ids,
                                                                               output_coll_ids=output_coll_ids,
                                                                               log_coll_ids=log_coll_ids)

    # work_name_to_coll_map = core_transforms.get_work_name_to_coll_map(request_id=transform['request_id'])
    # work.set_work_name_to_coll_map(work_name_to_coll_map)

    # new_input_output_maps = work.get_new_input_output_maps(mapped_input_output_maps)
    return mapped_input_output_maps


def get_new_contents(request_id, transform_id, workload_id, new_input_output_maps):
    new_input_contents, new_output_contents, new_log_contents = [], [], []
    new_input_dependency_contents = []
    for map_id in new_input_output_maps:
        inputs = new_input_output_maps[map_id]['inputs'] if 'inputs' in new_input_output_maps[map_id] else []
        inputs_dependency = new_input_output_maps[map_id]['inputs_dependency'] if 'inputs_dependency' in new_input_output_maps[map_id] else []
        outputs = new_input_output_maps[map_id]['outputs'] if 'outputs' in new_input_output_maps[map_id] else []
        logs = new_input_output_maps[map_id]['logs'] if 'logs' in new_input_output_maps[map_id] else []

        for input_content in inputs:
            content = get_new_content(request_id, transform_id, workload_id, map_id, input_content, content_relation_type=ContentRelationType.Input)
            new_input_contents.append(content)
        for input_content in inputs_dependency:
            content = get_new_content(request_id, transform_id, workload_id, map_id, input_content, content_relation_type=ContentRelationType.InputDependency)
            new_input_dependency_contents.append(content)
        for output_content in outputs:
            content = get_new_content(request_id, transform_id, workload_id, map_id, output_content, content_relation_type=ContentRelationType.Output)
            new_output_contents.append(content)
        for log_content in logs:
            content = get_new_content(request_id, transform_id, workload_id, map_id, log_content, content_relation_type=ContentRelationType.Log)
            new_log_contents.append(content)
    return new_input_contents, new_output_contents, new_log_contents, new_input_dependency_contents


def get_update_content(content):
    updated_content = {'content_id': content['content_id'],
                       'status': content['substatus'],
                       'substatus': content['substatus']}
    content['status'] = content['substatus']
    return updated_content, content


def get_update_contents(request_id, transform_id, workload_id, input_output_maps):
    updated_contents = []
    updated_input_contents_full, updated_output_contents_full = [], []

    for map_id in input_output_maps:
        inputs = input_output_maps[map_id]['inputs'] if 'inputs' in input_output_maps[map_id] else []
        inputs_dependency = input_output_maps[map_id]['inputs_dependency'] if 'inputs_dependency' in input_output_maps[map_id] else []
        outputs = input_output_maps[map_id]['outputs'] if 'outputs' in input_output_maps[map_id] else []
        # logs = input_output_maps[map_id]['logs'] if 'logs' in input_output_maps[map_id] else []

        content_update_status = None
        if is_all_inputs_dependency_available(inputs_dependency):
            # logger.debug("all input dependency available: %s, inputs: %s" % (str(inputs_dependency), str(inputs)))
            content_update_status = ContentStatus.Available
        elif is_all_inputs_dependency_terminated(inputs_dependency):
            # logger.debug("all input dependency terminated: %s, inputs: %s, outputs: %s" % (str(inputs_dependency), str(inputs), str(outputs)))
            content_update_status = ContentStatus.Missing

        if content_update_status:
            for content in inputs:
                content['substatus'] = content_update_status
                if content['status'] != content['substatus']:
                    updated_content, content = get_update_content(content)
                    updated_contents.append(updated_content)
                    updated_input_contents_full.append(content)
        if content_update_status in [ContentStatus.Missing]:
            for content in outputs:
                content['substatus'] = content_update_status
                if content['status'] != content['substatus']:
                    updated_content, content = get_update_content(content)
                    updated_contents.append(updated_content)
                    updated_output_contents_full.append(content)

        for content in outputs:
            if content['status'] != content['substatus']:
                updated_content, content = get_update_content(content)
                updated_contents.append(updated_content)
                updated_output_contents_full.append(content)
        return updated_contents, updated_input_contents_full, updated_output_contents_full


def get_message_type(work_type, input_type='file'):
    work_type_value = str(work_type.value)
    if work_type_value not in TransformType2MessageTypeMap:
        return TransformType2MessageTypeMap['0'][input_type]
    else:
        return TransformType2MessageTypeMap[work_type_value][input_type]


def generate_file_messages(request_id, transform_id, workload_id, work, files, relation_type):
    i_msg_type, i_msg_type_str = get_message_type(work.get_work_type(), input_type='file')
    files_message = []
    for file in files:
        file_status = file['status'].name
        if file['status'] == ContentStatus.FakeAvailable:
            file_status = ContentStatus.Available.name
        file_message = {'scope': file['scope'],
                        'name': file['name'],
                        'path': file['path'],
                        'status': file_status}
        files_message.append(file_message)
    msg_content = {'msg_type': i_msg_type_str.value,
                   'request_id': request_id,
                   'workload_id': workload_id,
                   'relation_type': relation_type,
                   'files': files_message}
    num_msg_content = len(files_message)
    return i_msg_type, msg_content, num_msg_content


def generate_collection_messages(request_id, transform_id, workload_id, work, collection, relation_type):
    coll_name = collection.name
    if coll_name.endswith(".idds.stagein"):
        coll_name = coll_name.replace(".idds.stagein", "")

    i_msg_type, i_msg_type_str = get_message_type(work.get_work_type(), input_type='collection')
    msg_content = {'msg_type': i_msg_type_str.value,
                   'request_id': request_id,
                   'workload_id': workload_id,
                   'relation_type': relation_type,
                   'collections': [{'scope': collection.scope,
                                    'name': coll_name,
                                    'status': collection.status.name}],
                   'output': work.get_output_data(),
                   'error': work.get_terminated_msg()}
    num_msg_content = 1
    return i_msg_type, msg_content, num_msg_content


def generate_work_messages(request_id, transform_id, workload_id, work, relation_type):
    i_msg_type, i_msg_type_str = get_message_type(work.get_work_type(), input_type='work')
    msg_content = {'msg_type': i_msg_type_str.value,
                   'request_id': request_id,
                   'workload_id': workload_id,
                   'relation_type': relation_type,
                   'status': work.get_status().name,
                   'output': work.get_output_data(),
                   'error': work.get_terminated_msg()}
    num_msg_content = 1
    return i_msg_type, msg_content, num_msg_content


def generate_messages(request_id, transform_id, workload_id, work, msg_type='file', files=[], relation_type='input'):
    if msg_type == 'file':
        i_msg_type, msg_content, num_msg_content = generate_file_messages(request_id, transform_id, workload_id, work, files=files, relation_type=relation_type)
        msg = {'msg_type': i_msg_type,
               'status': MessageStatus.New,
               'source': MessageSource.Transformer,
               'destination': MessageDestination.Outside,
               'request_id': request_id,
               'workload_id': workload_id,
               'transform_id': transform_id,
               'num_contents': num_msg_content,
               'msg_content': msg_content}
        return [msg]
    elif msg_type == 'work':
        # link collections
        input_collections = work.get_input_collections()
        output_collections = work.get_output_collections()
        log_collections = work.get_log_collections()

        msg_type_contents = []
        msg_type_content = generate_work_messages(request_id, transform_id, workload_id, work, relation_type='input')
        msg_type_contents.append(msg_type_content)
        for coll in input_collections:
            msg_type_content = generate_collection_messages(request_id, transform_id, workload_id, work, coll, relation_type='input')
            msg_type_contents.append(msg_type_content)
        for coll in output_collections:
            msg_type_content = generate_collection_messages(request_id, transform_id, workload_id, work, coll, relation_type='output')
            msg_type_contents.append(msg_type_content)
        for coll in log_collections:
            msg_type_content = generate_collection_messages(request_id, transform_id, workload_id, work, coll, relation_type='log')
            msg_type_contents.append(msg_type_content)

        msgs = []
        for i_msg_type, msg_content, num_msg_content in msg_type_contents:
            msg = {'msg_type': i_msg_type,
                   'status': MessageStatus.New,
                   'source': MessageSource.Transformer,
                   'destination': MessageDestination.Outside,
                   'request_id': request_id,
                   'workload_id': workload_id,
                   'transform_id': transform_id,
                   'num_contents': num_msg_content,
                   'msg_content': msg_content}
            msgs.append(msg)
        return msgs


def handle_new_processing(processing, agent_attributes, logger=None, log_prefix=''):
    logger = get_logger(logger)

    proc = processing['processing_metadata']['processing']
    work = proc.work
    work.set_agent_attributes(agent_attributes, processing)
    transform_id = processing['transform_id']

    status, workload_id, errors = work.submit_processing(processing)
    logger.info(log_prefix + "submit_processing (status: %s, workload_id: %s, errors: %s)" % (status, workload_id, errors))

    if not status:
        logger.error(log_prefix + "Failed to submit processing (status: %s, workload_id: %s, errors: %s)" % (status, workload_id, errors))
        return False, processing, [], [], errors

    ret_msgs = []
    new_contents = []
    if proc.workload_id:
        processing['workload_id'] = proc.workload_id
    if proc.submitted_at:
        input_output_maps = get_input_output_maps(transform_id, work)
        new_input_output_maps = work.get_new_input_output_maps(input_output_maps)
        request_id = processing['request_id']
        transform_id = processing['transform_id']
        workload_id = processing['workload_id']
        ret_new_contents = get_new_contents(request_id, transform_id, workload_id, new_input_output_maps)
        new_input_contents, new_output_contents, new_log_contents, new_input_dependency_contents = ret_new_contents
        new_contents = new_input_contents + new_output_contents + new_log_contents + new_input_dependency_contents

        if new_input_contents:
            msgs = generate_messages(request_id, transform_id, workload_id, work, msg_type='file', files=new_input_contents, relation_type='input')
            ret_msgs = ret_msgs + msgs
        if new_output_contents:
            msgs = generate_messages(request_id, transform_id, workload_id, work, msg_type='file', files=new_input_contents, relation_type='output')
            ret_msgs = ret_msgs + msgs
    return True, processing, new_contents, ret_msgs, errors


def get_updated_contents_by_request(request_id, transform_id, workload_id, work, logger=None, log_prefix=''):
    logger = get_logger(logger)

    status_to_check = [ContentStatus.Available, ContentStatus.FakeAvailable, ContentStatus.FinalFailed, ContentStatus.Missing]
    contents = core_catalog.get_contents_by_request_transform(request_id=request_id)
    updated_contents = []
    for content in contents:
        if content['substatus'] in status_to_check:
            updated_contents.append(content)

    logger.debug(log_prefix + "get_updated_contents_by_request: updated_contents[:3]: %s" % str(updated_contents[:3]))
    return updated_contents


def get_input_dependency_map_by_request(request_id, transform_id, workload_id, work, logger=None, log_prefix=''):
    logger = get_logger(logger)

    cache = get_redis_cache()
    content_dependcy_map_key = "request_content_dependcy_map_%s" % request_id
    content_dependcy_map = cache.get(content_dependcy_map_key, default={})
    request_dependcy_maps_key = "request_transform_dependcy_maps_%s" % request_id
    request_dependcy_maps = cache.get(request_dependcy_maps_key, default={})
    collection_dependcy_map_key = "request_collection_dependcy_map_%s" % request_id
    collection_dependcy_map = cache.get(collection_dependcy_map_key, default=[])

    refresh = False
    if transform_id and transform_id not in request_dependcy_maps:
        refresh = True
    elif work:
        output_collections = work.get_output_collections()
        for coll in output_collections:
            if coll.coll_id not in collection_dependcy_map:
                refresh = True

    if refresh or not content_dependcy_map:
        content_output_name2id = {}
        content_input_deps = []
        transform_dependcy_maps = {}

        contents = core_catalog.get_contents_by_request_transform(request_id=request_id)
        # logger.debug("contents: ", contents)
        for content in contents:
            if content['transform_id'] not in transform_dependcy_maps:
                transform_dependcy_maps[content['transform_id']] = {}
            if content['map_id'] not in transform_dependcy_maps[content['transform_id']]:
                transform_dependcy_maps[content['transform_id']][content['map_id']] = {'inputs': [], 'outputs': [], 'input_deps': []}

            if content['content_relation_type'] == ContentRelationType.Output:
                if content['coll_id'] not in content_output_name2id:
                    content_output_name2id[content['coll_id']] = {}
                    collection_dependcy_map.append(content['coll_id'])
                content_output_name2id[content['coll_id']][content['name']] = content
                # content_id, status
                transform_dependcy_maps[content['transform_id']][content['map_id']]['outputs'].append((content['content_id'], None))
            elif content['content_relation_type'] == ContentRelationType.InputDependency:
                content_input_deps.append(content)
                # content_id, status
                transform_dependcy_maps[content['transform_id']][content['map_id']]['input_deps'].append((content['content_id'], None))
            elif content['content_relation_type'] == ContentRelationType.Input:
                # content_id, status
                transform_dependcy_maps[content['transform_id']][content['map_id']]['inputs'].append((content['content_id'], None))
        # logger.debug("content_output_name2id: ", content_output_name2id)

        for content in content_input_deps:
            dep_coll_id = content['coll_id']
            if dep_coll_id not in content_output_name2id:
                logger.warn(log_prefix + "dep_coll_id: %s contents are not added yet" % dep_coll_id)
            else:
                dep_content = content_output_name2id[dep_coll_id].get(content['name'], None)
                if dep_content:
                    dep_content_id = dep_content['content_id']
                    if dep_content_id not in content_dependcy_map:
                        content_dependcy_map[dep_content_id] = []
                    content_dependcy_map[dep_content_id].append((content['content_id'], content['transform_id'], content['map_id']))
                else:
                    logger.error(log_prefix + "Failed to find input dependcy for content_id: %s" % content['content_id'])

        request_dependcy_maps = transform_dependcy_maps
        # logger.debug("content_dependcy_map_key: ", content_dependcy_map)
        # logger.debug("request_dependcy_maps: ", request_dependcy_maps)
        cache.set(content_dependcy_map_key, content_dependcy_map)
        cache.set(request_dependcy_maps_key, request_dependcy_maps)
        cache.set(collection_dependcy_map_key, collection_dependcy_map)

    return content_dependcy_map, request_dependcy_maps


def set_input_dependency_map_by_request(request_id, content_dependcy_map=None, request_dependcy_maps=None):
    cache = get_redis_cache()
    if content_dependcy_map:
        content_dependcy_map_key = "request_content_dependcy_map_%s" % request_id
        cache.set(content_dependcy_map_key, content_dependcy_map)
    if request_dependcy_maps:
        request_dependcy_maps_key = "request_content_dependcy_maps_%s" % request_id
        cache.set(request_dependcy_maps_key, request_dependcy_maps)


def trigger_release_inputs_no_deps(request_id, transform_id, workload_id, work, logger=None, log_prefix=''):
    logger = get_logger(logger)

    content_depency_map, transform_dependency_maps = get_input_dependency_map_by_request(request_id, transform_id, workload_id, work,
                                                                                         logger=logger, log_prefix=log_prefix)
    logger.debug(log_prefix + "content_depency_map[:2]: %s" % str({k: content_depency_map[k] for k in list(content_depency_map.keys())[:2]}))
    # logger.debug(log_prefix + "transform_dependency_map[:2]: %s" % str({key: transform_dependency_map[key] for k in list(transform_dependency_map.keys())[:2]}))
    logger.debug(log_prefix + "transform_dependency_maps.keys[:2]: %s" % str(list(transform_dependency_maps.keys())[:2]))

    update_contents, update_contents_dict = [], {}
    update_input_contents_full = {}
    update_content_ids = []

    # release jobs without inputs_dependency
    if transform_id not in transform_dependency_maps:
        logger.warn(log_prefix + "transform_id %s not in transform_dependency_maps" % transform_id)
    else:
        transform_dependency_map = transform_dependency_maps[transform_id]
        for map_id in transform_dependency_map:
            inputs_dependency = transform_dependency_map[map_id]['input_deps']
            if len(inputs_dependency) == 0:
                inputs = transform_dependency_map[map_id]['inputs']
                for content in inputs:
                    content_id, status = content
                    update_content_ids.append(content_id)
                    u_content = {'content_id': content_id,
                                 'status': ContentStatus.Available,
                                 'substatus': ContentStatus.Available}
                    # update_contents.append(u_content)
                    update_contents_dict[content_id] = u_content

    if update_content_ids:
        contents = core_catalog.get_contents_by_content_ids(update_content_ids, request_id=request_id)
        for content in contents:
            content_id = content['content_id']
            if update_contents_dict[content_id]['status'] != content['status']:
                update_contents.append(update_contents_dict[content_id])
                if content['transform_id'] not in update_input_contents_full:
                    update_input_contents_full[content['transform_id']] = []
                u_content_full = {'content_id': content_id,
                                  'request_id': content['request_id'],
                                  'transform_id': content['transform_id'],
                                  'workload_id': content['workload_id'],
                                  'status': ContentStatus.Available,
                                  'substatus': ContentStatus.Available,
                                  'scope': content['scope'],
                                  'name': content['name'],
                                  'path': content['path']}
                update_input_contents_full[content['transform_id']].append(u_content_full)
    return update_contents, update_input_contents_full


def trigger_release_inputs(request_id, transform_id, workload_id, work, updated_contents_full, logger=None, log_prefix=''):
    logger = get_logger(logger)

    status_to_check = [ContentStatus.Available, ContentStatus.FakeAvailable, ContentStatus.FinalFailed, ContentStatus.Missing]

    content_depency_map, transform_dependency_maps = get_input_dependency_map_by_request(request_id, transform_id, workload_id, work,
                                                                                         logger=logger, log_prefix=log_prefix)
    logger.debug(log_prefix + "content_depency_map[:2]: %s" % str({k: content_depency_map[k] for k in list(content_depency_map.keys())[:2]}))
    # logger.debug(log_prefix + "transform_dependency_map[:2]: %s" % str({key: transform_dependency_maps[key] for k in list(transform_dependency_maps.keys())[:2]}))
    logger.debug(log_prefix + "transform_dependency_maps.keys[:2]: %s" % str(list(transform_dependency_maps.keys())[:2]))

    if not updated_contents_full:
        # check all contents
        updated_contents_full = get_updated_contents_by_request(request_id, transform_id, workload_id, work,
                                                                logger=logger, log_prefix=log_prefix)

    triggered_map_ids = []
    update_contents = []
    update_contents_dict = {}
    update_contents_ids = []
    update_input_contents_full = {}
    # 1. use the outputs to check input_dependency
    for content in updated_contents_full:
        if content['substatus'] in status_to_check:
            content_id = content['content_id']
            if content_id in content_depency_map:
                # t_contents are the contents to be triggered
                t_contents = content_depency_map[content_id]
                for t_content in t_contents:
                    t_content_id, t_transform_id, t_map_id = t_content
                    # update the input_dependency
                    u_content = {'content_id': t_content_id,
                                 'status': content['status'],
                                 'substatus': content['substatus']}
                    # update_contents.append(u_content)
                    update_contents_dict[t_content_id] = u_content
                    update_contents_ids.append(t_content_id)

                    t_tf_map_id = (t_transform_id, t_map_id)
                    if t_tf_map_id not in triggered_map_ids:
                        triggered_map_ids.append(t_tf_map_id)

                    t_input_deps = transform_dependency_maps[t_transform_id][t_map_id]['input_deps']
                    if t_input_deps:
                        new_inputs_dependency = []
                        for input_dep in t_input_deps:
                            content_id, substatus = input_dep
                            if content_id == t_content_id:
                                substatus = content['substatus']
                            new_inputs_dependency.append((content_id, substatus))
                        transform_dependency_maps[t_transform_id][t_map_id]['input_deps'] = new_inputs_dependency

    if update_contents_ids:
        contents = core_catalog.get_contents_by_content_ids(update_contents_ids, request_id=request_id)
        for content in contents:
            if content['status'] != update_contents_dict[content['content_id']]['status']:
                update_contents.append(update_contents_dict[content['content_id']])

    # update the cached map
    set_input_dependency_map_by_request(request_id, content_dependcy_map=None, request_dependcy_maps=transform_dependency_maps)

    # 2. use the updated input_dependency to release inputs
    input_update_content_ids = []
    for t_tf_map_id in triggered_map_ids:
        transform_id, map_id = t_tf_map_id
        inputs_dependency = transform_dependency_maps[transform_id][map_id]['input_deps']

        content_update_status = None
        if is_all_inputs_dependency_available(inputs_dependency):
            content_update_status = ContentStatus.Available
        elif is_all_inputs_dependency_terminated(inputs_dependency):
            content_update_status = ContentStatus.Missing
        if content_update_status:
            inputs = transform_dependency_maps[transform_id][map_id]['inputs']
            for content in inputs:
                content_id, substatus = content
                u_content = {'content_id': content_id,
                             'status': content_update_status,
                             'substatus': content_update_status}
                # update_contents.append(u_content)
                update_contents_dict[content_id] = u_content
                input_update_content_ids.append(content_id)

    if input_update_content_ids:
        contents = core_catalog.get_contents_by_content_ids(input_update_content_ids, request_id=request_id)
        for content in contents:
            content_id = content['content_id']
            if update_contents_dict[content_id]['status'] != content['status']:
                update_contents.append(update_contents_dict[content_id])
                if content['transform_id'] not in update_input_contents_full:
                    update_input_contents_full[content['transform_id']] = []
                u_content_full = {'content_id': content_id,
                                  'request_id': content['request_id'],
                                  'transform_id': content['transform_id'],
                                  'workload_id': content['workload_id'],
                                  'status': update_contents_dict[content_id]['status'],
                                  'substatus': update_contents_dict[content_id]['substatus'],
                                  'scope': content['scope'],
                                  'name': content['name'],
                                  'path': content['path']}
                update_input_contents_full[content['transform_id']].append(u_content_full)

    return update_contents, update_input_contents_full


def handle_update_processing(processing, agent_attributes, logger=None, log_prefix=''):
    logger = get_logger(logger)

    request_id = processing['request_id']
    transform_id = processing['transform_id']
    workload_id = processing['workload_id']

    proc = processing['processing_metadata']['processing']
    work = proc.work
    work.set_agent_attributes(agent_attributes, processing)

    input_output_maps = get_input_output_maps(transform_id, work)
    logger.debug(log_prefix + "get_input_output_maps: len: %s" % len(input_output_maps))
    logger.debug(log_prefix + "get_input_output_maps.keys[:5]: %s" % str(list(input_output_maps.keys())[:5]))

    new_input_output_maps = work.get_new_input_output_maps(input_output_maps)
    logger.debug(log_prefix + "get_new_input_output_maps: len: %s" % len(new_input_output_maps))
    logger.debug(log_prefix + "get_new_input_output_maps.keys[:5]: %s" % str(list(new_input_output_maps.keys())[:5]))

    ret_poll_processing = work.poll_processing_updates(processing, input_output_maps, log_prefix=log_prefix)
    process_status, content_updates, new_input_output_maps1, updated_contents_full = ret_poll_processing
    new_input_output_maps.update(new_input_output_maps1)
    logger.debug(log_prefix + "poll_processing_updates process_status: %s" % process_status)
    logger.debug(log_prefix + "poll_processing_updates content_updates[:5]: %s" % content_updates[:5])
    logger.debug(log_prefix + "poll_processing_updates new_input_output_maps1.keys[:5]: %s" % (list(new_input_output_maps1.keys())[:5]))
    logger.debug(log_prefix + "poll_processing_updates updated_contents_full[:5]: %s" % (updated_contents_full[:5]))

    ret_new_contents = get_new_contents(request_id, transform_id, workload_id, new_input_output_maps)
    new_input_contents, new_output_contents, new_log_contents, new_input_dependency_contents = ret_new_contents
    new_contents = new_input_contents + new_output_contents + new_log_contents + new_input_dependency_contents

    ret_msgs = []
    new_contents = []
    if new_input_contents:
        msgs = generate_messages(request_id, transform_id, workload_id, work, msg_type='file',
                                 files=new_input_contents, relation_type='input')
        ret_msgs = ret_msgs + msgs
    if new_output_contents:
        msgs = generate_messages(request_id, transform_id, workload_id, work, msg_type='file',
                                 files=new_output_contents, relation_type='output')
        ret_msgs = ret_msgs + msgs

    if updated_contents_full:
        msgs = generate_messages(request_id, transform_id, workload_id, work, msg_type='file',
                                 files=updated_contents_full, relation_type='output')
        ret_msgs = ret_msgs + msgs

    content_updates_trigger_no_deps, updated_input_contents_no_deps = [], []
    if work.use_dependency_to_release_jobs():
        content_updates_trigger_no_deps, updated_input_contents_no_deps = trigger_release_inputs_no_deps(request_id, transform_id, workload_id, work,
                                                                                                         logger, log_prefix)
        logger.debug(log_prefix + "trigger_release_inputs_no_deps: content_updates_trigger_no_deps[:3] %s" % (content_updates_trigger_no_deps[:3]))
        # logger.debug(log_prefix + "trigger_release_inputs_no_deps: updated_input_contents_no_deps[:3] %s" % (updated_input_contents_no_deps[:3]))

        content_updates = content_updates + content_updates_trigger_no_deps
        if updated_input_contents_no_deps:
            for trigger_tf_id in updated_input_contents_no_deps:
                logger.debug(log_prefix + "trigger_release_inputs_no_deps: updated_input_contents_no_deps[%s][:3] %s" % (trigger_tf_id,
                                                                                                                         updated_input_contents_no_deps[trigger_tf_id][:3]))
                trigger_req_id = updated_input_contents_no_deps[trigger_tf_id][0]['request_id']
                trigger_workload_id = updated_input_contents_no_deps[trigger_tf_id][0]['workload_id']

                msgs = generate_messages(trigger_req_id, trigger_tf_id, trigger_workload_id, work, msg_type='file',
                                         files=updated_input_contents_no_deps[trigger_tf_id], relation_type='input')
                ret_msgs = ret_msgs + msgs

        content_updates_trigger, updated_input_contents = trigger_release_inputs(request_id, transform_id, workload_id, work, updated_contents_full,
                                                                                 logger, log_prefix)
        logger.debug(log_prefix + "trigger_release_inputs: content_updates_trigger[:3] %s" % (content_updates_trigger[:3]))
        # logger.debug(log_prefix + "trigger_release_inputs: updated_input_contents[:5] %s" % (updated_input_contents[:5]))

        if updated_input_contents:
            for trigger_tf_id in updated_input_contents:
                logger.debug(log_prefix + "trigger_release_inputs: updated_input_contents[%s][:3] %s" % (trigger_tf_id,
                                                                                                         updated_input_contents[trigger_tf_id][:3]))
                trigger_req_id = updated_input_contents[trigger_tf_id][0]['request_id']
                trigger_workload_id = updated_input_contents[trigger_tf_id][0]['workload_id']

                msgs = generate_messages(trigger_req_id, trigger_tf_id, trigger_workload_id, work, msg_type='file',
                                         files=updated_input_contents[trigger_tf_id], relation_type='input')
                ret_msgs = ret_msgs + msgs
        content_updates = content_updates + content_updates_trigger

    return process_status, new_contents, ret_msgs, content_updates


def get_content_status_from_panda_msg_status(status):
    status_map = {'starting': ContentStatus.New,
                  'running': ContentStatus.Processing,
                  'finished': ContentStatus.Available,
                  'failed': ContentStatus.Failed}
    if status in status_map:
        return status_map[status]
    return ContentStatus.New


def get_collection_id_transform_id_map(coll_id, request_id, request_ids=[]):
    cache = get_redis_cache()
    coll_tf_id_map_key = "collection_id_transform_id_map"
    coll_tf_id_map = cache.get(coll_tf_id_map_key, default={})

    if coll_id is None or coll_id not in coll_tf_id_map:
        if not request_ids:
            request_ids = []
        if request_id not in request_ids:
            request_ids.append(request_id)
        colls = core_catalog.get_collections_by_request_ids(request_ids)
        for coll in colls:
            coll_tf_id_map[coll['coll_id']] = (coll['request_id'], coll['transform_id'], coll['workload_id'])

        cache.set(coll_tf_id_map_key, coll_tf_id_map)

    if coll_id is None or coll_id not in coll_tf_id_map:
        return None, None, None
    return coll_tf_id_map[coll_id]


def get_workload_id_transform_id_map(workload_id):
    cache = get_redis_cache()
    workload_id_transform_id_map_key = "all_worloadid2transformid_map"
    workload_id_transform_id_map = cache.get(workload_id_transform_id_map_key, default={})

    workload_id_transform_id_map_notexist_key = "all_worloadid2transformid_map_notexist"
    workload_id_transform_id_map_notexist = cache.get(workload_id_transform_id_map_notexist_key, default=[])

    if workload_id in workload_id_transform_id_map_notexist:
        return None

    request_ids = []
    if not workload_id_transform_id_map or workload_id not in workload_id_transform_id_map:
        processing_status = [ProcessingStatus.New,
                             ProcessingStatus.Submitting, ProcessingStatus.Submitted,
                             ProcessingStatus.Running, ProcessingStatus.FinishedOnExec,
                             ProcessingStatus.Cancel, ProcessingStatus.FinishedOnStep,
                             ProcessingStatus.ToCancel, ProcessingStatus.Cancelling,
                             ProcessingStatus.ToSuspend, ProcessingStatus.Suspending,
                             ProcessingStatus.ToResume, ProcessingStatus.Resuming,
                             ProcessingStatus.ToExpire, ProcessingStatus.Expiring,
                             ProcessingStatus.ToFinish, ProcessingStatus.ToForceFinish]

        procs = core_processings.get_processings_by_status(status=processing_status)
        for proc in procs:
            processing = proc['processing_metadata']['processing']
            work = processing.work
            if work.use_dependency_to_release_jobs():
                workload_id_transform_id_map[proc['workload_id']] = (proc['request_id'], proc['transform_id'], proc['processing_id'])
                if proc['request_id'] not in request_ids:
                    request_ids.append(proc['request_id'])

        cache.set(workload_id_transform_id_map_key, workload_id_transform_id_map)

    # renew the collection to transform map
    if request_ids:
        get_collection_id_transform_id_map(coll_id=None, request_id=request_ids[0], request_ids=request_ids)

    # for tasks running in some other instances
    if workload_id not in workload_id_transform_id_map:
        workload_id_transform_id_map_notexist.append(workload_id)
        return None

    return workload_id_transform_id_map[workload_id]


def get_input_name_content_id_map(request_id, workload_id, transform_id):
    cache = get_redis_cache()
    input_name_content_id_map_key = "tf_input_contentid_map_%s" % transform_id
    input_name_content_id_map = cache.get(input_name_content_id_map_key)

    if not input_name_content_id_map:
        contents = core_catalog.get_contents_by_request_transform(request_id=request_id, transform_id=transform_id)
        input_name_content_id_map = {}
        for content in contents:
            if content['content_relation_type'] == ContentRelationType.Input:
                input_name_content_id_map[content['name']] = content['content_id']

        cache.set(input_name_content_id_map_key, input_name_content_id_map)
    return input_name_content_id_map


def get_jobid_content_id_map(request_id, workload_id, transform_id, job_id, inputs):
    cache = get_redis_cache()
    jobid_content_id_map_key = "tf_jobid_contentid_map_%s" % transform_id
    jobid_content_id_map = cache.get(jobid_content_id_map_key)

    to_update_jobid = False
    if not jobid_content_id_map or job_id not in jobid_content_id_map:
        to_update_jobid = True
        input_name_content_id_map = get_input_name_content_id_map(request_id, workload_id, transform_id)
        for ip in inputs:
            if ':' in ip:
                pos = ip.find(":")
                ip = ip[pos + 1:]
            if ip in input_name_content_id_map:
                content_id = input_name_content_id_map[ip]
                jobid_content_id_map[job_id] = content_id
                break

        cache.set(jobid_content_id_map_key, jobid_content_id_map)
    return jobid_content_id_map, to_update_jobid


def get_content_id_from_job_id(request_id, workload_id, transform_id, job_id, inputs):
    jobid_content_id_map, to_update_jobid = get_jobid_content_id_map(request_id, workload_id, transform_id, job_id, inputs)

    if job_id in jobid_content_id_map:
        content_id = jobid_content_id_map[job_id]
    else:
        content_id = None
    return content_id, to_update_jobid


def handle_messages_processing(messages):
    new_processings = []
    update_processings = []
    update_contents = []
    request_update_contents = {}

    for msg in messages:
        msg = json.loads(msg)
        if msg['msg_type'] in ['task_status']:
            workload_id = msg['taskid']
            status = msg['status']
            if status in ['defined']:   # 'prepared'
                ret_req_tf_pr_id = get_workload_id_transform_id_map(workload_id)
                if not ret_req_tf_pr_id:
                    # request is submitted by some other instances
                    pass
                else:
                    req_id, tf_id, processing_id = ret_req_tf_pr_id
                    new_processings.append((req_id, tf_id, processing_id, workload_id, status))
            elif status in ['finished', 'done']:
                ret_req_tf_pr_id = get_workload_id_transform_id_map(workload_id)
                if not ret_req_tf_pr_id:
                    # request is submitted by some other instances
                    pass
                else:
                    req_id, tf_id, processing_id = ret_req_tf_pr_id
                    update_processings.append((processing_id, status))

        if msg['msg_type'] in ['job_status']:
            workload_id = msg['taskid']
            job_id = msg['jobid']
            status = msg['status']
            inputs = msg['inputs']
            ret_req_tf_pr_id = get_workload_id_transform_id_map(workload_id)
            if not ret_req_tf_pr_id:
                # request is submitted by some other instances
                pass
            else:
                req_id, tf_id, processing_id = ret_req_tf_pr_id
                content_id, to_update_jobid = get_content_id_from_job_id(req_id, workload_id, tf_id, job_id, inputs)
                if content_id:
                    if to_update_jobid:
                        u_content = {'content_id': content_id,
                                     'status': get_content_status_from_panda_msg_status(status),
                                     'substatus': get_content_status_from_panda_msg_status(status),
                                     'content_metadata': {'panda_id': job_id}}
                    else:
                        u_content = {'content_id': content_id,
                                     'substatus': get_content_status_from_panda_msg_status(status),
                                     'status': get_content_status_from_panda_msg_status(status)}

                    update_contents.append(u_content)
                    if req_id not in request_update_contents:
                        request_update_contents[req_id] = []
                    request_update_contents[req_id].append(u_content)

    work = None
    msgs_no_deps = []
    content_updates_trigger_no_deps = []
    for new_processing in new_processings:
        req_id, tf_id, processing_id, workload_id, status = new_processing
        content_updates_trigger_no_dep, updated_input_contents_no_dep = trigger_release_inputs_no_deps(req_id, tf_id, workload_id, work)
        content_updates_trigger_no_deps += content_updates_trigger_no_dep
        if updated_input_contents_no_dep:
            msgs_no_dep = generate_messages(req_id, tf_id, workload_id, work, msg_type='file',
                                            files=updated_input_contents_no_dep, relation_type='input')
            msgs_no_deps += msgs_no_dep

    msgs = []
    content_updates_triggers = []
    for req_id in request_update_contents:
        content_updates_trigger, updated_input_contents = trigger_release_inputs(req_id, tf_id=None, workload_id=None, work=None,
                                                                                 updated_contents_full=request_update_contents[req_id])
        content_updates_triggers += content_updates_trigger
        if updated_input_contents:
            msg = generate_messages(req_id, tf_id, workload_id, work, msg_type='file',
                                    files=updated_input_contents, relation_type='input')
            msgs += msg
    return update_processings, update_contents + content_updates_triggers + content_updates_trigger_no_deps, msgs + msgs_no_deps


def sync_collection_status(request_id, transform_id, workload_id, work, input_output_maps=None,
                           close_collection=False, force_close_collection=False, terminate=False):
    if input_output_maps is None:
        input_output_maps = get_input_output_maps(transform_id, work)

    all_updates_flushed = True
    coll_status = {}
    for map_id in input_output_maps:
        inputs = input_output_maps[map_id]['inputs'] if 'inputs' in input_output_maps[map_id] else []
        # inputs_dependency = input_output_maps[map_id]['inputs_dependency'] if 'inputs_dependency' in input_output_maps[map_id] else []
        outputs = input_output_maps[map_id]['outputs'] if 'outputs' in input_output_maps[map_id] else []
        logs = input_output_maps[map_id]['logs'] if 'logs' in input_output_maps[map_id] else []

        for content in inputs + outputs + logs:
            if content['coll_id'] not in coll_status:
                coll_status[content['coll_id']] = {'total_files': 0, 'processed_files': 0, 'processing_files': 0, 'bytes': 0}
            coll_status[content['coll_id']]['total_files'] += 1

            if content['status'] in [ContentStatus.Available, ContentStatus.Mapped,
                                     ContentStatus.Available.value, ContentStatus.Mapped.value,
                                     ContentStatus.FakeAvailable, ContentStatus.FakeAvailable.value]:
                coll_status[content['coll_id']]['processed_files'] += 1
                coll_status[content['coll_id']]['bytes'] += content['bytes']
            else:
                coll_status[content['coll_id']]['processing_files'] += 1

            if content['status'] != content['substatus']:
                all_updates_flushed = False

    input_collections = work.get_input_collections(poll_externel=True)
    output_collections = work.get_output_collections()
    log_collections = work.get_log_collections()

    update_collections = []
    for coll in input_collections + output_collections + log_collections:
        if coll.coll_id in coll_status:
            if 'total_files' in coll.coll_metadata and coll.coll_metadata['total_files']:
                coll.total_files = coll.coll_metadata['total_files']
            else:
                coll.total_files = coll_status[coll.coll_id]['total_files']
            coll.processed_files = coll_status[coll.coll_id]['processed_files']
            coll.processing_files = coll_status[coll.coll_id]['processing_files']
            coll.bytes = coll_status[coll.coll_id]['bytes']

            u_coll = {'coll_id': coll.coll_id,
                      'total_files': coll.total_files,
                      'processed_files': coll.processed_files,
                      'processing_files': coll.processing_files,
                      'bytes': coll.bytes}
            if terminate:
                if force_close_collection or close_collection and all_updates_flushed or coll.status == CollectionStatus.Closed:
                    u_coll['status'] = CollectionStatus.Closed
                    u_coll['substatus'] = CollectionStatus.Closed
                    coll.status = CollectionStatus.Closed
                    coll.substatus = CollectionStatus.Closed

            update_collections.append(u_coll)
    return update_collections, all_updates_flushed


def sync_work_status(request_id, transform_id, workload_id, work):
    input_collections = work.get_input_collections()
    output_collections = work.get_output_collections()
    log_collections = work.get_log_collections()

    is_all_collections_closed = True
    is_all_files_processed = True
    is_all_files_failed = True
    for coll in input_collections + output_collections + log_collections:
        if coll.status != CollectionStatus.Closed:
            is_all_collections_closed = False
        if coll.total_files != coll.processed_files:
            is_all_files_processed = False
        if coll.processed_files > 0:
            is_all_files_failed = False

    if is_all_collections_closed:
        if is_all_files_failed:
            work.status = WorkStatus.Failed
        elif is_all_files_processed:
            work.status = WorkStatus.Finished
        else:
            work.status = WorkStatus.SubFinished


def sync_processing(processing, agent_attributes, terminate=False):
    request_id = processing['request_id']
    transform_id = processing['transform_id']
    workload_id = processing['workload_id']

    proc = processing['processing_metadata']['processing']
    work = proc.work
    work.set_agent_attributes(agent_attributes, processing)

    # input_output_maps = get_input_output_maps(transform_id, work)
    update_collections, all_updates_flushed = sync_collection_status(request_id, transform_id, workload_id, work,
                                                                     input_output_maps=None, close_collection=True,
                                                                     terminate=terminate)

    messages = []
    sync_work_status(request_id, transform_id, workload_id, work)
    if terminate and work.is_terminated():
        messages = generate_messages(request_id, transform_id, workload_id, work, msg_type='work')
        if work.is_finished():
            processing['status'] = ProcessingStatus.Finished
        elif work.is_failed:
            processing['status'] = ProcessingStatus.Failed
        else:
            processing['status'] = ProcessingStatus.SubFinished
    return processing, update_collections, messages


def handle_abort_processing(processing, agent_attributes, logger=None, log_prefix=''):
    logger = get_logger(logger)

    request_id = processing['request_id']
    transform_id = processing['transform_id']
    workload_id = processing['workload_id']

    proc = processing['processing_metadata']['processing']
    work = proc.work
    work.set_agent_attributes(agent_attributes, processing)

    work.abort_processing(processing, log_prefix=log_prefix)

    input_collections = work.get_input_collections()
    output_collections = work.get_output_collections()
    log_collections = work.get_log_collections()

    # input_output_maps = get_input_output_maps(transform_id, work)
    update_collections, all_updates_flushed = sync_collection_status(request_id, transform_id, workload_id, work,
                                                                     input_output_maps=None, close_collection=True,
                                                                     force_close_collection=True)

    for coll in input_collections + output_collections + log_collections:
        coll.status = CollectionStatus.Closed
        coll.substatus = CollectionStatus.Closed
    update_contents = []

    # processing['status'] = ProcessingStatus.Cancelled
    return processing, update_collections, update_contents


def reactive_contents(request_id, transform_id, workload_id, work, input_output_maps):
    updated_contents = []
    contents = core_catalog.get_contents_by_request_transform(request_id=request_id, transform_id=transform_id)
    for content in contents:
        if content['status'] not in [ContentStatus.Available, ContentStatus.Mapped,
                                     ContentStatus.Available.value, ContentStatus.Mapped.value,
                                     ContentStatus.FakeAvailable, ContentStatus.FakeAvailable.value]:
            u_content = {'content_id': content['content_id'],
                         'substatus': ContentStatus.New,
                         'status': ContentStatus.New}
            updated_contents.append(u_content)
    return updated_contents


def handle_resume_processing(processing, agent_attributes, logger=None, log_prefix=''):
    logger = get_logger(logger)

    request_id = processing['request_id']
    transform_id = processing['transform_id']
    workload_id = processing['workload_id']

    proc = processing['processing_metadata']['processing']
    work = proc.work
    work.set_agent_attributes(agent_attributes, processing)

    work.resume_processing(processing, log_prefix=log_prefix)

    input_collections = work.get_input_collections()
    output_collections = work.get_output_collections()
    log_collections = work.get_log_collections()

    update_collections = []
    for coll in input_collections + output_collections + log_collections:
        coll.status = CollectionStatus.Open
        coll.substatus = CollectionStatus.Open
        u_collection = {'coll_id': coll.coll_id,
                        'status': CollectionStatus.Open,
                        'substatus': CollectionStatus.Open}
        update_collections.append(u_collection)

    input_output_maps = get_input_output_maps(transform_id, work)
    update_contents = reactive_contents(request_id, transform_id, workload_id, work, input_output_maps)

    processing['status'] = ProcessingStatus.Processing
    return processing, update_collections, update_contents
