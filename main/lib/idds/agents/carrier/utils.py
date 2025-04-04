#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2022 - 2025

import concurrent.futures
import json
import logging
import time
import threading
import traceback

from idds.common.constants import (ProcessingStatus,
                                   CollectionStatus,
                                   ContentStatus, ContentType,
                                   ContentRelationType,
                                   WorkStatus,
                                   TransformType,
                                   TransformType2MessageTypeMap,
                                   MessageType, MessageTypeStr,
                                   MessageStatus, MessageSource,
                                   MessageDestination,
                                   get_work_status_from_transform_processing_status)
from idds.common.utils import setup_logging, get_list_chunks
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


def get_new_content(request_id, transform_id, workload_id, map_id, input_content, content_relation_type=ContentRelationType.Input,
                    es_name=None, sub_map_id=None, order_id=None):
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
    if 'sub_map_id' in input_content:
        content['sub_map_id'] = input_content['sub_map_id']
    if 'dep_sub_map_id' in input_content:
        content['dep_sub_map_id'] = input_content['dep_sub_map_id']

    if order_id is not None:
        content['min_id'] = order_id
        content['max_id'] = order_id
    if sub_map_id is not None:
        content['sub_map_id'] = sub_map_id
    if es_name is not None and content_relation_type == ContentRelationType.Output:
        content['path'] = es_name
    return content


def is_process_terminated(processing_status):
    if processing_status in [ProcessingStatus.Finished, ProcessingStatus.Failed,
                             ProcessingStatus.SubFinished, ProcessingStatus.Cancelled,
                             ProcessingStatus.Suspended, ProcessingStatus.Expired,
                             ProcessingStatus.Broken, ProcessingStatus.FinishedOnStep,
                             ProcessingStatus.FinishedOnExec, ProcessingStatus.FinishedTerm]:
        return True
    return False


def is_process_finished(processing_status):
    if processing_status in [ProcessingStatus.Finished]:
        return True
    return False


def is_all_contents_available(contents):
    for content in contents:
        if type(content) is dict:
            if content['substatus'] not in [ContentStatus.Available, ContentStatus.FakeAvailable]:
                return False
        else:
            # list of content_id, status,
            if content[1] not in [ContentStatus.Available, ContentStatus.FakeAvailable]:
                return False
    return True


def is_all_contents_terminated(contents, terminated=False):
    terminated_status = [ContentStatus.Available, ContentStatus.FakeAvailable,
                         ContentStatus.FinalFailed, ContentStatus.Missing]
    if terminated:
        terminated_status = [ContentStatus.Available, ContentStatus.FakeAvailable, ContentStatus.Failed,
                             ContentStatus.FinalFailed, ContentStatus.Missing]

    for content in contents:
        if type(content) is dict:
            if content['substatus'] not in terminated_status:
                return False
        else:
            if content[1] not in terminated_status:
                return False
    return True


def is_input_dependency_terminated(input_dependency):
    if type(input_dependency) is dict:
        if input_dependency['substatus'] in [ContentStatus.Available, ContentStatus.FakeAvailable,
                                             ContentStatus.FinalFailed, ContentStatus.Missing]:
            return True
    else:
        if input_dependency[1] in [ContentStatus.Available, ContentStatus.FakeAvailable,
                                   ContentStatus.FinalFailed, ContentStatus.Missing]:
            return True
    return False


def is_all_contents_terminated_but_not_available(inputs, terminated=False):
    terminated_status = [ContentStatus.Available, ContentStatus.FakeAvailable,
                         ContentStatus.FinalFailed, ContentStatus.Missing]
    if terminated:
        terminated_status = [ContentStatus.Available, ContentStatus.FakeAvailable, ContentStatus.Failed,
                             ContentStatus.FinalFailed, ContentStatus.Missing]

    all_contents_available = True
    for content in inputs:
        if type(content) is dict:
            if content['substatus'] not in terminated_status:
                return False
            if content['substatus'] not in [ContentStatus.Available]:
                all_contents_available = False
        else:
            if content[1] not in terminated_status:
                return False
            if content[1] not in [ContentStatus.Available]:
                all_contents_available = False
    if all_contents_available:
        return False
    return True


def is_all_contents_available_with_status_map(inputs_dependency, content_status_map):
    for content_id in inputs_dependency:
        status = content_status_map[str(content_id)]
        if status not in [ContentStatus.Available, ContentStatus.FakeAvailable]:
            return False
    return True


def is_all_contents_terminated_with_status_map(input_dependency, content_status_map):
    for content_id in input_dependency:
        status = content_status_map[str(content_id)]
        if status not in [ContentStatus.Available, ContentStatus.FakeAvailable,
                          ContentStatus.FinalFailed, ContentStatus.Missing]:
            return False
    return True


def get_collection_ids(collections):
    coll_ids = [coll.coll_id for coll in collections]
    return coll_ids


def get_input_output_maps(transform_id, work, with_deps=True):
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
                                                                               log_coll_ids=log_coll_ids,
                                                                               with_sub_map_id=work.with_sub_map_id(),
                                                                               with_deps=with_deps)

    # work_name_to_coll_map = core_transforms.get_work_name_to_coll_map(request_id=transform['request_id'])
    # work.set_work_name_to_coll_map(work_name_to_coll_map)

    # new_input_output_maps = work.get_new_input_output_maps(mapped_input_output_maps)
    return mapped_input_output_maps


def get_ext_contents(transform_id, work):
    contents_ids = core_catalog.get_contents_ext_ids(transform_id=transform_id)
    return contents_ids


def get_new_contents(request_id, transform_id, workload_id, new_input_output_maps, max_updates_per_round=2000, logger=None, log_prefix=''):
    logger = get_logger(logger)

    logger.debug(log_prefix + "get_new_contents")
    new_input_contents, new_output_contents, new_log_contents = [], [], []
    new_input_dependency_contents = []
    new_input_dep_coll_ids = []
    chunks = []
    for map_id in new_input_output_maps:
        if "sub_maps" not in new_input_output_maps[map_id] or not new_input_output_maps[map_id]["sub_maps"]:
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
                if content['coll_id'] not in new_input_dep_coll_ids:
                    new_input_dep_coll_ids.append(content['coll_id'])
            for output_content in outputs:
                content = get_new_content(request_id, transform_id, workload_id, map_id, output_content, content_relation_type=ContentRelationType.Output)
                new_output_contents.append(content)
            for log_content in logs:
                content = get_new_content(request_id, transform_id, workload_id, map_id, log_content, content_relation_type=ContentRelationType.Log)
                new_log_contents.append(content)

            total_num_updates = len(new_input_contents) + len(new_output_contents) + len(new_log_contents) + len(new_input_dependency_contents)
            if total_num_updates > max_updates_per_round:
                chunk = new_input_contents, new_output_contents, new_log_contents, new_input_dependency_contents
                chunks.append(chunk)

                new_input_contents, new_output_contents, new_log_contents = [], [], []
                new_input_dependency_contents = []
        else:
            sub_maps = new_input_output_maps[map_id]["sub_maps"]
            for sub_map in sub_maps:
                sub_map_id = sub_map['sub_map_id']
                order_id = sub_map['order_id']
                inputs = sub_map['inputs'] if 'inputs' in sub_map else []
                inputs_dependency = sub_map['inputs_dependency'] if 'inputs_dependency' in sub_map else []
                outputs = sub_map['outputs'] if 'outputs' in sub_map else []
                logs = sub_map['logs'] if 'logs' in sub_map else []

                for input_content in inputs:
                    content = get_new_content(request_id, transform_id, workload_id, map_id, input_content,
                                              content_relation_type=ContentRelationType.Input,
                                              sub_map_id=sub_map_id, order_id=order_id)
                    new_input_contents.append(content)
                for input_content in inputs_dependency:
                    content = get_new_content(request_id, transform_id, workload_id, map_id, input_content,
                                              content_relation_type=ContentRelationType.InputDependency,
                                              sub_map_id=sub_map_id, order_id=order_id)
                    new_input_dependency_contents.append(content)
                    if content['coll_id'] not in new_input_dep_coll_ids:
                        new_input_dep_coll_ids.append(content['coll_id'])
                for output_content in outputs:
                    content = get_new_content(request_id, transform_id, workload_id, map_id, output_content,
                                              content_relation_type=ContentRelationType.Output,
                                              sub_map_id=sub_map_id, order_id=order_id)
                    new_output_contents.append(content)
                for log_content in logs:
                    content = get_new_content(request_id, transform_id, workload_id, map_id, log_content,
                                              content_relation_type=ContentRelationType.Log,
                                              sub_map_id=sub_map_id, order_id=order_id)
                    new_log_contents.append(content)

            total_num_updates = len(new_input_contents) + len(new_output_contents) + len(new_log_contents) + len(new_input_dependency_contents)
            if total_num_updates > max_updates_per_round:
                chunk = new_input_contents, new_output_contents, new_log_contents, new_input_dependency_contents
                chunks.append(chunk)

                new_input_contents, new_output_contents, new_log_contents = [], [], []
                new_input_dependency_contents = []

    total_num_updates = len(new_input_contents) + len(new_output_contents) + len(new_log_contents) + len(new_input_dependency_contents)
    if total_num_updates > 0:
        chunk = new_input_contents, new_output_contents, new_log_contents, new_input_dependency_contents
        chunks.append(chunk)

    # return new_input_contents, new_output_contents, new_log_contents, new_input_dependency_contents
    return chunks


def get_update_content(content):
    updated_content = {'content_id': content['content_id'],
                       'request_id': content['request_id'],
                       # 'substatus': content['substatus'],
                       'status': content['substatus']}
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

        input_output_sub_maps = get_input_output_sub_maps(inputs, outputs, inputs_dependency)
        for sub_map_id in input_output_sub_maps:
            inputs_sub = input_output_sub_maps[sub_map_id]['inputs']
            outputs_sub = input_output_sub_maps[sub_map_id]['outputs']
            inputs_dependency_sub = input_output_sub_maps[sub_map_id]['inputs_dependency']

            content_update_status = None
            if is_all_contents_available(inputs_dependency_sub):
                # logger.debug("all input dependency available: %s, inputs: %s" % (str(inputs_dependency), str(inputs)))
                content_update_status = ContentStatus.Available
            elif is_all_contents_terminated(inputs_dependency_sub):
                # logger.debug("all input dependency terminated: %s, inputs: %s, outputs: %s" % (str(inputs_dependency), str(inputs), str(outputs)))
                content_update_status = ContentStatus.Missing

            if content_update_status:
                for content in inputs_sub:
                    content['substatus'] = content_update_status
                    if content['status'] != content['substatus']:
                        updated_content, content = get_update_content(content)
                        updated_contents.append(updated_content)
                        updated_input_contents_full.append(content)
            if content_update_status in [ContentStatus.Missing]:
                for content in outputs_sub:
                    content['substatus'] = content_update_status
                    if content['status'] != content['substatus']:
                        updated_content, content = get_update_content(content)
                        updated_contents.append(updated_content)
                        updated_output_contents_full.append(content)

            for content in outputs_sub:
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
    if work:
        work_type = work.get_work_type()
    else:
        work_type = TransformType.Processing

    i_msg_type, i_msg_type_str = get_message_type(work_type, input_type='file')
    no_dup_files = {}
    files_message = []
    for file in files:
        filename = file['name']
        if work and work.es:
            filename = file['path']
            if filename in no_dup_files:
                continue
            else:
                no_dup_files[filename] = None

        file_status = file['substatus'].name
        if file['substatus'] == ContentStatus.FakeAvailable:
            file_status = ContentStatus.Available.name
        file_message = {'scope': file['scope'],
                        'name': filename,
                        'path': file['path'],
                        'map_id': file['map_id'],
                        'content_id': file['content_id'] if 'content_id' in file else None,
                        'external_coll_id': file['external_coll_id'] if 'external_coll_id' in file else None,
                        'external_content_id': file['external_content_id'] if 'external_content_id' in file else None,
                        'status': file_status}
        files_message.append(file_message)
    msg_content = {'msg_type': i_msg_type_str.value,
                   'request_id': request_id,
                   'transform_id': transform_id,
                   'workload_id': workload_id,
                   'relation_type': relation_type,
                   'files': files_message}
    num_msg_content = len(files_message)
    return i_msg_type, msg_content, num_msg_content


def generate_content_ext_messages(request_id, transform_id, workload_id, work, files, relation_type, input_output_maps):
    i_msg_type = MessageType.ContentExt
    i_msg_type_str = MessageTypeStr.ContentExt

    output_contents = []
    for map_id in input_output_maps:
        outputs = input_output_maps[map_id]['outputs'] if 'outputs' in input_output_maps[map_id] else []
        for content in outputs:
            # content_map[content['content_id']] = content
            output_contents += outputs

    files_message = core_catalog.combine_contents_ext(output_contents, files, with_status_name=True)
    msg_content = {'msg_type': i_msg_type_str.value,
                   'request_id': request_id,
                   'workload_id': workload_id,
                   'transform_id': transform_id,
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
                   'transform_id': transform_id,
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
                   'transform_id': transform_id,
                   'relation_type': relation_type,
                   'status': work.get_status().name,
                   'output': work.get_output_data(),
                   'error': work.get_terminated_msg()}
    num_msg_content = 1
    return i_msg_type, msg_content, num_msg_content


def generate_messages(request_id, transform_id, workload_id, work, msg_type='file', files=[], relation_type='input', input_output_maps=None):
    if msg_type == 'file':
        i_msg_type, msg_content, num_msg_content = generate_file_messages(request_id, transform_id, workload_id, work, files=files, relation_type=relation_type)
        msg = {'msg_type': i_msg_type,
               'status': MessageStatus.New,
               'source': MessageSource.Carrier,
               'destination': MessageDestination.Outside,
               'request_id': request_id,
               'workload_id': workload_id,
               'transform_id': transform_id,
               'num_contents': num_msg_content,
               'msg_content': msg_content}
        return [msg]
    elif msg_type == 'content_ext':
        i_msg_type, msg_content, num_msg_content = generate_content_ext_messages(request_id, transform_id, workload_id, work, files=files,
                                                                                 relation_type=relation_type,
                                                                                 input_output_maps=input_output_maps)
        msg = {'msg_type': i_msg_type,
               'status': MessageStatus.New,
               'source': MessageSource.Carrier,
               'destination': MessageDestination.ContentExt,
               'request_id': request_id,
               'workload_id': workload_id,
               'transform_id': transform_id,
               'num_contents': num_msg_content,
               'msg_content': msg_content}
        return [msg]
    elif msg_type == 'collection':
        msg_type_contents = []
        for coll in files:
            msg_type_content = generate_collection_messages(request_id, transform_id, workload_id, work, coll, relation_type=relation_type)
            msg_type_contents.append(msg_type_content)

        msgs = []
        for i_msg_type, msg_content, num_msg_content in msg_type_contents:
            msg = {'msg_type': i_msg_type,
                   'status': MessageStatus.New,
                   'source': MessageSource.Carrier,
                   'destination': MessageDestination.Outside,
                   'request_id': request_id,
                   'workload_id': workload_id,
                   'transform_id': transform_id,
                   'num_contents': num_msg_content,
                   'msg_content': msg_content}
            msgs.append(msg)
        return msgs
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
                   'source': MessageSource.Carrier,
                   'destination': MessageDestination.Outside,
                   'request_id': request_id,
                   'workload_id': workload_id,
                   'transform_id': transform_id,
                   'num_contents': num_msg_content,
                   'msg_content': msg_content}
            msgs.append(msg)
        return msgs


def update_processing_contents_thread(logger, log_prefix, log_msg, kwargs):
    try:
        logger = get_logger(logger)
        logger.debug(log_prefix + log_msg)
        core_processings.update_processing_contents(**kwargs)
        logger.debug(log_prefix + " end")
    except Exception as ex:
        logger.error(log_prefix + "update_processing_contents_thread: %s" % str(ex))
    except:
        logger.error(traceback.format_exc())


def wait_futures_finish(ret_futures, func_name, logger, log_prefix):
    logger = get_logger(logger)
    logger.debug(log_prefix + "%s: wait_futures_finish" % func_name)
    # Wait for all subprocess to complete
    steps = 0
    while True:
        steps += 1
        # Wait for all subprocess to complete in 3 minutes
        completed, _ = concurrent.futures.wait(ret_futures, timeout=180, return_when=concurrent.futures.ALL_COMPLETED)
        ret_futures = ret_futures - completed
        if len(ret_futures) > 0:
            logger.debug(log_prefix + "%s thread: %s threads has been running for more than %s minutes" % (func_name, len(ret_futures), steps * 3))
        else:
            break
    logger.debug(log_prefix + "%s: wait_futures_finish end" % func_name)


def handle_new_processing(processing, agent_attributes, func_site_to_cloud=None, max_updates_per_round=2000, executors=None, logger=None, log_prefix=''):
    logger = get_logger(logger)

    proc = processing['processing_metadata']['processing']
    work = proc.work
    work.set_agent_attributes(agent_attributes, processing)
    transform_id = processing['transform_id']

    if func_site_to_cloud:
        work.set_func_site_to_cloud(func_site_to_cloud)
    status, workload_id, errors = work.submit_processing(processing)
    logger.info(log_prefix + "submit_processing (status: %s, workload_id: %s, errors: %s)" % (status, workload_id, errors))

    if not status:
        logger.error(log_prefix + "Failed to submit processing (status: %s, workload_id: %s, errors: %s)" % (status, workload_id, errors))
        return False, processing, [], [], [], [], errors

    ret_msgs = []
    new_contents = []
    new_input_dependency_contents = []
    update_collections = []
    if proc.workload_id:
        processing['workload_id'] = proc.workload_id
        input_collections = work.get_input_collections()
        output_collections = work.get_output_collections()
        log_collections = work.get_log_collections()
        for coll in input_collections + output_collections + log_collections:
            u_coll = {'coll_id': coll.coll_id, 'workload_id': proc.workload_id}
            update_collections.append(u_coll)

    if proc.submitted_at:
        input_output_maps = get_input_output_maps(transform_id, work, with_deps=False)
        new_input_output_maps = work.get_new_input_output_maps(input_output_maps)
        request_id = processing['request_id']
        transform_id = processing['transform_id']
        workload_id = processing['workload_id']
        ret_new_contents_chunks = get_new_contents(request_id, transform_id, workload_id, new_input_output_maps,
                                                   max_updates_per_round=max_updates_per_round, logger=logger, log_prefix=log_prefix)
        if executors is None:
            for ret_new_contents in ret_new_contents_chunks:
                new_input_contents, new_output_contents, new_log_contents, new_input_dependency_contents = ret_new_contents
                # new_contents = new_input_contents + new_output_contents + new_log_contents + new_input_dependency_contents
                new_contents = new_input_contents + new_output_contents + new_log_contents

                # not generate new messages
                # if new_input_contents:
                #     msgs = generate_messages(request_id, transform_id, workload_id, work, msg_type='file', files=new_input_contents, relation_type='input')
                #     ret_msgs = ret_msgs + msgs
                # if new_output_contents:
                #     msgs = generate_messages(request_id, transform_id, workload_id, work, msg_type='file', files=new_input_contents, relation_type='output')
                #     ret_msgs = ret_msgs + msgs
                logger.debug(log_prefix + "handle_new_processing: add %s new contents" % (len(new_contents)))
                core_processings.update_processing_contents(update_processing=None,
                                                            request_id=request_id,
                                                            new_contents=new_contents,
                                                            new_input_dependency_contents=new_input_dependency_contents,
                                                            messages=ret_msgs)
        else:
            ret_futures = set()
            for ret_new_contents in ret_new_contents_chunks:
                new_input_contents, new_output_contents, new_log_contents, new_input_dependency_contents = ret_new_contents
                new_contents = new_input_contents + new_output_contents + new_log_contents
                log_msg = "handle_new_processing thread: add %s new contents" % (len(new_contents))
                kwargs = {'update_processing': None,
                          'request_id': request_id,
                          'new_contents': new_contents,
                          'new_input_dependency_contents': new_input_dependency_contents,
                          'messages': ret_msgs}
                f = executors.submit(update_processing_contents_thread, logger, log_prefix, log_msg, kwargs)
                ret_futures.add(f)
            wait_futures_finish(ret_futures, "handle_new_processing", logger, log_prefix)

    # return True, processing, update_collections, new_contents, new_input_dependency_contents, ret_msgs, errors
    return True, processing, update_collections, [], [], ret_msgs, errors


def get_updated_contents_by_request(request_id, transform_id, workload_id, work, terminated=False, input_output_maps=None,
                                    logger=None, log_prefix=''):
    logger = get_logger(logger)

    status_to_check = [ContentStatus.Available, ContentStatus.FakeAvailable, ContentStatus.FinalFailed,
                       ContentStatus.Missing, ContentStatus.Failed, ContentStatus.Lost,
                       ContentStatus.Deleted]
    contents = core_catalog.get_contents_by_request_transform(request_id=request_id, transform_id=transform_id,
                                                              status=status_to_check, status_updated=True)
    updated_contents, updated_contents_full_input, updated_contents_full_output = [], [], []
    updated_contents_full_input_deps = []
    for content in contents:
        if (content['status'] != content['substatus']) and content['substatus'] in status_to_check:
            u_content = {'content_id': content['content_id'],
                         'request_id': content['request_id'],
                         'status': content['substatus']}
            updated_contents.append(u_content)
            if content['content_relation_type'] == ContentRelationType.Output:
                updated_contents_full_output.append(content)
            elif content['content_relation_type'] == ContentRelationType.Input:
                updated_contents_full_input.append(content)
            elif content['content_relation_type'] == ContentRelationType.InputDependency:
                updated_contents_full_input_deps.append(content)
    # logger.debug(log_prefix + "get_updated_contents_by_request: updated_contents[:3]: %s" % str(updated_contents[:3]))
    return updated_contents, updated_contents_full_input, updated_contents_full_output, updated_contents_full_input_deps


def get_input_output_sub_maps(inputs, outputs, inputs_dependency, logs=[]):
    input_output_sub_maps = {}
    for content in inputs:
        sub_map_id = content['sub_map_id']
        if sub_map_id not in input_output_sub_maps:
            input_output_sub_maps[sub_map_id] = {'inputs': [], 'outputs': [], 'logs': [], 'inputs_dependency': []}
            input_output_sub_maps[sub_map_id]['inputs'].append(content)
    for content in inputs_dependency:
        sub_map_id = content['sub_map_id']
        if sub_map_id not in input_output_sub_maps:
            input_output_sub_maps[sub_map_id] = {'inputs': [], 'outputs': [], 'logs': [], 'inputs_dependency': []}
        input_output_sub_maps[sub_map_id]['inputs_dependency'].append(content)
    for content in outputs:
        sub_map_id = content['sub_map_id']
        if sub_map_id not in input_output_sub_maps:
            input_output_sub_maps[sub_map_id] = {'inputs': [], 'outputs': [], 'logs': [], 'inputs_dependency': []}
        input_output_sub_maps[sub_map_id]['outputs'].append(content)
    for content in logs:
        sub_map_id = content['sub_map_id']
        if sub_map_id not in input_output_sub_maps:
            input_output_sub_maps[sub_map_id] = {'inputs': [], 'outputs': [], 'logs': [], 'inputs_dependency': []}
        input_output_sub_maps[sub_map_id]['logs'].append(content)
    return input_output_sub_maps


def get_updated_contents_by_input_output_maps(input_output_maps=None, terminated=False, max_updates_per_round=2000, with_deps=False, logger=None, log_prefix=''):
    updated_contents, updated_contents_full_input, updated_contents_full_output = [], [], []
    updated_contents_full_input_deps = []
    new_update_contents = []

    chunks = []
    status_to_check = [ContentStatus.Available, ContentStatus.FakeAvailable, ContentStatus.FinalFailed,
                       ContentStatus.Missing, ContentStatus.Failed, ContentStatus.Lost,
                       ContentStatus.Deleted]

    for map_id in input_output_maps:
        inputs = input_output_maps[map_id]['inputs'] if 'inputs' in input_output_maps[map_id] else []
        inputs_dependency = input_output_maps[map_id]['inputs_dependency'] if 'inputs_dependency' in input_output_maps[map_id] else []
        outputs = input_output_maps[map_id]['outputs'] if 'outputs' in input_output_maps[map_id] else []
        # logs = input_output_maps[map_id]['logs'] if 'logs' in input_output_maps[map_id] else []

        input_output_sub_maps = get_input_output_sub_maps(inputs, outputs, inputs_dependency)

        for content in inputs:
            if (content['status'] != content['substatus']) and content['substatus'] in status_to_check:
                u_content = {'content_id': content['content_id'],
                             'request_id': content['request_id'],
                             'status': content['substatus']}
                updated_contents.append(u_content)
                u_content_substatus = {'content_id': content['content_id'],
                                       'substatus': content['substatus'],
                                       'request_id': content['request_id'],
                                       'transform_id': content['transform_id'],
                                       'workload_id': content['workload_id'],
                                       'coll_id': content['coll_id']}
                new_update_contents.append(u_content_substatus)
                updated_contents_full_input.append(content)

        for content in outputs:
            if (content['status'] != content['substatus']) and content['substatus'] in status_to_check:
                u_content = {'content_id': content['content_id'],
                             'request_id': content['request_id'],
                             'status': content['substatus']}
                updated_contents.append(u_content)
                u_content_substatus = {'content_id': content['content_id'],
                                       'substatus': content['substatus'],
                                       'request_id': content['request_id'],
                                       'transform_id': content['transform_id'],
                                       'workload_id': content['workload_id'],
                                       'coll_id': content['coll_id']}
                new_update_contents.append(u_content_substatus)
                updated_contents_full_output.append(content)

        for content in inputs_dependency:
            if (content['status'] != content['substatus']) and content['substatus'] in status_to_check:
                u_content = {'content_id': content['content_id'],
                             'request_id': content['request_id'],
                             'status': content['substatus']}
                updated_contents.append(u_content)
                updated_contents_full_input_deps.append(content)

        for sub_map_id in input_output_sub_maps:
            inputs_sub = input_output_sub_maps[sub_map_id]['inputs']
            outputs_sub = input_output_sub_maps[sub_map_id]['outputs']
            inputs_dependency_sub = input_output_sub_maps[sub_map_id]['inputs_dependency']

            input_content_update_status = None
            if with_deps:
                # if deps are not loaded. This part should not be executed. Otherwise it will release all jobs
                if is_all_contents_available(inputs_dependency_sub):
                    input_content_update_status = ContentStatus.Available
                elif is_all_contents_terminated(inputs_dependency_sub, terminated):
                    input_content_update_status = ContentStatus.Missing
            if input_content_update_status:
                for content in inputs_sub:
                    if content['substatus'] != input_content_update_status:
                        u_content = {'content_id': content['content_id'],
                                     'request_id': content['request_id'],
                                     'status': input_content_update_status,
                                     'substatus': input_content_update_status}
                        updated_contents.append(u_content)
                        content['status'] = input_content_update_status
                        content['substatus'] = input_content_update_status
                        updated_contents_full_input.append(content)
                        u_content_substatus = {'content_id': content['content_id'],
                                               'status': content['substatus'],
                                               'substatus': content['substatus'],
                                               'request_id': content['request_id'],
                                               'transform_id': content['transform_id'],
                                               'workload_id': content['workload_id'],
                                               'coll_id': content['coll_id']}
                        new_update_contents.append(u_content_substatus)

            output_content_update_status = None
            if is_all_contents_available(inputs_sub):
                # wait for the job to finish
                pass
            elif is_all_contents_terminated_but_not_available(inputs_sub, terminated):
                output_content_update_status = ContentStatus.Missing
            if output_content_update_status:
                for content in outputs_sub:
                    if content['substatus'] != output_content_update_status:
                        u_content = {'content_id': content['content_id'],
                                     'request_id': content['request_id'],
                                     'status': output_content_update_status,
                                     'substatus': output_content_update_status}
                        updated_contents.append(u_content)
                        content['status'] = output_content_update_status
                        content['substatus'] = output_content_update_status
                        updated_contents_full_output.append(content)
                        u_content_substatus = {'content_id': content['content_id'],
                                               'status': content['substatus'],
                                               'substatus': content['substatus'],
                                               'request_id': content['request_id'],
                                               'transform_id': content['transform_id'],
                                               'workload_id': content['workload_id'],
                                               'coll_id': content['coll_id']}
                        new_update_contents.append(u_content_substatus)

        if len(updated_contents) > max_updates_per_round:
            chunk = updated_contents, updated_contents_full_input, updated_contents_full_output, updated_contents_full_input_deps, new_update_contents
            chunks.append(chunk)

            updated_contents, updated_contents_full_input, updated_contents_full_output = [], [], []
            updated_contents_full_input_deps = []
            new_update_contents = []

    if len(updated_contents) > 0:
        chunk = updated_contents, updated_contents_full_input, updated_contents_full_output, updated_contents_full_input_deps, new_update_contents
        chunks.append(chunk)

    # return updated_contents, updated_contents_full_input, updated_contents_full_output, updated_contents_full_input_deps, new_update_contents
    return chunks


def get_transform_dependency_map(transform_id, logger=None, log_prefix=''):
    cache = get_redis_cache()
    transform_dependcy_map_key = "transform_dependcy_map_%s" % transform_id
    transform_dependcy_map = cache.get(transform_dependcy_map_key, default={})
    return transform_dependcy_map


def set_transform_dependency_map(transform_id, transform_dependcy_map, logger=None, log_prefix=''):
    cache = get_redis_cache()
    transform_dependcy_map_key = "transform_dependcy_map_%s" % transform_id
    cache.set(transform_dependcy_map_key, transform_dependcy_map)


def get_content_dependcy_map(request_id, logger=None, log_prefix=''):
    cache = get_redis_cache()
    content_dependcy_map_key = "request_content_dependcy_map_%s" % request_id
    content_dependcy_map = cache.get(content_dependcy_map_key, default={})

    request_dependcy_map_key = "request_dependcy_map_%s" % request_id
    request_dependcy_map = cache.get(request_dependcy_map_key, default=[])

    collection_dependcy_map_key = "request_collections_dependcy_map_%s" % request_id
    collection_dependcy_map = cache.get(collection_dependcy_map_key, default=[])

    return content_dependcy_map, request_dependcy_map, collection_dependcy_map


def set_content_dependcy_map(request_id, content_dependcy_map, request_dependcy_map,
                             collection_dependcy_map, logger=None, log_prefix=''):
    cache = get_redis_cache()
    content_dependcy_map_key = "request_content_dependcy_map_%s" % request_id
    cache.set(content_dependcy_map_key, content_dependcy_map)

    request_dependcy_map_key = "request_dependcy_map_%s" % request_id
    cache.set(request_dependcy_map_key, request_dependcy_map)

    collection_dependcy_map_key = "request_collections_dependcy_map_%s" % request_id
    cache.set(collection_dependcy_map_key, collection_dependcy_map)


def get_content_status_map(request_id, logger=None, log_prefix=''):
    cache = get_redis_cache()
    content_status_map_key = "request_content_status_map_%s" % request_id
    content_status_map = cache.get(content_status_map_key, default={})
    return content_status_map


def set_content_status_map(request_id, content_status_map, logger=None, log_prefix=''):
    cache = get_redis_cache()
    content_status_map_key = "request_content_status_map_%s" % request_id
    cache.set(content_status_map_key, content_status_map)


def get_input_dependency_map_by_request(request_id, transform_id, workload_id, work, logger=None, log_prefix=''):
    logger = get_logger(logger)

    content_dependcy_map, request_dependcy_map, collection_dependcy_map = get_content_dependcy_map(request_id, logger=logger, log_prefix=log_prefix)
    content_status_map = get_content_status_map(request_id, logger=logger, log_prefix=log_prefix)

    transform_dependcy_maps = {}

    refresh = False
    if not content_dependcy_map or not content_status_map:
        refresh = True
    elif transform_id and transform_id not in request_dependcy_map:
        refresh = True
    elif work:
        output_collections = work.get_output_collections()
        for coll in output_collections:
            if coll.coll_id not in collection_dependcy_map:
                refresh = True

    for tf_id in request_dependcy_map:
        transform_dependcy_maps[str(tf_id)] = get_transform_dependency_map(tf_id, logger=logger, log_prefix=log_prefix)
        if not transform_dependcy_maps[str(tf_id)]:
            refresh = True

    if refresh:
        logger.debug(log_prefix + "refresh content_dependcy_map")
        content_dependcy_map = {}
        request_dependcy_map = []
        collection_dependcy_map = []
        content_status_map = {}
        transform_dependcy_maps = {}

        content_output_name2id = {}
        content_input_deps = []

        contents = core_catalog.get_contents_by_request_transform(request_id=request_id)
        # logger.debug("contents: ", contents)
        for content in contents:
            if content['transform_id'] not in request_dependcy_map:
                request_dependcy_map.append(content['transform_id'])
            if content['coll_id'] not in collection_dependcy_map:
                collection_dependcy_map.append(content['coll_id'])

            content_status_map[str(content['content_id'])] = content['substatus'].value

            str_tf_id = str(content['transform_id'])
            str_map_id = str(content['map_id'])
            if str_tf_id not in transform_dependcy_maps:
                transform_dependcy_maps[str_tf_id] = get_transform_dependency_map(str_tf_id, logger=logger, log_prefix=log_prefix)
            if str_map_id not in transform_dependcy_maps[str_tf_id]:
                transform_dependcy_maps[str_tf_id][str_map_id] = {'inputs': [], 'outputs': [], 'input_deps': []}

            if content['content_relation_type'] == ContentRelationType.Output:
                if content['coll_id'] not in content_output_name2id:
                    content_output_name2id[content['coll_id']] = {}
                    collection_dependcy_map.append(content['coll_id'])
                content_output_name2id[content['coll_id']][content['name']] = content
                # content_id, status
                transform_dependcy_maps[str_tf_id][str_map_id]['outputs'].append(content['content_id'])
            elif content['content_relation_type'] == ContentRelationType.InputDependency:
                content_input_deps.append(content)
                # content_id, status
                transform_dependcy_maps[str_tf_id][str_map_id]['input_deps'].append(content['content_id'])
            elif content['content_relation_type'] == ContentRelationType.Input:
                # content_id, status
                transform_dependcy_maps[str_tf_id][str_map_id]['inputs'].append(content['content_id'])
        # logger.debug("content_output_name2id: ", content_output_name2id)

        for content in content_input_deps:
            dep_coll_id = content['coll_id']
            if dep_coll_id not in content_output_name2id:
                logger.warn(log_prefix + "dep_coll_id: %s contents are not added yet" % dep_coll_id)
            else:
                dep_content = content_output_name2id[dep_coll_id].get(content['name'], None)
                if dep_content:
                    dep_content_id = str(dep_content['content_id'])
                    if dep_content_id not in content_dependcy_map:
                        content_dependcy_map[dep_content_id] = []
                    content_dependcy_map[dep_content_id].append((content['content_id'], content['transform_id'], content['map_id']))
                else:
                    logger.error(log_prefix + "Failed to find input dependcy for content_id: %s" % content['content_id'])

        set_content_dependcy_map(request_id, content_dependcy_map, request_dependcy_map,
                                 collection_dependcy_map, logger=logger, log_prefix=log_prefix)
        for str_tf_id in transform_dependcy_maps:
            set_transform_dependency_map(str_tf_id, transform_dependcy_maps[str_tf_id], logger=logger, log_prefix=log_prefix)
        set_content_status_map(request_id, content_status_map, logger=logger, log_prefix=log_prefix)

    return content_dependcy_map, transform_dependcy_maps, content_status_map


def get_content_status_with_status_map(content_ids, content_status_map):
    content_id_status = []
    for content_id in content_ids:
        status_value = content_status_map[str(content_id)]
        status = ContentStatus(status_value)
        content_id_status.append((content_id, status))
    return content_id_status


def trigger_release_inputs_no_deps(request_id, transform_id, workload_id, work, input_output_maps, logger=None, log_prefix=''):
    logger = get_logger(logger)

    update_contents = []
    update_input_contents_full = {}
    update_input_contents_full[transform_id] = []

    for map_id in input_output_maps:
        inputs = input_output_maps[map_id]['inputs'] if 'inputs' in input_output_maps[map_id] else []
        inputs_dependency = input_output_maps[map_id]['inputs_dependency'] if 'inputs_dependency' in input_output_maps[map_id] else []
        outputs = input_output_maps[map_id]['outputs'] if 'outputs' in input_output_maps[map_id] else []
        # logs = input_output_maps[map_id]['logs'] if 'logs' in input_output_maps[map_id] else []

        input_output_sub_maps = get_input_output_sub_maps(inputs, outputs, inputs_dependency)
        for sub_map_id in input_output_sub_maps:
            inputs_sub = input_output_sub_maps[sub_map_id]['inputs']
            # outputs_sub = input_output_sub_maps[sub_map_id]['outputs']
            inputs_dependency_sub = input_output_sub_maps[sub_map_id]['inputs_dependency']

            if not inputs_dependency_sub:
                for content in inputs_sub:
                    if content['substatus'] != ContentStatus.Available:
                        u_content = {'content_id': content['content_id'],
                                     'request_id': content['request_id'],
                                     # 'status': ContentStatus.Available,
                                     'substatus': ContentStatus.Available}
                        update_contents.append(u_content)
                        content['status'] = ContentStatus.Available
                        content['substatus'] = ContentStatus.Available
                        update_input_contents_full[transform_id].append(content)
    return update_contents, update_input_contents_full


def trigger_release_inputs(request_id, transform_id, workload_id, work, updated_contents_full_output, updated_contents_full_input,
                           updated_contents_full_input_deps, input_output_maps, logger=None, log_prefix=''):
    logger = get_logger(logger)

    status_to_check = [ContentStatus.Available, ContentStatus.FakeAvailable, ContentStatus.FinalFailed, ContentStatus.Missing]
    # status_to_check_fake = [ContentStatus.FakeAvailable, ContentStatus.Missing]

    update_contents = []
    update_contents_status = {}
    update_contents_status_name = {}
    update_input_contents_full = {}
    update_input_contents_full[transform_id] = []

    for status in status_to_check:
        update_contents_status[status.name] = []
        update_contents_status_name[status.name] = status

    for content in updated_contents_full_output:
        # update the status
        # u_content = {'content_id': content['content_id'], 'status': content['substatus']}
        # update_contents.append(u_content)

        if content['substatus'] in status_to_check:
            update_contents_status[content['substatus'].name].append(content['content_id'])

    for map_id in input_output_maps:
        inputs = input_output_maps[map_id]['inputs'] if 'inputs' in input_output_maps[map_id] else []
        inputs_dependency = input_output_maps[map_id]['inputs_dependency'] if 'inputs_dependency' in input_output_maps[map_id] else []
        outputs = input_output_maps[map_id]['outputs'] if 'outputs' in input_output_maps[map_id] else []
        # logs = input_output_maps[map_id]['logs'] if 'logs' in input_output_maps[map_id] else []

        input_output_sub_maps = get_input_output_sub_maps(inputs, outputs, inputs_dependency)
        for sub_map_id in input_output_sub_maps:
            inputs_sub = input_output_sub_maps[sub_map_id]['inputs']
            outputs_sub = input_output_sub_maps[sub_map_id]['outputs']
            inputs_dependency_sub = input_output_sub_maps[sub_map_id]['inputs_dependency']

            input_content_update_status = None
            if is_all_contents_available(inputs_dependency_sub):
                input_content_update_status = ContentStatus.Available
            elif is_all_contents_terminated(inputs_dependency_sub):
                input_content_update_status = ContentStatus.Missing
            if input_content_update_status:
                for content in inputs_dependency_sub:
                    # u_content = {'content_id': content['content_id'], 'status': content['substatus'])
                    # update_contents.append(u_content)
                    pass
                for content in inputs_sub:
                    u_content = {'content_id': content['content_id'],
                                 'request_id': content['request_id'],
                                 'substatus': input_content_update_status}
                    update_contents.append(u_content)
                    content['status'] = input_content_update_status
                    content['substatus'] = input_content_update_status
                    update_input_contents_full[transform_id].append(content)

            output_content_update_status = None
            if is_all_contents_available(inputs_sub):
                # wait for the job to finish
                # for content in inputs:
                #     u_content = {'content_id': content['content_id'], 'status': content['substatus'])
                #     update_contents.append(u_content)
                pass
            elif is_all_contents_terminated_but_not_available(inputs_sub):
                # for content in inputs:
                #     u_content = {'content_id': content['content_id'], 'status': content['substatus'])
                #     update_contents.append(u_content)
                pass
                output_content_update_status = ContentStatus.Missing
            if output_content_update_status:
                for content in outputs_sub:
                    u_content = {'content_id': content['content_id'],
                                 'request_id': content['request_id'],
                                 'substatus': output_content_update_status}
                    update_contents.append(u_content)

    return update_contents, update_input_contents_full, update_contents_status_name, update_contents_status


def poll_missing_outputs(input_output_maps, contents_ext=[], max_updates_per_round=2000):
    content_updates_missing, updated_contents_full_missing = [], []

    chunks = []
    for map_id in input_output_maps:
        inputs = input_output_maps[map_id]['inputs'] if 'inputs' in input_output_maps[map_id] else []
        inputs_dependency = input_output_maps[map_id]['inputs_dependency'] if 'inputs_dependency' in input_output_maps[map_id] else []
        outputs = input_output_maps[map_id]['outputs'] if 'outputs' in input_output_maps[map_id] else []
        # logs = input_output_maps[map_id]['logs'] if 'logs' in input_output_maps[map_id] else []

        input_output_sub_maps = get_input_output_sub_maps(inputs, outputs, inputs_dependency)
        for sub_map_id in input_output_sub_maps:
            inputs_sub = input_output_sub_maps[sub_map_id]['inputs']
            outputs_sub = input_output_sub_maps[sub_map_id]['outputs']
            # inputs_dependency_sub = input_output_sub_maps[sub_map_id]['inputs_dependency']

            content_update_status = None
            if is_all_contents_terminated_but_not_available(inputs_sub):
                content_update_status = ContentStatus.Missing

                for content in outputs_sub:
                    content['substatus'] = content_update_status
                    if content['status'] != content['substatus']:
                        u_content = {'content_id': content['content_id'],
                                     'request_id': content['request_id'],
                                     'substatus': content['substatus']}

                        content_updates_missing.append(u_content)
                        updated_contents_full_missing.append(content)

        if len(content_updates_missing) > max_updates_per_round:
            chunk = content_updates_missing, updated_contents_full_missing
            chunks.append(chunk)
            content_updates_missing, updated_contents_full_missing = [], []
    if len(content_updates_missing) > 0:
        chunk = content_updates_missing, updated_contents_full_missing
        chunks.append(chunk)
    # return content_updates_missing, updated_contents_full_missing
    return chunks


def has_external_content_id(input_output_maps):
    for map_id in input_output_maps:
        inputs = input_output_maps[map_id]['inputs'] if 'inputs' in input_output_maps[map_id] else []
        for content in inputs:
            if not content['external_content_id']:
                return False
    return True


def get_update_external_content_ids(input_output_maps, external_content_ids):
    name_to_id_map = {}
    update_contents = []
    for map_id in input_output_maps:
        inputs = input_output_maps[map_id]['inputs'] if 'inputs' in input_output_maps[map_id] else []
        outputs = input_output_maps[map_id]['outputs'] if 'outputs' in input_output_maps[map_id] else []
        for content in inputs + outputs:
            if content['name'] not in name_to_id_map:
                name_to_id_map[content['name']] = []
            name_to_id_map[content['name']].append(content['content_id'])
    for dataset in external_content_ids:
        dataset_id = dataset['dataset']['id']
        files = dataset['files']
        for file_item in files:
            lfn = file_item['lfn']
            # remove scope '00000:'
            pos = lfn.find(":")
            if pos >= 0:
                lfn = lfn[pos + 1:]
            file_id = file_item['id']
            content_ids = name_to_id_map.get(lfn, [])
            for content_id in content_ids:
                update_content = {'content_id': content_id,
                                  'request_id': content['request_id'],
                                  'external_coll_id': dataset_id,
                                  'external_content_id': file_id}
                update_contents.append(update_content)
    return update_contents


def handle_update_processing(processing, agent_attributes, max_updates_per_round=2000, use_bulk_update_mappings=True, executors=None, logger=None, log_prefix=''):
    logger = get_logger(logger)

    ret_msgs = []
    new_contents = []

    request_id = processing['request_id']
    transform_id = processing['transform_id']
    workload_id = processing['workload_id']

    proc = processing['processing_metadata']['processing']
    work = proc.work
    work.set_agent_attributes(agent_attributes, processing)

    input_output_maps = get_input_output_maps(transform_id, work, with_deps=False)
    logger.debug(log_prefix + "get_input_output_maps: len: %s" % len(input_output_maps))
    logger.debug(log_prefix + "get_input_output_maps.keys[:3]: %s" % str(list(input_output_maps.keys())[:3]))

    if work.has_external_content_id() and not has_external_content_id(input_output_maps):
        external_content_ids = work.get_external_content_ids(processing, log_prefix=log_prefix)
        update_external_content_ids = get_update_external_content_ids(input_output_maps, external_content_ids)
        core_catalog.update_contents(update_external_content_ids)

    new_input_output_maps = work.get_new_input_output_maps(input_output_maps)
    logger.debug(log_prefix + "get_new_input_output_maps: len: %s" % len(new_input_output_maps))
    logger.debug(log_prefix + "get_new_input_output_maps.keys[:3]: %s" % str(list(new_input_output_maps.keys())[:3]))

    contents_ext = []
    if work.require_ext_contents():
        contents_ext = get_ext_contents(transform_id, work)
        job_info_maps = core_catalog.get_contents_ext_maps()
        ret_poll_processing = work.poll_processing_updates(processing, input_output_maps, contents_ext=contents_ext,
                                                           job_info_maps=job_info_maps, executors=executors, log_prefix=log_prefix)
        process_status, content_updates, new_input_output_maps1, updated_contents_full, parameters, new_contents_ext, update_contents_ext = ret_poll_processing
    else:
        ret_poll_processing = work.poll_processing_updates(processing, input_output_maps, log_prefix=log_prefix)
        new_contents_ext, update_contents_ext = [], []
        process_status, content_updates, new_input_output_maps1, updated_contents_full, parameters = ret_poll_processing

    new_input_output_maps.update(new_input_output_maps1)
    logger.debug(log_prefix + "poll_processing_updates process_status: %s" % process_status)
    logger.debug(log_prefix + "poll_processing_updates content_updates[:3]: %s" % content_updates[:3])
    logger.debug(log_prefix + "poll_processing_updates new_input_output_maps1.keys[:3]: %s" % (list(new_input_output_maps1.keys())[:3]))
    logger.debug(log_prefix + "poll_processing_updates updated_contents_full[:3]: %s" % (updated_contents_full[:3]))

    ret_futures = set()

    ret_new_contents_chunks = get_new_contents(request_id, transform_id, workload_id, new_input_output_maps, max_updates_per_round=max_updates_per_round)
    for ret_new_contents in ret_new_contents_chunks:
        new_input_contents, new_output_contents, new_log_contents, new_input_dependency_contents = ret_new_contents

        ret_msgs = []
        if new_input_contents:
            msgs = generate_messages(request_id, transform_id, workload_id, work, msg_type='file',
                                     files=new_input_contents, relation_type='input')
            ret_msgs = ret_msgs + msgs
        if new_output_contents:
            msgs = generate_messages(request_id, transform_id, workload_id, work, msg_type='file',
                                     files=new_output_contents, relation_type='output')
            ret_msgs = ret_msgs + msgs

        # new_contents = new_input_contents + new_output_contents + new_log_contents + new_input_dependency_contents
        new_contents = new_input_contents + new_output_contents + new_log_contents

        if executors is None:
            logger.debug(log_prefix + "handle_update_processing: add %s new contents" % (len(new_contents)))
            core_processings.update_processing_contents(update_processing=None,
                                                        new_contents=new_contents,
                                                        new_input_dependency_contents=new_input_dependency_contents,
                                                        request_id=request_id,
                                                        # transform_id=transform_id,
                                                        use_bulk_update_mappings=use_bulk_update_mappings,
                                                        messages=ret_msgs)
        else:
            log_msg = "handle_update_processing thread: add %s new contents" % (len(new_contents))
            kwargs = {'update_processing': None,
                      'request_id': request_id,
                      'new_contents': new_contents,
                      'new_input_dependency_contents': new_input_dependency_contents,
                      'use_bulk_update_mappings': use_bulk_update_mappings,
                      'messages': ret_msgs}
            f = executors.submit(update_processing_contents_thread, logger, log_prefix, log_msg, kwargs)
            ret_futures.add(f)

    ret_msgs = []
    content_updates_missing_chunks = poll_missing_outputs(input_output_maps, contents_ext=contents_ext, max_updates_per_round=max_updates_per_round)
    for content_updates_missing_chunk in content_updates_missing_chunks:
        content_updates_missing, updated_contents_full_missing = content_updates_missing_chunk
        msgs = []
        if updated_contents_full_missing:
            msgs = generate_messages(request_id, transform_id, workload_id, work, msg_type='file',
                                     files=updated_contents_full, relation_type='output')
        if executors is None:
            logger.debug(log_prefix + "handle_update_processing: update %s missing contents" % (len(content_updates_missing)))
            core_processings.update_processing_contents(update_processing=None,
                                                        update_contents=content_updates_missing,
                                                        request_id=request_id,
                                                        # transform_id=transform_id,
                                                        # use_bulk_update_mappings=use_bulk_update_mappings,
                                                        use_bulk_update_mappings=False,
                                                        messages=msgs)
        else:
            log_msg = "handle_update_processing thread: update %s missing contents" % (len(content_updates_missing))
            kwargs = {'update_processing': None,
                      'request_id': request_id,
                      'update_contents': content_updates_missing,
                      'use_bulk_update_mappings': False,
                      'messages': msgs}
            f = executors.submit(update_processing_contents_thread, logger, log_prefix, log_msg, kwargs)
            ret_futures.add(f)

    if updated_contents_full:
        updated_contents_full_chunks = get_list_chunks(updated_contents_full, bulk_size=max_updates_per_round)
        for updated_contents_full_chunk in updated_contents_full_chunks:
            msgs = generate_messages(request_id, transform_id, workload_id, work, msg_type='file',
                                     files=updated_contents_full_chunk, relation_type='output')
            if executors is None:
                log_msg = "handle_update_processing: update %s messages" % (len(msgs))
                logger.debug(log_prefix + log_msg)
                core_processings.update_processing_contents(update_processing=None,
                                                            request_id=request_id,
                                                            messages=msgs)
            else:
                log_msg = "handle_update_processing thread: update %s messages" % (len(msgs))
                kwargs = {'update_processing': None,
                          'request_id': request_id,
                          'messages': msgs}
                f = executors.submit(update_processing_contents_thread, logger, log_prefix, log_msg, kwargs)
                ret_futures.add(f)

    if new_contents_ext:
        new_contents_ext_chunks = get_list_chunks(new_contents_ext, bulk_size=max_updates_per_round)
        for new_contents_ext_chunk in new_contents_ext_chunks:
            if executors is None:
                log_msg = "handle_update_processing: add %s ext contents" % (len(new_contents_ext_chunk))
                logger.debug(log_prefix + log_msg)
                core_processings.update_processing_contents(update_processing=None,
                                                            request_id=request_id,
                                                            new_contents_ext=new_contents_ext_chunk)
            else:
                log_msg = "handle_update_processing thread: add %s ext contents" % (len(new_contents_ext_chunk))
                kwargs = {'update_processing': None,
                          'request_id': request_id,
                          'new_contents_ext': new_contents_ext_chunk}
                f = executors.submit(update_processing_contents_thread, logger, log_prefix, log_msg, kwargs)
                ret_futures.add(f)

    if update_contents_ext:
        update_contents_ext_chunks = get_list_chunks(update_contents_ext, bulk_size=max_updates_per_round)
        for update_contents_ext_chunk in update_contents_ext_chunks:
            if executors is None:
                log_msg = "handle_update_processing: update %s ext contents" % (len(update_contents_ext_chunk))
                logger.debug(log_prefix + log_msg)
                core_processings.update_processing_contents(update_processing=None,
                                                            request_id=request_id,
                                                            update_contents_ext=update_contents_ext_chunk)
            else:
                log_msg = "handle_update_processing thread: update %s ext contents" % (len(update_contents_ext_chunk))
                kwargs = {'update_processing': None,
                          'request_id': request_id,
                          'update_contents_ext': update_contents_ext_chunk}
                f = executors.submit(update_processing_contents_thread, logger, log_prefix, log_msg, kwargs)
                ret_futures.add(f)

    if content_updates:
        content_updates_chunks = get_list_chunks(content_updates, bulk_size=max_updates_per_round)
        for content_updates_chunk in content_updates_chunks:
            if executors is None:
                log_msg = "handle_update_processing: update %s contents" % (len(content_updates_chunk))
                logger.debug(log_prefix + log_msg)
                core_processings.update_processing_contents(update_processing=None,
                                                            request_id=request_id,
                                                            # transform_id=transform_id,
                                                            use_bulk_update_mappings=use_bulk_update_mappings,
                                                            update_contents=content_updates_chunk)
            else:
                log_msg = "handle_update_processing thread: update %s contents" % (len(content_updates_chunk))
                kwargs = {'update_processing': None,
                          'request_id': request_id,
                          'use_bulk_update_mappings': use_bulk_update_mappings,
                          'update_contents': content_updates_chunk}
                f = executors.submit(update_processing_contents_thread, logger, log_prefix, log_msg, kwargs)
                ret_futures.add(f)

    if len(ret_futures) > 0:
        wait_futures_finish(ret_futures, "handle_update_processing", logger, log_prefix)

    # return process_status, new_contents, new_input_dependency_contents, ret_msgs, content_updates + content_updates_missing, parameters, new_contents_ext, update_contents_ext
    return process_status, [], [], ret_msgs, [], parameters, [], []


def get_transform_id_dependency_map(transform_id, logger=None, log_prefix=''):
    cache = get_redis_cache()
    transform_id_dependcy_map_key = "transform_id_dependcy_map_%s" % transform_id
    transform_id_dependcy_map = cache.get(transform_id_dependcy_map_key, default=[])
    return transform_id_dependcy_map


def set_transform_id_dependency_map(transform_id, transform_id_dependcy_map, logger=None, log_prefix=''):
    cache = get_redis_cache()
    transform_id_dependcy_map_key = "transform_id_dependcy_map_%s" % transform_id
    cache.set(transform_id_dependcy_map_key, transform_id_dependcy_map)


def get_updated_transforms_by_content_status(request_id=None, transform_id=None, logger=None, log_prefix=''):
    logger = get_logger(logger)
    logger.debug("get_updated_transforms_by_content_status starts")

    update_transforms = get_transform_id_dependency_map(transform_id=transform_id, logger=logger, log_prefix=log_prefix)
    if not update_transforms:
        update_transforms = core_catalog.get_updated_transforms_by_content_status(request_id=request_id,
                                                                                  transform_id=transform_id)
        set_transform_id_dependency_map(transform_id, update_transforms, logger=logger, log_prefix=log_prefix)
    logger.debug("get_updated_transforms_by_content_status ends")
    return update_transforms


def update_contents_thread(logger, log_prefix, log_msg, kwargs):
    try:
        logger = get_logger(logger)
        logger.debug(log_prefix + log_msg)
        core_catalog.update_contents(**kwargs)
        logger.debug(log_prefix + " end")
    except Exception as ex:
        logger.error(log_prefix + "update_contents_thread: %s" % str(ex))
    except:
        logger.error(traceback.format_exc())


def handle_trigger_processing(processing, agent_attributes, trigger_new_updates=False, max_updates_per_round=2000, executors=None, logger=None, log_prefix=''):
    logger = get_logger(logger)

    has_updates = False
    ret_msgs = []
    content_updates = []
    ret_update_transforms = []
    new_update_contents = []

    request_id = processing['request_id']
    transform_id = processing['transform_id']
    workload_id = processing['workload_id']
    processing_id = processing['processing_id']

    proc = processing['processing_metadata']['processing']
    work = proc.work
    work.set_agent_attributes(agent_attributes, processing)

    num_dependencies = None
    num_inputs = None
    default_input_dep_page_size = 500
    min_input_dep_page_size = 100
    max_dependencies = 5000
    try:
        num_inputs = work.num_inputs
        num_dependencies = work.num_dependencies
        if num_inputs is not None and num_dependencies is not None and num_dependencies > 0:
            input_dep_page_size = int(max_dependencies * num_inputs / num_dependencies)
            if input_dep_page_size < default_input_dep_page_size:
                default_input_dep_page_size = input_dep_page_size
                log_info = f"input_dep_page_size ({input_dep_page_size}) is smaller than default_input_dep_page_size ({default_input_dep_page_size}),"
                log_info = "update default_input_dep_page_size from input_dep_page_size"
                logger.info(log_info)
            if default_input_dep_page_size < min_input_dep_page_size:
                log_info = f"default_input_dep_page_size ({default_input_dep_page_size}) is smaller than min_input_dep_page_size ({min_input_dep_page_size}),"
                log_info = "update default_input_dep_page_size from min_input_dep_page_size"
                logger.info(log_info)
                default_input_dep_page_size = min_input_dep_page_size
    except Exception as ex:
        logger.warn(f"request_id ({request_id}) transform_id ({transform_id}) processing_id ({processing_id}) fails to get num_dependencies: {ex}")

    if (not work.use_dependency_to_release_jobs()) or workload_id is None:
        return processing['substatus'], [], [], {}, {}, {}, [], [], has_updates
    else:
        if trigger_new_updates:
            # delete information in the contents_update table, to invoke the trigger.
            # ret_update_transforms = core_catalog.delete_contents_update(request_id=request_id, transform_id=transform_id)
            # logger.debug(log_prefix + "delete_contents_update: %s" % str(ret_update_transforms))
            pass

        logger.debug(log_prefix + "sync contents_update to contents")
        core_catalog.set_fetching_contents_update(request_id=request_id, transform_id=transform_id, fetch=True)
        contents_update_list = core_catalog.get_contents_update(request_id=request_id, transform_id=transform_id, fetch=True)
        new_contents_update_list = []
        # contents_id_list = []
        for con in contents_update_list:
            has_updates = True
            if not work.es or con['substatus'] in [ContentStatus.Available]:
                con_dict = {'content_id': con['content_id'],
                            'request_id': con['request_id'],
                            'substatus': con['substatus'],
                            'status': con['substatus']}
                if 'content_metadata' in con and con['content_metadata']:
                    con_dict['content_metadata'] = con['content_metadata']
                new_contents_update_list.append(con_dict)
                # contents_id_list.append(con['content_id'])
        new_contents_update_list_chunks = [new_contents_update_list[i:i + max_updates_per_round] for i in range(0, len(new_contents_update_list), max_updates_per_round)]
        ret_futures = set()
        for chunk in new_contents_update_list_chunks:
            has_updates = True
            if executors is None:
                logger.debug(log_prefix + "new_contents_update chunk[:3](total: %s): %s" % (len(chunk), str(chunk[:3])))
                # core_catalog.update_contents(chunk, request_id=request_id, transform_id=transform_id, use_bulk_update_mappings=False)
                core_catalog.update_contents(chunk, request_id=request_id, transform_id=transform_id, use_bulk_update_mappings=True)
            else:
                log_msg = "new_contents_update thread chunk[:3](total: %s): %s" % (len(chunk), str(chunk[:3]))
                kwargs = {'parameters': chunk,
                          'request_id': request_id,
                          'transform_id': transform_id,
                          'use_bulk_update_mappings': True}
                f = executors.submit(update_contents_thread, logger, log_prefix, log_msg, kwargs)
                ret_futures.add(f)
        if len(ret_futures) > 0:
            wait_futures_finish(ret_futures, "new_contents_update", logger, log_prefix)

        # core_catalog.delete_contents_update(contents=contents_id_list)
        core_catalog.delete_contents_update(request_id=request_id, transform_id=transform_id, fetch=True)
        logger.debug(log_prefix + "sync contents_update to contents done")

        """
        logger.debug(log_prefix + "update_contents_from_others_by_dep_id")
        # core_catalog.update_contents_from_others_by_dep_id(request_id=request_id, transform_id=transform_id)
        to_triggered_contents = core_catalog.get_update_contents_from_others_by_dep_id(request_id=request_id, transform_id=transform_id)
        to_triggered_contents_chunks = [to_triggered_contents[i:i + max_updates_per_round] for i in range(0, len(to_triggered_contents), max_updates_per_round)]

        ret_futures = set()
        for chunk in to_triggered_contents_chunks:
            has_updates = True
            if executors is None:
                logger.debug(log_prefix + "update_contents_from_others_by_dep_id chunk[:3](total: %s): %s" % (len(chunk), str(chunk[:3])))
                core_catalog.update_contents(chunk, request_id=request_id, transform_id=transform_id, use_bulk_update_mappings=False)
            else:
                log_msg = "update_contents_from_others_by_dep_id thread chunk[:3](total: %s): %s" % (len(chunk), str(chunk[:3]))
                kwargs = {'parameters': chunk,
                          'request_id': request_id,
                          'transform_id': transform_id,
                          'use_bulk_update_mappings': False}
                f = executors.submit(update_contents_thread, logger, log_prefix, log_msg, kwargs)
                ret_futures.add(f)
        if len(ret_futures) > 0:
            wait_futures_finish(ret_futures, "update_contents_from_others_by_dep_id", logger, log_prefix)

        logger.debug(log_prefix + "update_contents_from_others_by_dep_id done")
        """

        logger.debug(log_prefix + "update_contents_from_others_by_dep_id_pages")
        status_not_to_check = [ContentStatus.Available, ContentStatus.FakeAvailable,
                               ContentStatus.FinalFailed, ContentStatus.Missing]
        core_catalog.update_contents_from_others_by_dep_id_pages(request_id=request_id, transform_id=transform_id,
                                                                 page_size=1000, status_not_to_check=status_not_to_check,
                                                                 logger=logger, log_prefix=log_prefix)
        logger.debug(log_prefix + "update_contents_from_others_by_dep_id_pages done")

        terminated_processing = False
        terminated_status = [ProcessingStatus.Finished, ProcessingStatus.Failed, ProcessingStatus.SubFinished,
                             ProcessingStatus.Terminating, ProcessingStatus.Cancelled]
        if processing['status'] in terminated_status or processing['substatus'] in terminated_status:
            terminated_processing = True

        logger.debug(log_prefix + "update_input_contents_by_dependency_pages")
        status_not_to_check = [ContentStatus.Available, ContentStatus.FakeAvailable,
                               ContentStatus.FinalFailed, ContentStatus.Missing]
        core_catalog.update_input_contents_by_dependency_pages(request_id=request_id, transform_id=transform_id,
                                                               page_size=default_input_dep_page_size,
                                                               terminated=terminated_processing,
                                                               batch_size=1000, status_not_to_check=status_not_to_check,
                                                               logger=logger, log_prefix=log_prefix)
        logger.debug(log_prefix + "update_input_contents_by_dependency_pages done")

        with_deps = False
        input_output_maps = get_input_output_maps(transform_id, work, with_deps=with_deps)
        logger.debug(log_prefix + "input_output_maps.keys[:2]: %s" % str(list(input_output_maps.keys())[:2]))

        updated_contents_ret_chunks = get_updated_contents_by_input_output_maps(input_output_maps=input_output_maps,
                                                                                terminated=terminated_processing,
                                                                                max_updates_per_round=max_updates_per_round,
                                                                                with_deps=with_deps,
                                                                                logger=logger,
                                                                                log_prefix=log_prefix)

        ret_futures = set()
        for updated_contents_ret in updated_contents_ret_chunks:
            updated_contents, updated_contents_full_input, updated_contents_full_output, updated_contents_full_input_deps, new_update_contents = updated_contents_ret

            if updated_contents_full_input:
                # if the content is updated by receiver, here is the place to broadcast the messages
                msgs = generate_messages(request_id, transform_id, workload_id, work, msg_type='file',
                                         files=updated_contents_full_input, relation_type='input')
                ret_msgs = ret_msgs + msgs
            if updated_contents_full_output:
                # if the content is updated by receiver, here is the place to broadcast the messages
                msgs = generate_messages(request_id, transform_id, workload_id, work, msg_type='file',
                                         files=updated_contents_full_output, relation_type='output')
                ret_msgs = ret_msgs + msgs

            # content_updates = content_updates + updated_contents

            if updated_contents or new_update_contents:
                has_updates = True

            if executors is None:
                logger.debug(log_prefix + "handle_trigger_processing: updated_contents[:3] (total: %s): %s" % (len(updated_contents), updated_contents[:3]))
                core_processings.update_processing_contents(update_processing=None,
                                                            update_contents=updated_contents,
                                                            # new_update_contents=new_update_contents,
                                                            messages=ret_msgs,
                                                            request_id=request_id,
                                                            # transform_id=transform_id,
                                                            use_bulk_update_mappings=False)
            else:
                log_msg = "handle_trigger_processing thread: updated_contents[:3] (total: %s): %s" % (len(updated_contents), updated_contents[:3])
                kwargs = {'update_processing': None,
                          'request_id': request_id,
                          'update_contents': updated_contents,
                          'messages': ret_msgs,
                          'use_bulk_update_mappings': False}
                f = executors.submit(update_processing_contents_thread, logger, log_prefix, log_msg, kwargs)
                ret_futures.add(f)

            updated_contents = []
            new_update_contents = []
            ret_msgs = []
        if len(ret_futures) > 0:
            wait_futures_finish(ret_futures, "handle_trigger_processing", logger, log_prefix)

        if has_updates:
            ret_update_transforms = get_updated_transforms_by_content_status(request_id=request_id,
                                                                             transform_id=transform_id,
                                                                             logger=logger,
                                                                             log_prefix=log_prefix)
    # update_dep_contents_status_name = {}
    # update_dep_contents_status = {}
    # for content in new_update_contents:
    #     if content['substatus'] not in update_dep_contents_status_name:
    #         update_dep_contents_status_name[content['substatus'].name] = content['substatus']
    #         update_dep_contents_status[content['substatus'].name] = []
    #     update_dep_contents_status[content['substatus'].name].append(content['content_id'])

    # return processing['substatus'], content_updates, ret_msgs, {}, {}, {}, new_update_contents, ret_update_transforms
    # return processing['substatus'], content_updates, ret_msgs, {}, update_dep_contents_status_name, update_dep_contents_status, [], ret_update_transforms
    # return processing['substatus'], content_updates, ret_msgs, {}, {}, {}, [], ret_update_transforms
    return processing['substatus'], content_updates, ret_msgs, {}, {}, {}, new_update_contents, ret_update_transforms, has_updates


def get_content_status_from_panda_msg_status(status):
    status_map = {'starting': ContentStatus.New,
                  'activated': ContentStatus.Activated,
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


workload_id_lock = threading.Lock()


def get_workload_id_transform_id_map(workload_id, logger=None, log_prefix=''):
    cache = get_redis_cache()
    workload_id_transform_id_map_key = "all_worloadid2transformid_map"
    workload_id_transform_id_map = cache.get(workload_id_transform_id_map_key, default={})

    workload_id_transform_id_map_notexist_key = "all_worloadid2transformid_map_notexist"
    workload_id_transform_id_map_notexist = cache.get(workload_id_transform_id_map_notexist_key, default={})

    if type(workload_id_transform_id_map_notexist) in (list, tuple):
        workload_id_transform_id_map_notexist = {}

    workload_id_str = str(workload_id)
    if workload_id_str in workload_id_transform_id_map:
        return workload_id_transform_id_map[workload_id_str]

    if workload_id_str in workload_id_transform_id_map_notexist and workload_id_transform_id_map_notexist[workload_id_str] + 600 < time.time():
        return None

    # lock area
    workload_id_lock.acquire()

    workload_id_transform_id_map = cache.get(workload_id_transform_id_map_key, default={})
    workload_id_transform_id_map_notexist = cache.get(workload_id_transform_id_map_notexist_key, default={})

    if type(workload_id_transform_id_map_notexist) in (list, tuple):
        workload_id_transform_id_map_notexist = {}

    request_ids = []
    if not workload_id_transform_id_map or workload_id_str not in workload_id_transform_id_map or len(workload_id_transform_id_map[workload_id_str]) < 5:
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
                workload_id_transform_id_map[str(proc['workload_id'])] = (proc['request_id'],
                                                                          proc['transform_id'],
                                                                          proc['processing_id'],
                                                                          proc['status'].value,
                                                                          proc['substatus'].value)
                if proc['request_id'] not in request_ids:
                    request_ids.append(proc['request_id'])

        cache.set(workload_id_transform_id_map_key, workload_id_transform_id_map)

        for key in workload_id_transform_id_map:
            if key in workload_id_transform_id_map_notexist:
                del workload_id_transform_id_map_notexist[key]

    # renew the collection to transform map
    if request_ids:
        get_collection_id_transform_id_map(coll_id=None, request_id=request_ids[0], request_ids=request_ids)

    keys = list(workload_id_transform_id_map_notexist.keys())
    for key in keys:
        if workload_id_transform_id_map_notexist[key] + 7200 < time.time():
            del workload_id_transform_id_map_notexist[key]

    cache.set(workload_id_transform_id_map_notexist_key, workload_id_transform_id_map_notexist)
    # for tasks running in some other instances
    if workload_id_str not in workload_id_transform_id_map:
        if workload_id_str not in workload_id_transform_id_map_notexist:
            workload_id_transform_id_map_notexist[workload_id_str] = time.time()
            cache.set(workload_id_transform_id_map_notexist_key, workload_id_transform_id_map_notexist)

        workload_id_lock.release()
        return None
    else:
        workload_id_lock.release()

    return workload_id_transform_id_map[workload_id_str]


content_id_lock = threading.Lock()


def get_input_name_content_id_map(request_id, workload_id, transform_id):
    cache = get_redis_cache()
    input_name_content_id_map_key = "transform_input_contentid_map_%s" % transform_id
    input_name_content_id_map = cache.get(input_name_content_id_map_key, default={})

    if not input_name_content_id_map:
        content_id_lock.acquire()

        contents = core_catalog.get_contents_by_request_transform(request_id=request_id, transform_id=transform_id)
        input_name_content_id_map = {}
        for content in contents:
            if content['content_relation_type'] == ContentRelationType.Output:
                if content['name'] not in input_name_content_id_map:
                    input_name_content_id_map[content['name']] = []
                input_name_content_id_map[content['name']].append(content['content_id'])
                if content['path']:
                    if content['path'] not in input_name_content_id_map:
                        input_name_content_id_map[content['path']] = []
                    input_name_content_id_map[content['path']].append(content['content_id'])

        cache.set(input_name_content_id_map_key, input_name_content_id_map)

        content_id_lock.release()
    return input_name_content_id_map


def get_jobid_content_id_map(request_id, workload_id, transform_id, job_id, inputs):
    cache = get_redis_cache()
    jobid_content_id_map_key = "transform_jobid_contentid_map_%s" % transform_id
    jobid_content_id_map = cache.get(jobid_content_id_map_key, default={})

    to_update_jobid = False
    job_id = str(job_id)
    if not jobid_content_id_map or job_id not in jobid_content_id_map:
        to_update_jobid = True
        input_name_content_id_map = get_input_name_content_id_map(request_id, workload_id, transform_id)
        for ip in inputs:
            if ':' in ip:
                pos = ip.find(":")
                ip = ip[pos + 1:]
            if ip in input_name_content_id_map:
                content_ids = input_name_content_id_map[ip]
                jobid_content_id_map[job_id] = content_ids
                break

        cache.set(jobid_content_id_map_key, jobid_content_id_map)
    return jobid_content_id_map, to_update_jobid


def get_content_id_from_job_id(request_id, workload_id, transform_id, job_id, inputs):
    jobid_content_id_map, to_update_jobid = get_jobid_content_id_map(request_id, workload_id, transform_id, job_id, inputs)

    if str(job_id) in jobid_content_id_map:
        content_ids = jobid_content_id_map[str(job_id)]
    else:
        content_ids = None
    return content_ids, to_update_jobid


pending_lock = threading.Lock()


def whether_to_process_pending_workload_id(workload_id, logger=None, log_prefix=''):
    cache = get_redis_cache()
    processed_pending_workload_id_map_key = "processed_pending_workload_id_map"
    processed_pending_workload_id_map = cache.get(processed_pending_workload_id_map_key, default={})
    processed_pending_workload_id_map_time_key = "processed_pending_workload_id_map_time"
    processed_pending_workload_id_map_time = cache.get(processed_pending_workload_id_map_time_key, default=None)

    workload_id = str(workload_id)
    if workload_id in processed_pending_workload_id_map:
        return False

    # lock area
    pending_lock.acquire()
    processed_pending_workload_id_map = cache.get(processed_pending_workload_id_map_key, default={})

    processed_pending_workload_id_map[workload_id] = time.time()
    if processed_pending_workload_id_map_time is None or processed_pending_workload_id_map_time + 86400 < time.time():
        cache.set(processed_pending_workload_id_map_time_key, int(time.time()), expire_seconds=86400)

        keys = list(processed_pending_workload_id_map.keys())
        for workload_id in keys:
            if processed_pending_workload_id_map[workload_id] + 86400 < time.time():
                del processed_pending_workload_id_map[workload_id]

    cache.set(processed_pending_workload_id_map_key, processed_pending_workload_id_map, expire_seconds=86400)
    pending_lock.release()
    return True


update_processing_lock = threading.Lock()


def whether_to_update_processing(processing_id, interval=300):
    cache = get_redis_cache()
    ret = False

    update_processing_lock.acquire()
    update_processing_map_key = "update_processing_map"
    update_processing_map = cache.get(update_processing_map_key, default={})

    processing_id_str = str(processing_id)
    if processing_id_str not in update_processing_map or update_processing_map[processing_id_str] + interval < time.time():
        update_processing_map[processing_id_str] = time.time()
        ret = True

    keys = list(update_processing_map.keys())
    for key in keys:
        if update_processing_map[key] + 86400 < time.time():
            del update_processing_map[key]

    cache.set(update_processing_map_key, update_processing_map, expire_seconds=86400)
    update_processing_lock.release()
    return ret


def handle_messages_processing(messages, logger=None, log_prefix='', update_processing_interval=300):
    logger = get_logger(logger)
    if not log_prefix:
        log_prefix = "<Message>"

    update_processings = []
    update_processings_by_job = []
    terminated_processings = []
    update_contents = []

    for ori_msg in messages:
        if type(ori_msg) in [dict]:
            msg = ori_msg
        else:
            msg = json.loads(ori_msg)
        if 'taskid' not in msg or not msg['taskid']:
            continue

        if msg['msg_type'] in ['task_status']:
            workload_id = msg['taskid']
            status = msg['status']
            if status in ['pending1']:   # 'prepared'
                logger.debug(log_prefix + "Received message: %s" % str(ori_msg))

                ret_req_tf_pr_id = get_workload_id_transform_id_map(workload_id, logger=logger, log_prefix=log_prefix)
                if not ret_req_tf_pr_id:
                    # request is submitted by some other instances
                    logger.debug(log_prefix + "No matched workload_id, discard message: %s" % str(ori_msg))
                    continue

                logger.debug(log_prefix + "(request_id, transform_id, processing_id, status, substatus): %s" % str(ret_req_tf_pr_id))
                req_id, tf_id, processing_id, r_status, r_substatus = ret_req_tf_pr_id
                if whether_to_process_pending_workload_id(workload_id, logger=logger, log_prefix=log_prefix):
                    # new_processings.append((req_id, tf_id, processing_id, workload_id, status))
                    if processing_id not in update_processings:
                        update_processings.append(processing_id)
                        logger.debug(log_prefix + "Add to update processing: %s" % str(processing_id))
                else:
                    logger.debug(log_prefix + "Processing %s is already processed, not add it to update processing" % (str(processing_id)))
            elif status in ['finished', 'done']:
                logger.debug(log_prefix + "Received message: %s" % str(ori_msg))

                ret_req_tf_pr_id = get_workload_id_transform_id_map(workload_id, logger=logger, log_prefix=log_prefix)
                if not ret_req_tf_pr_id:
                    # request is submitted by some other instances
                    logger.debug(log_prefix + "No matched workload_id, discard message: %s" % str(ori_msg))
                    continue

                logger.debug(log_prefix + "(request_id, transform_id, processing_id, status, substatus): %s" % str(ret_req_tf_pr_id))
                req_id, tf_id, processing_id, r_status, r_substatus = ret_req_tf_pr_id
                # update_processings.append((processing_id, status))
                if processing_id not in update_processings:
                    terminated_processings.append(processing_id)
                    logger.debug(log_prefix + "Add to terminated processing: %s" % str(processing_id))

        if msg['msg_type'] in ['job_status']:
            workload_id = msg['taskid']
            job_id = msg['jobid']
            status = msg['status']
            inputs = msg['inputs']
            # if inputs and status in ['finished']:
            # add activated
            if inputs and status in ['finished', 'activated']:
                logger.debug(log_prefix + "Received message: %s" % str(ori_msg))

                ret_req_tf_pr_id = get_workload_id_transform_id_map(workload_id, logger=logger, log_prefix=log_prefix)
                if not ret_req_tf_pr_id:
                    # request is submitted by some other instances
                    logger.debug(log_prefix + "No matched workload_id, discard message: %s" % str(ori_msg))
                    continue

                logger.debug(log_prefix + "(request_id, transform_id, processing_id, status, substatus): %s" % str(ret_req_tf_pr_id))

                req_id, tf_id, processing_id, r_status, r_substatus = ret_req_tf_pr_id
                content_ids, to_update_jobid = get_content_id_from_job_id(req_id, workload_id, tf_id, job_id, inputs)
                if content_ids:
                    for content_id in content_ids:
                        if to_update_jobid:
                            u_content = {'content_id': content_id,
                                         'request_id': req_id,
                                         'transform_id': tf_id,
                                         'workload_id': workload_id,
                                         # 'status': get_content_status_from_panda_msg_status(status),
                                         'substatus': get_content_status_from_panda_msg_status(status),
                                         'content_metadata': {'panda_id': job_id}}
                        else:
                            u_content = {'content_id': content_id,
                                         'request_id': req_id,
                                         'transform_id': tf_id,
                                         'workload_id': workload_id,
                                         'substatus': get_content_status_from_panda_msg_status(status)}
                            #             # 'status': get_content_status_from_panda_msg_status(status)}

                        update_contents.append(u_content)
                        # if processing_id not in update_processings:
                        # if processing_id not in update_processings and whether_to_update_processing(processing_id, update_processing_interval):
                        if processing_id not in update_processings_by_job:
                            update_processings_by_job.append(processing_id)
                            logger.debug(log_prefix + "Add to update processing by job: %s" % str(processing_id))

    return update_processings, update_processings_by_job, terminated_processings, update_contents, []


def sync_collection_status(request_id, transform_id, workload_id, work, input_output_maps=None,
                           close_collection=False, force_close_collection=False, abort=False, terminate=False):
    if input_output_maps is None:
        input_output_maps = get_input_output_maps(transform_id, work, with_deps=False)

    all_updates_flushed = True
    coll_status = {}
    messages = []
    for map_id in input_output_maps:
        inputs = input_output_maps[map_id]['inputs'] if 'inputs' in input_output_maps[map_id] else []
        # inputs_dependency = input_output_maps[map_id]['inputs_dependency'] if 'inputs_dependency' in input_output_maps[map_id] else []
        outputs = input_output_maps[map_id]['outputs'] if 'outputs' in input_output_maps[map_id] else []
        logs = input_output_maps[map_id]['logs'] if 'logs' in input_output_maps[map_id] else []

        for content in inputs + outputs + logs:
            if content['coll_id'] not in coll_status:
                coll_status[content['coll_id']] = {'total_files': 0, 'processed_files': 0, 'processing_files': 0, 'bytes': 0,
                                                   'new_files': 0, 'failed_files': 0, 'missing_files': 0,
                                                   'ext_files': 0, 'processed_ext_files': 0, 'failed_ext_files': 0,
                                                   'missing_ext_files': 0}
            coll_status[content['coll_id']]['total_files'] += 1

            if content['status'] in [ContentStatus.Available, ContentStatus.Mapped,
                                     ContentStatus.Available.value, ContentStatus.Mapped.value,
                                     ContentStatus.FakeAvailable, ContentStatus.FakeAvailable.value]:
                coll_status[content['coll_id']]['processed_files'] += 1
                coll_status[content['coll_id']]['bytes'] += content['bytes']
            elif content['status'] in [ContentStatus.New]:
                coll_status[content['coll_id']]['new_files'] += 1
            elif content['status'] in [ContentStatus.Failed, ContentStatus.FinalFailed,
                                       ContentStatus.SubAvailable, ContentStatus.FinalSubAvailable]:
                coll_status[content['coll_id']]['failed_files'] += 1
            elif content['status'] in [ContentStatus.Lost, ContentStatus.Deleted, ContentStatus.Missing]:
                coll_status[content['coll_id']]['missing_files'] += 1
            else:
                coll_status[content['coll_id']]['processing_files'] += 1

            if content['status'] != content['substatus']:
                all_updates_flushed = False

    all_ext_updated = True
    if work.require_ext_contents():
        all_ext_updated = False
        contents_ext = core_catalog.get_contents_ext(request_id=request_id, transform_id=transform_id)
        for content in contents_ext:
            coll_status[content['coll_id']]['ext_files'] += 1

            if content['status'] in [ContentStatus.Available, ContentStatus.Mapped,
                                     ContentStatus.Available.value, ContentStatus.Mapped.value,
                                     ContentStatus.FakeAvailable, ContentStatus.FakeAvailable.value]:
                coll_status[content['coll_id']]['processed_ext_files'] += 1
            # elif content['status'] in [ContentStatus.Failed, ContentStatus.FinalFailed]:
            elif content['status'] in [ContentStatus.Failed, ContentStatus.FinalFailed,
                                       ContentStatus.SubAvailable, ContentStatus.FinalSubAvailable]:
                coll_status[content['coll_id']]['failed_ext_files'] += 1
            elif content['status'] in [ContentStatus.Lost, ContentStatus.Deleted, ContentStatus.Missing]:
                coll_status[content['coll_id']]['missing_ext_files'] += 1

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
            coll.new_files = coll_status[coll.coll_id]['new_files']
            coll.failed_files = coll_status[coll.coll_id]['failed_files']
            coll.missing_files = coll_status[coll.coll_id]['missing_files']
            coll.ext_files = coll_status[coll.coll_id]['ext_files']
            coll.processed_ext_files = coll_status[coll.coll_id]['processed_ext_files']
            coll.failed_ext_files = coll_status[coll.coll_id]['failed_ext_files']
            coll.missing_ext_files = coll_status[coll.coll_id]['missing_ext_files']
        else:
            coll.total_files = 0
            coll.processed_files = 0
            coll.processing_files = 0
            coll.new_files = 0
            coll.failed_files = 0
            coll.missing_files = 0
            coll.ext_files = 0
            coll.processed_ext_files = 0
            coll.failed_ext_files = 0
            coll.missing_ext_files = 0

        u_coll = {'coll_id': coll.coll_id,
                  'total_files': coll.total_files,
                  'processed_files': coll.processed_files,
                  'processing_files': coll.processing_files,
                  'new_files': coll.new_files,
                  'failed_files': coll.failed_files,
                  'missing_files': coll.missing_files,
                  'bytes': coll.bytes,
                  'ext_files': coll.ext_files,
                  'processed_ext_files': coll.processed_ext_files,
                  'failed_ext_files': coll.failed_ext_files,
                  'missing_ext_files': coll.missing_ext_files}

        if (not work.generating_new_inputs()) and (coll in input_collections and (workload_id is not None)):
            if coll.total_files == coll.processed_files + coll.failed_files + coll.missing_files:
                coll_db = core_catalog.get_collection(coll_id=coll.coll_id)
                coll.status = coll_db['status']
                if coll.status is not None and coll.status != CollectionStatus.Closed:
                    u_coll['status'] = CollectionStatus.Closed
                    u_coll['substatus'] = CollectionStatus.Closed
                    coll.status = CollectionStatus.Closed
                    coll.substatus = CollectionStatus.Closed

                    msgs = generate_messages(request_id, transform_id, workload_id, work, msg_type='collection', files=[coll], relation_type='input')
                    messages += msgs

        if terminate:
            all_files_monitored = False
            if coll.total_files == coll.processed_files + coll.failed_files + coll.missing_files:
                all_files_monitored = True

            if abort:
                u_coll['status'] = CollectionStatus.Closed
                u_coll['substatus'] = CollectionStatus.Closed
                coll.status = CollectionStatus.Closed
                coll.substatus = CollectionStatus.Closed
            elif coll in output_collections:
                if (not work.require_ext_contents() or (work.require_ext_contents()
                    and coll.processed_files == coll.processed_ext_files and coll.failed_files <= coll.failed_ext_files)):     # noqa E129, W503
                    all_ext_updated = True
                if (force_close_collection or (close_collection and all_updates_flushed and all_ext_updated and all_files_monitored)
                   or coll.status == CollectionStatus.Closed):        # noqa W503
                    u_coll['status'] = CollectionStatus.Closed
                    u_coll['substatus'] = CollectionStatus.Closed
                    coll.status = CollectionStatus.Closed
                    coll.substatus = CollectionStatus.Closed
            elif force_close_collection or (close_collection and all_updates_flushed and all_files_monitored) or coll.status == CollectionStatus.Closed:
                u_coll['status'] = CollectionStatus.Closed
                u_coll['substatus'] = CollectionStatus.Closed
                coll.status = CollectionStatus.Closed
                coll.substatus = CollectionStatus.Closed

        update_collections.append(u_coll)
    return update_collections, all_updates_flushed, messages


def sync_work_status(request_id, transform_id, workload_id, work, substatus=None, log_prefix=""):
    logger = get_logger()

    input_collections = work.get_input_collections()
    output_collections = work.get_output_collections()
    log_collections = work.get_log_collections()

    is_all_collections_closed = True
    is_all_files_processed = True
    is_all_files_failed = True
    has_files = False
    for coll in input_collections + output_collections + log_collections:
        if coll.status != CollectionStatus.Closed:
            is_all_collections_closed = False
    for coll in output_collections:
        if coll.total_files > 0:
            has_files = True
        if coll.total_files != coll.processed_files:
            is_all_files_processed = False
        if coll.processed_files > 0 or coll.total_files == coll.processed_files:
            is_all_files_failed = False

    if is_all_collections_closed:
        logger.debug(log_prefix + "has_files: %s, is_all_files_processed: %s, is_all_files_failed: %s, substatus: %s" % (has_files,
                                                                                                                         is_all_files_processed,
                                                                                                                         is_all_files_failed,
                                                                                                                         substatus))
        if has_files:
            if is_all_files_processed:
                work.status = WorkStatus.Finished
            elif is_all_files_failed:
                work.status = WorkStatus.Failed
            else:
                work.status = WorkStatus.SubFinished
        else:
            if substatus:
                work.status = get_work_status_from_transform_processing_status(substatus)
            else:
                work.status = WorkStatus.Failed
    elif substatus and substatus in [ProcessingStatus.Broken]:
        work.status = get_work_status_from_transform_processing_status(substatus)
    logger.debug(log_prefix + "work status: %s, substatus: %s" % (str(work.status), substatus))


def sync_processing(processing, agent_attributes, terminate=False, abort=False, logger=None, log_prefix=""):
    logger = get_logger()

    terminated_status = [ProcessingStatus.Finished, ProcessingStatus.Failed, ProcessingStatus.SubFinished,
                         ProcessingStatus.Terminating, ProcessingStatus.Cancelled]

    request_id = processing['request_id']
    transform_id = processing['transform_id']
    workload_id = processing['workload_id']

    proc = processing['processing_metadata']['processing']
    work = proc.work
    work.set_agent_attributes(agent_attributes, processing)

    messages = []
    input_output_maps = get_input_output_maps(transform_id, work, with_deps=False)
    if processing['substatus'] in terminated_status or processing['substatus'] in terminated_status:
        terminate = True
    update_collections, all_updates_flushed, msgs = sync_collection_status(request_id, transform_id, workload_id, work,
                                                                           input_output_maps=input_output_maps,
                                                                           close_collection=True, abort=abort, terminate=terminate)

    messages += msgs

    sync_work_status(request_id, transform_id, workload_id, work, processing['substatus'], log_prefix)
    logger.info(log_prefix + "sync_processing: work status: %s" % work.get_status())
    if terminate and work.is_terminated():
        msgs = generate_messages(request_id, transform_id, workload_id, work, msg_type='work')
        messages += msgs
        if work.is_finished():
            processing['status'] = ProcessingStatus.Finished
            # processing['status'] = processing['substatus']
        elif work.is_subfinished():
            processing['status'] = ProcessingStatus.SubFinished
        elif work.is_failed():
            processing['status'] = ProcessingStatus.Failed
        else:
            processing['status'] = ProcessingStatus.SubFinished

        if work.require_ext_contents():
            contents_ext = core_catalog.get_contents_ext(request_id=request_id, transform_id=transform_id)
            msgs = generate_messages(request_id, transform_id, workload_id, work, msg_type='content_ext', files=contents_ext,
                                     relation_type='output', input_output_maps=input_output_maps)
            messages += msgs

        if processing['status'] == ProcessingStatus.Terminating and is_process_terminated(processing['substatus']):
            processing['status'] = processing['substatus']

    return processing, update_collections, messages


def handle_abort_processing(processing, agent_attributes, logger=None, log_prefix=''):
    logger = get_logger(logger)

    # request_id = processing['request_id']
    # transform_id = processing['transform_id']
    # workload_id = processing['workload_id']

    proc = processing['processing_metadata']['processing']
    work = proc.work
    work.set_agent_attributes(agent_attributes, processing)

    work.abort_processing(processing, log_prefix=log_prefix)

    # input_collections = work.get_input_collections()
    # output_collections = work.get_output_collections()
    # log_collections = work.get_log_collections()

    # input_output_maps = get_input_output_maps(transform_id, work)
    # update_collections, all_updates_flushed = sync_collection_status(request_id, transform_id, workload_id, work,
    #                                                                  input_output_maps=None, close_collection=True,
    #                                                                  force_close_collection=True)

    # for coll in input_collections + output_collections + log_collections:
    #     coll.status = CollectionStatus.Closed
    #     coll.substatus = CollectionStatus.Closed
    processing, update_collections, messages = sync_processing(processing, agent_attributes, terminate=True, abort=True, logger=logger, log_prefix=log_prefix)
    update_contents = []

    # processing['status'] = ProcessingStatus.Cancelled
    return processing, update_collections, update_contents, messages


def reactive_contents(request_id, transform_id, workload_id, work, input_output_maps):
    updated_contents = []
    contents = core_catalog.get_contents_by_request_transform(request_id=request_id, transform_id=transform_id)
    for content in contents:
        if content['status'] not in [ContentStatus.Available, ContentStatus.Mapped,
                                     ContentStatus.Available.value, ContentStatus.Mapped.value,
                                     ContentStatus.FakeAvailable, ContentStatus.FakeAvailable.value]:
            u_content = {'content_id': content['content_id'],
                         'request_id': content['request_id'],
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

    input_output_maps = get_input_output_maps(transform_id, work, with_deps=False)
    update_contents = reactive_contents(request_id, transform_id, workload_id, work, input_output_maps)

    processing['status'] = ProcessingStatus.Running
    return processing, update_collections, update_contents
