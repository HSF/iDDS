#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019

import copy
import traceback
try:
    # python 3
    from queue import Queue
except ImportError:
    # Python 2
    from Queue import Queue

from idds.common.constants import (Sections, RequestStatus, RequestLocking, RequestType,
                                   TransformStatus, CollectionRelationType,
                                   CollectionType, CollectionStatus, TransformType)
from idds.common.exceptions import AgentPluginError
from idds.common.utils import setup_logging, convert_request_type_to_transform_type
from idds.core import requests as core_requests, transforms as core_transforms
from idds.agents.common.baseagent import BaseAgent

setup_logging(__name__)


class Clerk(BaseAgent):
    """
    Clerk works to process requests and converts requests to transforms.
    """

    def __init__(self, num_threads=1, poll_time_period=10, retrieve_bulk_size=None, **kwargs):
        super(Clerk, self).__init__(num_threads=num_threads, **kwargs)
        self.poll_time_period = int(poll_time_period)
        self.retrieve_bulk_size = int(retrieve_bulk_size)
        self.config_section = Sections.Clerk

        self.new_task_queue = Queue()
        self.new_output_queue = Queue()
        self.monitor_task_queue = Queue()
        self.monitor_output_queue = Queue()

    def get_new_requests(self):
        """
        Get new requests to process
        """
        # req_status = [RequestStatus.TransformingOpen]
        # reqs_open = core_requests.get_requests_by_status_type(status=req_status, time_period=3600)
        # self.logger.info("Main thread get %s TransformingOpen requests to process" % len(reqs_open))

        req_status = [RequestStatus.New, RequestStatus.Extend]
        reqs_new = core_requests.get_requests_by_status_type(status=req_status, locking=True,
                                                             bulk_size=self.retrieve_bulk_size)

        self.logger.debug("Main thread get %s [New+Extend] requests to process" % len(reqs_new))
        if reqs_new:
            self.logger.info("Main thread get %s [New+Extend] requests to process" % len(reqs_new))

        return reqs_new

    def get_transform_with_input_collection(self, transform_type, transform_tag, coll_scope, coll_name, request_metadata):
        transforms = core_transforms.get_transforms_with_input_collection(transform_type=transform_type,
                                                                          transform_tag=transform_tag,
                                                                          coll_scope=coll_scope,
                                                                          coll_name=coll_name)
        if not transforms:
            return None

        if transform_type in [TransformType.StageIn, TransformType.StageIn.value]:
            for transform in transforms:
                transform_metadata = transform['transform_metadata']
                if ((('workload_id' in transform_metadata and 'workload_id' in request_metadata               # noqa: W503
                    and transform_metadata['workload_id'] == request_metadata['workload_id'])                 # noqa: W503
                    or ('workload_id' not in transform_metadata and 'workload_id' not in request_metadata))   # noqa: W503
                    and (('rule_id' in transform_metadata and 'rule_id' in request_metadata                   # noqa: W503
                    and transform_metadata['rule_id'] == request_metadata['rule_id'])                         # noqa: W503
                    or ('rule_id' not in transform_metadata and 'rule_id' not in request_metadata))):         # noqa: W503
                    return transform
        else:
            return None

    def get_collections(self, scope, name, req=None):
        if (req and req['request_metadata'] and 'is_pseudo_input' in req['request_metadata']                  # noqa: W503
            and req['request_metadata']['is_pseudo_input'] == True):                                          # noqa: W503
            collection = {'scope': scope, 'name': name, 'total_files': 1, 'bytes': 0, 'processed_files': 1,
                          'coll_type': CollectionType.PseudoDataset}
            return [collection]
        else:
            if 'collection_lister' not in self.plugins:
                raise AgentPluginError('Plugin collection_lister is required')
            return self.plugins['collection_lister'](scope, name)

    def get_output_collection(self, input_collection, request_type, transform_tag):
        if request_type in [RequestType.StageIn, RequestType.StageIn.value,
                            RequestType.ActiveLearning, RequestType.ActiveLearning.value,
                            RequestType.HyperParameterOpt, RequestType.HyperParameterOpt.value]:
            collection = input_collection
            output_collection = copy.deepcopy(collection)
            # output_collection['coll_type'] = CollectionType.Dataset
            output_collection['relation_type'] = CollectionRelationType.Output
            output_collection['status'] = CollectionStatus.New
        else:
            collection = input_collection
            output_collection = copy.deepcopy(collection)
            output_collection['name'] = collection['name'] + '_iDDS.%s.%s' % (request_type.name, transform_tag)
            output_collection['coll_type'] = CollectionType.Dataset
            output_collection['relation_type'] = CollectionRelationType.Output
            output_collection['status'] = CollectionStatus.New
        return output_collection

    def get_log_collection(self, input_collection, request_type, transform_tag):
        if request_type in [RequestType.StageIn, RequestType.StageIn.value]:
            return None

        collection = input_collection
        log_collection = copy.deepcopy(collection)
        log_collection['name'] = collection['name'] + '_iDDS.%s.%s.log' % (request_type.name, transform_tag)
        log_collection['coll_type'] = CollectionType.Dataset
        log_collection['relation_type'] = CollectionRelationType.Log
        log_collection['status'] = CollectionStatus.New
        return log_collection

    def process_new_request(self, req):
        try:
            collections = self.get_collections(req['scope'], req['name'], req)
            if collections:
                transforms_to_add = []
                transforms_to_extend = []
                for collection in collections:
                    transform_type = convert_request_type_to_transform_type(req['request_type'])
                    transform = self.get_transform_with_input_collection(transform_type=transform_type,
                                                                         transform_tag=req['transform_tag'],
                                                                         coll_scope=collection['scope'],
                                                                         coll_name=collection['name'],
                                                                         request_metadata=req['request_metadata'])

                    if transform:
                        new_transform_metadata = copy.deepcopy(transform['transform_metadata'])
                        if req['request_metadata']:
                            for key in req['request_metadata']:
                                new_transform_metadata[key] = req['request_metadata'][key]

                        to_extend = {'transform_id': transform['transform_id'],
                                     'priority': req['priority'] if req['priority'] > transform['priority'] else transform['priority'],
                                     'status': TransformStatus.Extend,
                                     'transform_metadata': new_transform_metadata,
                                     'expired_at': req['expired_at'] if req['expired_at'] > transform['expired_at'] else transform['expired_at']}
                        transforms_to_extend.append(to_extend)
                    else:
                        related_collections = {'input_collections': [], 'output_collections': [], 'log_collections': []}
                        input_collection = {'transform_id': None,
                                            'coll_type': collection['coll_type'],
                                            'scope': collection['scope'],
                                            'name': collection['name'],
                                            'relation_type': CollectionRelationType.Input,
                                            'status': CollectionStatus.New,
                                            'expired_at': req['expired_at']}
                        related_collections['input_collections'].append(input_collection)
                        output_collection = self.get_output_collection(input_collection, req['request_type'], req['transform_tag'])
                        if output_collection:
                            related_collections['output_collections'].append(output_collection)
                        log_collection = self.get_log_collection(input_collection, req['request_type'], req['transform_tag'])
                        if log_collection:
                            related_collections['log_collections'].append(log_collection)

                        transform_metadata = copy.deepcopy(req['request_metadata'])
                        if 'processing_metadata' in transform_metadata:
                            del transform_metadata['processing_metadata']

                        transform = {'request_id': req['request_id'],
                                     'transform_type': convert_request_type_to_transform_type(req['request_type']),
                                     'transform_tag': req['transform_tag'],
                                     'priority': req['priority'],
                                     'status': TransformStatus.New,
                                     'retries': 0,
                                     'expired_at': req['expired_at'],
                                     'transform_metadata': transform_metadata,
                                     'collections': related_collections
                                     }
                        # core_transforms.add_transform(**transform)
                        transforms_to_add.append(transform)
                ret_req = {'request_id': req['request_id'],
                           'status': RequestStatus.Transforming,
                           'processing_metadata': {'total_collections': len(collections),
                                                   'transforms_to_add': len(transforms_to_add),
                                                   'transforms_to_extend': len(transforms_to_extend)},
                           'transforms_to_add': transforms_to_add,
                           'transforms_to_extend': transforms_to_extend}
                if not req['processing_metadata'] or 'request_metadata_history' not in req['processing_metadata']:
                    ret_req['processing_metadata']['request_metadata_history'] = []
                ret_req['processing_metadata']['request_metadata_history'].append(req['request_metadata'])
            else:
                ret_req = {'request_id': req['request_id'],
                           'status': RequestStatus.Failed,
                           'errors': {'msg': 'No matching datasets with %s:%s' % (req['scope'], req['name'])}}
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            ret_req = {'request_id': req['request_id'],
                       'status': RequestStatus.Failed,
                       'errors': {'msg': '%s: %s' % (ex, traceback.format_exc())}}
        return ret_req

    def process_new_requests(self):
        """
        Process new request
        """
        ret = []
        while not self.new_task_queue.empty():
            try:
                req = self.new_task_queue.get()
                if req:
                    self.logger.info("Main thread processing new requst: %s" % req)
                    ret_req = self.process_new_request(req)
                    if ret_req:
                        ret.append(ret_req)
            except Exception as ex:
                self.logger.error(ex)
                self.logger.error(traceback.format_exc())
        return ret

    def finish_new_requests(self):
        while not self.new_output_queue.empty():
            try:
                req = self.new_output_queue.get()
                self.logger.info("Main thread finished processing requst: %s" % req)
                parameter = {'status': req['status'], 'locking': RequestLocking.Idle}

                if 'processing_metadata' not in req or not req['processing_metadata']:
                    processing_metadata = {}
                else:
                    processing_metadata = req['processing_metadata']

                parameter['processing_metadata'] = processing_metadata

                if 'errors' in req:
                    parameter['errors'] = req['errors']

                if 'transforms_to_add' in req:
                    transforms_to_add = req['transforms_to_add']
                else:
                    transforms_to_add = []
                if 'transforms_to_extend' in req:
                    transforms_to_extend = req['transforms_to_extend']
                else:
                    transforms_to_extend = []
                core_requests.update_request_with_transforms(req['request_id'], parameter, transforms_to_add, transforms_to_extend)
            except Exception as ex:
                self.logger.error(ex)
                self.logger.error(traceback.format_exc())

    def get_monitor_requests(self):
        """
        Get requests to monitor
        """
        req_status = [RequestStatus.Transforming]
        reqs = core_requests.get_requests_by_status_type(status=req_status, time_period=self.poll_time_period,
                                                         locking=True, bulk_size=self.retrieve_bulk_size)

        self.logger.debug("Main thread get %s Transforming requests to monitor" % len(reqs))
        if reqs:
            self.logger.info("Main thread get %s Transforming requests to monitor" % len(reqs))
        return reqs

    def process_monitor_request(self, req):
        """
        process monitor request
        """
        transforms = core_transforms.get_transforms(request_id=req['request_id'])
        transform_status = {}
        for transform in transforms:
            status_name = transform['status'].name
            if status_name not in transform_status:
                transform_status[status_name] = 1
            else:
                transform_status[status_name] += 1
        processing_metadata = req['processing_metadata']

        processing_metadata['transform_status'] = transform_status

        transform_status_keys = list(transform_status.keys())
        if len(transform_status_keys) == 0:
            ret_req = {'request_id': req['request_id'],
                       'status': RequestStatus.Failed,
                       'processing_metadata': processing_metadata,
                       'errors': {'msg': 'No transforms founded(no collections founded)'}
                       }
        elif len(transform_status_keys) == 1:
            if transform_status_keys[0] in [TransformStatus.New, TransformStatus.New.name,
                                            TransformStatus.Transforming, TransformStatus.Transforming.name,
                                            TransformStatus.Extend, TransformStatus.Extend.name]:
                ret_req = {'request_id': req['request_id'],
                           'status': RequestStatus.Transforming,
                           'processing_metadata': processing_metadata
                           }
            else:
                ret_req = {'request_id': req['request_id'],
                           'status': dict(RequestStatus.__members__)[transform_status_keys[0]],
                           'processing_metadata': processing_metadata
                           }
        else:
            ret_req = {'request_id': req['request_id'],
                       'status': RequestStatus.Transforming,
                       'processing_metadata': processing_metadata
                       }
        return ret_req

    def process_monitor_requests(self):
        """
        Process monitor request
        """
        ret = []
        while not self.monitor_task_queue.empty():
            try:
                req = self.monitor_task_queue.get()
                if req:
                    self.logger.info("Main thread processing monitor requst: %s" % req)
                    ret_req = self.process_monitor_request(req)
                    if ret_req:
                        ret.append(ret_req)
            except Exception as ex:
                self.logger.error(ex)
                self.logger.error(traceback.format_exc())
        return ret

    def finish_monitor_requests(self):
        while not self.monitor_output_queue.empty():
            req = self.monitor_output_queue.get()
            self.logger.debug("finish_monitor_requests: req: %s" % req)
            parameter = {'locking': RequestLocking.Idle}
            for key in ['status', 'errors', 'request_metadata']:
                if key in req:
                    parameter[key] = req[key]
            core_requests.update_request(req['request_id'], parameter)

    def clean_locks(self):
        self.logger.info("clean locking")
        core_requests.clean_locking()

    def run(self):
        """
        Main run function.
        """
        try:
            self.logger.info("Starting main thread")

            self.load_plugins()

            task = self.create_task(task_func=self.get_new_requests, task_output_queue=self.new_task_queue, task_args=tuple(), task_kwargs={}, delay_time=1, priority=1)
            self.add_task(task)
            task = self.create_task(task_func=self.process_new_requests, task_output_queue=self.new_output_queue, task_args=tuple(), task_kwargs={}, delay_time=1, priority=1)
            self.add_task(task)
            task = self.create_task(task_func=self.finish_new_requests, task_output_queue=None, task_args=tuple(), task_kwargs={}, delay_time=1, priority=1)
            self.add_task(task)

            task = self.create_task(task_func=self.get_monitor_requests, task_output_queue=self.monitor_task_queue, task_args=tuple(), task_kwargs={}, delay_time=1, priority=1)
            self.add_task(task)
            task = self.create_task(task_func=self.process_monitor_requests, task_output_queue=self.monitor_output_queue, task_args=tuple(), task_kwargs={}, delay_time=1, priority=1)
            self.add_task(task)
            task = self.create_task(task_func=self.finish_monitor_requests, task_output_queue=None, task_args=tuple(), task_kwargs={}, delay_time=1, priority=1)
            self.add_task(task)

            task = self.create_task(task_func=self.clean_locks, task_output_queue=None, task_args=tuple(), task_kwargs={}, delay_time=1800, priority=1)
            self.add_task(task)

            self.execute()
        except KeyboardInterrupt:
            self.stop()


if __name__ == '__main__':
    agent = Clerk()
    agent()
