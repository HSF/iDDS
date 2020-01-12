#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019

import traceback
try:
    # python 3
    from queue import Queue
except ImportError:
    # Python 2
    from Queue import Queue

from idds.common.constants import (Sections, RequestStatus, TransformStatus,
                                   CollectionRelationType, CollectionStatus)
from idds.common.exceptions import AgentPluginError, IDDSException
from idds.common.utils import setup_logging
from idds.core import requests as core_requests, transforms as core_transforms
from idds.agents.common.baseagent import BaseAgent

setup_logging(__name__)


class Clerk(BaseAgent):
    """
    Clerk works to process requests and converts requests to transforms.
    """

    def __init__(self, num_threads=1, **kwargs):
        super(Clerk, self).__init__(num_threads=num_threads, **kwargs)
        self.config_section = Sections.Clerk
        self.new_output_queue = Queue()
        self.monitor_output_queue = Queue()

    def get_new_requests(self):
        """
        Get new requests to process
        """
        # req_status = [RequestStatus.TransformingOpen]
        # reqs_open = core_requests.get_requests_by_status_type(status=req_status, time_period=3600)
        # self.logger.info("Main thread get %s TransformingOpen requests to process" % len(reqs_open))

        req_status = [RequestStatus.New]
        reqs_new = core_requests.get_requests_by_status_type(status=req_status)
        self.logger.info("Main thread get %s New requests to process" % len(reqs_new))

        return reqs_new

    def get_collections(self, scope, name):
        if 'collection_lister' not in self.plugins:
            raise AgentPluginError('Plugin collection_lister is required')
        return self.plugins['collection_lister'](scope, name)

    def process_new_request(self, req):
        """
        Process new request
        """
        collections = self.get_collections(req['scope'], req['name'])
        transforms = []
        for collection in collections:
            related_collections = []
            input_collection = {'request_id': req['request_id'],
                                'transform_id': None,
                                'coll_type': collection['coll_type'],
                                'scope': collection['scope'],
                                'name': collection['name'],
                                'relation_type': CollectionRelationType.Input,
                                'coll_status': CollectionStatus.New,
                                'expired_at': req['expired_at']}
            related_collections.append(input_collection)

            transform = {'request_id': req['request_id'],
                         'transform_type': req['request_type'],
                         'transform_tag': req['transform_tag'],
                         'priority': req['priority'],
                         'status': TransformStatus.New,
                         'retries': 0,
                         'expired_at': req['expired_at'],
                         'transform_metadata': req['request_metadata'],
                         'collections': related_collections
                         }
            # core_transforms.add_transform(**transform)
            transforms.append(transform)
        ret_req = {'request_id': req['request_id'],
                   'status': RequestStatus.Transforming,
                   'request_metadata': req['request_metadata'],
                   'processing_metadata': {'total_collections': len(collections)},
                   'transforms': transforms}
        return ret_req

    def finish_new_requests(self):
        while not self.new_output_queue.empty():
            req = self.new_output_queue.get()
            self.logger.info("Main thread finished processing requst: %s" % req)
            transforms = req['transforms']
            core_transforms.add_transforms(transforms)
            parameter = {'status': req['status'],
                         'request_metadata': req['request_metadata'],
                         'processing_metadata': req['processing_metadata']}
            core_requests.update_request(req['request_id'], parameter)

    def get_monitor_requests(self):
        """
        Get requests to monitor
        """
        req_status = [RequestStatus.Transforming]
        reqs = core_requests.get_requests_by_status_type(status=req_status, time_period=3600)
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

        transform_status_keys = list(transform_status.keys())
        if len(transform_status_keys) == 0:
            ret_req = {'request_id': req['request_id'],
                       'status': RequestStatus.Failed,
                       'errors': 'No transforms founded(no collections founded)'
                       }
        elif (TransformStatus.New.name in transform_status_keys
            or TransformStatus.Transforming.name in transform_status_keys   # noqa: W503
            or TransformStatus.Transporting.name in transform_status_keys   # noqa: W503
            or TransformStatus.Processing.name in transform_status_keys):   # noqa: W503
            processing_metadata = req['processing_metadata']
            processing_metadata['transform_status'] = transform_status
            ret_req = {'request_id': req['request_id'],
                       'status': RequestStatus.Transforming,
                       'processing_metadata': processing_metadata
                       }
        elif TransformStatus.Failed.name in transform_status_keys:
            processing_metadata = req['processing_metadata']
            processing_metadata['transform_status'] = transform_status
            ret_req = {'request_id': req['request_id'],
                       'status': RequestStatus.Failed,
                       'processing_metadata': processing_metadata
                       }
        elif TransformStatus.Cancel.name in transform_status_keys:
            processing_metadata = req['processing_metadata']
            processing_metadata['transform_status'] = transform_status
            ret_req = {'request_id': req['request_id'],
                       'status': RequestStatus.Cancel,
                       'processing_metadata': processing_metadata
                       }
        else:
            processing_metadata = req['processing_metadata']
            processing_metadata['transform_status'] = transform_status
            ret_req = {'request_id': req['request_id'],
                       'status': RequestStatus.Finished,
                       'processing_metadata': processing_metadata
                       }
        return ret_req

    def finish_monitor_requests(self):
        while not self.monitor_output_queue.empty():
            req = self.monitor_output_queue.get()
            parameter = {}
            for key in ['status', 'errors', 'processing_metadata']:
                if key in req:
                    parameter[key] = req[key]
            core_requests.update_request(req['request_id'], parameter)

    def prepare_finish_tasks(self):
        """
        Prepare tasks and finished tasks
        """
        # finish tasks
        self.finish_new_requests()
        self.finish_monitor_requests()

        # prepare tasks
        reqs = self.get_new_requests()
        for req in reqs:
            self.submit_task(self.process_new_request, self.new_output_queue, req)

        reqs = self.get_monitor_requests()
        for req in reqs:
            self.submit_task(self.process_monitor_request, self.monitor_output_queue, req)

    def run(self):
        """
        Main run function.
        """
        try:
            self.logger.info("Starting main thread")

            self.load_plugins()

            for i in range(self.num_threads):
                self.executors.submit(self.run_tasks, i)

            while not self.graceful_stop.is_set():
                try:
                    self.prepare_finish_tasks()
                    self.sleep_for_tasks()
                except IDDSException as error:
                    self.logger.error("Main thread IDDSException: %s" % str(error))
                except Exception as error:
                    self.logger.critical("Main thread exception: %s\n%s" % (str(error), traceback.format_exc()))
        except KeyboardInterrupt:
            self.stop()


if __name__ == '__main__':
    agent = Clerk()
    agent()
