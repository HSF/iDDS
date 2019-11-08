#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019


import Queue


from idds.common.constants import Sections, RequestStatus, TransformType, TransformStatus
from idds.common.exceptions import AgentPluginError
from idds.common.utils import setup_logging
from idds.core import requests as core_requests, transforms as core_transforms
from idds.agents.common.baseagent import BaseAgent

setup_logging(__name__)


class Transformer(BaseAgent):
    """
    Transformer works to process requests and converts requests to transforms.
    """

    def __init__(self, num_threads=1, **kwargs):
        super(Transformer, self).__init__(num_threads=num_threads, **kwargs)
        self.config_section = Sections.Transformer
        self.processed_queue = Queue.Queue()
        self.monitor_queue = Queue.Queue()

    def get_requests(self):
        """
        Get requests to process
        """
        req_status = [RequestStatus.TransformingOpen]
        reqs_open = core_requests.get_requests_by_status_type(status=req_status, time_period=3600)
        self.logger.info("Main thread get %s TransformingOpen requests to process" % len(reqs_open))

        req_status = [RequestStatus.New, RequestStatus.Extend, RequestStatus.TransformingOpen]
        reqs_new = core_requests.get_requests_by_status_type(status=req_status)
        self.logger.info("Main thread get %s New+Extend requests to process" % len(reqs_new))

        reqs = reqs_open + reqs_new
        self.logger.info("Main thread get totally %s requests to process" % len(reqs))

        return reqs

    def get_monitor_requests(self):
        """
        Get requests to monitor
        """
        req_status = [RequestStatus.Transforming, RequestStatus.TransformingOpen]
        reqs = core_requests.get_requests_by_status_type(status=req_status, time_period=3600)
        self.logger.info("Main thread get %s TransformingOpen requests to monitor" % len(reqs))
        return reqs

    def get_output_collection(self, collection, transform_type, transform_tag):
        if transform_type in [TransformType.StageIn, TransformType.StageIn.value]:
            return None

        output_collection = {'scope': collection['scope'],
                             'name': collection['name'] + '_iDDS.%s.%s' % (transform_type.name, transform_tag)}
        return output_collection

    def get_log_collection(self, collection, transform_type, transform_tag):
        if transform_type in [TransformType.StageIn, TransformType.StageIn.value]:
            return None

        output_collection = {'scope': collection['scope'],
                             'name': collection['name'] + '_iDDS.%s.%s.log' % (transform_type.name, transform_tag)}
        return output_collection

    def get_collections(self, scope, name):
        if 'collection_lister' not in self.plugins:
            raise AgentPluginError('Plugin collection_lister is required')
        return self.plugins['collection_lister'](scope, name)

    def process_request(self, req):
        """
        Process request
        """
        collections = self.get_collections(req['scope'], req['name'])
        transforms = []
        for collection in collections:
            transform = {'request_id': req['request_id'],
                         'transform_type': req['request_type'],
                         'transform_tag': req['transform_tag'],
                         'priority': req['priority'],
                         'status': TransformStatus.New,
                         'retries': 0,
                         'expired_at': req['expired_at'],
                         'request_metadata': req['request_metadata'],
                         'input_collection': collection,
                         'output_collection': self.get_output_collection(collection),
                         'log_collection': self.get_log_collection(collection)}
            # core_transforms.add_transform(**transform)
            transforms.append(transform)
        ret_req = {'request_id': req['request_id'],
                   'status': TransformStatus.Transforming,
                   'request_metadata': req['request_metadata'],
                   'transforms': transforms}
        return ret_req

    def monitor_request(self, req):
        """
        monitor request
        """
        pass
        return req

    def finish_processed_requests(self):
        while not self.processed_queue.empty():
            req = self.finished_tasks.get()
            self.logger.info("Main thread finished processing requst: %s" % req)
            transforms = req['transforms']
            core_transforms.add_transforms(transforms)
            parameter = {'status': req['status'], 'request_metadata': req['request_metadata']}
            core_requests.update_request(req['request_id'], parameter)

    def finish_monitor_requests(self):
        while not self.monitor_queue.empty():
            req = self.monitor_queue.get()
            # todo
            parameter = {'status': req['status'], 'request_metadata': req['request_metadata']}
            core_requests.update_request(req['request_id'], parameter)

    def prepare_finish_tasks(self):
        """
        Prepare tasks and finished tasks
        """
        self.finish_processed_requests()
        self.finish_monitor_requests()

        reqs = self.get_requests()
        for req in reqs:
            self.submit_task(self.process_task, self.processed_queue, req)

        reqs = self.get_monitor_requests()
        for req in reqs:
            self.submit_task(self.monitor_request, self.monitor_queue, req)


if __name__ == '__main__':
    agent = Transformer()
    agent()
