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

from idds.common.constants import (Sections, RequestStatus, RequestLocking,
                                   WorkprogressStatus)
from idds.common.utils import setup_logging
from idds.common.status_utils import get_workprogresses_status
from idds.core import (requests as core_requests,
                       workprogress as core_workprogress)
from idds.agents.common.baseagent import BaseAgent

setup_logging(__name__)


class Clerk(BaseAgent):
    """
    Clerk works to process requests and converts requests to transforms.
    """

    def __init__(self, num_threads=1, poll_time_period=10, retrieve_bulk_size=10, **kwargs):
        super(Clerk, self).__init__(num_threads=num_threads, **kwargs)
        self.poll_time_period = int(poll_time_period)
        self.retrieve_bulk_size = int(retrieve_bulk_size)
        self.config_section = Sections.Clerk

        self.new_task_queue = Queue()
        self.new_output_queue = Queue()
        self.running_task_queue = Queue()
        self.running_output_queue = Queue()

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

    def process_new_request(self, req):
        try:
            req_id = req['request_id']
            workload_id = req['workload_id']
            workflow = req['request_metadata']['workflow']
            workflows = workflow.get_exact_workflows()
            existed_wps = core_workprogress.get_workprogresses(req_id)
            existed_workflows = [wp['workprogress_metadata']['workflow'] for wp in existed_wps]
            new_workflows = []
            for wf in workflows:
                if wf not in existed_workflows:
                    new_workflows.append(wf)

            wps = []
            for wf in new_workflows:
                primary_init_collection = wf.get_primary_initial_collection()
                workprogress = {'request_id': req_id,
                                'workload_id': workload_id,
                                'scope': primary_init_collection['scope'],
                                'name': primary_init_collection['name'],
                                'priority': req['priority'],
                                'status': req['status'],
                                'locking': req['locking'],
                                'expired_at': req['expired_at'],
                                'errors': None,
                                'workprogress_metadata': {'workflow': wf},
                                'processing_metadata': None}
                wps.append(workprogress)

            ret_req = {'request_id': req['request_id'],
                       'status': RequestStatus.Transforming,
                       'processing_metadata': {'total_workprogresses': len(workflows),
                                               'new_workprogresses': len(new_workflows)},
                       'new_workprogresses': wps}
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

                if 'new_workprogresses' in req:
                    new_workprogresses = req['new_workprogresses']
                else:
                    new_workprogresses = []
                core_requests.update_request_with_workprogresses(req['request_id'], parameter, new_workprogresses)
            except Exception as ex:
                self.logger.error(ex)
                self.logger.error(traceback.format_exc())

    def get_running_requests(self):
        """
        Get running requests
        """
        req_status = [RequestStatus.Transforming, RequestStatus.ToCancel, RequestStatus.Cancelling]
        reqs = core_requests.get_requests_by_status_type(status=req_status, time_period=self.poll_time_period,
                                                         locking=True, bulk_size=self.retrieve_bulk_size)

        self.logger.debug("Main thread get %s Transforming requests to running" % len(reqs))
        if reqs:
            self.logger.info("Main thread get %s Transforming requests to running" % len(reqs))
        return reqs

    def process_running_request(self, req):
        """
        process running request
        """
        wps = core_workprogress.get_workprogresses(request_id=req['request_id'])
        wps_status = {}
        for wp in wps:
            status_name = wp['status']
            if status_name not in wps_status:
                wps_status[status_name] = 1
            else:
                wps_status[status_name] += 1
        processing_metadata = req['processing_metadata']

        processing_metadata['workprogresses_status'] = wps_status

        wps_status_keys = list(wps_status.keys())
        if len(wps_status_keys) == 0:
            ret_req = {'request_id': req['request_id'],
                       'status': RequestStatus.Failed,
                       'processing_metadata': processing_metadata,
                       'errors': {'msg': 'No transforms founded(no collections founded)'}
                       }
        else:
            final_wp_status = get_workprogresses_status(wps_status_keys)
            ret_req = {'request_id': req['request_id'],
                       'status': dict(RequestStatus.__members__)[final_wp_status.name],
                       'processing_metadata': processing_metadata
                       }
        return ret_req

    def process_tocancel_request(self, req):
        """
        process ToCancel request
        """
        wps = core_workprogress.get_workprogresses(request_id=req['request_id'])
        wps_status = {}
        for wp in wps:
            if wp['status'] not in [WorkprogressStatus.Finished, WorkprogressStatus.SubFinished,
                                    WorkprogressStatus.Failed, WorkprogressStatus.Cancelling,
                                    WorkprogressStatus.Cancelled]:
                wps_status[wp['workprogress_id']] = {'status': WorkprogressStatus.ToCancel}

        ret_req = {'request_id': req['request_id'],
                   'status': RequestStatus.Cancelling,
                   'workprogress_updates': wps_status
                   }
        return ret_req

    def process_running_requests(self):
        """
        Process running request
        """
        ret = []
        while not self.running_task_queue.empty():
            try:
                req = self.running_task_queue.get()
                if req:
                    if req['status'] in [RequestStatus.Transforming, RequestStatus.Cancelling]:
                        self.logger.info("Main thread processing running requst: %s" % req)
                        ret_req = self.process_running_request(req)
                    elif req['status'] in [RequestStatus.ToCancel]:
                        self.logger.info("Main thread processing ToCancelrequst: %s" % req)
                        ret_req = self.process_tocancel_request(req)

                    if ret_req:
                        ret.append(ret_req)
            except Exception as ex:
                self.logger.error(ex)
                self.logger.error(traceback.format_exc())
        return ret

    def finish_running_requests(self):
        while not self.running_output_queue.empty():
            req = self.running_output_queue.get()
            self.logger.info("finish_running_requests: req: %s" % req)
            parameter = {'locking': RequestLocking.Idle}
            for key in ['status', 'errors', 'request_metadata', 'processing_metadata']:
                if key in req:
                    parameter[key] = req[key]

            update_workprogresses = None
            if 'workprogress_updates' in req:
                update_workprogresses = req['workprogress_updates']
            core_requests.update_request_with_workprogresses(req['request_id'], parameter, update_workprogresses=update_workprogresses)

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

            self.add_default_tasks()

            task = self.create_task(task_func=self.get_new_requests, task_output_queue=self.new_task_queue, task_args=tuple(), task_kwargs={}, delay_time=1, priority=1)
            self.add_task(task)
            task = self.create_task(task_func=self.process_new_requests, task_output_queue=self.new_output_queue, task_args=tuple(), task_kwargs={}, delay_time=1, priority=1)
            self.add_task(task)
            task = self.create_task(task_func=self.finish_new_requests, task_output_queue=None, task_args=tuple(), task_kwargs={}, delay_time=1, priority=1)
            self.add_task(task)

            task = self.create_task(task_func=self.get_running_requests, task_output_queue=self.running_task_queue, task_args=tuple(), task_kwargs={}, delay_time=1, priority=1)
            self.add_task(task)
            task = self.create_task(task_func=self.process_running_requests, task_output_queue=self.running_output_queue, task_args=tuple(), task_kwargs={}, delay_time=1, priority=1)
            self.add_task(task)
            task = self.create_task(task_func=self.finish_running_requests, task_output_queue=None, task_args=tuple(), task_kwargs={}, delay_time=1, priority=1)
            self.add_task(task)

            task = self.create_task(task_func=self.clean_locks, task_output_queue=None, task_args=tuple(), task_kwargs={}, delay_time=1800, priority=1)
            self.add_task(task)

            self.execute()
        except KeyboardInterrupt:
            self.stop()


if __name__ == '__main__':
    agent = Clerk()
    agent()
