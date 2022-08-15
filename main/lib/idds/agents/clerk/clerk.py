#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019 - 2022

import time
import traceback

from idds.common import exceptions
from idds.common.constants import (Sections, RequestStatus, RequestLocking,
                                   TransformStatus)
from idds.common.utils import setup_logging, truncate_string
from idds.core import (requests as core_requests,
                       transforms as core_transforms)
from idds.agents.common.baseagent import BaseAgent
from idds.agents.common.eventbus.event import (NewRequestEvent,
                                               UpdateRequestEvent,
                                               AbortRequestEvent,
                                               ResumeRequestEvent,
                                               NewTransformEvent,
                                               UpdateTransformEvent,
                                               AbortTransformEvent,
                                               ResumeTransformEvent,
                                               ExpireTransformEvent)

setup_logging(__name__)


class Clerk(BaseAgent):
    """
    Clerk works to process requests and converts requests to transforms.
    """

    def __init__(self, num_threads=1, poll_time_period=10, retrieve_bulk_size=10, pending_time=None, **kwargs):
        super(Clerk, self).__init__(num_threads=num_threads, **kwargs)
        self.poll_time_period = int(poll_time_period)
        self.retrieve_bulk_size = int(retrieve_bulk_size)
        self.config_section = Sections.Clerk
        if pending_time:
            self.pending_time = float(pending_time)
        else:
            self.pending_time = None

        if not hasattr(self, 'release_helper') or not self.release_helper:
            self.release_helper = False
        elif str(self.release_helper).lower() == 'true':
            self.release_helper = True
        else:
            self.release_helper = False

        if not hasattr(self, 'new_poll_time_period') or not self.new_poll_time_period:
            self.new_poll_time_period = self.poll_time_period
        else:
            self.new_poll_time_period = int(self.new_poll_time_period)
        if not hasattr(self, 'update_poll_time_period') or not self.update_poll_time_period:
            self.update_poll_time_period = self.poll_time_period
        else:
            self.update_poll_time_period = int(self.update_poll_time_period)

        if hasattr(self, 'max_new_retries'):
            self.max_new_retries = int(self.max_new_retries)
        else:
            self.max_new_retries = 3
        if hasattr(self, 'max_update_retries'):
            self.max_update_retries = int(self.max_update_retries)
        else:
            # 0 or None means no limitations.
            self.max_update_retries = 0

        self.number_workers = 0
        if not hasattr(self, 'max_number_workers') or not self.max_number_workers:
            self.max_number_workers = 3
        else:
            self.max_number_workers = int(self.max_number_workers)

    def is_ok_to_run_more_requests(self):
        if self.number_workers >= self.max_number_workers:
            return False
        return True

    def show_queue_size(self):
        q_str = "number of requests: %s, max number of requests: %s" % (self.number_workers, self.max_number_workers)
        self.logger.debug(q_str)

    def get_new_requests(self):
        """
        Get new requests to process
        """
        try:
            # req_status = [RequestStatus.TransformingOpen]
            # reqs_open = core_requests.get_requests_by_status_type(status=req_status, time_period=3600)
            # self.logger.info("Main thread get %s TransformingOpen requests to process" % len(reqs_open))

            if not self.is_ok_to_run_more_requests():
                return []

            self.show_queue_size()

            req_status = [RequestStatus.New, RequestStatus.Extend]
            reqs_new = core_requests.get_requests_by_status_type(status=req_status, locking=True,
                                                                 not_lock=True,
                                                                 new_poll=True, only_return_id=True,
                                                                 bulk_size=self.retrieve_bulk_size)

            self.logger.debug("Main thread get %s [New+Extend] requests to process" % len(reqs_new))
            if reqs_new:
                self.logger.info("Main thread get %s [New+Extend] requests to process" % len(reqs_new))

            for req in reqs_new:
                event = NewRequestEvent(publisher_id=self.id, request_id=req.request_id)
                self.event_bus.send(event)

            return reqs_new
        except exceptions.DatabaseException as ex:
            if 'ORA-00060' in str(ex):
                self.logger.warn("(cx_Oracle.DatabaseError) ORA-00060: deadlock detected while waiting for resource")
            else:
                # raise ex
                self.logger.error(ex)
                self.logger.error(traceback.format_exc())
        return []

    def get_running_requests(self):
        """
        Get running requests
        """
        try:
            if not self.is_ok_to_run_more_requests():
                return []

            self.show_queue_size()

            req_status = [RequestStatus.Transforming, RequestStatus.ToCancel, RequestStatus.Cancelling,
                          RequestStatus.ToSuspend, RequestStatus.Suspending,
                          RequestStatus.ToExpire, RequestStatus.Expiring,
                          RequestStatus.ToFinish, RequestStatus.ToForceFinish,
                          RequestStatus.ToResume, RequestStatus.Resuming]
            reqs = core_requests.get_requests_by_status_type(status=req_status, time_period=None,
                                                             locking=True, bulk_size=self.retrieve_bulk_size,
                                                             not_lock=True, update_poll=True, only_return_id=True)

            self.logger.debug("Main thread get %s Transforming requests to running" % len(reqs))
            if reqs:
                self.logger.info("Main thread get %s Transforming requests to running" % len(reqs))

            for req in reqs:
                event = UpdateRequestEvent(publisher_id=self.id, request_id=req.request_id)
                self.event_bus.send(event)

            return reqs
        except exceptions.DatabaseException as ex:
            if 'ORA-00060' in str(ex):
                self.logger.warn("(cx_Oracle.DatabaseError) ORA-00060: deadlock detected while waiting for resource")
            else:
                # raise ex
                self.logger.error(ex)
                self.logger.error(traceback.format_exc())
        return []

    def get_operation_requests(self):
        """
        Get running requests
        """
        try:
            if not self.is_ok_to_run_more_requests():
                return []

            self.show_queue_size()

            req_msgs = core_requests.get_operation_request_msgs(locking=True, bulk_size=self.retrieve_bulk_size)

            self.logger.debug("Main thread get %s operation requests to running" % len(req_msgs))
            if req_msgs:
                self.logger.info("Main thread get %s operation requests to running" % len(req_msgs))

            for req_msg in req_msgs:
                message = req_msg['msg_content']
                if 'command' in message and message['command'] == 'update_request':
                    parameters = message['parameters']
                    if 'status' in parameters and parameters['status'] in [RequestStatus.ToCancel, RequestStatus.ToSuspend]:
                        event = AbortRequestEvent(publisher_id=self.id, request_id=req_msg['request_id'])
                        self.event_bus.send(event)
                    elif 'status' in parameters and parameters['status'] in [RequestStatus.ToResume]:
                        event = ResumeRequestEvent(publisher_id=self.id, request_id=req_msg['request_id'])
                        self.event_bus.send(event)
                    else:
                        self.logger.info("Unkonw request message: %s. Only ToCancel and ToResume messages are allowed." % str(req_msg))
                else:
                    self.logger.info("Unkonw request message: %s. Only update_request message is allowed." % str(req_msg))

            return req_msgs
        except exceptions.DatabaseException as ex:
            if 'ORA-00060' in str(ex):
                self.logger.warn("(cx_Oracle.DatabaseError) ORA-00060: deadlock detected while waiting for resource")
            else:
                # raise ex
                self.logger.error(ex)
                self.logger.error(traceback.format_exc())
        return []

    def get_request(self, request_id, status=None, locking=False):
        try:
            return core_requests.get_request_by_id_status(request_id=request_id, status=status, locking=locking)
        except exceptions.DatabaseException as ex:
            if 'ORA-00060' in str(ex):
                self.logger.warn("(cx_Oracle.DatabaseError) ORA-00060: deadlock detected while waiting for resource")
            else:
                # raise ex
                self.logger.error(ex)
                self.logger.error(traceback.format_exc())
        return None

    def load_poll_period(self, req, parameters):
        if self.new_poll_period and req['new_poll_period'] != self.new_poll_period:
            parameters['new_poll_period'] = self.new_poll_period
        if self.update_poll_period and req['update_poll_period'] != self.update_poll_period:
            parameters['update_poll_period'] = self.update_poll_period
        parameters['max_new_retries'] = req['max_new_retries'] if req['max_new_retries'] is not None else self.max_new_retries
        parameters['max_update_retries'] = req['max_update_retries'] if req['max_update_retries'] is not None else self.max_update_retries
        return parameters

    def generate_transform(self, req, work):
        wf = req['request_metadata']['workflow']

        work.set_request_id(req['request_id'])
        work.username = req['username']

        new_transform = {'request_id': req['request_id'],
                         'workload_id': req['workload_id'],
                         'transform_type': work.get_work_type(),
                         'transform_tag': work.get_work_tag(),
                         'priority': req['priority'],
                         'status': TransformStatus.New,
                         'retries': 0,
                         'new_poll_period': self.new_poll_period,
                         'update_poll_period': self.update_poll_period,
                         'max_new_retries': req['max_new_retries'] if req['max_new_retries'] is not None else self.max_new_retries,
                         'max_update_retries': req['max_update_retries'] if req['max_update_retries'] is not None else self.max_update_retries,
                         # 'expired_at': req['expired_at'],
                         'expired_at': None,
                         'transform_metadata': {'internal_id': work.get_internal_id(),
                                                'template_work_id': work.get_template_work_id(),
                                                'sequence_id': work.get_sequence_id(),
                                                'work_name': work.get_work_name(),
                                                'work': work,
                                                'workflow': wf}
                         # 'running_metadata': {'work_data': new_work.get_running_data()}
                         # 'collections': related_collections
                         }

        return new_transform

    def handle_new_request(self, req):
        try:
            self.logger.info("handle new request(%s)" % (req['request_id']))
            workflow = req['request_metadata']['workflow']

            # wf = workflow.copy()
            wf = workflow
            works = wf.get_new_works()
            transforms = []
            for work in works:
                # new_work = work.copy()
                new_work = work
                new_work.add_proxy(wf.get_proxy())
                # new_work.set_request_id(req['request_id'])
                # new_work.create_processing()

                transform = self.generate_transform(req, work)
                transforms.append(transform)
            self.logger.debug("Processing request(%s): new transforms: %s" % (req['request_id'],
                                                                              str(transforms)))
            # processing_metadata = req['processing_metadata']
            # processing_metadata = {'workflow_data': wf.get_running_data()}

            ret_req = {'request_id': req['request_id'],
                       'parameters': {'status': RequestStatus.Transforming,
                                      'locking': RequestLocking.Idle,
                                      # 'processing_metadata': processing_metadata,
                                      'request_metadata': req['request_metadata']},
                       'new_transforms': transforms}
            ret_req['parameters'] = self.load_poll_period(req, ret_req['parameters'])
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            retries = req['new_retries'] + 1
            if not req['max_new_retries'] or retries < req['max_new_retries']:
                req_status = req['status']
            else:
                req_status = RequestStatus.Failed
            error = {'submit_err': {'msg': truncate_string('%s: %s' % (ex, traceback.format_exc()), length=200)}}

            ret_req = {'request_id': req['request_id'],
                       'parameters': {'status': req_status,
                                      'locking': RequestLocking.Idle,
                                      'new_retries': retries,
                                      'errors': req['errors'] if req['errors'] else {}}}
            ret_req['parameters'].update(error)
        return ret_req

    def update_request(self, req):
        new_tf_ids, update_tf_ids = [], []
        try:
            self.logger.info("update request: %s" % req)
            req['parameters']['locking'] = RequestLocking.Idle

            if 'new_transforms' in req:
                new_transforms = req['new_transforms']
            else:
                new_transforms = []

            if 'update_transforms' in req:
                update_transforms = req['update_transforms']
            else:
                update_transforms = {}

            retry = True
            retry_num = 0
            while retry:
                retry = False
                retry_num += 1
                try:
                    _, new_tf_ids, update_tf_ids = core_requests.update_request_with_transforms(req['request_id'], req['parameters'],
                                                                                                new_transforms=new_transforms,
                                                                                                update_transforms=update_transforms)
                except exceptions.DatabaseException as ex:
                    if 'ORA-00060' in str(ex):
                        self.logger.warn("(cx_Oracle.DatabaseError) ORA-00060: deadlock detected while waiting for resource")
                        if retry_num < 5:
                            retry = True
                            time.sleep(60 * retry_num * 2)
                        else:
                            raise ex
                    else:
                        # self.logger.error(ex)
                        # self.logger.error(traceback.format_exc())
                        raise ex
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            try:
                req_parameters = {'status': RequestStatus.Transforming,
                                  'locking': RequestLocking.Idle}
                if 'new_retries' in req['parameters']:
                    req_parameters['new_retries'] = req['parameters']['new_retries']
                if 'update_retries' in req['parameters']:
                    req_parameters['update_retries'] = req['parameters']['update_retries']
                if 'errors' in req['parameters']:
                    req_parameters['errors'] = req['parameters']['errors']
                core_requests.update_request_with_transforms(req['request_id'], req_parameters)
            except Exception as ex:
                self.logger.error(ex)
                self.logger.error(traceback.format_exc())
        return new_tf_ids, update_tf_ids

    def process_new_request(self, event):
        self.number_workers += 1
        try:
            if event:
                req_status = [RequestStatus.New, RequestStatus.Extend]
                req = self.get_request(request_id=event.request_id, status=req_status, locking=True)
                if req:
                    ret = self.handle_new_request(req)
                    new_tf_ids, update_tf_ids = self.update_request(ret)
                    for tf_id in new_tf_ids:
                        event = NewTransformEvent(publisher_id=self.id, transform_id=tf_id)
                        self.event_bus.send(event)
                    for tf_id in update_tf_ids:
                        event = UpdateTransformEvent(publisher_id=self.id, transform_id=tf_id)
                        self.event_bus.send(event)
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
        self.number_workers -= 1

    def handle_update_request_real(self, req):
        """
        process running request
        """
        self.logger.info("handle_update_request: request_id: %s" % req['request_id'])
        wf = req['request_metadata']['workflow']

        # current works
        works = wf.get_all_works()
        # print(works)
        for work in works:
            # print(work.get_work_id())
            tf = core_transforms.get_transform(transform_id=work.get_work_id())
            if tf:
                transform_work = tf['transform_metadata']['work']
                # work_status = WorkStatus(tf['status'].value)
                # work.set_status(work_status)
                work.sync_work_data(status=tf['status'], substatus=tf['substatus'], work=transform_work)
        wf.refresh_works()

        new_transforms = []
        if req['status'] in [RequestStatus.Transforming]:
            # new works
            works = wf.get_new_works()
            for work in works:
                # new_work = work.copy()
                new_work = work
                new_work.add_proxy(wf.get_proxy())
                new_transform = self.generate_transform(req, new_work)
                new_transforms.append(new_transform)
            self.logger.debug("Processing request(%s): new transforms: %s" % (req['request_id'], str(new_transforms)))

        # is_operation = False
        if wf.is_terminated():
            if wf.is_finished():
                req_status = RequestStatus.Finished
            elif wf.is_subfinished():
                req_status = RequestStatus.SubFinished
            elif wf.is_expired():
                req_status = RequestStatus.Expired
            elif wf.is_failed():
                req_status = RequestStatus.Failed
            elif wf.is_cancelled():
                req_status = RequestStatus.Cancelled
            elif wf.is_suspended():
                req_status = RequestStatus.Suspended
            else:
                req_status = RequestStatus.Failed
            # req_msg = wf.get_terminated_msg()
        else:
            # req_msg = None
            if req['status'] in [RequestStatus.ToSuspend, RequestStatus.Suspending]:
                req_status = RequestStatus.Suspending
                # is_operation = True
            elif req['status'] in [RequestStatus.ToCancel, RequestStatus.Cancelling]:
                req_status = RequestStatus.Cancelling
                # is_operation = True
            elif wf.is_to_expire(req['expired_at'], self.pending_time, request_id=req['request_id']) and req['status'] not in [RequestStatus.ToExpire, RequestStatus.Expiring]:
                wf.expired = True
                req_status = RequestStatus.ToExpire
                # is_operation = True
                # req_msg = "Workflow expired"
            elif req['status'] in [RequestStatus.ToExpire, RequestStatus.Expiring]:
                req_status = RequestStatus.Expiring
                # is_operation = True
                # req_msg = "Workflow expired"
            else:
                req_status = RequestStatus.Transforming

        parameters = {'status': req_status,
                      'locking': RequestLocking.Idle,
                      'request_metadata': req['request_metadata']
                      }
        parameters = self.load_poll_period(req, parameters)

        if req_status == RequestStatus.ToExpire:
            # parameters['substatus'] = req_status
            works = wf.get_all_works()
            for work in works:
                transform_id = work.get_work_id()
                event = ExpireTransformEvent(publisher_id=self.id, transform_id=transform_id)
                self.event_bus.send(event)

        ret = {'request_id': req['request_id'],
               'parameters': parameters,
               'new_transforms': new_transforms}   # 'update_transforms': update_transforms}
        return ret

    def handle_update_request(self, req):
        """
        process running request
        """
        try:
            # if self.release_helper:
            #     self.release_inputs(req['request_id'])
            ret_req = self.handle_update_request_real(req)
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            retries = req['update_retries'] + 1
            if not req['max_update_retries'] or retries < req['max_update_retries']:
                req_status = req['status']
            else:
                req_status = RequestStatus.Failed
            error = {'submit_err': {'msg': truncate_string('%s: %s' % (ex, traceback.format_exc()), length=200)}}

            ret_req = {'request_id': req['request_id'],
                       'parameters': {'status': req_status,
                                      'locking': RequestLocking.Idle,
                                      'update_retries': retries,
                                      'errors': req['errors'] if req['errors'] else {}}}
            ret_req['parameters'].update(error)

        return ret_req

    def process_update_request(self, event):
        self.number_workers += 1
        try:
            if event:
                req_status = [RequestStatus.Transforming, RequestStatus.ToCancel, RequestStatus.Cancelling,
                              RequestStatus.ToSuspend, RequestStatus.Suspending,
                              RequestStatus.ToExpire, RequestStatus.Expiring,
                              RequestStatus.ToFinish, RequestStatus.ToForceFinish,
                              RequestStatus.ToResume, RequestStatus.Resuming]

                req = self.get_request(request_id=event.request_id, status=req_status, locking=True)
                if req:
                    ret = self.handle_update_request(req)
                    new_tf_ids, update_tf_ids = self.update_request(ret)
                    for tf_id in new_tf_ids:
                        event = NewTransformEvent(publisher_id=self.id, transform_id=tf_id)
                        self.event_bus.send(event)
                    for tf_id in update_tf_ids:
                        event = UpdateTransformEvent(publisher_id=self.id, transform_id=tf_id)
                        self.event_bus.send(event)
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
        self.number_workers -= 1

    def handle_abort_request(self, req):
        """
        process abort request
        """
        try:
            req_status = RequestStatus.Cancelling

            ret_req = {'request_id': req['request_id'],
                       'parameters': {'status': req_status,
                                      'locking': RequestLocking.Idle},
                       }
            return ret_req
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            error = {'abort_err': {'msg': truncate_string('%s: %s' % (ex, traceback.format_exc()), length=200)}}
            ret_req = {'request_id': req['request_id'],
                       'parameters': {'status': RequestStatus.ToCancel,
                                      'locking': RequestLocking.Idle,
                                      'errors': req['errors'] if req['errors'] else {}}}
            ret_req['parameters']['errors'].update(error)
        return ret_req

    def process_abort_request(self, event):
        self.number_workers += 1
        try:
            if event:
                req = self.get_request(request_id=event.request_id, locking=True)
                if req['status'] in [RequestStatus.Finished, RequestStatus.SubFinished,
                                     RequestStatus.Failed, RequestStatus.Cancelled,
                                     RequestStatus.Suspended, RequestStatus.Expired]:
                    ret = {'request_id': req['request_id'],
                           'parameters': {'locking': RequestLocking.Idle,
                                          'errors': {'extra_msg': "Request is already terminated. Cannot be aborted"}}}
                    if 'msg' in req['errors']:
                        ret['parameters']['errors']['msg'] = req['errors']['msg']
                    self.update_request(ret)
                else:
                    ret = self.handle_abort_request(req)
                    self.update_request(ret)
                    wf = req['request_metadata']['workflow']
                    works = wf.get_all_works()
                    if works:
                        for work in works:
                            event = AbortTransformEvent(publisher_id=self.id, transform_id=work.get_work_id())
                            self.event_bus.send(event)
                    else:
                        # no works. should trigger update request
                        event = UpdateRequestEvent(publisher_id=self.id, request_id=req['request_id'])
                        self.event_bus.send(event)
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
        self.number_workers -= 1

    def handle_resume_request(self, req):
        """
        process resume request
        """
        try:
            req_status = RequestStatus.ToResume

            processing_metadata = req['processing_metadata']

            wf = req['request_metadata']['workflow']
            if req['status'] == RequestStatus.ToResume:
                wf.resume_works()

            ret_req = {'request_id': req['request_id'],
                       'parameters': {'status': req_status,
                                      'request_metadata': req['request_metadata'],
                                      'processing_metadata': processing_metadata,
                                      'locking': RequestLocking.Idle},
                       }
            return ret_req
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            error = {'abort_err': {'msg': truncate_string('%s: %s' % (ex, traceback.format_exc()), length=200)}}
            ret_req = {'request_id': req['request_id'],
                       'parameters': {'status': RequestStatus.ToResume,
                                      'locking': RequestLocking.Idle,
                                      'errors': req['errors'] if req['errors'] else {}}}
            ret_req['parameters']['errors'].update(error)
        return ret_req

    def process_resume_request(self, event):
        self.number_workers += 1
        try:
            if event:
                req = self.get_request(request_id=event.request_id, locking=True)
                if req['status'] in [RequestStatus.Finished]:
                    ret = {'request_id': req['request_id'],
                           'parameters': {'locking': RequestLocking.Idle,
                                          'errors': {'extra_msg': "Request is already finished. Cannot be resumed"}}}
                    if 'msg' in req['errors']:
                        ret['parameters']['errors']['msg'] = req['errors']['msg']
                    self.update_request(ret)
                else:
                    ret = self.handle_resume_request(req)
                    self.update_request(ret)
                    wf = req['request_metadata']['workflow']
                    works = wf.get_all_works()
                    if works:
                        for work in works:
                            event = ResumeTransformEvent(publisher_id=self.id, transform_id=work.get_work_id())
                            self.event_bus.send(event)
                    else:
                        event = UpdateRequestEvent(publisher_id=self.id, request_id=req['request_id'])
                        self.event_bus.send(event)
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
        self.number_workers -= 1

    def clean_locks(self):
        self.logger.info("clean locking")
        core_requests.clean_locking()

    def init_event_function_map(self):
        self.event_func_map = {
            NewRequestEvent._event_type: {
                'pre_check': self.is_ok_to_run_more_requests,
                'exec_func': self.process_new_request
            },
            UpdateRequestEvent._event_type: {
                'pre_check': self.is_ok_to_run_more_requests,
                'exec_func': self.process_update_request
            },
            AbortRequestEvent._event_type: {
                'pre_check': self.is_ok_to_run_more_requests,
                'exec_func': self.process_abort_request
            },
            ResumeRequestEvent._event_type: {
                'pre_check': self.is_ok_to_run_more_requests,
                'exec_func': self.process_resume_request
            }
        }

    def run(self):
        """
        Main run function.
        """
        try:
            self.logger.info("Starting main thread")

            self.load_plugins()

            self.add_default_tasks()

            self.init_event_function_map()

            task = self.create_task(task_func=self.get_new_requests, task_output_queue=None, task_args=tuple(), task_kwargs={}, delay_time=10, priority=1)
            self.add_task(task)
            task = self.create_task(task_func=self.get_running_requests, task_output_queue=None, task_args=tuple(), task_kwargs={}, delay_time=60, priority=1)
            self.add_task(task)
            task = self.create_task(task_func=self.get_operation_requests, task_output_queue=None, task_args=tuple(), task_kwargs={}, delay_time=10, priority=1)
            self.add_task(task)
            task = self.create_task(task_func=self.clean_locks, task_output_queue=None, task_args=tuple(), task_kwargs={}, delay_time=1800, priority=1)
            self.add_task(task)

            self.execute()
        except KeyboardInterrupt:
            self.stop()


if __name__ == '__main__':
    agent = Clerk()
    agent()
