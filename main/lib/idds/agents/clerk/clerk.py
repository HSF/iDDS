#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019 - 2022

import datetime
import random
import time
import traceback

from idds.common import exceptions
from idds.common.constants import (Sections, RequestStatus, RequestLocking,
                                   TransformStatus, CommandType,
                                   CommandStatus, CommandLocking)
from idds.common.utils import setup_logging, truncate_string
from idds.core import (requests as core_requests,
                       transforms as core_transforms,
                       commands as core_commands)
from idds.agents.common.baseagent import BaseAgent
from idds.agents.common.eventbus.event import (EventType,
                                               NewRequestEvent,
                                               UpdateRequestEvent,
                                               AbortRequestEvent,
                                               ResumeRequestEvent,
                                               NewTransformEvent,
                                               UpdateTransformEvent,
                                               AbortTransformEvent,
                                               ResumeTransformEvent,
                                               ExpireRequestEvent)

setup_logging(__name__)


class Clerk(BaseAgent):
    """
    Clerk works to process requests and converts requests to transforms.
    """

    def __init__(self, num_threads=1, poll_period=10, retrieve_bulk_size=10, pending_time=None, **kwargs):
        super(Clerk, self).__init__(num_threads=num_threads, name='Clerk', **kwargs)
        self.poll_period = int(poll_period)
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

        if not hasattr(self, 'new_poll_period') or not self.new_poll_period:
            self.new_poll_period = self.poll_period
        else:
            self.new_poll_period = int(self.new_poll_period)
        if not hasattr(self, 'update_poll_period') or not self.update_poll_period:
            self.update_poll_period = self.poll_period
        else:
            self.update_poll_period = int(self.update_poll_period)

        if hasattr(self, 'poll_period_increase_rate'):
            self.poll_period_increase_rate = float(self.poll_period_increase_rate)
        else:
            self.poll_period_increase_rate = 2

        if hasattr(self, 'max_new_poll_period'):
            self.max_new_poll_period = int(self.max_new_poll_period)
        else:
            self.max_new_poll_period = 3600 * 6
        if hasattr(self, 'max_update_poll_period'):
            self.max_update_poll_period = int(self.max_update_poll_period)
        else:
            self.max_update_poll_period = 3600 * 6

        if not hasattr(self, 'new_command_poll_period') or not self.new_command_poll_period:
            self.new_command_poll_period = 1
        else:
            self.new_command_poll_period = int(self.new_command_poll_period)
        if not hasattr(self, 'update_command_poll_period') or not self.update_command_poll_period:
            self.update_command_poll_period = self.poll_period
        else:
            self.update_command_poll_period = int(self.update_command_poll_period)

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
        if self.number_workers > 0:
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

            req_status = [RequestStatus.New, RequestStatus.Extend, RequestStatus.Built]
            reqs_new = core_requests.get_requests_by_status_type(status=req_status, locking=True,
                                                                 not_lock=True,
                                                                 new_poll=True, only_return_id=True,
                                                                 bulk_size=self.retrieve_bulk_size)

            # self.logger.debug("Main thread get %s [New+Extend] requests to process" % len(reqs_new))
            if reqs_new:
                self.logger.info("Main thread get [New+Extend] requests to process: %s" % str(reqs_new))

            for req_id in reqs_new:
                event = NewRequestEvent(publisher_id=self.id, request_id=req_id)
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
                          RequestStatus.ToResume, RequestStatus.Resuming,
                          RequestStatus.Building]
            reqs = core_requests.get_requests_by_status_type(status=req_status, time_period=None,
                                                             locking=True, bulk_size=self.retrieve_bulk_size,
                                                             not_lock=True, update_poll=True, only_return_id=True)

            # self.logger.debug("Main thread get %s Transforming requests to running" % len(reqs))
            if reqs:
                self.logger.info("Main thread get Transforming requests to running: %s" % str(reqs))

            for req_id in reqs:
                event = UpdateRequestEvent(publisher_id=self.id, request_id=req_id)
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

            status = [CommandStatus.New]
            new_commands = core_commands.get_commands_by_status(status=status, locking=True, period=self.new_command_poll_period)
            status = [CommandStatus.Processing]
            processing_commands = core_commands.get_commands_by_status(status=status, locking=True,
                                                                       period=self.update_command_poll_period)
            commands = new_commands + processing_commands

            # self.logger.debug("Main thread get %s commands" % len(commands))
            if commands:
                self.logger.info("Main thread get %s commands" % len(commands))

            update_commands = []
            for cmd in commands:
                request_id = cmd['request_id']
                cmd_content = cmd['cmd_content']
                cmd_type = cmd['cmd_type']
                cmd_status = cmd['status']
                event_content = {'request_id': request_id,
                                 'cmd_type': cmd_type,
                                 'cmd_id': cmd['cmd_id'],
                                 'cmd_content': cmd_content}

                event = None
                if cmd_status in [CommandStatus.New, CommandStatus.Processing]:
                    if cmd_type in [CommandType.AbortRequest]:
                        event = AbortRequestEvent(publisher_id=self.id, request_id=request_id, content=event_content)
                    elif cmd_type in [CommandType.ResumeRequest]:
                        event = ResumeRequestEvent(publisher_id=self.id, request_id=request_id, content=event_content)
                # elif cmd_status in [CommandStatus.Processing]:
                #     event = UpdateRequestEvent(publisher_id=self.id, request_id=request_id, content=event_content)

                if event:
                    self.event_bus.send(event)

                    u_command = {'cmd_id': cmd['cmd_id'],
                                 'status': CommandStatus.Processing,
                                 'locking': CommandLocking.Idle}
                    update_commands.append(u_command)
                else:
                    u_command = {'cmd_id': cmd['cmd_id'],
                                 'status': CommandStatus.UnknownCommand,
                                 'locking': CommandLocking.Idle}
                    update_commands.append(u_command)
                core_commands.update_commands(update_commands)
            return commands
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

    def get_work_tag_attribute(self, work_tag, attribute):
        work_tag_attribute = work_tag + "_" + attribute
        work_tag_attribute_value = None
        if hasattr(self, work_tag_attribute):
            work_tag_attribute_value = int(getattr(self, work_tag_attribute))
        return work_tag_attribute_value

    def generate_transform(self, req, work, build=False):
        if build:
            wf = req['request_metadata']['build_workflow']
        else:
            wf = req['request_metadata']['workflow']

        work.set_request_id(req['request_id'])
        work.username = req['username']

        transform_tag = work.get_work_tag()
        if req['max_new_retries']:
            max_new_retries = req['max_new_retries']
        else:
            work_tag_max_new_retries = self.get_work_tag_attribute(transform_tag, "max_new_retries")
            if work_tag_max_new_retries:
                max_new_retries = work_tag_max_new_retries
            else:
                max_new_retries = self.max_new_retries

        if req['max_update_retries']:
            max_update_retries = req['max_update_retries']
        else:
            work_tag_max_update_retries = self.get_work_tag_attribute(transform_tag, "max_update_retries")
            if work_tag_max_update_retries:
                max_update_retries = work_tag_max_update_retries
            else:
                max_update_retries = self.max_update_retries

        new_transform = {'request_id': req['request_id'],
                         'workload_id': req['workload_id'],
                         'transform_type': work.get_work_type(),
                         'transform_tag': work.get_work_tag(),
                         'priority': req['priority'],
                         'status': TransformStatus.New,
                         'retries': 0,
                         'name': work.get_work_name(),
                         'new_poll_period': self.new_poll_period,
                         'update_poll_period': self.update_poll_period,
                         'max_new_retries': max_new_retries,
                         'max_update_retries': max_update_retries,
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

    def get_log_prefix(self, req):
        return "<request_id=%s>" % req['request_id']

    def handle_new_request(self, req):
        try:
            log_pre = self.get_log_prefix(req)
            self.logger.info(log_pre + "Handle new request")
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
            self.logger.debug(log_pre + "Processing request(%s): new transforms: %s" % (req['request_id'],
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
            self.logger.info(log_pre + "Handle new request result: %s" % str(ret_req))
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            retries = req['new_retries'] + 1
            if not req['max_new_retries'] or retries < req['max_new_retries']:
                req_status = req['status']
            else:
                req_status = RequestStatus.Failed

            # increase poll period
            new_poll_period = int(req['new_poll_period'].total_seconds() * self.poll_period_increase_rate)
            if new_poll_period > self.max_new_poll_period:
                new_poll_period = self.max_new_poll_period

            error = {'submit_err': {'msg': truncate_string('%s' % (ex), length=200)}}

            ret_req = {'request_id': req['request_id'],
                       'parameters': {'status': req_status,
                                      'locking': RequestLocking.Idle,
                                      'new_retries': retries,
                                      'new_poll_period': new_poll_period,
                                      'errors': req['errors'] if req['errors'] else {}}}
            ret_req['parameters']['errors'].update(error)
            self.logger.warn(log_pre + "Handle new request error result: %s" % str(ret_req))
        return ret_req

    def has_to_build_work(self, req):
        try:
            if req['status'] in [RequestStatus.New] and 'build_workflow' in req['request_metadata']:
                log_pre = self.get_log_prefix(req)
                self.logger.info(log_pre + "has build work")
                return True
                # workflow = req['request_metadata']['build_workflow']
                # if workflow.has_to_build_work():
                #     log_pre = self.get_log_prefix(req)
                #     self.logger.info(log_pre + "has to_build work")
                #     return True
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
        return False

    def handle_build_request(self, req):
        try:
            log_pre = self.get_log_prefix(req)
            self.logger.info(log_pre + "handle build request")

            workflow = req['request_metadata']['build_workflow']
            works = workflow.get_new_works()
            transforms = []
            for work in works:
                new_work = work
                new_work.add_proxy(workflow.get_proxy())
                transform = self.generate_transform(req, new_work, build=True)
                transforms.append(transform)
            self.logger.debug(log_pre + "Processing request(%s): new build transforms: %s" % (req['request_id'],
                                                                                              str(transforms)))

            ret_req = {'request_id': req['request_id'],
                       'parameters': {'status': RequestStatus.Building,
                                      'locking': RequestLocking.Idle,
                                      # 'processing_metadata': processing_metadata,
                                      'request_metadata': req['request_metadata']},
                       'new_transforms': transforms}
            ret_req['parameters'] = self.load_poll_period(req, ret_req['parameters'])
            self.logger.info(log_pre + "Handle build request result: %s" % str(ret_req))
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            retries = req['new_retries'] + 1
            if not req['max_new_retries'] or retries < req['max_new_retries']:
                req_status = req['status']
            else:
                req_status = RequestStatus.Failed

            # increase poll period
            new_poll_period = int(req['new_poll_period'].total_seconds() * self.poll_period_increase_rate)
            if new_poll_period > self.max_new_poll_period:
                new_poll_period = self.max_new_poll_period

            error = {'submit_err': {'msg': truncate_string('%s' % (ex), length=200)}}

            ret_req = {'request_id': req['request_id'],
                       'parameters': {'status': req_status,
                                      'locking': RequestLocking.Idle,
                                      'new_retries': retries,
                                      'new_poll_period': new_poll_period,
                                      'errors': req['errors'] if req['errors'] else {}}}
            ret_req['parameters']['errors'].update(error)
            self.logger.warn(log_pre + "Handle build request error result: %s" % str(ret_req))
        return ret_req

    def update_request(self, req):
        new_tf_ids, update_tf_ids = [], []
        try:
            log_pre = self.get_log_prefix(req)
            self.logger.info(log_pre + "update request: %s" % req)
            req['parameters']['locking'] = RequestLocking.Idle
            req['parameters']['updated_at'] = datetime.datetime.utcnow()

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
                            if retry_num <= 1:
                                random_sleep = random.randint(1, 10)
                            elif retry_num <= 2:
                                random_sleep = random.randint(1, 60)
                            else:
                                random_sleep = random.randint(1, 120)
                            time.sleep(random_sleep)
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
                self.logger.warn(log_pre + "Update request in exception: %s" % str(req_parameters))
                core_requests.update_request_with_transforms(req['request_id'], req_parameters)
            except Exception as ex:
                self.logger.error(ex)
                self.logger.error(traceback.format_exc())
        return new_tf_ids, update_tf_ids

    def process_new_request(self, event):
        self.number_workers += 1
        try:
            if event:
                req_status = [RequestStatus.New, RequestStatus.Extend, RequestStatus.Built]
                req = self.get_request(request_id=event._request_id, status=req_status, locking=True)
                if not req:
                    self.logger.error("Cannot find request for event: %s" % str(event))
                elif req:
                    log_pre = self.get_log_prefix(req)
                    if self.has_to_build_work(req):
                        ret = self.handle_build_request(req)
                    else:
                        ret = self.handle_new_request(req)
                    new_tf_ids, update_tf_ids = self.update_request(ret)
                    for tf_id in new_tf_ids:
                        self.logger.info(log_pre + "NewTransformEvent(transform_id: %s)" % str(tf_id))
                        event = NewTransformEvent(publisher_id=self.id, transform_id=tf_id)
                        self.event_bus.send(event)
                        time.sleep(1)
                    for tf_id in update_tf_ids:
                        self.logger.info(log_pre + "UpdateTransformEvent(transform_id: %s)" % str(tf_id))
                        event = UpdateTransformEvent(publisher_id=self.id, transform_id=tf_id)
                        self.event_bus.send(event)
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
        self.number_workers -= 1

    def handle_update_request_real(self, req, event):
        """
        process running request
        """
        log_pre = self.get_log_prefix(req)
        self.logger.info(log_pre + " handle_update_request: request_id: %s" % req['request_id'])
        if 'workflow' in req['request_metadata']:
            wf = req['request_metadata']['workflow']
        else:
            wf = req['request_metadata']['build_workflow']

        to_abort = False
        to_abort_transform_id = None
        if (event and event._content and 'cmd_type' in event._content and event._content['cmd_type']
            and event._content['cmd_type'] in [CommandType.AbortRequest, CommandType.ExpireRequest]):    # noqa W503
            to_abort = True
            self.logger.info(log_pre + "to_abort: %s" % to_abort)
            if (event and event._content and 'cmd_content' in event._content and event._content['cmd_content']
                and 'transform_id' in event._content['cmd_content']):                                    # noqa W503
                to_abort_transform_id = event._content['cmd_content']['transform_id']
                self.logger.info(log_pre + "to_abort_transform_id: %s" % to_abort_transform_id)

        if to_abort and not to_abort_transform_id:
            wf.to_cancel = True

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
                work.sync_work_data(status=tf['status'], substatus=tf['substatus'], work=transform_work, workload_id=tf['workload_id'])
                self.logger.info(log_pre + "transform status: %s, work status: %s" % (tf['status'], work.status))
        wf.refresh_works()

        new_transforms = []
        if req['status'] in [RequestStatus.Transforming] and not wf.to_cancel:
            # new works
            works = wf.get_new_works()
            for work in works:
                # new_work = work.copy()
                new_work = work
                new_work.add_proxy(wf.get_proxy())
                new_transform = self.generate_transform(req, new_work)
                new_transforms.append(new_transform)
            self.logger.debug(log_pre + " Processing request(%s): new transforms: %s" % (req['request_id'], str(new_transforms)))

        req_status = RequestStatus.Transforming
        if wf.is_terminated():
            if wf.is_finished(synchronize=False):
                req_status = RequestStatus.Finished
            else:
                if to_abort and not to_abort_transform_id:
                    req_status = RequestStatus.Cancelled
                elif wf.is_expired(synchronize=False):
                    req_status = RequestStatus.Expired
                elif wf.is_subfinished(synchronize=False):
                    req_status = RequestStatus.SubFinished
                elif wf.is_failed(synchronize=False):
                    req_status = RequestStatus.Failed
                else:
                    req_status = RequestStatus.Failed
            # req_msg = wf.get_terminated_msg()
        else:
            if wf.is_to_expire(req['expired_at'], self.pending_time, request_id=req['request_id']):
                wf.expired = True
                event_content = {'request_id': req['request_id'],
                                 'cmd_type': CommandType.ExpireRequest,
                                 'cmd_content': {}}
                self.logger.debug(log_pre + "ExpireRequestEvent(request_id: %s)" % req['request_id'])
                event = ExpireRequestEvent(publisher_id=self.id, request_id=req['request_id'], content=event_content)
                self.event_bus.send(event)

        parameters = {'status': req_status,
                      'locking': RequestLocking.Idle,
                      'request_metadata': req['request_metadata']
                      }
        parameters = self.load_poll_period(req, parameters)

        ret = {'request_id': req['request_id'],
               'parameters': parameters,
               'new_transforms': new_transforms}   # 'update_transforms': update_transforms}
        self.logger.info(log_pre + "Handle update request result: %s" % str(ret))
        return ret

    def handle_update_build_request_real(self, req, event):
        """
        process build request
        """
        log_pre = self.get_log_prefix(req)
        self.logger.info(log_pre + " handle_update_build_request: request_id: %s" % req['request_id'])
        wf = req['request_metadata']['build_workflow']

        to_abort = False
        to_abort_transform_id = None
        if (event and event._content and 'cmd_type' in event._content and event._content['cmd_type']
            and event._content['cmd_type'] in [CommandType.AbortRequest, CommandType.ExpireRequest]):    # noqa W503
            to_abort = True
            self.logger.info(log_pre + "to_abort: %s" % to_abort)
            if (event and event._content and 'cmd_content' in event._content and event._content['cmd_content']
                and 'transform_id' in event._content['cmd_content']):                                    # noqa W503
                to_abort_transform_id = event._content['cmd_content']['transform_id']
                self.logger.info(log_pre + "to_abort_transform_id: %s" % to_abort_transform_id)

        if to_abort and not to_abort_transform_id:
            wf.to_cancel = True

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
                work.sync_work_data(status=tf['status'], substatus=tf['substatus'], work=transform_work, workload_id=tf['workload_id'])
                self.logger.info(log_pre + "transform status: %s, work status: %s" % (tf['status'], work.status))
        wf.refresh_works()

        new_transforms = []
        if req['status'] in [RequestStatus.Building] and not wf.to_cancel:
            # new works
            works = wf.get_new_works()
            for work in works:
                # new_work = work.copy()
                new_work = work
                new_work.add_proxy(wf.get_proxy())
                new_transform = self.generate_transform(req, new_work, build=True)
                new_transforms.append(new_transform)
            self.logger.debug(log_pre + " Processing build request(%s): new transforms: %s" % (req['request_id'], str(new_transforms)))

        req_status = RequestStatus.Building
        if wf.is_terminated():
            if wf.is_finished(synchronize=False):
                req_status = RequestStatus.Failed
            else:
                if to_abort and not to_abort_transform_id:
                    req_status = RequestStatus.Cancelled
                elif wf.is_expired(synchronize=False):
                    req_status = RequestStatus.Expired
                elif wf.is_subfinished(synchronize=False):
                    req_status = RequestStatus.SubFinished
                elif wf.is_failed(synchronize=False):
                    req_status = RequestStatus.Failed
                else:
                    req_status = RequestStatus.Failed
            # req_msg = wf.get_terminated_msg()
        else:
            if wf.is_to_expire(req['expired_at'], self.pending_time, request_id=req['request_id']):
                wf.expired = True
                event_content = {'request_id': req['request_id'],
                                 'cmd_type': CommandType.ExpireRequest,
                                 'cmd_content': {}}
                self.logger.debug(log_pre + "ExpireRequestEvent(request_id: %s)" % req['request_id'])
                event = ExpireRequestEvent(publisher_id=self.id, request_id=req['request_id'], content=event_content)
                self.event_bus.send(event)

        parameters = {'status': req_status,
                      'locking': RequestLocking.Idle,
                      'request_metadata': req['request_metadata']
                      }
        parameters = self.load_poll_period(req, parameters)

        ret = {'request_id': req['request_id'],
               'parameters': parameters,
               'new_transforms': new_transforms}   # 'update_transforms': update_transforms}
        self.logger.info(log_pre + "Handle update request result: %s" % str(ret))
        return ret

    def handle_update_request(self, req, event):
        """
        process running request
        """
        try:
            # if self.release_helper:
            #     self.release_inputs(req['request_id'])
            if req['status'] in [RequestStatus.Building]:
                ret_req = self.handle_update_build_request_real(req, event=event)
            else:
                ret_req = self.handle_update_request_real(req, event)
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            retries = req['update_retries'] + 1
            if not req['max_update_retries'] or retries < req['max_update_retries']:
                req_status = req['status']
            else:
                req_status = RequestStatus.Failed
            error = {'update_err': {'msg': truncate_string('%s' % (ex), length=200)}}

            # increase poll period
            update_poll_period = int(req['update_poll_period'].total_seconds() * self.poll_period_increase_rate)
            if update_poll_period > self.max_update_poll_period:
                update_poll_period = self.max_update_poll_period

            ret_req = {'request_id': req['request_id'],
                       'parameters': {'status': req_status,
                                      'locking': RequestLocking.Idle,
                                      'update_retries': retries,
                                      'update_poll_period': update_poll_period,
                                      'errors': req['errors'] if req['errors'] else {}}}
            ret_req['parameters']['errors'].update(error)
            log_pre = self.get_log_prefix(req)
            self.logger.warn(log_pre + "Handle new request exception result: %s" % str(ret_req))
        return ret_req

    def process_update_request(self, event):
        self.number_workers += 1
        try:
            if event:
                req_status = [RequestStatus.Transforming, RequestStatus.ToCancel, RequestStatus.Cancelling,
                              RequestStatus.ToSuspend, RequestStatus.Suspending,
                              RequestStatus.ToExpire, RequestStatus.Expiring,
                              RequestStatus.ToFinish, RequestStatus.ToForceFinish,
                              RequestStatus.ToResume, RequestStatus.Resuming,
                              RequestStatus.Building]

                req = self.get_request(request_id=event._request_id, status=req_status, locking=True)
                if not req:
                    self.logger.error("Cannot find request for event: %s" % str(event))
                else:
                    log_pre = self.get_log_prefix(req)
                    ret = self.handle_update_request(req, event=event)
                    new_tf_ids, update_tf_ids = self.update_request(ret)
                    for tf_id in new_tf_ids:
                        self.logger.info(log_pre + "NewTransformEvent(transform_id: %s)" % tf_id)
                        event = NewTransformEvent(publisher_id=self.id, transform_id=tf_id, content=event._content)
                        self.event_bus.send(event)
                    for tf_id in update_tf_ids:
                        self.logger.info(log_pre + "UpdateTransformEvent(transform_id: %s)" % tf_id)
                        event = UpdateTransformEvent(publisher_id=self.id, transform_id=tf_id, content=event._content)
                        self.event_bus.send(event)
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
        self.number_workers -= 1

    def handle_abort_request(self, req, event):
        """
        process abort request
        """
        try:
            log_pre = self.get_log_prefix(req)
            self.logger.info(log_pre + "handle_abort_request event: %s" % str(event))

            to_abort = False
            to_abort_transform_id = None
            if (event and event._content and 'cmd_type' in event._content and event._content['cmd_type']
                and event._content['cmd_type'] in [CommandType.AbortRequest, CommandType.ExpireRequest]):    # noqa W503
                to_abort = True
                self.logger.info(log_pre + "to_abort: %s" % to_abort)
                if (event and event._content and 'cmd_content' in event._content and event._content['cmd_content']
                    and 'transform_id' in event._content['cmd_content']):                                    # noqa W503
                    to_abort_transform_id = event._content['cmd_content']['transform_id']
                    self.logger.info(log_pre + "to_abort_transform_id: %s" % to_abort_transform_id)

            if to_abort and to_abort_transform_id:
                req_status = req['status']
            else:
                if req['status'] in [RequestStatus.Building]:
                    wf = req['request_metadata']['build_workflow']
                else:
                    if 'workflow' in req['request_metadata']:
                        wf = req['request_metadata']['workflow']
                    else:
                        wf = req['request_metadata']['build_workflow']
                wf.to_cancel = True
                req_status = RequestStatus.Cancelling

            ret_req = {'request_id': req['request_id'],
                       'parameters': {'status': req_status,
                                      'locking': RequestLocking.Idle,
                                      'request_metadata': req['request_metadata']},
                       }
            self.logger.info(log_pre + "handle_abort_request result: %s" % str(ret_req))
            return ret_req
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            error = {'abort_err': {'msg': truncate_string('%s' % (ex), length=200)}}
            ret_req = {'request_id': req['request_id'],
                       'parameters': {'status': RequestStatus.ToCancel,
                                      'locking': RequestLocking.Idle,
                                      'errors': req['errors'] if req['errors'] else {}}}
            ret_req['parameters']['errors'].update(error)
            self.logger.info(log_pre + "handle_abort_request exception result: %s" % str(ret_req))
        return ret_req

    def handle_command(self, event, cmd_status, errors=None):
        if (event and event._content and 'cmd_id' in event._content and event._content['cmd_id']):
            u_command = {'cmd_id': event._content['cmd_id'],
                         'status': cmd_status,
                         'locking': CommandLocking.Idle}
            if errors:
                u_command['errors'] = errors
            core_commands.update_commands([u_command])

    def process_abort_request(self, event):
        self.number_workers += 1
        try:
            if event:
                req = self.get_request(request_id=event._request_id, locking=True)
                if not req:
                    self.logger.error("Cannot find request for event: %s" % str(event))
                else:
                    log_pre = self.get_log_prefix(req)
                    self.logger.info(log_pre + "process_abort_request event: %s" % str(event))

                    if req['status'] in [RequestStatus.Finished, RequestStatus.SubFinished,
                                         RequestStatus.Failed, RequestStatus.Cancelled,
                                         RequestStatus.Suspended, RequestStatus.Expired]:
                        ret = {'request_id': req['request_id'],
                               'parameters': {'locking': RequestLocking.Idle,
                                              'errors': {'extra_msg': "Request is already terminated. Cannot be aborted"}}}
                        if req['errors'] and 'msg' in req['errors']:
                            ret['parameters']['errors']['msg'] = req['errors']['msg']
                        self.logger.info(log_pre + "process_abort_request result: %s" % str(ret))
                        self.update_request(ret)
                        self.handle_command(event, cmd_status=CommandStatus.Failed, errors="Request is already terminated. Cannot be aborted")
                    else:
                        ret = self.handle_abort_request(req, event)
                        self.logger.info(log_pre + "process_abort_request result: %s" % str(ret))
                        self.update_request(ret)
                        to_abort_transform_id = None
                        if event and event._content and event._content['cmd_content'] and 'transform_id' in event._content['cmd_content']:
                            to_abort_transform_id = event._content['cmd_content']['transform_id']

                        if req['status'] in [RequestStatus.Building]:
                            wf = req['request_metadata']['build_workflow']
                        else:
                            if 'workflow' in req['request_metadata']:
                                wf = req['request_metadata']['workflow']
                            else:
                                wf = req['request_metadata']['build_workflow']
                        works = wf.get_all_works()
                        if works:
                            for work in works:
                                if (work.is_started() or work.is_starting()) and not work.is_terminated():
                                    if not to_abort_transform_id or to_abort_transform_id == work.get_work_id():
                                        self.logger.info(log_pre + "AbortTransformEvent(transform_id: %s)" % str(work.get_work_id()))
                                        event = AbortTransformEvent(publisher_id=self.id,
                                                                    transform_id=work.get_work_id(),
                                                                    content=event._content)
                                        self.event_bus.send(event)
                        else:
                            # no works. should trigger update request
                            self.logger.info(log_pre + "UpdateRequestEvent(request_id: %s)" % str(req['request_id']))
                            event = UpdateRequestEvent(publisher_id=self.id, request_id=req['request_id'], content=event._content)
                            self.event_bus.send(event)

                        self.handle_command(event, cmd_status=CommandStatus.Processed, errors=None)
        except AssertionError as ex:
            self.logger.error("process_abort_request, Failed to process event: %s" % str(event))
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            self.handle_command(event, cmd_status=CommandStatus.Processed, errors=str(ex))
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
        self.number_workers -= 1

    def handle_resume_request(self, req):
        """
        process resume request
        """
        try:
            req_status = RequestStatus.Resuming

            processing_metadata = req['processing_metadata']

            wf = req['request_metadata']['workflow']
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
            error = {'abort_err': {'msg': truncate_string('%s' % (ex), length=200)}}
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
                req = self.get_request(request_id=event._request_id, locking=True)
                if not req:
                    self.logger.error("Cannot find request for event: %s" % str(event))
                else:
                    log_pre = self.get_log_prefix(req)
                    self.logger.info(log_pre + "process_resume_request event: %s" % str(event))

                    if req['status'] in [RequestStatus.Finished]:
                        ret = {'request_id': req['request_id'],
                               'parameters': {'locking': RequestLocking.Idle,
                                              'errors': {'extra_msg': "Request is already finished. Cannot be resumed"}}}
                        if req['errors'] and 'msg' in req['errors']:
                            ret['parameters']['errors']['msg'] = req['errors']['msg']
                        self.logger.info(log_pre + "process_resume_request result: %s" % str(ret))

                        self.update_request(ret)
                        self.handle_command(event, cmd_status=CommandStatus.Failed, errors="Request is already finished. Cannot be resumed")
                    else:
                        ret = self.handle_resume_request(req)
                        self.logger.info(log_pre + "process_resume_request result: %s" % str(ret))

                        self.update_request(ret)
                        wf = req['request_metadata']['workflow']
                        works = wf.get_all_works()
                        if works:
                            for work in works:
                                # if not work.is_finished():
                                self.logger.info(log_pre + "ResumeTransformEvent(transform_id: %s)" % str(work.get_work_id()))
                                event = ResumeTransformEvent(publisher_id=self.id,
                                                             transform_id=work.get_work_id(),
                                                             content=event._content)
                                self.event_bus.send(event)
                        else:
                            self.logger.info(log_pre + "UpdateRequestEvent(request_id: %s)" % str(req['request_id']))
                            event = UpdateRequestEvent(publisher_id=self.id, request_id=req['request_id'], content=event._content)
                            self.event_bus.send(event)

                        self.handle_command(event, cmd_status=CommandStatus.Processed, errors=None)
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
        self.number_workers -= 1

    def clean_locks(self):
        self.logger.info("clean locking")
        core_requests.clean_locking()

    def init_event_function_map(self):
        self.event_func_map = {
            EventType.NewRequest: {
                'pre_check': self.is_ok_to_run_more_requests,
                'exec_func': self.process_new_request
            },
            EventType.UpdateRequest: {
                'pre_check': self.is_ok_to_run_more_requests,
                'exec_func': self.process_update_request
            },
            EventType.AbortRequest: {
                'pre_check': self.is_ok_to_run_more_requests,
                'exec_func': self.process_abort_request
            },
            EventType.ExpireRequest: {
                'pre_check': self.is_ok_to_run_more_requests,
                'exec_func': self.process_abort_request
            },
            EventType.ResumeRequest: {
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
