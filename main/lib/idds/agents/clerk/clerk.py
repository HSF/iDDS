#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019 - 2024

import datetime
import random
import time
import traceback

from idds.common import exceptions
from idds.common.constants import (Sections, ReturnCode,
                                   RequestType, RequestStatus, RequestLocking,
                                   TransformType, WorkflowType, ConditionStatus,
                                   TransformStatus, ProcessingStatus,
                                   ContentStatus, ContentRelationType,
                                   CommandType, CommandStatus, CommandLocking)
from idds.common.utils import setup_logging, truncate_string, str_to_date
from idds.core import (requests as core_requests,
                       transforms as core_transforms,
                       processings as core_processings,
                       catalog as core_catalog,
                       throttlers as core_throttlers,
                       commands as core_commands)
from idds.agents.common.baseagent import BaseAgent
from idds.agents.common.eventbus.event import (EventType,
                                               NewRequestEvent,
                                               UpdateRequestEvent,
                                               AbortRequestEvent,
                                               CloseRequestEvent,
                                               ResumeRequestEvent,
                                               NewTransformEvent,
                                               UpdateTransformEvent,
                                               AbortTransformEvent,
                                               ResumeTransformEvent,
                                               ExpireRequestEvent)

from idds.agents.common.cache.redis import get_redis_cache


setup_logging(__name__)


class Clerk(BaseAgent):
    """
    Clerk works to process requests and converts requests to transforms.
    """

    def __init__(self, num_threads=1, max_number_workers=8, poll_period=10, retrieve_bulk_size=10, cache_expire_seconds=300, pending_time=None, **kwargs):
        self.max_number_workers = max_number_workers
        self.set_max_workers()
        num_threads = self.max_number_workers
        super(Clerk, self).__init__(num_threads=num_threads, name='Clerk', **kwargs)
        self.poll_period = int(poll_period)
        self.retrieve_bulk_size = int(retrieve_bulk_size)
        self.config_section = Sections.Clerk
        self.start_at = time.time()

        if pending_time:
            self.pending_time = float(pending_time)
        else:
            self.pending_time = None

        self.cache_expire_seconds = int(cache_expire_seconds)

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
        if not hasattr(self, 'throttle_poll_period') or not self.throttle_poll_period:
            self.throttle_poll_period = self.poll_period
        else:
            self.throttle_poll_period = int(self.new_poll_period)

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

        self.show_queue_size_time = None

    def is_ok_to_run_more_requests(self):
        if self.number_workers >= self.max_number_workers:
            return False
        return True

    def show_queue_size(self):
        if self.show_queue_size_time is None or time.time() - self.show_queue_size_time >= 600:
            self.show_queue_size_time = time.time()
            q_str = "min request_id: %s, number of requests: %s, max number of requests: %s" % (BaseAgent.min_request_id,
                                                                                                self.number_workers,
                                                                                                self.max_number_workers)
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

            if time.time() < self.start_at + 3600:
                if BaseAgent.poll_new_min_request_id_times % 30 == 0:
                    # get_new_requests is called every 10 seconds. 30 * 10 = 300 seconds, which is 5 minutes.
                    if BaseAgent.min_request_id:
                        min_request_id = BaseAgent.min_request_id - 1000
                    else:
                        min_request_id = None
                else:
                    min_request_id = BaseAgent.min_request_id
            else:
                if BaseAgent.poll_new_min_request_id_times % 180 == 0:
                    # get_new_requests is called every 10 seconds. 180 * 10 = 300 seconds, which is 30 minutes.
                    if BaseAgent.min_request_id:
                        min_request_id = BaseAgent.min_request_id - 1000
                    else:
                        min_request_id = None
                else:
                    min_request_id = BaseAgent.min_request_id

            BaseAgent.poll_new_min_request_id_times += 1

            req_status = [RequestStatus.New, RequestStatus.Extend, RequestStatus.Built, RequestStatus.Throttling]
            reqs_new = core_requests.get_requests_by_status_type(status=req_status, locking=True,
                                                                 not_lock=True, min_request_id=min_request_id,
                                                                 new_poll=True, only_return_id=True,
                                                                 bulk_size=self.retrieve_bulk_size)

            # self.logger.debug("Main thread get %s [New+Extend] requests to process" % len(reqs_new))
            if reqs_new:
                self.logger.info("Main thread get [New+Extend] requests to process: %s" % str(reqs_new))

            events = []
            for req_id in reqs_new:
                BaseAgent.min_request_id_cache[req_id] = time.time()
                if BaseAgent.min_request_id is None or BaseAgent.min_request_id > req_id:
                    BaseAgent.min_request_id = req_id
                    self.logger.info("new min_request_id: %s" % BaseAgent.min_request_id)
                    core_requests.set_min_request_id(BaseAgent.min_request_id)

                event = NewRequestEvent(publisher_id=self.id, request_id=req_id)
                events.append(event)
            self.event_bus.send_bulk(events)

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

            if time.time() < self.start_at + 3600:
                if BaseAgent.poll_running_min_request_id_times % 30 == 0:
                    # get_new_requests is called every 10 seconds. 30 * 10 = 300 seconds, which is 5 minutes.
                    if BaseAgent.min_request_id:
                        min_request_id = BaseAgent.min_request_id - 1000
                    else:
                        min_request_id = None
                else:
                    min_request_id = BaseAgent.min_request_id
            else:
                if BaseAgent.poll_running_min_request_id_times % 180 == 0:
                    # get_new_requests is called every 10 seconds. 180 * 10 = 1800 seconds, which is 30 minutes.
                    if BaseAgent.min_request_id:
                        min_request_id = BaseAgent.min_request_id - 1000
                    else:
                        min_request_id = None
                else:
                    min_request_id = BaseAgent.min_request_id

            BaseAgent.poll_running_min_request_id_times += 1

            req_status = [RequestStatus.Transforming, RequestStatus.ToCancel, RequestStatus.Cancelling,
                          RequestStatus.ToSuspend, RequestStatus.Suspending,
                          RequestStatus.ToExpire, RequestStatus.Expiring,
                          RequestStatus.ToFinish, RequestStatus.ToForceFinish,
                          RequestStatus.ToResume, RequestStatus.Resuming,
                          RequestStatus.Building, RequestStatus.ToClose]
            reqs = core_requests.get_requests_by_status_type(status=req_status, time_period=None,
                                                             min_request_id=min_request_id,
                                                             locking=True, bulk_size=self.retrieve_bulk_size,
                                                             not_lock=True, update_poll=True, only_return_id=True)

            # self.logger.debug("Main thread get %s Transforming requests to running" % len(reqs))
            if reqs:
                self.logger.info("Main thread get Transforming requests to running: %s" % str(reqs))

            events = []
            for req_id in reqs:
                BaseAgent.min_request_id_cache[req_id] = time.time()
                if BaseAgent.min_request_id is None or BaseAgent.min_request_id > req_id:
                    BaseAgent.min_request_id = req_id
                    self.logger.info("new min_request_id: %s" % BaseAgent.min_request_id)
                    core_requests.set_min_request_id(BaseAgent.min_request_id)

                event = UpdateRequestEvent(publisher_id=self.id, request_id=req_id)
                events.append(event)
            self.event_bus.send_bulk(events)

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

                if BaseAgent.min_request_id is None or BaseAgent.min_request_id > request_id:
                    BaseAgent.min_request_id = request_id
                    self.logger.info("new min_request_id: %s" % BaseAgent.min_request_id)
                    BaseAgent.min_request_id_cache[request_id] = time.time()
                    core_requests.set_min_request_id(BaseAgent.min_request_id)

                event = None
                if cmd_status in [CommandStatus.New, CommandStatus.Processing]:
                    if cmd_type in [CommandType.AbortRequest]:
                        event = AbortRequestEvent(publisher_id=self.id, request_id=request_id, content=event_content)
                    elif cmd_type in [CommandType.ResumeRequest]:
                        event = ResumeRequestEvent(publisher_id=self.id, request_id=request_id, content=event_content)
                    elif cmd_type in [CommandType.CloseRequest]:
                        event = CloseRequestEvent(publisher_id=self.id, request_id=request_id, content=event_content)
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

    def clean_min_request_id(self):
        try:
            if BaseAgent.checking_min_request_id_times <= 0:
                old_min_request_id = core_requests.get_min_request_id()
                self.logger.info("old_min_request_id: %s" % old_min_request_id)
                if not old_min_request_id:
                    min_request_id = 0
                else:
                    min_request_id = old_min_request_id - 1000
                BaseAgent.min_request_id = min_request_id
            else:
                for req_id in list(BaseAgent.min_request_id_cache.keys()):
                    time_stamp = BaseAgent.min_request_id_cache[req_id]
                    if time_stamp < time.time() - 12 * 3600:       # older than 12 hours
                        del BaseAgent.min_request_id_cache[req_id]

                if BaseAgent.min_request_id_cache:
                    min_request_id = min(list(BaseAgent.min_request_id_cache.keys()))
                    BaseAgent.min_request_id = min_request_id
                    core_requests.set_min_request_id(BaseAgent.min_request_id)

            BaseAgent.checking_min_request_id_times += 1
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())

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

    def load_poll_period(self, req, parameters, throttling=False):
        if self.new_poll_period and req['new_poll_period'] != self.new_poll_period:
            parameters['new_poll_period'] = self.new_poll_period
        if throttling:
            parameters['new_poll_period'] = self.throttle_poll_period
        if self.update_poll_period and req['update_poll_period'] != self.update_poll_period:
            parameters['update_poll_period'] = self.update_poll_period
        parameters['max_new_retries'] = req['max_new_retries'] if req['max_new_retries'] is not None else self.max_new_retries
        parameters['max_update_retries'] = req['max_update_retries'] if req['max_update_retries'] is not None else self.max_update_retries
        return parameters

    def get_work_tag_attribute(self, work_tag, attribute):
        work_tag_attribute_value = None
        if work_tag:
            work_tag_attribute = work_tag + "_" + attribute
            if hasattr(self, work_tag_attribute):
                work_tag_attribute_value = int(getattr(self, work_tag_attribute))
        return work_tag_attribute_value

    def generate_transform(self, req, work, build=False, iworkflow=False):
        if iworkflow:
            wf = None
        else:
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

        transform_type = TransformType.Workflow
        try:
            work_type = work.get_work_type()
            if work_type in [WorkflowType.iWorkflowLocal]:
                # no need to generate transform
                return None
            elif work_type in [WorkflowType.iWorkflow]:
                transform_type = TransformType.iWorkflow
            elif work_type in [WorkflowType.iWork]:
                transform_type = TransformType.iWork
            elif work_type in [WorkflowType.GenericWorkflow]:
                transform_type = TransformType.GenericWorkflow
            elif work_type in [WorkflowType.GenericWork]:
                transform_type = TransformType.GenericWork
        except Exception:
            pass

        has_previous_conditions = None
        try:
            if hasattr(work, 'get_previous_conditions'):
                work_previous_conditions = work.get_previous_conditions()
            if work_previous_conditions:
                has_previous_conditions = len(work_previous_conditions)
        except Exception:
            pass

        triggered_conditions = []
        untriggered_conditions = []
        try:
            if hasattr(work, 'get_following_conditions'):
                following_conditions = work.get_following_conditions()
                for cond in following_conditions:
                    untriggered_conditions.append(cond)
        except Exception:
            pass

        loop_index = None
        try:
            if hasattr(work, 'get_loop_index'):
                loop_index = work.get_loop_index()
        except Exception:
            pass

        # transform_status = TransformStatus.New
        transform_status = TransformStatus.Queue
        if has_previous_conditions:
            transform_status = TransformStatus.WaitForTrigger

        site = req['site']
        if not site:
            try:
                cloud = None
                if hasattr(work, 'task_cloud') and work.task_cloud:
                    cloud = work.task_cloud

                if hasattr(work, 'task_queue') and work.task_queue:
                    queue = work.task_queue
                elif hasattr(work, 'queue') and work.queue:
                    queue = work.queue
                else:
                    queue = None

                task_site = None
                if hasattr(work, 'task_site') and work.task_site:
                    task_site = work.task_site
                site = f"{cloud},{task_site},{queue}"
            except Exception:
                pass

        new_transform = {'request_id': req['request_id'],
                         'workload_id': req['workload_id'],
                         'transform_type': transform_type,
                         'transform_tag': work.get_work_tag(),
                         'priority': req['priority'],
                         'status': transform_status,
                         'retries': 0,
                         'parent_transform_id': None,
                         'previous_transform_id': None,
                         'name': work.get_work_name(),
                         'new_poll_period': self.new_poll_period,
                         'update_poll_period': self.update_poll_period,
                         'max_new_retries': max_new_retries,
                         'max_update_retries': max_update_retries,
                         # 'expired_at': req['expired_at'],
                         'expired_at': None,
                         'internal_id': work.internal_id,
                         'has_previous_conditions': has_previous_conditions,
                         'triggered_conditions': triggered_conditions,
                         'untriggered_conditions': untriggered_conditions,
                         'loop_index': loop_index,
                         'site': site,
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

    def generate_condition(self, req, cond):
        previous_works = cond.previous_works
        following_works = cond.following_works
        previous_transforms, following_transforms = [], []
        previous_transforms = previous_works
        following_transforms = following_works

        new_condition = {'request_id': req['request_id'],
                         'internal_id': cond.internal_id,
                         'status': ConditionStatus.WaitForTrigger,
                         'substatus': None,
                         'is_loop': False,
                         'loop_index': None,
                         'cloned_from': None,
                         'evaluate_result': None,
                         'previous_transforms': previous_transforms,
                         'following_transforms': following_transforms,
                         'condition': {'condition': cond}}
        return new_condition

    def get_num_active_requests(self, site_name):
        cache = get_redis_cache()
        num_requests = cache.get("num_requests", default=None)
        if num_requests is None:
            num_requests = {}
            active_status = [RequestStatus.New, RequestStatus.Ready, RequestStatus.Throttling]
            active_status1 = [RequestStatus.Transforming, RequestStatus.Terminating]
            rets = core_requests.get_num_active_requests(active_status + active_status1)
            for ret in rets:
                status, site, count = ret
                if site is None:
                    site = 'Default'
                if site not in num_requests:
                    num_requests[site] = {'new': 0, 'processing': 0}
                if status in active_status:
                    num_requests[site]['new'] += count
                elif status in active_status1:
                    num_requests[site]['processing'] += count
            cache.set("num_requests", num_requests, expire_seconds=self.cache_expire_seconds)
        default_value = {'new': 0, 'processing': 0}
        return num_requests.get(site_name, default_value)

    def get_num_active_transforms(self, site_name):
        cache = get_redis_cache()
        num_transforms = cache.get("num_transforms", default=None)
        if num_transforms is None:
            num_transforms = {}
            active_status = [TransformStatus.New, TransformStatus.Ready]
            active_status1 = [TransformStatus.Transforming, TransformStatus.Terminating]
            rets = core_transforms.get_num_active_transforms(active_status + active_status1)
            for ret in rets:
                status, site, count = ret
                if site is None:
                    site = 'Default'
                if site not in num_transforms:
                    num_transforms[site] = {'new': 0, 'processing': 0}
                if status in active_status:
                    num_transforms[site]['new'] += count
                elif status in active_status1:
                    num_transforms[site]['processing'] += count
            cache.set("num_transforms", num_transforms, expire_seconds=self.cache_expire_seconds)
        default_value = {'new': 0, 'processing': 0}
        return num_transforms.get(site_name, default_value)

    def get_num_active_processings(self, site_name):
        cache = get_redis_cache()
        num_processings = cache.get("num_processings", default=None)
        active_transforms = cache.get("active_transforms", default={})
        if num_processings is None:
            num_processings = {}
            active_transforms = {}
            active_status = [ProcessingStatus.New]
            active_status1 = [ProcessingStatus.Submitting, ProcessingStatus.Submitted,
                              ProcessingStatus.Running, ProcessingStatus.Terminating, ProcessingStatus.ToTrigger,
                              ProcessingStatus.Triggering]
            rets = core_processings.get_active_processings(active_status + active_status1)
            for ret in rets:
                req_id, trf_id, pr_id, site, status = ret
                if site is None:
                    site = 'Default'
                if site not in num_processings:
                    num_processings[site] = {'new': 0, 'processing': 0}
                    active_transforms[site] = []
                if status in active_status:
                    num_processings[site]['new'] += 1
                elif status in active_status1:
                    num_processings[site]['processing'] += 1
                active_transforms[site].append(trf_id)
            cache.set("num_processings", num_processings, expire_seconds=self.cache_expire_seconds)
            cache.set("active_transforms", active_transforms, expire_seconds=self.cache_expire_seconds)
        default_value = {'new': 0, 'processing': 0}
        return num_processings.get(site_name, default_value), active_transforms

    def get_num_active_contents(self, site_name, active_transform_ids):
        cache = get_redis_cache()
        # 1. input contents not terminated
        # 2. output contents not terminated
        tf_id_site_map = {}
        all_tf_ids = []
        for site in active_transform_ids:
            all_tf_ids += active_transform_ids[site]
            for tf_id in active_transform_ids[site]:
                tf_id_site_map[tf_id] = site

        num_input_contents = cache.get("num_input_contents", default=None)
        num_output_contents = cache.get("num_output_contents", default=None)
        if num_input_contents is None or num_output_contents is None:
            num_input_contents, num_output_contents = {}, {}
            if all_tf_ids:
                ret = core_catalog.get_content_status_statistics_by_relation_type(all_tf_ids)
                for item in ret:
                    status, relation_type, transform_id, count = item
                    site = tf_id_site_map[transform_id]
                    if site not in num_input_contents:
                        num_input_contents[site] = {'new': 0, 'activated': 0, 'processed': 0}
                        num_output_contents[site] = {'new': 0, 'activated': 0, 'processed': 0}
                    if status in [ContentStatus.New]:
                        if relation_type == ContentRelationType.Input:
                            num_input_contents[site]['new'] += count
                        elif relation_type == ContentRelationType.Output:
                            num_output_contents[site]['new'] += count
                    if status in [ContentStatus.Activated]:
                        if relation_type == ContentRelationType.Input:
                            num_input_contents[site]['activated'] += count
                        elif relation_type == ContentRelationType.Output:
                            num_output_contents[site]['activated'] += count
                    else:
                        if relation_type == ContentRelationType.Input:
                            num_input_contents[site]['processed'] += count
                        elif relation_type == ContentRelationType.Output:
                            num_output_contents[site]['processed'] += count

            cache.set("num_input_contents", num_input_contents, expire_seconds=self.cache_expire_seconds)
            cache.set("num_output_contents", num_output_contents, expire_seconds=self.cache_expire_seconds)
        default_value = {'new': 0, 'activated': 0, 'processed': 0}
        return num_input_contents.get(site_name, default_value), num_output_contents.get(site_name, default_value)

    def get_throttlers(self):
        cache = get_redis_cache()
        throttlers = cache.get("throttlers", default=None)
        if throttlers is None:
            throttler_items = core_throttlers.get_throttlers()
            throttlers = {}
            for item in throttler_items:
                throttlers[item['site']] = {'num_requests': item['num_requests'],
                                            'num_transforms': item['num_transforms'],
                                            'num_processings': item['num_processings'],
                                            'new_contents': item['new_contents'],
                                            'queue_contents': item['queue_contents'],
                                            'others': item['others'],
                                            'status': item['status']}
            cache.set("throttlers", throttlers, expire_seconds=self.cache_expire_seconds)
        return throttlers

    def whether_to_throttle(self, request):
        # disable throttler in clerk. throttler will run in transformer
        return False

        try:
            site = request['site']
            if site is None:
                site = 'Default'
            throttlers = self.get_throttlers()
            num_requests = self.get_num_active_requests(site)
            num_transforms = self.get_num_active_transforms(site)
            num_processings, active_transforms = self.get_num_active_processings(site)
            num_input_contents, num_output_contents = self.get_num_active_contents(site, active_transforms)
            self.logger.info("throttler(site: %s): active requests(%s), transforms(%s), processings(%s)" % (site, num_requests, num_transforms, num_processings))
            self.logger.info("throttler(site: %s): active input contents(%s), output contents(%s)" % (site, num_input_contents, num_output_contents))

            throttle_requests = throttlers.get(site, {}).get('num_requests', None)
            throttle_transforms = throttlers.get(site, {}).get('num_transforms', None)
            throttle_processings = throttlers.get(site, {}).get('num_processings', None)
            throttle_new_jobs = throttlers.get(site, {}).get('new_contents', None)
            throttle_queue_jobs = throttlers.get(site, {}).get('queue_contents', None)
            self.logger.info("throttler(site: %s): throttle_requests %s, throttle_transforms: %s, throttle_processings: %s" % (site, throttle_requests, throttle_transforms, throttle_processings))
            if throttle_requests:
                if num_requests['processing'] >= throttle_requests:
                    self.logger.info("throttler(site: %s): num of processing requests (%s) is bigger than throttle_requests (%s), set throttling" % (site, num_requests['processing'], throttle_requests))
                    return True
            if throttle_transforms:
                if num_transforms['processing'] >= throttle_transforms:
                    self.logger.info("throttler(site: %s): num of processing transforms (%s) is bigger than throttle_transforms (%s), set throttling" % (site, num_transforms['processing'], throttle_transforms))
                    return True
            if throttle_processings:
                if num_processings['processing'] >= throttle_processings:
                    self.logger.info("throttler(site: %s): num of processing processings (%s) is bigger than throttle_processings (%s), set throttling" % (site, num_processings['processing'], throttle_processings))
                    return True

            new_jobs = num_input_contents['new']
            released_jobs = num_input_contents['processed']
            terminated_jobs = num_output_contents['processed']
            queue_jobs = released_jobs - terminated_jobs

            self.logger.info("throttler(site: %s): throttle_new_jobs: %s, throttle_queue_jobs: %s" % (site, throttle_new_jobs, throttle_queue_jobs))
            self.logger.info("throttler(site: %s): new_jobs: %s, queue_jobs: %s" % (site, new_jobs, queue_jobs))
            if throttle_new_jobs:
                if new_jobs >= throttle_new_jobs:
                    self.logger.info("throttler(site: %s): num of new jobs(not released) (%s) is bigger than throttle_new_jobs (%s), set throttling" % (site, new_jobs, throttle_new_jobs))
                    return True
            if throttle_queue_jobs:
                if queue_jobs >= throttle_queue_jobs:
                    self.logger.info("throttler(site: %s): num of queue jobs(released but not terminated) (%s) is bigger than throttle_queue_jobs (%s), set throttling" % (site, queue_jobs, throttle_queue_jobs))
                    return True

            return False
        except Exception as ex:
            self.logger.error("whether_to_throttle: %s" % str(ex))
            self.logger.error(traceback.format_exc())
        return False

    def get_log_prefix(self, req):
        return "<request_id=%s>" % req['request_id']

    def handle_new_request(self, req):
        try:
            log_pre = self.get_log_prefix(req)
            self.logger.info(log_pre + "Handle new request")
            to_throttle = self.whether_to_throttle(req)
            if to_throttle:
                ret_req = {'request_id': req['request_id'],
                           'parameters': {'status': RequestStatus.Throttling,
                                          'locking': RequestLocking.Idle}}
                ret_req['parameters'] = self.load_poll_period(req, ret_req['parameters'], throttling=True)
                self.logger.info(log_pre + "Throttle new request result: %s" % str(ret_req))
            else:
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
                    if transform:
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

    def handle_new_irequest(self, req):
        try:
            log_pre = self.get_log_prefix(req)
            self.logger.info(log_pre + "Handle new irequest")
            to_throttle = self.whether_to_throttle(req)
            if to_throttle:
                ret_req = {'request_id': req['request_id'],
                           'parameters': {'status': RequestStatus.Throttling,
                                          'locking': RequestLocking.Idle}}
                ret_req['parameters'] = self.load_poll_period(req, ret_req['parameters'], throttling=True)
                self.logger.info(log_pre + "Throttle new irequest result: %s" % str(ret_req))
            else:
                workflow = req['request_metadata']['workflow']

                transforms = []
                transform = self.generate_transform(req, workflow)
                if transform:
                    transforms.append(transform)
                self.logger.debug(log_pre + "Processing request(%s): new transforms: %s" % (req['request_id'],
                                                                                            str(transforms)))
                ret_req = {'request_id': req['request_id'],
                           'parameters': {'status': RequestStatus.Transforming,
                                          'locking': RequestLocking.Idle},
                           'new_transforms': transforms}
                ret_req['parameters'] = self.load_poll_period(req, ret_req['parameters'])
                self.logger.info(log_pre + "Handle new irequest result: %s" % str(ret_req))
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
            self.logger.warn(log_pre + "Handle new irequest error result: %s" % str(ret_req))
        return ret_req

    def handle_new_generic_request(self, req):
        try:
            log_pre = self.get_log_prefix(req)
            self.logger.info(log_pre + "Handle new generic request")
            to_throttle = self.whether_to_throttle(req)
            if to_throttle:
                ret_req = {'request_id': req['request_id'],
                           'parameters': {'status': RequestStatus.Throttling,
                                          'locking': RequestLocking.Idle}}
                ret_req['parameters'] = self.load_poll_period(req, ret_req['parameters'], throttling=True)
                self.logger.info(log_pre + "Throttle new generic request result: %s" % str(ret_req))
            else:
                workflow = req['request_metadata']['workflow']

                transforms = []
                works = workflow.get_works()
                for w in works:
                    # todo
                    # set has_previous_conditions, has_conditions, all_conditions_triggered(False), cloned_from(None)
                    transform = self.generate_transform(req, w)
                    if transform:
                        transforms.append(transform)
                self.logger.debug(log_pre + f"Processing request({req['request_id']}): new transforms: {transforms}")

                conds = workflow.get_conditions()
                conditions = []
                for cond in conds:
                    condition = self.generate_condition(req, cond)
                    if condition:
                        conditions.append(condition)
                self.logger.debug(log_pre + f"Processing request({req['request_id']}), new conditions: {conditions}")

                ret_req = {'request_id': req['request_id'],
                           'parameters': {'status': RequestStatus.Transforming,
                                          'locking': RequestLocking.Idle},
                           'new_transforms': transforms,
                           'new_conditions': conditions}
                ret_req['parameters'] = self.load_poll_period(req, ret_req['parameters'])
                self.logger.info(log_pre + "Handle new generic request result: %s" % str(ret_req))
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
            self.logger.warn(log_pre + "Handle new irequest error result: %s" % str(ret_req))
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

    def update_request(self, req, origin_req=None):
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

            if 'new_conditions' in req:
                new_conditions = req['new_conditions']
            else:
                new_conditions = []

            if origin_req:
                origin_status = origin_req['status']
            else:
                origin_status = None

            retry = True
            retry_num = 0
            while retry:
                retry = False
                retry_num += 1
                try:
                    _, new_tf_ids, update_tf_ids = core_requests.update_request_with_transforms(req['request_id'], req['parameters'],
                                                                                                origin_status=origin_status,
                                                                                                new_transforms=new_transforms,
                                                                                                update_transforms=update_transforms,
                                                                                                new_conditions=new_conditions)
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

                if origin_req:
                    origin_status = origin_req['status']
                else:
                    origin_status = None

                self.logger.warn(log_pre + "Update request in exception: %s" % str(req_parameters))
                core_requests.update_request_with_transforms(req['request_id'], req_parameters, origin_status=origin_status)
            except Exception as ex:
                self.logger.error(ex)
                self.logger.error(traceback.format_exc())
        return new_tf_ids, update_tf_ids

    def process_new_request(self, event):
        self.number_workers += 1
        try:
            if event:
                # req_status = [RequestStatus.New, RequestStatus.Extend, RequestStatus.Built]
                req_status = [RequestStatus.New, RequestStatus.Extend, RequestStatus.Built, RequestStatus.Throttling]
                req = self.get_request(request_id=event._request_id, status=req_status, locking=True)
                if not req:
                    self.logger.error("Cannot find request for event: %s" % str(event))
                elif req:
                    log_pre = self.get_log_prefix(req)
                    if self.has_to_build_work(req):
                        ret = self.handle_build_request(req)
                    elif req['request_type'] in [RequestType.iWorkflow, RequestType.iWorkflowLocal]:
                        ret = self.handle_new_irequest(req)
                    elif req['request_type'] in [RequestType.GenericWorkflow]:
                        ret = self.handle_new_generic_request(req)
                    else:
                        ret = self.handle_new_request(req)
                    new_tf_ids, update_tf_ids = self.update_request(ret, origin_req=req)
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
            self.logger.warn(log_pre + "Handle update request exception result: %s" % str(ret_req))
        return ret_req

    def is_to_expire(self, expired_at=None, pending_time=None, request_id=None):
        if expired_at:
            if type(expired_at) in [str]:
                expired_at = str_to_date(expired_at)
            if expired_at < datetime.datetime.utcnow():
                self.logger.info("Request(%s) expired_at(%s) is smaller than utc now(%s), expiring" % (request_id,
                                                                                                       expired_at,
                                                                                                       datetime.datetime.utcnow()))
                return True
        return False

    def handle_update_irequest_real(self, req, event):
        """
        process running request
        """
        log_pre = self.get_log_prefix(req)
        self.logger.info(log_pre + " handle_update_irequest: request_id: %s" % req['request_id'])

        tfs = core_transforms.get_transforms(request_id=req['request_id'])
        total_tfs, finished_tfs, subfinished_tfs, failed_tfs = 0, 0, 0, 0
        for tf in tfs:
            total_tfs += 1
            if tf['status'] in [TransformStatus.Finished, TransformStatus.Built]:
                finished_tfs += 1
            elif tf['status'] in [TransformStatus.SubFinished]:
                subfinished_tfs += 1
            elif tf['status'] in [TransformStatus.Failed, TransformStatus.Cancelled,
                                  TransformStatus.Suspended, TransformStatus.Expired]:
                failed_tfs += 1

        req_status = RequestStatus.Transforming
        if req['request_type'] in [RequestType.iWorkflowLocal]:
            workflow = req['request_metadata'].get('workflow', None)
            if workflow and req['created_at'] + datetime.timedelta(seconds=workflow.max_walltime) < datetime.datetime.utcnow():
                req_status = RequestStatus.Finished
        else:
            if total_tfs == finished_tfs:
                req_status = RequestStatus.Finished
            elif total_tfs == finished_tfs + subfinished_tfs + failed_tfs:
                if finished_tfs + subfinished_tfs > 0:
                    req_status = RequestStatus.SubFinished
                else:
                    req_status = RequestStatus.Failed

        log_msg = log_pre + "ireqeust %s status: %s" % (req['request_id'], req_status)
        log_msg = log_msg + "(transforms: total %s, finished: %s, subfinished: %s, failed %s)" % (total_tfs, finished_tfs, subfinished_tfs, failed_tfs)
        self.logger.debug(log_msg)

        if req_status not in [RequestStatus.Finished, RequestStatus.SubFinished, RequestStatus.Failed]:
            if self.is_to_expire(req['expired_at'], self.pending_time, request_id=req['request_id']):
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
               'parameters': parameters}
        self.logger.info(log_pre + "Handle update irequest result: %s" % str(ret))
        return ret

    def handle_update_irequest(self, req, event):
        """
        process running irequest
        """
        try:
            ret_req = self.handle_update_irequest_real(req, event)
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
            self.logger.warn(log_pre + "Handle update irequest exception result: %s" % str(ret_req))
        return ret_req

    def process_update_request(self, event):
        self.number_workers += 1
        pro_ret = ReturnCode.Ok.value
        try:
            if event:
                # req_status = [RequestStatus.Transforming, RequestStatus.ToCancel, RequestStatus.Cancelling,
                #               RequestStatus.ToSuspend, RequestStatus.Suspending,
                #               RequestStatus.ToExpire, RequestStatus.Expiring,
                #               RequestStatus.ToFinish, RequestStatus.ToForceFinish,
                #               RequestStatus.ToResume, RequestStatus.Resuming,
                #               RequestStatus.Building]

                # req = self.get_request(request_id=event._request_id, status=req_status, locking=True)
                self.logger.debug("process_update_request: event: %s" % str(event))
                req = self.get_request(request_id=event._request_id, locking=True)
                if not req:
                    self.logger.error("Cannot find request for event: %s" % str(event))
                    # pro_ret = ReturnCode.Locked.value
                    pro_ret = ReturnCode.Ok.value
                else:
                    log_pre = self.get_log_prefix(req)
                    if req['request_type'] in [RequestType.iWorkflow, RequestType.iWorkflowLocal]:
                        ret = self.handle_update_irequest(req, event=event)
                    else:
                        ret = self.handle_update_request(req, event=event)
                    new_tf_ids, update_tf_ids = self.update_request(ret, origin_req=req)
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
            pro_ret = ReturnCode.Failed.value
        self.number_workers -= 1
        return pro_ret

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
        pro_ret = ReturnCode.Ok.value
        try:
            if event:
                req = self.get_request(request_id=event._request_id, locking=True)
                if not req:
                    self.logger.warn("Cannot find request for event: %s" % str(event))
                    pro_ret = ReturnCode.Locked.value
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
                        self.update_request(ret, origin_req=req)
                        self.handle_command(event, cmd_status=CommandStatus.Failed, errors="Request is already terminated. Cannot be aborted")
                    elif req['request_type'] in [RequestType.iWorkflow, RequestType.iWorkflowLocal]:
                        ret = self.handle_close_irequest(req, event=event)
                        self.update_request(ret, origin_req=req)

                        # self.handle_command(event, cmd_status=CommandStatus.Failed, errors="Not support abortion for iWorkflow")
                        self.handle_command(event, cmd_status=CommandStatus.Processed, errors=None)
                    else:
                        ret = self.handle_abort_request(req, event)
                        self.logger.info(log_pre + "process_abort_request result: %s" % str(ret))
                        self.update_request(ret, origin_req=req)
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
                            has_abort_work = False
                            for work in works:
                                if (work.is_started() or work.is_starting()) and not work.is_terminated():
                                    if not to_abort_transform_id or to_abort_transform_id == work.get_work_id():
                                        self.logger.info(log_pre + "AbortTransformEvent(transform_id: %s)" % str(work.get_work_id()))
                                        event = AbortTransformEvent(publisher_id=self.id,
                                                                    transform_id=work.get_work_id(),
                                                                    content=event._content)
                                        self.event_bus.send(event)
                                        has_abort_work = True
                            if not has_abort_work:
                                self.logger.info(log_pre + "not has abort work")
                                self.logger.info(log_pre + "UpdateRequestEvent(request_id: %s)" % str(req['request_id']))
                                event = UpdateRequestEvent(publisher_id=self.id, request_id=req['request_id'], content=event._content)
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
            pro_ret = ReturnCode.Failed.value
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            pro_ret = ReturnCode.Failed.value
        self.number_workers -= 1
        return pro_ret

    def handle_close_irequest(self, req, event):
        """
        process close irequest
        """
        try:
            log_pre = self.get_log_prefix(req)
            self.logger.info(log_pre + "handle_close_irequest event: %s" % str(event))

            tfs = core_transforms.get_transforms(request_id=req['request_id'])
            total_tfs, finished_tfs, subfinished_tfs, failed_tfs = 0, 0, 0, 0
            for tf in tfs:
                total_tfs += 1
                if tf['status'] in [TransformStatus.Finished, TransformStatus.Built]:
                    finished_tfs += 1
                elif tf['status'] in [TransformStatus.SubFinished]:
                    subfinished_tfs += 1
                elif tf['status'] in [TransformStatus.Failed, TransformStatus.Cancelled,
                                      TransformStatus.Suspended, TransformStatus.Expired]:
                    failed_tfs += 1
                else:
                    event = AbortTransformEvent(publisher_id=self.id,
                                                transform_id=tf['transform_id'],
                                                content=event._content)
                    self.event_bus.send(event)

            req_status = RequestStatus.Transforming
            if req['request_type'] in [RequestType.iWorkflowLocal] and total_tfs == 0:
                req_status = RequestStatus.Finished
            else:
                if total_tfs == finished_tfs:
                    req_status = RequestStatus.Finished
                elif total_tfs == finished_tfs + subfinished_tfs + failed_tfs:
                    if finished_tfs + subfinished_tfs > 0:
                        req_status = RequestStatus.SubFinished
                    else:
                        req_status = RequestStatus.Failed

            log_msg = log_pre + "ireqeust %s status: %s" % (req['request_id'], req_status)
            log_msg = log_msg + "(transforms: total %s, finished: %s, subfinished: %s, failed %s)" % (total_tfs, finished_tfs, subfinished_tfs, failed_tfs)
            self.logger.debug(log_msg)

            parameters = {'status': req_status,
                          'substatus': RequestStatus.ToClose,
                          'locking': RequestLocking.Idle,
                          'request_metadata': req['request_metadata']
                          }
            parameters = self.load_poll_period(req, parameters)

            ret = {'request_id': req['request_id'],
                   'parameters': parameters}
            self.logger.info(log_pre + "Handle close irequest result: %s" % str(ret))
            return ret

        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            error = {'close_err': {'msg': truncate_string('%s' % (ex), length=200)}}
            ret_req = {'request_id': req['request_id'],
                       'parameters': {'status': RequestStatus.ToClose,
                                      'locking': RequestLocking.Idle,
                                      'errors': req['errors'] if req['errors'] else {}}}
            ret_req['parameters']['errors'].update(error)
            self.logger.info(log_pre + "handle_close_irequest exception result: %s" % str(ret_req))
        return ret_req

    def process_close_request(self, event):
        self.number_workers += 1
        pro_ret = ReturnCode.Ok.value
        try:
            if event:
                req = self.get_request(request_id=event._request_id, locking=True)
                if not req:
                    self.logger.warn("Cannot find request for event: %s" % str(event))
                    pro_ret = ReturnCode.Locked.value
                else:
                    log_pre = self.get_log_prefix(req)
                    self.logger.info(log_pre + "process_close_request event: %s" % str(event))

                    if req['status'] in [RequestStatus.Finished, RequestStatus.SubFinished,
                                         RequestStatus.Failed, RequestStatus.Cancelled,
                                         RequestStatus.Suspended, RequestStatus.Expired]:
                        ret = {'request_id': req['request_id'],
                               'parameters': {'locking': RequestLocking.Idle,
                                              'errors': {'extra_msg': "Request is already terminated. Cannot be closed"}}}
                        if req['errors'] and 'msg' in req['errors']:
                            ret['parameters']['errors']['msg'] = req['errors']['msg']
                        self.logger.info(log_pre + "process_abort_request result: %s" % str(ret))
                        self.update_request(ret, origin_req=req)
                        self.handle_command(event, cmd_status=CommandStatus.Failed, errors="Request is already terminated. Cannot be closed")
                    else:
                        if req['request_type'] in [RequestType.iWorkflow, RequestType.iWorkflowLocal]:
                            ret = self.handle_close_irequest(req, event=event)
                            self.update_request(ret, origin_req=req)
                        else:
                            pass
                            ret = self.handle_abort_request(req, event)
                            self.logger.info(log_pre + "process_abort_request result: %s" % str(ret))
                            self.update_request(ret, origin_req=req)
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
                                has_abort_work = False
                                for work in works:
                                    if (work.is_started() or work.is_starting()) and not work.is_terminated():
                                        if not to_abort_transform_id or to_abort_transform_id == work.get_work_id():
                                            self.logger.info(log_pre + "AbortTransformEvent(transform_id: %s)" % str(work.get_work_id()))
                                            event = AbortTransformEvent(publisher_id=self.id,
                                                                        transform_id=work.get_work_id(),
                                                                        content=event._content)
                                            self.event_bus.send(event)
                                            has_abort_work = True
                                if not has_abort_work:
                                    self.logger.info(log_pre + "not has abort work")
                                    self.logger.info(log_pre + "UpdateRequestEvent(request_id: %s)" % str(req['request_id']))
                                    event = UpdateRequestEvent(publisher_id=self.id, request_id=req['request_id'], content=event._content)
                                    self.event_bus.send(event)
                            else:
                                # no works. should trigger update request
                                self.logger.info(log_pre + "UpdateRequestEvent(request_id: %s)" % str(req['request_id']))
                                event = UpdateRequestEvent(publisher_id=self.id, request_id=req['request_id'], content=event._content)
                                self.event_bus.send(event)

                        self.handle_command(event, cmd_status=CommandStatus.Processed, errors=None)
        except AssertionError as ex:
            self.logger.error("process_close_request, Failed to process event: %s" % str(event))
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            self.handle_command(event, cmd_status=CommandStatus.Processed, errors=str(ex))
            pro_ret = ReturnCode.Failed.value
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            pro_ret = ReturnCode.Failed.value
        self.number_workers -= 1
        return pro_ret

    def handle_resume_irequest(self, req, event):
        """
        process resume irequest
        """
        try:
            log_pre = self.get_log_prefix(req)
            self.logger.info(log_pre + "handle_resume_irequest event: %s" % str(event))

            tfs = core_transforms.get_transforms(request_id=req['request_id'])
            for tf in tfs:
                if tf['status'] in [TransformStatus.Finished, TransformStatus.Built]:
                    continue
                else:
                    event = ResumeTransformEvent(publisher_id=self.id,
                                                 transform_id=tf['transform_id'],
                                                 content=event._content)
                    self.event_bus.send(event)

            parameters = {'status': RequestStatus.Transforming,
                          'substatus': RequestStatus.ToResume,
                          'locking': RequestLocking.Idle,
                          'request_metadata': req['request_metadata']
                          }
            parameters = self.load_poll_period(req, parameters)

            ret = {'request_id': req['request_id'],
                   'parameters': parameters}
            self.logger.info(log_pre + "Handle resume irequest result: %s" % str(ret))
            return ret
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            error = {'close_err': {'msg': truncate_string('%s' % (ex), length=200)}}
            ret_req = {'request_id': req['request_id'],
                       'parameters': {'status': RequestStatus.ToClose,
                                      'locking': RequestLocking.Idle,
                                      'errors': req['errors'] if req['errors'] else {}}}
            ret_req['parameters']['errors'].update(error)
            self.logger.info(log_pre + "handle_close_irequest exception result: %s" % str(ret_req))
        return ret_req

    def handle_resume_request(self, req):
        """
        process resume request
        """
        try:
            req_status = RequestStatus.Resuming

            processing_metadata = req['processing_metadata']

            if 'workflow' in req['request_metadata'] and req['request_metadata']['workflow'] is not None:
                wf = req['request_metadata']['workflow']
                wf.resume_works()
            elif 'build_workflow' in req['request_metadata'] and req['request_metadata']['build_workflow'] is not None:
                req_status = RequestStatus.Building
            else:
                req_status = RequestStatus.Failed

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
        pro_ret = ReturnCode.Ok.value
        try:
            if event:
                req = self.get_request(request_id=event._request_id, locking=True)
                if not req:
                    self.logger.error("Cannot find request for event: %s" % str(event))
                    pro_ret = ReturnCode.Locked.value
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

                        self.update_request(ret, origin_req=req)
                        self.handle_command(event, cmd_status=CommandStatus.Failed, errors="Request is already finished. Cannot be resumed")
                    elif req['request_type'] in [RequestType.iWorkflow, RequestType.iWorkflowLocal]:
                        ret = self.handle_resume_irequest(req)
                        self.update_request(ret, origin_req=req)
                        # self.handle_command(event, cmd_status=CommandStatus.Failed, errors="Not support to reusme for iWorkflow")
                        self.handle_command(event, cmd_status=CommandStatus.Processed, errors=None)
                    else:
                        ret = self.handle_resume_request(req)
                        self.logger.info(log_pre + "process_resume_request result: %s" % str(ret))

                        self.update_request(ret, origin_req=req)
                        if 'workflow' in req['request_metadata']:
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
            pro_ret = ReturnCode.Failed.value
        self.number_workers -= 1
        return pro_ret

    def clean_locks(self):
        self.logger.info("clean locking")
        health_items = self.get_health_items()
        min_request_id = BaseAgent.min_request_id
        core_requests.clean_locking(health_items=health_items, min_request_id=min_request_id, time_period=None)

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
            },
            EventType.CloseRequest: {
                'pre_check': self.is_ok_to_run_more_requests,
                'exec_func': self.process_close_request
            }
        }

    def run(self):
        """
        Main run function.
        """
        try:
            self.logger.info("Starting main thread")
            self.init_thread_info()

            self.load_plugins()

            self.add_default_tasks()

            self.init_event_function_map()

            task = self.create_task(task_func=self.get_new_requests, task_output_queue=None, task_args=tuple(), task_kwargs={}, delay_time=10, priority=1)
            self.add_task(task)
            task = self.create_task(task_func=self.get_running_requests, task_output_queue=None, task_args=tuple(), task_kwargs={}, delay_time=10, priority=1)
            self.add_task(task)
            task = self.create_task(task_func=self.get_operation_requests, task_output_queue=None, task_args=tuple(), task_kwargs={}, delay_time=10, priority=1)
            self.add_task(task)
            task = self.create_task(task_func=self.clean_min_request_id, task_output_queue=None, task_args=tuple(), task_kwargs={}, delay_time=3600, priority=1)
            self.add_task(task)
            task = self.create_task(task_func=self.clean_locks, task_output_queue=None, task_args=tuple(), task_kwargs={}, delay_time=60, priority=1)
            self.add_task(task)

            self.execute()
        except KeyboardInterrupt:
            self.stop()


if __name__ == '__main__':
    agent = Clerk()
    agent()
