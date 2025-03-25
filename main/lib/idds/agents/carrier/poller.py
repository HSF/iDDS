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
from idds.common.constants import (Sections, ReturnCode, ProcessingType,
                                   ProcessingStatus, ProcessingLocking,
                                   Terminated_processing_status)
from idds.common.utils import setup_logging, truncate_string, json_dumps
from idds.core import processings as core_processings
from idds.agents.common.baseagent import BaseAgent
from idds.agents.common.eventbus.event import (EventType,
                                               UpdateProcessingEvent,
                                               TriggerProcessingEvent,
                                               SyncProcessingEvent,
                                               TerminatedProcessingEvent)

from .utils import handle_update_processing, is_process_terminated, is_process_finished
from .iutils import handle_update_iprocessing

setup_logging(__name__)


class Poller(BaseAgent):
    """
    Poller works to submit and running tasks to WFMS.
    """

    def __init__(self, num_threads=1, max_number_workers=3, poll_period=10, retries=3, retrieve_bulk_size=2,
                 max_updates_per_round=2000, name='Poller', message_bulk_size=1000, locking_period=1800, **kwargs):
        self.max_number_workers = max_number_workers
        if int(num_threads) < int(self.max_number_workers):
            num_threads = int(self.max_number_workers)

        self.set_max_workers()

        super(Poller, self).__init__(num_threads=num_threads, name=name, **kwargs)
        self.config_section = Sections.Carrier
        self.poll_period = int(poll_period)
        self.locking_period = int(locking_period)
        self.retries = int(retries)
        self.retrieve_bulk_size = int(retrieve_bulk_size)
        self.message_bulk_size = int(message_bulk_size)

        if not hasattr(self, 'new_poll_period') or not self.new_poll_period:
            self.new_poll_period = self.poll_period
        else:
            self.new_poll_period = int(self.new_poll_period)
        if not hasattr(self, 'update_poll_period') or not self.update_poll_period:
            self.update_poll_period = self.poll_period
        else:
            self.update_poll_period = int(self.update_poll_period)

        if not hasattr(self, 'update_poll_period_for_new_task') or not self.update_poll_period_for_new_task:
            self.update_poll_period_for_new_task = 180
        else:
            self.update_poll_period_for_new_task = int(self.update_poll_period_for_new_task)

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

        self.number_workers = 0
        if not hasattr(self, 'max_number_workers') or not self.max_number_workers:
            self.max_number_workers = 3
        else:
            self.max_number_workers = int(self.max_number_workers)

        self.max_updates_per_round = max_updates_per_round
        self.logger.info("max_updates_per_round: %s" % self.max_updates_per_round)

        if not hasattr(self, 'enable_executors') or not self.enable_executors:
            self.enable_executors = False
        else:
            if str(self.enable_executors).lower() == 'true':
                self.enable_executors = True
            else:
                self.enable_executors = False
        self.logger.info("enable_executors: %s" % self.enable_executors)

        self.show_queue_size_time = None

        self.extra_executors = None

        self._running_processing_status = None

    def get_extra_executors(self):
        if self.enable_executors:
            if self.extra_executors is None:
                name = self.executor_name + "_Extra"
                self.extra_executors = self.create_executors(name, max_workers=self.num_threads)
        return self.extra_executors

    def is_ok_to_run_more_processings(self):
        if self.number_workers >= self.max_number_workers:
            return False
        return True

    def show_queue_size(self):
        if self.show_queue_size_time is None or time.time() - self.show_queue_size_time >= 600:
            self.show_queue_size_time = time.time()

            q_str = "number of processings: %s, max number of processings: %s" % (self.number_workers, self.max_number_workers)
            self.logger.debug(q_str)

            exec_max_workers = self.executors.get_max_workers()
            exec_num_workers = self.executors.get_num_workers()
            q_str = "Executor number of processings: %s, max number of processings: %s" % (exec_num_workers, exec_max_workers)
            self.logger.debug(q_str)

    def init(self):
        status = [ProcessingStatus.New, ProcessingStatus.Submitting, ProcessingStatus.Submitted,
                  ProcessingStatus.Running, ProcessingStatus.FinishedOnExec]
        core_processings.clean_next_poll_at(status)

    def get_running_processings(self):
        """
        Get running processing
        """
        try:
            if not self.is_ok_to_run_more_processings():
                return []

            self.show_queue_size()

            if BaseAgent.min_request_id is None:
                return []

            processing_status = [ProcessingStatus.Submitting, ProcessingStatus.Submitted,
                                 ProcessingStatus.Running, ProcessingStatus.FinishedOnExec,
                                 ProcessingStatus.ToCancel, ProcessingStatus.Cancelling,
                                 ProcessingStatus.ToSuspend, ProcessingStatus.Suspending,
                                 ProcessingStatus.ToResume, ProcessingStatus.Resuming,
                                 ProcessingStatus.ToExpire, ProcessingStatus.Expiring,
                                 ProcessingStatus.ToFinish, ProcessingStatus.ToForceFinish]
            self._running_processing_status = processing_status

            # next_poll_at = datetime.datetime.utcnow() + datetime.timedelta(seconds=self.poll_period)
            processings = core_processings.get_processings_by_status(status=processing_status,
                                                                     locking=True, update_poll=True,
                                                                     not_lock=True,
                                                                     only_return_id=True,
                                                                     min_request_id=BaseAgent.min_request_id,
                                                                     bulk_size=self.retrieve_bulk_size)

            # self.logger.debug("Main thread get %s [submitting + submitted + running] processings to process" % (len(processings)))
            if processings:
                self.logger.info("Main thread get [submitting + submitted + running] processings to process: %s" % (str(processings)))

            events = []
            for pr_id in processings:
                self.logger.info("UpdateProcessingEvent(processing_id: %s)" % pr_id)
                event = UpdateProcessingEvent(publisher_id=self.id, processing_id=pr_id)
                events.append(event)
            self.event_bus.send_bulk(events)

            return processings
        except exceptions.DatabaseException as ex:
            if 'ORA-00060' in str(ex):
                self.logger.warn("(cx_Oracle.DatabaseError) ORA-00060: deadlock detected while waiting for resource")
            else:
                # raise ex
                self.logger.error(ex)
                self.logger.error(traceback.format_exc())
        return []

    def get_processing(self, processing_id, status=None, locking=False):
        try:
            return core_processings.get_processing_by_id_status(processing_id=processing_id,
                                                                status=status,
                                                                locking=locking,
                                                                lock_period=self.locking_period)
        except exceptions.DatabaseException as ex:
            if 'ORA-00060' in str(ex):
                self.logger.warn("(cx_Oracle.DatabaseError) ORA-00060: deadlock detected while waiting for resource")
            else:
                # raise ex
                self.logger.error(ex)
                self.logger.error(traceback.format_exc())
        return None

    def get_work_tag_attribute(self, work_tag, attribute):
        work_tag_attribute_value = None
        if work_tag:
            work_tag_attribute = work_tag + "_" + attribute
            if hasattr(self, work_tag_attribute):
                work_tag_attribute_value = int(getattr(self, work_tag_attribute))
        return work_tag_attribute_value

    def load_poll_period(self, processing, parameters, new=False):
        if 'processing' in processing['processing_metadata']:
            proc = processing['processing_metadata']['processing']
            work = proc.work
        else:
            work = processing['processing_metadata']['work']

        work_tag = work.get_work_tag()

        work_tag_new_poll_period = self.get_work_tag_attribute(work_tag, "new_poll_period")
        if work_tag_new_poll_period:
            parameters['new_poll_period'] = work_tag_new_poll_period
        elif self.new_poll_period and processing['new_poll_period'] != self.new_poll_period:
            parameters['new_poll_period'] = self.new_poll_period

        if new:
            parameters['update_poll_period'] = self.update_poll_period_for_new_task
        else:
            work_tag_update_poll_period = self.get_work_tag_attribute(work_tag, "update_poll_period")
            if work_tag_update_poll_period:
                parameters['update_poll_period'] = work_tag_update_poll_period
            elif self.update_poll_period and processing['update_poll_period'] != self.update_poll_period:
                parameters['update_poll_period'] = self.update_poll_period
        return parameters

    def get_log_prefix(self, processing):
        return "<request_id=%s,transform_id=%s,processing_id=%s>" % (processing['request_id'],
                                                                     processing['transform_id'],
                                                                     processing['processing_id'])

    def update_processing(self, processing, processing_model, use_bulk_update_mappings=True):
        try:
            if processing:
                log_prefix = self.get_log_prefix(processing_model)

                self.logger.info(log_prefix + "update_processing: %s" % (processing['update_processing']['parameters']))

                processing['update_processing']['parameters']['locking'] = ProcessingLocking.Idle
                # self.logger.debug("wen: %s" % str(processing))
                processing['update_processing']['parameters']['updated_at'] = datetime.datetime.utcnow()
                # check update_processing status
                if 'status' in processing['update_processing']['parameters']:
                    new_status = processing['update_processing']['parameters']['status']
                    if new_status == ProcessingStatus.Submitting and processing_model['status'].value > ProcessingStatus.Submitting.value:
                        processing['update_processing']['parameters']['status'] = ProcessingStatus.Submitted

                self.logger.info(log_prefix + "update_processing: %s" % (processing['update_processing']['parameters']))

                retry = True
                retry_num = 0
                while retry:
                    retry = False
                    retry_num += 1
                    try:
                        core_processings.update_processing_contents(update_processing=processing.get('update_processing', None),
                                                                    request_id=processing_model['request_id'],
                                                                    update_collections=processing.get('update_collections', None),
                                                                    update_contents=processing.get('update_contents', None),
                                                                    update_dep_contents=processing.get('update_dep_contents', None),
                                                                    messages=processing.get('messages', None),
                                                                    update_messages=processing.get('update_messages', None),
                                                                    new_contents=processing.get('new_contents', None),
                                                                    new_update_contents=processing.get('new_update_contents', None),
                                                                    new_contents_ext=processing.get('new_contents_ext', None),
                                                                    update_contents_ext=processing.get('update_contents_ext', None),
                                                                    new_input_dependency_contents=processing.get('new_input_dependency_contents', None),
                                                                    use_bulk_update_mappings=use_bulk_update_mappings,
                                                                    message_bulk_size=self.message_bulk_size)
                    except exceptions.DatabaseException as ex:
                        if 'ORA-00060' in str(ex):
                            self.logger.warn(log_prefix + "update_processing (cx_Oracle.DatabaseError) ORA-00060: deadlock detected while waiting for resource")
                        else:
                            self.logger.error(ex)
                            self.logger.error(traceback.format_exc())
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
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            self.logger.warn("Failed to update_processings: %s" % json_dumps(processing))
            try:
                processing_id = processing['update_processing']['processing_id']

                parameters = {'status': processing['update_processing']['parameters']['status'],
                              'locking': ProcessingLocking.Idle}
                if 'new_retries' in processing['update_processing']['parameters']:
                    parameters['new_retries'] = processing['update_processing']['parameters']['new_retries']
                if 'update_retries' in processing['update_processing']['parameters']:
                    parameters['update_retries'] = processing['update_processing']['parameters']['update_retries']
                if 'errors' in processing['update_processing']['parameters']:
                    parameters['errors'] = processing['update_processing']['parameters']['errors']

                self.logger.warn(log_prefix + "update_processing exception result: %s" % (parameters))
                core_processings.update_processing(processing_id=processing_id, parameters=parameters)
            except Exception as ex:
                self.logger.error(ex)
                self.logger.error(traceback.format_exc())

    def handle_update_processing(self, processing):
        try:
            log_prefix = self.get_log_prefix(processing)
            executors = None
            if self.enable_executors:
                executors = self.get_extra_executors()

            ret_handle_update_processing = handle_update_processing(processing,
                                                                    self.agent_attributes,
                                                                    max_updates_per_round=self.max_updates_per_round,
                                                                    executors=executors,
                                                                    logger=self.logger,
                                                                    log_prefix=log_prefix)

            process_status, new_contents, new_input_dependency_contents, ret_msgs, update_contents, parameters, new_contents_ext, update_contents_ext = ret_handle_update_processing

            proc = processing['processing_metadata']['processing']
            work = proc.work
            if work.use_dependency_to_release_jobs():
                new_process_status = ProcessingStatus.Triggering
            else:
                new_process_status = process_status
                if is_process_terminated(process_status):
                    new_process_status = ProcessingStatus.Terminating
                    if is_process_finished(process_status):
                        new_process_status = ProcessingStatus.Terminating
                    else:
                        retries = processing['update_retries'] + 1
                        if processing['max_update_retries'] and retries < processing['max_update_retries']:
                            work.reactivate_processing(processing, log_prefix=log_prefix)
                            process_status = ProcessingStatus.Running
                            new_process_status = ProcessingStatus.Running
                else:
                    if (update_contents or new_contents or new_contents_ext or update_contents_ext or ret_msgs):
                        new_process_status = ProcessingStatus.Synchronizing

            update_processing = {'processing_id': processing['processing_id'],
                                 'parameters': {'status': new_process_status,
                                                'substatus': process_status,
                                                'locking': ProcessingLocking.Idle}}

            update_processing['parameters'] = self.load_poll_period(processing, update_processing['parameters'])

            if proc.submitted_at:
                if not processing['submitted_at'] or processing['submitted_at'] < proc.submitted_at:
                    update_processing['parameters']['submitted_at'] = proc.submitted_at

            if proc.workload_id:
                update_processing['parameters']['workload_id'] = proc.workload_id

            # update_processing['parameters']['expired_at'] = work.get_expired_at(processing)
            update_processing['parameters']['processing_metadata'] = processing['processing_metadata']

            if parameters:
                # special parameters such as 'output_metadata'
                for p in parameters:
                    update_processing['parameters'][p] = parameters[p]

            ret = {'update_processing': update_processing,
                   'update_contents': update_contents,
                   'new_contents': new_contents,
                   'new_input_dependency_contents': new_input_dependency_contents,
                   'messages': ret_msgs,
                   'new_contents_ext': new_contents_ext,
                   'update_contents_ext': update_contents_ext,
                   'processing_status': new_process_status}

        except exceptions.ProcessFormatNotSupported as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())

            retries = processing['update_retries'] + 1
            if not processing['max_update_retries'] or retries < processing['max_update_retries']:
                proc_status = ProcessingStatus.Running
            else:
                proc_status = ProcessingStatus.Failed
            error = {'update_err': {'msg': truncate_string('%s' % (ex), length=200)}}

            # increase poll period
            update_poll_period = int(processing['update_poll_period'].total_seconds() * self.poll_period_increase_rate)
            if update_poll_period > self.max_update_poll_period:
                update_poll_period = self.max_update_poll_period

            update_processing = {'processing_id': processing['processing_id'],
                                 'parameters': {'status': proc_status,
                                                'locking': ProcessingLocking.Idle,
                                                'update_retries': retries,
                                                'update_poll_period': update_poll_period,
                                                'errors': processing['errors'] if processing['errors'] else {}}}
            update_processing['parameters']['errors'].update(error)

            ret = {'update_processing': update_processing,
                   'update_contents': []}
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())

            retries = processing['update_retries'] + 1
            if not processing['max_update_retries'] or retries < processing['max_update_retries']:
                proc_status = ProcessingStatus.Running
            else:
                proc_status = ProcessingStatus.Failed
            error = {'update_err': {'msg': truncate_string('%s' % (ex), length=200)}}
            update_processing = {'processing_id': processing['processing_id'],
                                 'parameters': {'status': proc_status,
                                                'locking': ProcessingLocking.Idle,
                                                'update_retries': retries,
                                                'errors': processing['errors'] if processing['errors'] else {}}}
            update_processing['parameters']['errors'].update(error)
            update_processing['parameters'] = self.load_poll_period(processing, update_processing['parameters'])

            ret = {'update_processing': update_processing,
                   'update_contents': []}
        return ret

    def handle_update_iprocessing(self, processing):
        try:
            log_prefix = self.get_log_prefix(processing)

            executors, plugin = None, None
            if processing['processing_type']:
                plugin_name = processing['processing_type'].name.lower() + '_poller'
                plugin = self.get_plugin(plugin_name)
            else:
                raise exceptions.ProcessSubmitFailed('No corresponding poller plugins for %s' % processing['processing_type'])

            ret_handle_update_processing = handle_update_iprocessing(processing,
                                                                     self.agent_attributes,
                                                                     plugin=plugin,
                                                                     max_updates_per_round=self.max_updates_per_round,
                                                                     executors=executors,
                                                                     logger=self.logger,
                                                                     log_prefix=log_prefix)

            process_status, new_contents, new_input_dependency_contents, ret_msgs, update_contents, parameters, new_contents_ext, update_contents_ext = ret_handle_update_processing

            new_process_status = process_status
            if is_process_terminated(process_status):
                new_process_status = ProcessingStatus.Terminating
                if is_process_finished(process_status):
                    new_process_status = ProcessingStatus.Terminating
                else:
                    new_process_status = ProcessingStatus.Terminating

            update_processing = {'processing_id': processing['processing_id'],
                                 'parameters': {'status': new_process_status,
                                                'substatus': process_status,
                                                'locking': ProcessingLocking.Idle}}

            update_processing['parameters'] = self.load_poll_period(processing, update_processing['parameters'])

            if 'submitted_at' in processing['processing_metadata']:
                if not processing['submitted_at'] or processing['submitted_at'] < processing['processing_metadata']['submitted_at']:
                    parameters['submitted_at'] = processing['processing_metadata']['submitted_at']

            if 'workload_id' in processing['processing_metadata']:
                parameters['workload_id'] = processing['processing_metadata']['workload_id']

            # update_processing['parameters']['expired_at'] = work.get_expired_at(processing)
            update_processing['parameters']['processing_metadata'] = processing['processing_metadata']

            if parameters:
                # special parameters such as 'output_metadata'
                for p in parameters:
                    update_processing['parameters'][p] = parameters[p]

            ret = {'update_processing': update_processing,
                   'update_contents': update_contents,
                   'new_contents': new_contents,
                   'new_input_dependency_contents': new_input_dependency_contents,
                   'messages': ret_msgs,
                   'new_contents_ext': new_contents_ext,
                   'update_contents_ext': update_contents_ext,
                   'processing_status': new_process_status}
        except exceptions.ProcessFormatNotSupported as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())

            retries = processing['update_retries'] + 1
            if not processing['max_update_retries'] or retries < processing['max_update_retries']:
                proc_status = ProcessingStatus.Running
            else:
                proc_status = ProcessingStatus.Failed
            error = {'update_err': {'msg': truncate_string('%s' % (ex), length=200)}}

            # increase poll period
            update_poll_period = int(processing['update_poll_period'].total_seconds() * self.poll_period_increase_rate)
            if update_poll_period > self.max_update_poll_period:
                update_poll_period = self.max_update_poll_period

            update_processing = {'processing_id': processing['processing_id'],
                                 'parameters': {'status': proc_status,
                                                'locking': ProcessingLocking.Idle,
                                                'update_retries': retries,
                                                'update_poll_period': update_poll_period,
                                                'errors': processing['errors'] if processing['errors'] else {}}}
            update_processing['parameters']['errors'].update(error)

            ret = {'update_processing': update_processing,
                   'update_contents': []}
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())

            retries = processing['update_retries'] + 1
            if not processing['max_update_retries'] or retries < processing['max_update_retries']:
                proc_status = ProcessingStatus.Running
            else:
                proc_status = ProcessingStatus.Failed
            error = {'update_err': {'msg': truncate_string('%s' % (ex), length=200)}}
            update_processing = {'processing_id': processing['processing_id'],
                                 'parameters': {'status': proc_status,
                                                'locking': ProcessingLocking.Idle,
                                                'update_retries': retries,
                                                'errors': processing['errors'] if processing['errors'] else {}}}
            update_processing['parameters']['errors'].update(error)
            update_processing['parameters'] = self.load_poll_period(processing, update_processing['parameters'])

            ret = {'update_processing': update_processing,
                   'update_contents': []}
        return ret

    def process_update_processing(self, event):
        self.number_workers += 1
        pro_ret = ReturnCode.Ok.value
        try:
            if event:
                original_event = event
                self.logger.info("process_update_processing, event: %s" % str(event))

                pr = self.get_processing(processing_id=event._processing_id, status=None, locking=True)
                if not pr:
                    self.logger.warn("Cannot find processing for event: %s" % str(event))
                    # pro_ret = ReturnCode.Locked.value
                    pro_ret = ReturnCode.Ok.value
                elif pr['status'] in Terminated_processing_status:
                    parameters = {'locking': ProcessingLocking.Idle}
                    update_processing = {'processing_id': pr['processing_id'],
                                         'parameters': parameters}
                    ret = {'update_processing': update_processing,
                           'update_contents': []}
                    self.update_processing(ret, pr)
                    pro_ret = ReturnCode.Ok.value
                else:
                    log_pre = self.get_log_prefix(pr)

                    self.logger.info(log_pre + "process_update_processing")
                    if pr['processing_type'] and pr['processing_type'] in [ProcessingType.iWorkflow, ProcessingType.iWork]:
                        ret = self.handle_update_iprocessing(pr)
                    else:
                        ret = self.handle_update_processing(pr)
                    # self.logger.info(log_pre + "process_update_processing result: %s" % str(ret))

                    self.update_processing(ret, pr)

                    if 'processing_status' in ret and ret['processing_status'] == ProcessingStatus.Triggering:
                        event_content = {}
                        if (('update_contents' in ret and ret['update_contents']) or ('new_contents' in ret and ret['new_contents'])):
                            event_content['has_updates'] = True
                        if is_process_terminated(pr['substatus']):
                            event_content['Terminated'] = True
                            event_content['is_terminating'] = True
                        self.logger.info(log_pre + "TriggerProcessingEvent(processing_id: %s)" % pr['processing_id'])
                        event = TriggerProcessingEvent(publisher_id=self.id, processing_id=pr['processing_id'], content=event_content,
                                                       counter=original_event._counter)
                        self.event_bus.send(event)
                    elif 'processing_status' in ret and ret['processing_status'] == ProcessingStatus.Terminating:
                        self.logger.info(log_pre + "TerminatedProcessingEvent(processing_id: %s)" % pr['processing_id'])
                        event = TerminatedProcessingEvent(publisher_id=self.id, processing_id=pr['processing_id'],
                                                          counter=original_event._counter)
                        event.set_terminating()
                        self.event_bus.send(event)
                    else:
                        if 'processing_status' in ret and ret['processing_status'] == ProcessingStatus.Synchronizing:
                            self.logger.info(log_pre + "SyncProcessingEvent(processing_id: %s)" % pr['processing_id'])
                            event = SyncProcessingEvent(publisher_id=self.id, processing_id=pr['processing_id'],
                                                        counter=original_event._counter)
                            event.set_has_updates()
                            self.event_bus.send(event)
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
        core_processings.clean_locking(health_items=health_items, min_request_id=min_request_id, time_period=None)

    def init_event_function_map(self):
        self.event_func_map = {
            EventType.UpdateProcessing: {
                'pre_check': self.is_ok_to_run_more_processings,
                'exec_func': self.process_update_processing
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
            self.init()

            self.add_default_tasks()

            self.init_event_function_map()

            task = self.create_task(task_func=self.get_running_processings, task_output_queue=None, task_args=tuple(), task_kwargs={}, delay_time=10, priority=1)
            self.add_task(task)

            task = self.create_task(task_func=self.clean_locks, task_output_queue=None, task_args=tuple(), task_kwargs={}, delay_time=1800, priority=1)
            self.add_task(task)

            self.execute()
        except KeyboardInterrupt:
            self.stop()


if __name__ == '__main__':
    agent = Poller()
    agent()
