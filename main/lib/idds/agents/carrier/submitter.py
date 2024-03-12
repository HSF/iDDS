#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019 - 2024

import traceback

from idds.common import exceptions
from idds.common.constants import ProcessingType, ProcessingStatus, ProcessingLocking
from idds.common.utils import setup_logging, truncate_string
from idds.core import processings as core_processings
from idds.agents.common.baseagent import BaseAgent
from idds.agents.common.eventbus.event import (EventType,
                                               NewProcessingEvent,
                                               SyncProcessingEvent,
                                               UpdateTransformEvent)

from .utils import handle_new_processing
from .iutils import handle_new_iprocessing
from .poller import Poller

setup_logging(__name__)


class Submitter(Poller):
    """
    Submitter works to submit and running tasks to WFMS.
    """

    def __init__(self, num_threads=1, max_number_workers=3, poll_period=10, retries=3, retrieve_bulk_size=2,
                 name='Submitter', message_bulk_size=1000, **kwargs):
        self.max_number_workers = max_number_workers
        self.set_max_workers()
        num_threads = self.max_number_workers

        super(Submitter, self).__init__(num_threads=num_threads, max_number_workers=self.max_number_workers,
                                        name=name, retrieve_bulk_size=retrieve_bulk_size, **kwargs)
        self.site_to_cloud = None

    def get_new_processings(self):
        """
        Get new processing
        """
        try:
            if not self.is_ok_to_run_more_processings():
                return []

            self.show_queue_size()

            if BaseAgent.min_request_id is None:
                return []

            processing_status = [ProcessingStatus.New]
            processings = core_processings.get_processings_by_status(status=processing_status, locking=True,
                                                                     not_lock=True,
                                                                     new_poll=True, only_return_id=True,
                                                                     min_request_id=BaseAgent.min_request_id,
                                                                     bulk_size=self.retrieve_bulk_size)

            # self.logger.debug("Main thread get %s [new] processings to process" % len(processings))
            if processings:
                self.logger.info("Main thread get [new] processings to process: %s" % str(processings))

            events = []
            for pr_id in processings:
                self.logger.info("NewProcessingEvent(processing_id: %s)" % pr_id)
                event = NewProcessingEvent(publisher_id=self.id, processing_id=pr_id)
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

    def get_site_to_cloud(self, site, log_prefix=''):
        try:
            if self.site_to_cloud is None:
                self.logger.debug(log_prefix + " agent_attributes: %s" % str(self.agent_attributes))
                self.site_to_cloud = {}
                if self.agent_attributes and 'domapandawork' in self.agent_attributes and self.agent_attributes['domapandawork']:
                    if 'site_to_cloud' in self.agent_attributes['domapandawork'] and self.agent_attributes['domapandawork']['site_to_cloud']:
                        site_to_clouds = self.agent_attributes['domapandawork']['site_to_cloud'].split(",")
                        for site_to_cloud in site_to_clouds:
                            local_site, cloud = site_to_cloud.split(':')
                            if local_site not in self.site_to_cloud:
                                self.site_to_cloud[local_site] = cloud
                self.logger.debug(log_prefix + " site_to_cloud: %s" % self.site_to_cloud)

            if site and self.site_to_cloud:
                cloud = self.site_to_cloud.get(site, None)
                self.logger.debug(log_prefix + "cloud for site(%s): %s" % (site, cloud))
                return cloud
        except Exception as ex:
            self.logger.error(ex)
        return None

    def handle_new_processing(self, processing):
        try:
            log_prefix = self.get_log_prefix(processing)

            # transform_id = processing['transform_id']
            # transform = core_transforms.get_transform(transform_id=transform_id)
            # work = transform['transform_metadata']['work']
            executors = None
            if self.enable_executors:
                executors = self.get_extra_executors()

            ret_new_processing = handle_new_processing(processing,
                                                       self.agent_attributes,
                                                       func_site_to_cloud=self.get_site_to_cloud,
                                                       max_updates_per_round=self.max_updates_per_round,
                                                       executors=executors,
                                                       logger=self.logger,
                                                       log_prefix=log_prefix)
            status, processing, update_colls, new_contents, new_input_dependency_contents, msgs, errors = ret_new_processing

            if not status:
                raise exceptions.ProcessSubmitFailed(str(errors))

            parameters = {'status': ProcessingStatus.Submitting,
                          'substatus': ProcessingStatus.Submitting,
                          'locking': ProcessingLocking.Idle,
                          'processing_metadata': processing['processing_metadata']}
            parameters = self.load_poll_period(processing, parameters, new=True)

            proc = processing['processing_metadata']['processing']
            if proc.submitted_at:
                if not processing['submitted_at'] or processing['submitted_at'] < proc.submitted_at:
                    parameters['submitted_at'] = proc.submitted_at

            # if processing['processing_metadata'] and 'processing' in processing['processing_metadata']:
            if proc.workload_id:
                parameters['workload_id'] = proc.workload_id

            update_processing = {'processing_id': processing['processing_id'],
                                 'parameters': parameters}
            ret = {'update_processing': update_processing,
                   'update_collections': update_colls,
                   'update_contents': [],
                   'new_contents': new_contents,
                   'new_input_dependency_contents': new_input_dependency_contents,
                   'messages': msgs,
                   }
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())

            retries = processing['new_retries'] + 1
            if not processing['max_new_retries'] or retries < processing['max_new_retries']:
                pr_status = processing['status']
            else:
                pr_status = ProcessingStatus.Failed
            # increase poll period
            new_poll_period = int(processing['new_poll_period'].total_seconds() * self.poll_period_increase_rate)
            if new_poll_period > self.max_new_poll_period:
                new_poll_period = self.max_new_poll_period

            error = {'submit_err': {'msg': truncate_string('%s' % str(ex), length=200)}}
            parameters = {'status': pr_status,
                          'new_poll_period': new_poll_period,
                          'errors': processing['errors'] if processing['errors'] else {},
                          'new_retries': retries}
            parameters['errors'].update(error)

            update_processing = {'processing_id': processing['processing_id'],
                                 'parameters': parameters}
            ret = {'update_processing': update_processing,
                   'update_contents': []}
        return ret

    def handle_new_iprocessing(self, processing):
        try:
            log_prefix = self.get_log_prefix(processing)

            # transform_id = processing['transform_id']
            # transform = core_transforms.get_transform(transform_id=transform_id)
            # work = transform['transform_metadata']['work']
            executors, plugin = None, None
            if processing['processing_type']:
                plugin_name = processing['processing_type'].name.lower() + '_submitter'
                plugin = self.get_plugin(plugin_name)
            else:
                raise exceptions.ProcessSubmitFailed('No corresponding submitter plugins for %s' % processing['processing_type'])
            ret_new_processing = handle_new_iprocessing(processing,
                                                        self.agent_attributes,
                                                        plugin=plugin,
                                                        func_site_to_cloud=self.get_site_to_cloud,
                                                        max_updates_per_round=self.max_updates_per_round,
                                                        executors=executors,
                                                        logger=self.logger,
                                                        log_prefix=log_prefix)
            status, processing, update_colls, new_contents, new_input_dependency_contents, msgs, errors = ret_new_processing

            if not status:
                raise exceptions.ProcessSubmitFailed(str(errors))

            parameters = {'status': ProcessingStatus.Submitting,
                          'substatus': ProcessingStatus.Submitting,
                          'locking': ProcessingLocking.Idle,
                          'processing_metadata': processing['processing_metadata']}
            parameters = self.load_poll_period(processing, parameters, new=True)

            if 'submitted_at' in processing:
                parameters['submitted_at'] = processing['submitted_at']

            if 'workload_id' in processing:
                parameters['workload_id'] = processing['workload_id']

            update_processing = {'processing_id': processing['processing_id'],
                                 'parameters': parameters}
            ret = {'update_processing': update_processing,
                   'update_collections': update_colls,
                   'update_contents': [],
                   'new_contents': new_contents,
                   'new_input_dependency_contents': new_input_dependency_contents,
                   'messages': msgs,
                   }
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())

            retries = processing['new_retries'] + 1
            if not processing['max_new_retries'] or retries < processing['max_new_retries']:
                pr_status = processing['status']
            else:
                pr_status = ProcessingStatus.Failed
            # increase poll period
            new_poll_period = int(processing['new_poll_period'].total_seconds() * self.poll_period_increase_rate)
            if new_poll_period > self.max_new_poll_period:
                new_poll_period = self.max_new_poll_period

            error = {'submit_err': {'msg': truncate_string('%s' % str(ex), length=200)}}
            parameters = {'status': pr_status,
                          'new_poll_period': new_poll_period,
                          'errors': processing['errors'] if processing['errors'] else {},
                          'new_retries': retries}
            parameters['errors'].update(error)

            update_processing = {'processing_id': processing['processing_id'],
                                 'parameters': parameters}
            ret = {'update_processing': update_processing,
                   'update_contents': []}
        return ret

    def process_new_processing(self, event):
        self.number_workers += 1
        try:
            if event:
                # pr_status = [ProcessingStatus.New]
                self.logger.info("process_new_processing, event: %s" % str(event))
                pr = self.get_processing(processing_id=event._processing_id, status=None, locking=True)
                if not pr:
                    self.logger.error("Cannot find processing for event: %s" % str(event))
                else:
                    log_pre = self.get_log_prefix(pr)
                    self.logger.info(log_pre + "process_new_processing")
                    if pr['processing_type'] and pr['processing_type'] in [ProcessingType.iWorkflow, ProcessingType.iWork]:
                        ret = self.handle_new_iprocessing(pr)
                    else:
                        ret = self.handle_new_processing(pr)
                    # self.logger.info(log_pre + "process_new_processing result: %s" % str(ret))

                    self.update_processing(ret, pr)

                    self.logger.info(log_pre + "UpdateTransformEvent(transform_id: %s)" % pr['transform_id'])
                    submit_event_content = {'event': 'submitted'}
                    event = UpdateTransformEvent(publisher_id=self.id, transform_id=pr['transform_id'], content=submit_event_content)
                    event.set_has_updates()
                    self.event_bus.send(event)

                    self.logger.info(log_pre + "SyncProcessingEvent(processing_id: %s)" % pr['processing_id'])
                    event = SyncProcessingEvent(publisher_id=self.id, processing_id=pr['processing_id'])
                    event.set_has_updates()
                    self.event_bus.send(event)
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
        self.number_workers -= 1

    def init_event_function_map(self):
        self.event_func_map = {
            EventType.NewProcessing: {
                'pre_check': self.is_ok_to_run_more_processings,
                'exec_func': self.process_new_processing
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

            task = self.create_task(task_func=self.get_new_processings, task_output_queue=None, task_args=tuple(), task_kwargs={}, delay_time=10, priority=1)
            self.add_task(task)

            self.execute()
        except KeyboardInterrupt:
            self.stop()


if __name__ == '__main__':
    agent = Submitter()
    agent()
