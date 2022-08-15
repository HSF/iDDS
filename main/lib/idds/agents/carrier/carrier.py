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
from idds.common.constants import (Sections, ProcessingStatus, ProcessingLocking,
                                   ContentStatus, ContentType,
                                   ContentRelationType)
from idds.common.utils import setup_logging, truncate_string
from idds.core import (transforms as core_transforms,
                       processings as core_processings)
from idds.agents.common.baseagent import BaseAgent
from idds.agents.common.eventbus.event import (NewProcessingEvent,
                                               UpdateProcessingEvent,
                                               AbortProcessingEvent,
                                               ResumeProcessingEvent,
                                               UpdateTransformEvent)

setup_logging(__name__)


class Carrier(BaseAgent):
    """
    Carrier works to submit and running tasks to WFMS.
    """

    def __init__(self, num_threads=1, poll_time_period=10, retries=3, retrieve_bulk_size=None,
                 message_bulk_size=1000, **kwargs):
        super(Carrier, self).__init__(num_threads=num_threads, **kwargs)
        self.config_section = Sections.Carrier
        self.poll_time_period = int(poll_time_period)
        self.retries = int(retries)
        self.retrieve_bulk_size = int(retrieve_bulk_size)
        self.message_bulk_size = int(message_bulk_size)

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
        q_str = "number of processings: %s, max number of processings: %s" % (self.number_workers, self.max_number_workers)
        self.logger.debug(q_str)

    def init(self):
        status = [ProcessingStatus.New, ProcessingStatus.Submitting, ProcessingStatus.Submitted,
                  ProcessingStatus.Running, ProcessingStatus.FinishedOnExec]
        core_processings.clean_next_poll_at(status)

    def get_new_processings(self):
        """
        Get new processing
        """
        try:
            if not self.is_ok_to_run_more_requests():
                return []

            self.show_queue_size()

            processing_status = [ProcessingStatus.New]
            processings = core_processings.get_processings_by_status(status=processing_status, locking=True,
                                                                     not_lock=True,
                                                                     new_poll=True, only_return_id=True,
                                                                     bulk_size=self.retrieve_bulk_size)

            self.logger.debug("Main thread get %s [new] processings to process" % len(processings))
            if processings:
                self.logger.info("Main thread get %s [new] processings to process" % len(processings))

            for pr in processings:
                event = NewProcessingEvent(publisher_id=self.id, processing_id=pr['processing_id'])
                self.event_bus.send(event)

            return processings
        except exceptions.DatabaseException as ex:
            if 'ORA-00060' in str(ex):
                self.logger.warn("(cx_Oracle.DatabaseError) ORA-00060: deadlock detected while waiting for resource")
            else:
                # raise ex
                self.logger.error(ex)
                self.logger.error(traceback.format_exc())
        return []

    def get_running_processings(self):
        """
        Get running processing
        """
        try:
            if not self.is_ok_to_run_more_requests():
                return []

            self.show_queue_size()

            processing_status = [ProcessingStatus.Submitting, ProcessingStatus.Submitted,
                                 ProcessingStatus.Running, ProcessingStatus.FinishedOnExec,
                                 ProcessingStatus.ToCancel, ProcessingStatus.Cancelling,
                                 ProcessingStatus.ToSuspend, ProcessingStatus.Suspending,
                                 ProcessingStatus.ToResume, ProcessingStatus.Resuming,
                                 ProcessingStatus.ToExpire, ProcessingStatus.Expiring,
                                 ProcessingStatus.ToFinish, ProcessingStatus.ToForceFinish]
            # next_poll_at = datetime.datetime.utcnow() + datetime.timedelta(seconds=self.poll_time_period)
            processings = core_processings.get_processings_by_status(status=processing_status,
                                                                     locking=True, update_poll=True,
                                                                     not_lock=True,
                                                                     only_return_id=True,
                                                                     bulk_size=self.retrieve_bulk_size)

            self.logger.debug("Main thread get %s [submitting + submitted + running] processings to process: %s" % (len(processings), str([processing['processing_id'] for processing in processings])))
            if processings:
                self.logger.info("Main thread get %s [submitting + submitted + running] processings to process: %s" % (len(processings), str([processing['processing_id'] for processing in processings])))

            for pr in processings:
                event = UpdateProcessingEvent(publisher_id=self.id, processing_id=pr['processing_id'])
                self.event_bus.send(event)

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
            return core_processings.get_processing_by_id_status(processing_id=processing_id, status=status, locking=locking)
        except exceptions.DatabaseException as ex:
            if 'ORA-00060' in str(ex):
                self.logger.warn("(cx_Oracle.DatabaseError) ORA-00060: deadlock detected while waiting for resource")
            else:
                # raise ex
                self.logger.error(ex)
                self.logger.error(traceback.format_exc())
        return None

    def load_poll_period(self, processing, parameters):
        if self.new_poll_period and processing['new_poll_period'] != self.new_poll_period:
            parameters['new_poll_period'] = self.new_poll_period
        if self.update_poll_period and processing['update_poll_period'] != self.update_poll_period:
            parameters['update_poll_period'] = self.update_poll_period
        return parameters

    def handle_new_processing(self, processing):
        try:
            # transform_id = processing['transform_id']
            # transform = core_transforms.get_transform(transform_id=transform_id)
            # work = transform['transform_metadata']['work']
            proc = processing['processing_metadata']['processing']
            work = proc.work
            work.set_agent_attributes(self.agent_attributes, processing)

            work.submit_processing(processing)
            parameters = {'status': ProcessingStatus.Submitting,
                          'locking': ProcessingLocking.Idle,
                          'processing_metadata': processing['processing_metadata']}
            parameters = self.load_poll_period(processing, parameters)

            if proc.submitted_at:
                if not processing['submitted_at'] or processing['submitted_at'] < proc.submitted_at:
                    parameters['submitted_at'] = proc.submitted_at

            # if processing['processing_metadata'] and 'processing' in processing['processing_metadata']:
            if proc.workload_id:
                parameters['workload_id'] = proc.workload_id

            processing_update = {'processing_id': processing['processing_id'],
                                 'parameters': parameters}
            ret = {'processing_update': processing_update,
                   'content_updates': []}
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())

            retries = processing['new_retries'] + 1
            if not processing['max_new_retries'] or retries < processing['max_new_retries']:
                parameters = {'status': ProcessingStatus.New,
                              'new_retries': retries}
            else:
                error = {'submit_err': {'msg': truncate_string('%s: %s' % (ex, traceback.format_exc()), length=200)}}
                parameters = {'status': ProcessingStatus.Failed,
                              'errors': processing['errors'] if processing['errors'] else {},
                              'new_retries': retries}
                parameters['errors'].update(error)
            parameters = self.load_poll_period(processing, parameters)

            processing_update = {'processing_id': processing['processing_id'],
                                 'parameters': parameters}
            ret = {'processing_update': processing_update,
                   'content_updates': []}
        return ret

    def update_processing(self, processing):
        try:
            if processing:
                self.logger.info("Main thread processing(processing_id: %s) updates: %s" % (processing['processing_update']['processing_id'],
                                                                                            processing['processing_update']['parameters']))
                processing['processing_update']['parameters']['locking'] = ProcessingLocking.Idle
                # self.logger.debug("wen: %s" % str(processing))

                retry = True
                retry_num = 0
                while retry:
                    retry = False
                    retry_num += 1
                    try:
                        core_processings.update_processing_contents(processing_update=processing.get('processing_update', None),
                                                                    content_updates=processing.get('content_updates', None),
                                                                    update_messages=processing.get('update_messages', None),
                                                                    new_contents=processing.get('new_contents', None))
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
                processing_id = processing['processing_update']['processing_id']

                parameters = {'status': processing['processing_update']['status'],
                              'locking': ProcessingLocking.Idle}
                if 'new_retries' in processing['processing_update']:
                    parameters['new_retries'] = processing['processing_update']['new_retries']
                if 'update_retries' in processing['processing_update']:
                    parameters['update_retries'] = processing['processing_update']['update_retries']
                if 'errors' in processing['processing_update']:
                    parameters['errors'] = processing['processing_update']['errors']
                core_processings.update_processing(processing_id=processing_id, parameters=parameters)
            except Exception as ex:
                self.logger.error(ex)
                self.logger.error(traceback.format_exc())

    def process_new_processing(self, event):
        self.number_workers += 1
        try:
            if event:
                pr_status = [ProcessingStatus.New]
                pr = self.get_processing(processing_id=event.processing_id, status=pr_status, locking=True)
                if pr:
                    ret = self.handle_new_processing(pr)
                    self.update_processing(ret)
                    event = UpdateTransformEvent(publisher_id=self.id, transform_id=pr['processing_id'])
                    self.event_bus.send(event)
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
        self.number_workers -= 1

    def get_collection_ids(self, collections):
        coll_ids = []
        for coll in collections:
            coll_ids.append(coll.coll_id)
        return coll_ids

    def get_new_contents(self, processing, new_input_output_maps):
        new_input_contents, new_output_contents, new_log_contents = [], [], []
        new_input_dependency_contents = []
        for map_id in new_input_output_maps:
            inputs = new_input_output_maps[map_id]['inputs'] if 'inputs' in new_input_output_maps[map_id] else []
            inputs_dependency = new_input_output_maps[map_id]['inputs_dependency'] if 'inputs_dependency' in new_input_output_maps[map_id] else []
            outputs = new_input_output_maps[map_id]['outputs'] if 'outputs' in new_input_output_maps[map_id] else []
            logs = new_input_output_maps[map_id]['logs'] if 'logs' in new_input_output_maps[map_id] else []

            for input_content in inputs:
                content = {'transform_id': processing['transform_id'],
                           'coll_id': input_content['coll_id'],
                           'request_id': processing['request_id'],
                           'workload_id': processing['workload_id'],
                           'map_id': map_id,
                           'scope': input_content['scope'],
                           'name': input_content['name'],
                           'min_id': input_content['min_id'] if 'min_id' in input_content else 0,
                           'max_id': input_content['max_id'] if 'max_id' in input_content else 0,
                           'status': input_content['status'] if 'status' in input_content and input_content['status'] is not None else ContentStatus.New,
                           'substatus': input_content['substatus'] if 'substatus' in input_content and input_content['substatus'] is not None else ContentStatus.New,
                           'path': input_content['path'] if 'path' in input_content else None,
                           'content_type': input_content['content_type'] if 'content_type' in input_content else ContentType.File,
                           'content_relation_type': ContentRelationType.Input,
                           'bytes': input_content['bytes'],
                           'adler32': input_content['adler32'],
                           'content_metadata': input_content['content_metadata']}
                if content['min_id'] is None:
                    content['min_id'] = 0
                if content['max_id'] is None:
                    content['max_id'] = 0
                new_input_contents.append(content)
            for input_content in inputs_dependency:
                content = {'transform_id': processing['transform_id'],
                           'coll_id': input_content['coll_id'],
                           'request_id': processing['request_id'],
                           'workload_id': processing['workload_id'],
                           'map_id': map_id,
                           'scope': input_content['scope'],
                           'name': input_content['name'],
                           'min_id': input_content['min_id'] if 'min_id' in input_content else 0,
                           'max_id': input_content['max_id'] if 'max_id' in input_content else 0,
                           'status': input_content['status'] if 'status' in input_content and input_content['status'] is not None else ContentStatus.New,
                           'substatus': input_content['substatus'] if 'substatus' in input_content and input_content['substatus'] is not None else ContentStatus.New,
                           'path': input_content['path'] if 'path' in input_content else None,
                           'content_type': input_content['content_type'] if 'content_type' in input_content else ContentType.File,
                           'content_relation_type': ContentRelationType.InputDependency,
                           'bytes': input_content['bytes'],
                           'adler32': input_content['adler32'],
                           'content_metadata': input_content['content_metadata']}
                if content['min_id'] is None:
                    content['min_id'] = 0
                if content['max_id'] is None:
                    content['max_id'] = 0
                new_input_dependency_contents.append(content)
            for output_content in outputs:
                content = {'transform_id': processing['transform_id'],
                           'coll_id': output_content['coll_id'],
                           'request_id': processing['request_id'],
                           'workload_id': processing['workload_id'],
                           'map_id': map_id,
                           'scope': output_content['scope'],
                           'name': output_content['name'],
                           'min_id': output_content['min_id'] if 'min_id' in output_content else 0,
                           'max_id': output_content['max_id'] if 'max_id' in output_content else 0,
                           'status': ContentStatus.New,
                           'substatus': ContentStatus.New,
                           'path': output_content['path'] if 'path' in output_content else None,
                           'content_type': output_content['content_type'] if 'content_type' in output_content else ContentType.File,
                           'content_relation_type': ContentRelationType.Output,
                           'bytes': output_content['bytes'],
                           'adler32': output_content['adler32'],
                           'content_metadata': output_content['content_metadata']}
                if content['min_id'] is None:
                    content['min_id'] = 0
                if content['max_id'] is None:
                    content['max_id'] = 0
                new_output_contents.append(content)
            for log_content in logs:
                content = {'transform_id': processing['transform_id'],
                           'coll_id': log_content['coll_id'],
                           'request_id': processing['request_id'],
                           'workload_id': processing['workload_id'],
                           'map_id': map_id,
                           'scope': log_content['scope'],
                           'name': log_content['name'],
                           'min_id': log_content['min_id'] if 'min_id' in log_content else 0,
                           'max_id': log_content['max_id'] if 'max_id' in log_content else 0,
                           'status': ContentStatus.New,
                           'substatus': ContentStatus.New,
                           'path': log_content['path'] if 'path' in log_content else None,
                           'content_type': log_content['content_type'] if 'content_type' in log_content else ContentType.File,
                           'content_relation_type': ContentRelationType.Log,
                           'bytes': log_content['bytes'],
                           'adler32': log_content['adler32'],
                           'content_metadata': log_content['content_metadata']}
                if content['min_id'] is None:
                    content['min_id'] = 0
                if content['max_id'] is None:
                    content['max_id'] = 0
                new_output_contents.append(content)
        return new_input_contents + new_output_contents + new_log_contents + new_input_dependency_contents

    def handle_update_processing(self, processing):
        try:
            transform_id = processing['transform_id']
            # transform = core_transforms.get_transform(transform_id=transform_id)
            # work = transform['transform_metadata']['work']
            # work = processing['processing_metadata']['work']
            # work.set_agent_attributes(self.agent_attributes)
            if 'processing' not in processing['processing_metadata']:
                raise exceptions.ProcessFormatNotSupported

            proc = processing['processing_metadata']['processing']
            work = proc.work
            work.set_agent_attributes(self.agent_attributes, processing)

            input_collections = work.get_input_collections()
            output_collections = work.get_output_collections()
            log_collections = work.get_log_collections()

            input_coll_ids = self.get_collection_ids(input_collections)
            output_coll_ids = self.get_collection_ids(output_collections)
            log_coll_ids = self.get_collection_ids(log_collections)

            input_output_maps = core_transforms.get_transform_input_output_maps(transform_id,
                                                                                input_coll_ids=input_coll_ids,
                                                                                output_coll_ids=output_coll_ids,
                                                                                log_coll_ids=log_coll_ids)

            # processing_substatus = None
            # is_operation = False
            if processing['status'] in [ProcessingStatus.ToCancel]:
                work.abort_processing(processing)
                # is_operation = True
                # processing_substatus = ProcessingStatus.Cancelling
            if processing['status'] in [ProcessingStatus.ToSuspend]:
                work.suspend_processing(processing)
                # is_operation = True
                # processing_substatus = ProcessingStatus.Suspending
            if processing['status'] in [ProcessingStatus.ToResume]:
                work.resume_processing(processing)
                # is_operation = True
                # processing_substatus = ProcessingStatus.Resuming
            if processing['status'] in [ProcessingStatus.ToExpire]:
                work.expire_processing(processing)
                # is_operation = True
                # processing_substatus = ProcessingStatus.Expiring
            if processing['status'] in [ProcessingStatus.ToFinish]:
                work.finish_processing(processing)
                # is_operation = True
                # processing_substatus = ProcessingStatus.Running
            if processing['status'] in [ProcessingStatus.ToForceFinish]:
                work.finish_processing(processing, forcing=True)
                # is_operation = True
                # processing_substatus = ProcessingStatus.Running

            # work = processing['processing_metadata']['work']
            # outputs = work.poll_processing()
            processing_update, content_updates, new_input_output_maps = work.poll_processing_updates(processing, input_output_maps)
            new_contents = self.get_new_contents(processing, new_input_output_maps)
            if processing_update:
                processing_update['parameters']['locking'] = ProcessingLocking.Idle
            else:
                processing_update = {'processing_id': processing['processing_id'],
                                     'parameters': {'locking': ProcessingLocking.Idle}}

            processing_update['parameters'] = self.load_poll_period(processing, processing_update['parameters'])

            if proc.submitted_at:
                if not processing['submitted_at'] or processing['submitted_at'] < proc.submitted_at:
                    processing_update['parameters']['submitted_at'] = proc.submitted_at

            if proc.workload_id:
                processing_update['parameters']['workload_id'] = proc.workload_id

            # processing_update['parameters']['expired_at'] = work.get_expired_at(processing)
            processing_update['parameters']['processing_metadata'] = processing['processing_metadata']

            ret = {'processing_update': processing_update,
                   'content_updates': content_updates,
                   'new_contents': new_contents}

        except exceptions.ProcessFormatNotSupported as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())

            retries = processing['update_retries'] + 1
            if not processing['max_update_retries'] or retries < processing['max_update_retries']:
                proc_status = ProcessingStatus.Running
            else:
                proc_status = ProcessingStatus.Failed
            error = {'update_err': {'msg': truncate_string('%s: %s' % (ex, traceback.format_exc()), length=200)}}
            processing_update = {'processing_id': processing['processing_id'],
                                 'parameters': {'status': proc_status,
                                                'locking': ProcessingLocking.Idle,
                                                'update_retries': retries,
                                                'errors': processing['errors'] if processing['errors'] else {}}}
            processing_update['parameters']['errors'].update(error)
            processing_update['parameters'] = self.load_poll_period(processing, processing_update['parameters'])

            ret = {'processing_update': processing_update,
                   'content_updates': []}
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())

            retries = processing['update_retries'] + 1
            if not processing['max_update_retries'] or retries < processing['max_update_retries']:
                proc_status = ProcessingStatus.Running
            else:
                proc_status = ProcessingStatus.Failed
            error = {'update_err': {'msg': truncate_string('%s: %s' % (ex, traceback.format_exc()), length=200)}}
            processing_update = {'processing_id': processing['processing_id'],
                                 'parameters': {'status': proc_status,
                                                'locking': ProcessingLocking.Idle,
                                                'update_retries': retries,
                                                'errors': processing['errors'] if processing['errors'] else {}}}
            processing_update['parameters']['errors'].update(error)
            processing_update['parameters'] = self.load_poll_period(processing, processing_update['parameters'])

            ret = {'processing_update': processing_update,
                   'content_updates': []}
        return ret

    def process_update_processing(self, event):
        self.number_workers += 1
        try:
            if event:
                processing_status = [ProcessingStatus.Submitting, ProcessingStatus.Submitted,
                                     ProcessingStatus.Running, ProcessingStatus.FinishedOnExec,
                                     ProcessingStatus.ToCancel, ProcessingStatus.Cancelling,
                                     ProcessingStatus.ToSuspend, ProcessingStatus.Suspending,
                                     ProcessingStatus.ToResume, ProcessingStatus.Resuming,
                                     ProcessingStatus.ToExpire, ProcessingStatus.Expiring,
                                     ProcessingStatus.ToFinish, ProcessingStatus.ToForceFinish]

                pr = self.get_processing(processing_id=event.processing_id, status=processing_status, locking=True)
                if pr:
                    ret = self.handle_update_processing(pr)
                    self.update_processing(ret)
                    event = UpdateTransformEvent(publisher_id=self.id, transform_id=pr['processing_id'])
                    self.event_bus.send(event)
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
        self.number_workers -= 1

    def handle_abort_processing(self, processing):
        """
        process abort processing
        """
        try:
            proc = processing['processing_metadata']['processing']
            work = proc.work
            work.set_agent_attributes(self.agent_attributes, processing)
            work.abort_processing(processing)

            processing_update = {'processing_id': processing['processing_id'],
                                 'parameters': {'status': ProcessingStatus.Cancelling,
                                                'locking': ProcessingLocking.Idle}}
            ret = {'processing_update': processing_update}
            return ret
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            error = {'abort_err': {'msg': truncate_string('%s: %s' % (ex, traceback.format_exc()), length=200)}}
            processing_update = {'processing_id': processing['processing_id'],
                                 'parameters': {'status': ProcessingStatus.ToCancel,
                                                'locking': ProcessingLocking.Idle,
                                                'errors': processing['errors'] if processing['errors'] else {}}}
            processing_update['parameters']['errors'].update(error)
            ret = {'processing_update': processing_update}
            return ret
        return None

    def process_abort_processing(self, event):
        self.number_workers += 1
        try:
            if event:
                processing_status = [ProcessingStatus.Finished, ProcessingStatus.Failed,
                                     ProcessingStatus.Lost, ProcessingStatus.Cancelled,
                                     ProcessingStatus.Suspended, ProcessingStatus.Expired,
                                     ProcessingStatus.Broken]

                pr = self.get_processing(processing_id=event.processing_id, status=processing_status, locking=True)
                if pr and pr['status'] in processing_status:
                    processing_update = {'processing_id': pr['processing_id'],
                                         'parameters': {'locking': ProcessingLocking.Idle,
                                                        'errors': {'abort_err': {'msg': truncate_string("Processing is already terminated. Cannot be aborted", length=200)}}}}
                    ret = {'processing_update': processing_update}
                    self.update_processing(ret)
                elif pr:
                    ret = self.handle_abort_processing(pr)
                    self.update_processing(ret)
                    event = UpdateTransformEvent(publisher_id=self.id, transform_id=pr['processing_id'])
                    self.event_bus.send(event)
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
        self.number_workers -= 1

    def handle_resume_processing(self, processing):
        """
        process resume processing
        """
        try:
            proc = processing['processing_metadata']['processing']
            work = proc.work
            work.set_agent_attributes(self.agent_attributes, processing)
            work.resume_processing(processing)

            processing_update = {'processing_id': processing['processing_id'],
                                 'parameters': {'status': ProcessingStatus.Resuming,
                                                'locking': ProcessingLocking.Idle}}
            ret = {'processing_update': processing_update}
            return ret
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            error = {'abort_err': {'msg': truncate_string('%s: %s' % (ex, traceback.format_exc()), length=200)}}
            processing_update = {'processing_id': processing['processing_id'],
                                 'parameters': {'status': ProcessingStatus.ToResume,
                                                'locking': ProcessingLocking.Idle,
                                                'errors': processing['errors'] if processing['errors'] else {}}}
            processing_update['parameters']['errors'].update(error)
            ret = {'processing_update': processing_update}
            return ret
        return None

    def process_resume_processing(self, event):
        self.number_workers += 1
        try:
            if event:
                processing_status = [ProcessingStatus.Finished]

                pr = self.get_processing(processing_id=event.processing_id, status=processing_status, locking=True)
                if pr and pr['status'] in processing_status:
                    processing_update = {'processing_id': pr['processing_id'],
                                         'parameters': {'locking': ProcessingLocking.Idle,
                                                        'errors': {'abort_err': {'msg': truncate_string("Processing has already finished. Cannot be resumed", length=200)}}}}
                    ret = {'processing_update': processing_update}
                    self.update_processing(ret)
                elif pr:
                    ret = self.handle_resume_processing(pr)
                    self.update_processing(ret)
                    event = UpdateTransformEvent(publisher_id=self.id, transform_id=pr['processing_id'])
                    self.event_bus.send(event)
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
        self.number_workers -= 1

    def clean_locks(self):
        self.logger.info("clean locking")
        core_processings.clean_locking()

    def init_event_function_map(self):
        self.event_func_map = {
            NewProcessingEvent._event_type: {
                'pre_check': self.is_ok_to_run_more_requests,
                'exec_func': self.process_new_processing
            },
            UpdateProcessingEvent._event_type: {
                'pre_check': self.is_ok_to_run_more_requests,
                'exec_func': self.process_update_processing
            },
            AbortProcessingEvent._event_type: {
                'pre_check': self.is_ok_to_run_more_requests,
                'exec_func': self.process_abort_processing
            },
            ResumeProcessingEvent._event_type: {
                'pre_check': self.is_ok_to_run_more_requests,
                'exec_func': self.process_resume_processing
            }
        }

    def run(self):
        """
        Main run function.
        """
        try:
            self.logger.info("Starting main thread")

            self.load_plugins()
            self.init()

            self.add_default_tasks()

            task = self.create_task(task_func=self.get_new_processings, task_output_queue=None, task_args=tuple(), task_kwargs={}, delay_time=60, priority=1)
            self.add_task(task)

            task = self.create_task(task_func=self.get_running_processings, task_output_queue=None, task_args=tuple(), task_kwargs={}, delay_time=60, priority=1)
            self.add_task(task)

            task = self.create_task(task_func=self.clean_locks, task_output_queue=None, task_args=tuple(), task_kwargs={}, delay_time=1800, priority=1)
            self.add_task(task)

            self.execute()
        except KeyboardInterrupt:
            self.stop()


if __name__ == '__main__':
    agent = Carrier()
    agent()
