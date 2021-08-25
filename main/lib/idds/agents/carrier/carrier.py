#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019 - 2020

import datetime
import traceback
try:
    # python 3
    from queue import Queue
except ImportError:
    # Python 2
    from Queue import Queue

from idds.common import exceptions
from idds.common.constants import (Sections, ProcessingStatus, ProcessingLocking,
                                   MessageStatus, ContentStatus, ContentType,
                                   ContentRelationType)
from idds.common.utils import setup_logging
from idds.core import (transforms as core_transforms,
                       processings as core_processings)
from idds.agents.common.baseagent import BaseAgent

setup_logging(__name__)


class Carrier(BaseAgent):
    """
    Carrier works to submit and running tasks to WFMS.
    """

    def __init__(self, num_threads=1, poll_time_period=10, retrieve_bulk_size=None,
                 message_bulk_size=1000, **kwargs):
        super(Carrier, self).__init__(num_threads=num_threads, **kwargs)
        self.config_section = Sections.Carrier
        self.poll_time_period = int(poll_time_period)
        self.retrieve_bulk_size = int(retrieve_bulk_size)
        self.message_bulk_size = int(message_bulk_size)

        self.new_task_queue = Queue()
        self.new_output_queue = Queue()
        self.running_task_queue = Queue()
        self.running_output_queue = Queue()
        self.new_processing_size = 0
        self.running_processing_size = 0

    def show_queue_size(self):
        q_str = "new queue size: %s, processing size: %s, output queue size: %s, " % (self.new_task_queue.qsize(),
                                                                                      self.new_processing_size,
                                                                                      self.new_output_queue.qsize())
        q_str += "running queue size: %s, processing size: %s,  output queue size: %s" % (self.running_task_queue.qsize(),
                                                                                          self.running_processing_size,
                                                                                          self.running_output_queue.qsize())
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
            if self.new_task_queue.qsize() > 0 or self.new_output_queue.qsize() > 0:
                return []

            self.show_queue_size()

            processing_status = [ProcessingStatus.New]
            processings = core_processings.get_processings_by_status(status=processing_status, locking=True, bulk_size=self.retrieve_bulk_size)

            self.logger.debug("Main thread get %s [new] processings to process" % len(processings))
            if processings:
                self.logger.info("Main thread get %s [new] processings to process" % len(processings))
            return processings
        except exceptions.DatabaseException as ex:
            if 'ORA-00060' in str(ex):
                self.logger.warn("(cx_Oracle.DatabaseError) ORA-00060: deadlock detected while waiting for resource")
            else:
                # raise ex
                self.logger.error(ex)
                self.logger.error(traceback.format_exc())
        return []

    def process_new_processing(self, processing):
        try:
            # transform_id = processing['transform_id']
            # transform = core_transforms.get_transform(transform_id=transform_id)
            # work = transform['transform_metadata']['work']
            proc = processing['processing_metadata']['processing']
            work = proc.work
            work.set_agent_attributes(self.agent_attributes, processing)

            work.submit_processing(processing)
            ret = {'processing_id': processing['processing_id'],
                   'status': ProcessingStatus.Submitting,
                   'next_poll_at': datetime.datetime.utcnow() + datetime.timedelta(seconds=self.poll_time_period),
                   # 'expired_at': work.get_expired_at(processing),
                   'processing_metadata': processing['processing_metadata']}
            if proc.submitted_at:
                if not processing['submitted_at'] or processing['submitted_at'] < proc.submitted_at:
                    ret['submitted_at'] = proc.submitted_at

            # if processing['processing_metadata'] and 'processing' in processing['processing_metadata']:
            if proc.workload_id:
                ret['workload_id'] = proc.workload_id
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            ret = {'processing_id': processing['processing_id'],
                   'status': ProcessingStatus.Running,
                   'next_poll_at': datetime.datetime.utcnow() + datetime.timedelta(seconds=self.poll_time_period * 4)}
        return ret

    def process_new_processings(self):
        ret = []
        while not self.new_task_queue.empty():
            try:
                processing = self.new_task_queue.get()
                if processing:
                    self.new_processing_size += 1
                    self.logger.info("Main thread processing new processing: %s" % processing)
                    ret_processing = self.process_new_processing(processing)
                    self.new_processing_size -= 1
                    if ret_processing:
                        # ret.append(ret_processing)
                        self.new_output_queue.put(ret_processing)
            except Exception as ex:
                self.logger.error(ex)
                self.logger.error(traceback.format_exc())
        return ret

    def finish_new_processings(self):
        while not self.new_output_queue.empty():
            try:
                processing = self.new_output_queue.get()
                self.logger.info("Main thread submitted new processing: %s" % (processing['processing_id']))
                processing_id = processing['processing_id']
                if 'next_poll_at' not in processing:
                    processing['next_poll_at'] = datetime.datetime.utcnow() + datetime.timedelta(seconds=self.poll_time_period)
                del processing['processing_id']
                processing['locking'] = ProcessingLocking.Idle
                # self.logger.debug("wen: %s" % str(processing))
                core_processings.update_processing(processing_id=processing_id, parameters=processing)
            except Exception as ex:
                self.logger.error(ex)
                self.logger.error(traceback.format_exc())

    def get_running_processings(self):
        """
        Get running processing
        """
        try:
            if self.running_task_queue.qsize() > 0 or self.running_output_queue.qsize() > 0:
                return []

            self.show_queue_size()

            processing_status = [ProcessingStatus.Submitting, ProcessingStatus.Submitted,
                                 ProcessingStatus.Running, ProcessingStatus.FinishedOnExec,
                                 ProcessingStatus.ToCancel, ProcessingStatus.Cancelling,
                                 ProcessingStatus.ToSuspend, ProcessingStatus.Suspending,
                                 ProcessingStatus.ToResume, ProcessingStatus.Resuming,
                                 ProcessingStatus.ToExpire, ProcessingStatus.Expiring,
                                 ProcessingStatus.ToFinish, ProcessingStatus.ToForceFinish]
            processings = core_processings.get_processings_by_status(status=processing_status,
                                                                     # time_period=self.poll_time_period,
                                                                     locking=True,
                                                                     with_messaging=True,
                                                                     bulk_size=self.retrieve_bulk_size)

            self.logger.debug("Main thread get %s [submitting + submitted + running] processings to process: %s" % (len(processings), str([processing['processing_id'] for processing in processings])))
            if processings:
                self.logger.info("Main thread get %s [submitting + submitted + running] processings to process: %s" % (len(processings), str([processing['processing_id'] for processing in processings])))
            return processings
        except exceptions.DatabaseException as ex:
            if 'ORA-00060' in str(ex):
                self.logger.warn("(cx_Oracle.DatabaseError) ORA-00060: deadlock detected while waiting for resource")
            else:
                # raise ex
                self.logger.error(ex)
                self.logger.error(traceback.format_exc())
        return []

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

    def process_running_processing(self, processing):
        try:
            transform_id = processing['transform_id']
            # transform = core_transforms.get_transform(transform_id=transform_id)
            # work = transform['transform_metadata']['work']
            # work = processing['processing_metadata']['work']
            # work.set_agent_attributes(self.agent_attributes)
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
            is_operation = False
            if processing['status'] in [ProcessingStatus.ToCancel]:
                work.abort_processing(processing)
                is_operation = True
                # processing_substatus = ProcessingStatus.Cancelling
            if processing['status'] in [ProcessingStatus.ToSuspend]:
                work.suspend_processing(processing)
                is_operation = True
                # processing_substatus = ProcessingStatus.Suspending
            if processing['status'] in [ProcessingStatus.ToResume]:
                work.resume_processing(processing)
                is_operation = True
                # processing_substatus = ProcessingStatus.Resuming
            if processing['status'] in [ProcessingStatus.ToExpire]:
                work.expire_processing(processing)
                is_operation = True
                # processing_substatus = ProcessingStatus.Expiring
            if processing['status'] in [ProcessingStatus.ToFinish]:
                work.finish_processing(processing)
                is_operation = True
                # processing_substatus = ProcessingStatus.Running
            if processing['status'] in [ProcessingStatus.ToForceFinish]:
                work.finish_processing(processing, forcing=True)
                is_operation = True
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

            # if processing_substatus:
            #     processing_update['parameters']['substatus'] = processing_substatus

            if not is_operation:
                next_poll_at = datetime.datetime.utcnow() + datetime.timedelta(seconds=self.poll_time_period)
            else:
                if processing['status'] in [ProcessingStatus.ToResume]:
                    next_poll_at = datetime.datetime.utcnow() + datetime.timedelta(seconds=self.poll_operation_time_period * 5)
                else:
                    next_poll_at = datetime.datetime.utcnow() + datetime.timedelta(seconds=self.poll_operation_time_period)

            if proc.submitted_at:
                if not processing['submitted_at'] or processing['submitted_at'] < proc.submitted_at:
                    processing_update['parameters']['submitted_at'] = proc.submitted_at

            processing_update['parameters']['next_poll_at'] = next_poll_at
            # processing_update['parameters']['expired_at'] = work.get_expired_at(processing)
            processing_update['parameters']['processing_metadata'] = processing['processing_metadata']

            ret = {'processing_update': processing_update,
                   'content_updates': content_updates,
                   'new_contents': new_contents}
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            processing_update = {'processing_id': processing['processing_id'],
                                 'parameters': {'status': ProcessingStatus.Running,
                                                'locking': ProcessingLocking.Idle,
                                                'next_poll_at': datetime.datetime.utcnow() + datetime.timedelta(seconds=self.poll_time_period * 4)}}
            ret = {'processing_update': processing_update,
                   'content_updates': []}
        return ret

    def process_running_processing_message(self, processing, messages):
        """
        process running processing message
        """
        try:
            self.logger.info("process_running_processing_message: processing_id: %s, messages: %s" % (processing['processing_id'], str(messages) if messages else messages))
            msg = messages[0]
            message = messages[0]['msg_content']
            if message['command'] == 'update_processing':
                parameters = message['parameters']
                parameters['locking'] = ProcessingLocking.Idle
                processing_update = {'processing_id': processing['processing_id'],
                                     'parameters': parameters,
                                     }
                update_messages = [{'msg_id': msg['msg_id'], 'status': MessageStatus.Delivered}]
            else:
                self.logger.error("Unknown message: %s" % str(msg))
                processing_update = {'processing_id': processing['processing_id'],
                                     'parameters': {'locking': ProcessingLocking.Idle}
                                     }
                update_messages = [{'msg_id': msg['msg_id'], 'status': MessageStatus.Failed}]

            ret = {'processing_update': processing_update,
                   'content_updates': [],
                   'update_messages': update_messages}
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            processing_update = {'processing_id': processing['processing_id'],
                                 'parameters': {'status': ProcessingStatus.Failed,
                                                'locking': ProcessingLocking.Idle,
                                                'errors': {'msg': '%s: %s' % (ex, traceback.format_exc())}}}
            ret = {'processing_update': processing_update,
                   'content_updates': []}
        return ret

    def process_running_processings(self):
        ret = []
        while not self.running_task_queue.empty():
            try:
                processing = self.running_task_queue.get()
                if processing:
                    self.running_processing_size += 1
                    self.logger.debug("Main thread processing running processing: %s" % processing)
                    self.logger.info("Main thread processing running processing: %s" % processing['processing_id'])

                    msgs = self.get_processing_message(processing_id=processing['processing_id'], bulk_size=1)
                    if msgs:
                        ret_processing = self.process_running_processing_message(processing, msgs)
                    else:
                        ret_processing = self.process_running_processing(processing)
                    self.running_processing_size -= 1
                    if ret_processing:
                        # ret.append(ret_processing)
                        self.running_output_queue.put(ret_processing)
            except Exception as ex:
                self.logger.error(ex)
                self.logger.error(traceback.format_exc())
        return ret

    def finish_running_processings(self):
        while not self.running_output_queue.empty():
            try:
                processing = self.running_output_queue.get()
                if processing:
                    self.logger.info("Main thread processing(processing_id: %s) updates: %s" % (processing['processing_update']['processing_id'],
                                                                                                processing['processing_update']['parameters']))

                    # self.logger.info("Main thread finishing running processing %s" % str(processing))
                    core_processings.update_processing_contents(processing_update=processing.get('processing_update', None),
                                                                content_updates=processing.get('content_updates', None),
                                                                update_messages=processing.get('update_messages', None),
                                                                new_contents=processing.get('new_contents', None))
            except Exception as ex:
                self.logger.error(ex)
                self.logger.error(traceback.format_exc())

    def clean_locks(self):
        self.logger.info("clean locking")
        core_processings.clean_locking()

    def run(self):
        """
        Main run function.
        """
        try:
            self.logger.info("Starting main thread")

            self.load_plugins()
            self.init()

            self.add_default_tasks()

            task = self.create_task(task_func=self.get_new_processings, task_output_queue=self.new_task_queue, task_args=tuple(), task_kwargs={}, delay_time=1, priority=1)
            self.add_task(task)
            for _ in range(self.num_threads):
                # task = self.create_task(task_func=self.process_new_processings, task_output_queue=self.new_output_queue, task_args=tuple(), task_kwargs={}, delay_time=1, priority=1)
                task = self.create_task(task_func=self.process_new_processings, task_output_queue=None, task_args=tuple(), task_kwargs={}, delay_time=1, priority=1)
                self.add_task(task)
            task = self.create_task(task_func=self.finish_new_processings, task_output_queue=None, task_args=tuple(), task_kwargs={}, delay_time=1, priority=1)
            self.add_task(task)

            task = self.create_task(task_func=self.get_running_processings, task_output_queue=self.running_task_queue, task_args=tuple(), task_kwargs={}, delay_time=1, priority=1)
            self.add_task(task)
            for _ in range(self.num_threads):
                # task = self.create_task(task_func=self.process_running_processings, task_output_queue=self.running_output_queue, task_args=tuple(), task_kwargs={}, delay_time=1, priority=1)
                task = self.create_task(task_func=self.process_running_processings, task_output_queue=None, task_args=tuple(), task_kwargs={}, delay_time=1, priority=1)
                self.add_task(task)
            task = self.create_task(task_func=self.finish_running_processings, task_output_queue=None, task_args=tuple(), task_kwargs={}, delay_time=1, priority=1)
            self.add_task(task)

            task = self.create_task(task_func=self.clean_locks, task_output_queue=None, task_args=tuple(), task_kwargs={}, delay_time=1800, priority=1)
            self.add_task(task)

            self.execute()
        except KeyboardInterrupt:
            self.stop()


if __name__ == '__main__':
    agent = Carrier()
    agent()
