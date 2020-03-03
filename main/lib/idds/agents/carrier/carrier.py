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

from idds.common.constants import (Sections, TransformType, ProcessingStatus, ProcessingLocking,
                                   MessageType, MessageStatus, MessageSource)
from idds.common.exceptions import (AgentPluginError, IDDSException)
from idds.common.utils import setup_logging
from idds.core import (catalog as core_catalog, transforms as core_transforms,
                       processings as core_processings)
from idds.agents.common.baseagent import BaseAgent

setup_logging(__name__)


class Carrier(BaseAgent):
    """
    Carrier works to submit and monitor tasks to WFMS.
    """

    def __init__(self, num_threads=1, poll_time_period=1800, retrieve_bulk_size=None,
                 message_bulk_size=1000, **kwargs):
        super(Carrier, self).__init__(num_threads=num_threads, **kwargs)
        self.config_section = Sections.Carrier
        self.poll_time_period = int(poll_time_period)
        self.retrieve_bulk_size = int(retrieve_bulk_size)
        self.message_bulk_size = int(message_bulk_size)

        self.new_output_queue = Queue()
        self.monitor_output_queue = Queue()

    def get_new_processings(self):
        """
        Get new processing
        """
        processing_status = [ProcessingStatus.New]
        processings = core_processings.get_processings_by_status(status=processing_status, locking=True, bulk_size=self.retrieve_bulk_size)
        self.logger.info("Main thread get %s [new] processings to process" % len(processings))
        return processings

    def submit_processing(self, processing, transform, input_collection, output_collection):
        if transform['transform_type'] == TransformType.StageIn:
            if 'stagein_submitter' not in self.plugins:
                raise AgentPluginError('Plugin stagein_submitter is required')
            return self.plugins['stagein_submitter'](processing, transform, input_collection, output_collection)

        return None

    def process_new_processings(self, processing):
        transform_id = processing['transform_id']
        processing_metadata = processing['processing_metadata']
        input_coll_id = processing_metadata['input_collection']
        input_collection = core_catalog.get_collection(coll_id=input_coll_id)
        output_coll_id = processing_metadata['output_collection']
        output_collection = core_catalog.get_collection(coll_id=output_coll_id)
        transform = core_transforms.get_transform(transform_id)

        ret = self.submit_processing(processing, transform, input_collection, output_collection)
        if ret:
            return ret
        else:
            return {'processing_id': processing['processing_id'],
                    'locking': ProcessingLocking.Idle}

    def finish_new_processings(self):
        while not self.new_output_queue.empty():
            processing = self.new_output_queue.get()
            self.logger.info("Main thread submitted new processing: %s" % (processing['processing_id']))
            processing_id = processing['processing_id']
            del processing['processing_id']
            core_processings.update_processing(processing_id=processing_id, parameters=processing)

    def get_monitor_processings(self):
        """
        Get monitor processing
        """
        processing_status = [ProcessingStatus.Submitting, ProcessingStatus.Submitted, ProcessingStatus.Running]
        processings = core_processings.get_processings_by_status(status=processing_status,
                                                                 time_period=self.poll_time_period,
                                                                 locking=True,
                                                                 bulk_size=self.retrieve_bulk_size)
        self.logger.info("Main thread get %s [submitting + submitted + running] processings to process: %s" % (len(processings), str([processing['processing_id'] for processing in processings])))
        return processings

    def poll_processing(self, processing, transform, input_collection, output_collection, output_contents):
        if transform['transform_type'] == TransformType.StageIn:
            if 'stagein_poller' not in self.plugins:
                raise AgentPluginError('Plugin stagein_poller is required')
            return self.plugins['stagein_poller'](processing, transform, input_collection, output_collection, output_contents)

        return None

    def generate_file_message(self, transform, files):
        if not files:
            return None

        updated_files_message = []
        for file in files:
            updated_file_message = {'scope': file['scope'],
                                    'name': file['name'],
                                    'status': file['status'].name}
            updated_files_message.append(updated_file_message)

        workload_id = None
        if 'workload_id' in transform['transform_metadata']:
            workload_id = transform['transform_metadata']['workload_id']

        msg_content = {'msg_type': 'file_stagein',
                       'workload_id': workload_id,
                       'files': updated_files_message}
        file_msg_content = {'msg_type': MessageType.StageInFile,
                            'status': MessageStatus.New,
                            'source': MessageSource.Carrier,
                            'transform_id': transform['transform_id'],
                            'num_contents': len(updated_files_message),
                            'msg_content': msg_content}
        return file_msg_content

    def process_monitor_processings(self, processing):
        transform_id = processing['transform_id']
        processing_metadata = processing['processing_metadata']
        input_coll_id = processing_metadata['input_collection']
        input_collection = core_catalog.get_collection(coll_id=input_coll_id)
        output_coll_id = processing_metadata['output_collection']
        output_collection = core_catalog.get_collection(coll_id=output_coll_id)
        output_contents = core_catalog.get_contents_by_coll_id_status(coll_id=output_coll_id)
        transform = core_transforms.get_transform(transform_id)

        ret_poll = self.poll_processing(processing, transform, input_collection, output_collection, output_contents)
        if not ret_poll:
            return {'processing_id': processing['processing_id'],
                    'locking': ProcessingLocking.Idle}

        updated_files = ret_poll['updated_files']
        file_msg = []
        if updated_files:
            file_msg = self.generate_file_message(transform, updated_files)

        processing_parameters = {'status': ret_poll['processing_updates']['status'],
                                 'locking': ProcessingLocking.Idle,
                                 'processing_metadata': ret_poll['processing_updates']['processing_metadata']}
        updated_processing = {'processing_id': processing['processing_id'],
                              'parameters': processing_parameters}

        ret = {'transform': transform,
               'processing_updates': updated_processing,
               'updated_files': updated_files,
               'file_message': file_msg}
        return ret

    def finish_monitor_processings(self):
        while not self.monitor_output_queue.empty():
            processing = self.monitor_output_queue.get()
            if processing:
                self.logger.info("Main thread processing(processing_id: %s) status changed to %s" % (processing['processing_updates']['processing_id'],
                                                                                                     processing['processing_updates']['parameters']['status']))

                core_processings.update_processing_with_collection_contents(updated_processing=processing['processing_updates'],
                                                                            updated_files=processing['updated_files'],
                                                                            file_msg_content=processing['file_message'],
                                                                            message_bulk_size=self.message_bulk_size)

    def prepare_finish_tasks(self):
        """
        Prepare tasks and finished tasks
        """
        self.finish_new_processings()
        self.finish_monitor_processings()

        processings = self.get_new_processings()
        for processing in processings:
            self.submit_task(self.process_new_processings, self.new_output_queue, (processing,))

        processings = self.get_monitor_processings()
        for processing in processings:
            self.submit_task(self.process_monitor_processings, self.monitor_output_queue, (processing,))

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
    agent = Carrier()
    agent()
