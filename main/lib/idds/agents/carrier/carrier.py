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

from idds.common.constants import (Sections, CollectionStatus, ContentStatus,
                                   TransformType, ProcessingStatus,
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

    def __init__(self, num_threads=1, poll_time_period=1800, **kwargs):
        super(Carrier, self).__init__(num_threads=num_threads, **kwargs)
        self.config_section = Sections.Carrier
        self.poll_time_period = int(poll_time_period)

        self.new_output_queue = Queue()
        self.monitor_output_queue = Queue()

    def get_new_processings(self):
        """
        Get new processing
        """
        processing_status = [ProcessingStatus.New]
        processings = core_processings.get_processings_by_status(status=processing_status)
        self.logger.info("Main thread get %s [new] processings to process" % len(processings))
        return processings

    def submit_processing(self, processing):
        transform_id = processing['transform_id']
        processing_metadata = processing['processing_metadata']
        input_coll_id = processing_metadata['input_collection']
        input_collection = core_catalog.get_collection(coll_id=input_coll_id)
        transform = core_transforms.get_transform(transform_id)

        if transform['transform_type'] == TransformType.StageIn:
            if 'rule_id' in processing_metadata:
                ret = {'processing_id': processing['processing_id'],
                       'status': ProcessingStatus.Submitted}
            else:
                if 'rule_submitter' not in self.plugins:
                    raise AgentPluginError('Plugin rule_submitter is required')
                rule_id = self.plugins['rule_submitter'](processing, transform, input_collection)
                processing_metadata['rule_id'] = rule_id
                ret = {'processing_id': processing['processing_id'],
                       'status': ProcessingStatus.Submitted,
                       'processing_metadata': processing_metadata}
            return ret
        return None

    def process_new_processings(self, processing):
        ret = self.submit_processing(processing)
        return ret

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
                                                                 time_period=self.poll_time_period)
        self.logger.info("Main thread get %s [submitting + submitted + running] processings to process" % len(processings))
        return processings

    def process_output_collection(self, coll_id, content_status_statistics):
        content_status_keys = list(content_status_statistics.keys())
        total_files = sum(content_status_statistics.values())
        processed_files = 0
        if ContentStatus.Available in content_status_statistics:
            processed_files = content_status_statistics[ContentStatus.Available]
        if ContentStatus.Available.value in content_status_statistics:
            processed_files = content_status_statistics[ContentStatus.Available.value]

        if content_status_keys == [ContentStatus.Available] or content_status_keys == [ContentStatus.Available.value]:
            ret_coll = {'coll_id': coll_id,
                        'total_files': total_files,
                        'coll_status': CollectionStatus.Closed,
                        'processing_files': 0,
                        'processed_files': processed_files,
                        'coll_metadata': {'status_statistics': content_status_statistics}}
        elif content_status_keys == [ContentStatus.FinalFailed] or content_status_keys == [ContentStatus.FinalFailed.value]:
            ret_coll = {'coll_id': coll_id,
                        'total_files': total_files,
                        'coll_status': CollectionStatus.Failed,
                        'processing_files': 0,
                        'processed_files': processed_files,
                        'coll_metadata': {'status_statistics': content_status_statistics}}
        elif (len(content_status_keys) == 2                                                                                   # noqa: W503
            and (ContentStatus.FinalFailed in content_status_keys or ContentStatus.FinalFailed.value in content_status_keys)  # noqa: W503
            and (ContentStatus.Available in content_status_keys or ContentStatus.Available.value in content_status_keys)):    # noqa: W503
            ret_coll = {'coll_id': coll_id,
                        'total_files': total_files,
                        'coll_status': CollectionStatus.SubClosed,
                        'processing_files': 0,
                        'processed_files': processed_files,
                        'coll_metadata': {'status_statistics': content_status_statistics}}
        elif (ContentStatus.New in content_status_keys or ContentStatus.New.value in content_status_keys            # noqa: W503
            or ContentStatus.Failed in content_status_keys or ContentStatus.Failed.value in content_status_keys):   # noqa: W503
            ret_coll = {'coll_id': coll_id,
                        'total_files': total_files,
                        'coll_status': CollectionStatus.Open,
                        'processing_files': 0,
                        'processed_files': processed_files,
                        'coll_metadata': {'status_statistics': content_status_statistics}}

        return ret_coll

    def process_monitor_processings(self, processing):
        transform_id = processing['transform_id']
        processing_status = processing['status']
        processing_metadata = processing['processing_metadata']

        transform = core_transforms.get_transform(transform_id)
        updated_files = []
        updated_files_message = []
        coll_ret = None
        if transform['transform_type'] == TransformType.StageIn:
            rule_id = processing_metadata['rule_id']
            if 'rule_poller' not in self.plugins:
                raise AgentPluginError('Plugin rule_poller is required')
            replicases_status = self.plugins['rule_poller'](rule_id)

            if replicases_status:
                output_coll_id = processing_metadata['output_collection']
                files = core_catalog.get_contents_by_coll_id_status(coll_id=output_coll_id)
                file_status_statistics = {}
                for file in files:
                    file_key = '%s:%s' % (file['scope'], file['name'])
                    if file_key in replicases_status:
                        new_file_status = replicases_status[file_key]
                        if not new_file_status == file['status']:
                            file['status'] = new_file_status

                            updated_file = {'content_id': file['content_id'],
                                            'status': new_file_status,
                                            'path': None}
                            updated_files.append(updated_file)

                            updated_file_message = {'scope': file['scope'],
                                                    'name': file['name'],
                                                    'status': new_file_status.name}
                            updated_files_message.append(updated_file_message)

                    if file['status'] not in file_status_statistics:
                        file_status_statistics[file['status']] = 0
                    file_status_statistics[file['status']] += 1

                file_status_keys = list(file_status_statistics.keys())
                if len(file_status_keys) == 1:
                    if file_status_keys == [ContentStatus.Available]:
                        processing_status = ProcessingStatus.Finished
                    elif file_status_keys == [ContentStatus.Failed]:
                        processing_status = ProcessingStatus.Failed
                else:
                    processing_status = ProcessingStatus.Running

                file_statusvalue_statistics = {}
                for key in file_status_statistics:
                    file_statusvalue_statistics[key.name] = file_status_statistics[key]

                processing_metadata['content_status_statistics'] = file_statusvalue_statistics
                coll_ret = self.process_output_collection(output_coll_id, file_status_statistics)
                if coll_ret:
                    coll_ret['coll_metadata']['status_statistics'] = file_statusvalue_statistics

            ret = {'processing_id': processing['processing_id'],
                   'status': processing_status,
                   'processing_metadata': processing_metadata,
                   'transform': transform}
            if updated_files:
                ret['updated_files'] = updated_files
            if coll_ret:
                ret['coll_updates'] = coll_ret
            if updated_files_message:
                ret['updated_files_message'] = updated_files_message
            return ret
        return None

    def finish_monitor_processings(self):
        while not self.monitor_output_queue.empty():
            processing = self.monitor_output_queue.get()
            if processing:
                self.logger.info("Main thread processing(processing_id: %s) status changed to %s" % (processing['processing_id'], processing['status']))
                self.logger.info(processing)
                updated_processing = None
                updated_collection = None
                updated_files = None
                coll_msg_content = None
                file_msg_content = None
                transform_updates = None

                if 'updated_files' in processing:
                    updated_files = processing['updated_files']
                    transform = processing['transform']
                    transform['transform_metadata']['output_collection_changed'] = True
                    transform_updates = {'transform_id': transform['transform_id'],
                                         'parameters': {'transform_metadata': transform['transform_metadata']}}
                if 'updated_files_message' in processing:
                    transform = processing['transform']
                    transform_metadata = transform['transform_metadata']
                    workload_id = None
                    if 'workload_id' in transform_metadata:
                        workload_id = transform_metadata['workload_id']
                    msg_content = {'msg_type': 'file_stagein',
                                   'workload_id': workload_id,
                                   'files': processing['updated_files_message']}
                    file_msg_content = {'msg_type': MessageType.StageInFile,
                                        'status': MessageStatus.New,
                                        'source': MessageSource.Carrier,
                                        'msg_content': msg_content}
                if 'coll_updates' in processing:
                    coll_updates = processing['coll_updates']
                    coll_id = coll_updates['coll_id']
                    parameters = coll_updates
                    del parameters['coll_id']
                    updated_collection = {'coll_id': coll_id,
                                          'parameters': parameters}

                    if coll_updates['coll_status'] == CollectionStatus.Closed:
                        transform = processing['transform']
                        transform_metadata = transform['transform_metadata']
                        processing_metadata = processing['processing_metadata']
                        input_coll_id = processing_metadata['input_collection']
                        input_collection = core_catalog.get_collection(coll_id=input_coll_id)
                        workload_id = None
                        if 'workload_id' in transform_metadata:
                            workload_id = transform_metadata['workload_id']
                        msg_content = {'msg_type': 'collection_stagein',
                                       'workload_id': workload_id,
                                       'collections': [{'scope': input_collection['scope'],
                                                        'name': input_collection['scope'],
                                                        'status': 'Available'}]}
                        coll_msg_content = {'msg_type': MessageType.StageInCollection,
                                            'status': MessageStatus.New,
                                            'source': MessageSource.Carrier,
                                            'msg_content': msg_content}

                parameters = {'status': processing['status'],
                              'processing_metadata': processing['processing_metadata']}
                updated_processing = {'processing_id': processing['processing_id'],
                                      'parameters': parameters}

                core_processings.update_processing_with_collection_contents(updated_processing,
                                                                            updated_collection,
                                                                            updated_files,
                                                                            coll_msg_content,
                                                                            file_msg_content,
                                                                            transform_updates)

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
