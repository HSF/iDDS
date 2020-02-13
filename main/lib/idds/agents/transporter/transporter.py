#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019

import copy
import traceback
try:
    # python 3
    from queue import Queue
except ImportError:
    # Python 2
    from Queue import Queue

from idds.common.constants import (Sections, CollectionRelationType, CollectionStatus,
                                   CollectionSubStatus, ContentType, ContentStatus)
from idds.common.exceptions import AgentPluginError, IDDSException
from idds.common.utils import setup_logging
from idds.core import (catalog as core_catalog)
from idds.agents.common.baseagent import BaseAgent

setup_logging(__name__)


class Transporter(BaseAgent):
    """
    Transporter works to list collections from DDM and register contents to DDM.
    """

    def __init__(self, num_threads=1, poll_time_period=1800, **kwargs):
        super(Transporter, self).__init__(num_threads=num_threads, **kwargs)
        self.poll_time_period = int(poll_time_period)
        self.config_section = Sections.Transporter
        self.processed_input_queue = Queue()
        self.processed_output_queue = Queue()

    def get_new_input_collections(self):
        """
        Get new input collections
        """
        coll_status = [CollectionStatus.Open]
        colls_open = core_catalog.get_collections_by_status(status=coll_status,
                                                            relation_type=CollectionRelationType.Input,
                                                            time_period=self.poll_time_period,
                                                            locking=True)
        self.logger.info("Main thread get %s [open] input collections to process" % len(colls_open))

        coll_status = [CollectionStatus.New]
        colls_new = core_catalog.get_collections_by_status(status=coll_status,
                                                           relation_type=CollectionRelationType.Input,
                                                           locking=True)
        self.logger.info("Main thread get %s [new] input collections to process" % len(colls_new))

        colls = colls_open + colls_new
        self.logger.info("Main thread get totally %s [open + new] collections to process" % len(colls))

        return colls

    def get_collection_metadata(self, scope, name):
        if 'collection_metadata_reader' not in self.plugins:
            raise AgentPluginError('Plugin collection_metadata_reader is required')
        return self.plugins['collection_metadata_reader'](scope, name)

    def get_contents(self, scope, name):
        if 'contents_lister' not in self.plugins:
            raise AgentPluginError('Plugin contents_lister is required')
        return self.plugins['contents_lister'](scope, name)

    def process_input_collection(self, coll):
        """
        Process input collection
        """
        coll_metadata = self.get_collection_metadata(coll['scope'], coll['name'])
        contents = self.get_contents(coll['scope'], coll['name'])
        new_contents = []
        for content in contents:
            new_content = {'coll_id': coll['coll_id'],
                           'scope': content['scope'],
                           'name': content['name'],
                           'min_id': 0,
                           'max_id': content['events'],
                           'content_type': ContentType.File,
                           'status': ContentStatus.New,
                           'bytes': content['bytes'],
                           'md5': content['md5'] if 'md5' in content else None,
                           'adler32': content['adler32'] if 'adler32' in content else None,
                           'expired_at': coll['expired_at']}
            new_contents.append(new_content)
        new_coll = {'coll_id': coll['coll_id'],
                    'transform_id': coll['transform_id'],
                    'bytes': coll_metadata['bytes'],
                    'status': coll_metadata['status'],
                    'total_files': coll_metadata['total_files'],
                    'coll_metadata': {'availability': coll_metadata['availability'],
                                      'events': coll_metadata['events'],
                                      'is_open': coll_metadata['is_open'],
                                      'run_number': coll_metadata['run_number']},
                    'contents': new_contents}
        return new_coll

    def finish_processing_input_collections(self):
        while not self.processed_input_queue.empty():
            coll = self.processed_input_queue.get()
            self.logger.info("Main thread finished processing intput collection(%s) with number of contents: %s" % (coll['coll_id'], len(coll['contents'])))
            parameters = copy.deepcopy(coll)
            del parameters['contents']
            del parameters['coll_id']
            del parameters['transform_id']
            parameters['substatus'] = CollectionSubStatus.Idle
            core_catalog.update_input_collection_with_contents(coll_id=coll['coll_id'],
                                                               parameters=parameters,
                                                               contents=coll['contents'])

            # new_contents = core_catalog.update_input_collection_with_contents(coll_id=coll['coll_id'],
            #                                                                   parameters=parameters,
            #                                                                   contents=coll['contents'])
            # if new_contents:
            #     core_transforms.trigger_update_transform_status(coll['transform_id'], input_collection_changed=True)

    def get_new_output_collections(self):
        """
        Get new output collections
        """
        coll_status = [CollectionStatus.Updated, CollectionStatus.Processing]
        colls = core_catalog.get_collections_by_status(status=coll_status,
                                                       relation_type=CollectionRelationType.Output,
                                                       time_period=1800,
                                                       locking=True)
        self.logger.info("Main thread get %s [Updated] output collections to process" % len(colls))
        return colls

    def register_contents(self, scope, name, contents):
        if 'contents_register' not in self.plugins:
            raise AgentPluginError('Plugin contents_register is required')
        return self.plugins['contents_register'](scope, name, contents)

    def is_input_collection_all_processed(self, coll_id_list):
        is_all_processed = True
        for coll_id in coll_id_list:
            coll = core_catalog.get_collection(coll_id)
            if not (coll['status'] == CollectionStatus.Closed and coll['total_files'] == coll['processed_files']):
                is_all_processed = False
                return is_all_processed
        return is_all_processed

    def process_output_collection(self, coll):
        """
        Process output collection
        """
        if 'to_register' in coll['coll_metadata'] and coll['coll_metadata']['to_register'] is True:
            contents = core_catalog.get_contents(coll_id=coll['coll_id'])
            registered_contents = self.get_contents(coll['scope'], coll['name'])
            registered_content_names = [con['name'] for con in registered_contents]
            new_contents = []
            for content in contents:
                if content['name'] not in registered_content_names:
                    new_contents.append(content)
            if new_contents:
                self.register_contents(coll['scope'], coll['name'], new_contents)

        is_input_collection_all_processed = False
        if 'input_collections' in coll['coll_metadata'] and coll['coll_metadata']['input_collections']:
            input_coll_list = coll['coll_metadata']['input_collections']
            is_input_collection_all_processed = self.is_input_collection_all_processed(input_coll_list)

        contents_statistics = core_catalog.get_content_status_statistics(coll_id=coll['coll_id'])
        contents_statistics_with_name = {}
        for key in contents_statistics:
            contents_statistics_with_name[key.name] = contents_statistics[key]

        content_status_keys = list(contents_statistics.keys())
        total_files = sum(contents_statistics.values())
        new_files = 0
        processed_files = 0
        if ContentStatus.Available in contents_statistics:
            processed_files += contents_statistics[ContentStatus.Available]
        if ContentStatus.Available.value in contents_statistics:
            processed_files += contents_statistics[ContentStatus.Available.value]
        if ContentStatus.New in contents_statistics:
            new_files += contents_statistics[ContentStatus.New]
        if ContentStatus.New.value in contents_statistics:
            new_files += contents_statistics[ContentStatus.New.value]

        if not is_input_collection_all_processed:
            coll_status = CollectionStatus.Open
        elif content_status_keys == [ContentStatus.Available] or content_status_keys == [ContentStatus.Available.value]:
            coll_status = CollectionStatus.Closed
        elif content_status_keys == [ContentStatus.FinalFailed] or content_status_keys == [ContentStatus.FinalFailed.value]:
            coll_status = CollectionStatus.Failed
        elif (len(content_status_keys) == 2                                                                                   # noqa: W503
            and (ContentStatus.FinalFailed in content_status_keys or ContentStatus.FinalFailed.value in content_status_keys)  # noqa: W503
            and (ContentStatus.Available in content_status_keys or ContentStatus.Available.value in content_status_keys)):    # noqa: W503
            coll_status = CollectionStatus.SubClosed
        elif (ContentStatus.New in content_status_keys or ContentStatus.New.value in content_status_keys            # noqa: W503
            or ContentStatus.Failed in content_status_keys or ContentStatus.Failed.value in content_status_keys):   # noqa: W503
            coll_status = CollectionStatus.Open

        ret_coll = {'coll_id': coll['coll_id'],
                    'total_files': total_files,
                    'status': coll_status,
                    'new_files': new_files,
                    'processing_files': 0,
                    'processed_files': processed_files,
                    'coll_metadata': {'status_statistics': contents_statistics_with_name}}
        return ret_coll

    def finish_processing_output_collections(self):
        while not self.processed_output_queue.empty():
            coll = self.processed_output_queue.get()
            self.logger.info("Main thread finished processing output collection(%s) with number of contents: %s" % (coll['coll_id']))
            coll_id = coll['coll_id']
            parameters = coll
            del parameters['coll_id']
            parameters['substatus'] = CollectionSubStatus.Idle
            core_catalog.update_collection(coll_id=coll_id, parameters=parameters)

    def prepare_finish_tasks(self):
        """
        Prepare tasks and finished tasks
        """
        self.finish_processing_input_collections()
        self.finish_processing_output_collections()

        colls = self.get_new_input_collections()
        for coll in colls:
            self.submit_task(self.process_input_collection, self.processed_input_queue, (coll,))

        colls = self.get_new_output_collections()
        for coll in colls:
            self.submit_task(self.process_output_collection, self.processed_output_queue, (coll,))

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
    agent = Transporter()
    agent()
