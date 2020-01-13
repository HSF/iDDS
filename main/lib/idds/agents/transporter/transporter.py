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
                                   ContentType, ContentStatus)
from idds.common.exceptions import AgentPluginError, IDDSException
from idds.common.utils import setup_logging
from idds.core import catalog as core_catalog
from idds.agents.common.baseagent import BaseAgent

setup_logging(__name__)


class Transporter(BaseAgent):
    """
    Transporter works to list collections from DDM and register contents to DDM.
    """

    def __init__(self, num_threads=1, **kwargs):
        super(Transporter, self).__init__(num_threads=num_threads, **kwargs)
        self.config_section = Sections.Transporter
        self.processed_input_queue = Queue()
        self.processed_output_queue = Queue()

    def get_new_input_collections(self):
        """
        Get new input collections
        """
        coll_status = [CollectionStatus.Open]
        colls_open = core_catalog.get_collections_by_status(status=coll_status,
                                                            relations_type=CollectionRelationType.Input,
                                                            time_period=3600)
        self.logger.info("Main thread get %s [open] input collections to process" % len(colls_open))

        coll_status = [CollectionStatus.New]
        colls_new = core_catalog.get_collections_by_status(status=coll_status,
                                                           relations_type=CollectionRelationType.Input)
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
                           'content_size': content['size'],
                           'md5': content['md5'],
                           'adler32': content['adler32'],
                           'expired_at': coll['expired_at']}
            new_contents.append(new_content)
        new_coll = {'coll_id': coll['coll_id'],
                    'coll_size': coll_metadata['coll_size'],
                    'coll_status': coll_metadata['coll_status'],
                    'total_files': coll_metadata['total_files'],
                    'contents': new_contents}
        return new_coll

    def finish_processing_input_collections(self):
        while not self.processed_input_queue.empty():
            coll = self.processed_input_queue.get()
            self.logger.info("Main thread finished processing intput collection(%s) with number of contents: %s" % (coll['coll_id'], len(coll['contents'])))
            parameters = copy.deepcopy(coll)
            del parameters['contents']
            del parameters['coll_id']
            core_catalog.update_collection(coll_id=coll['coll_id'], parameters=parameters)
            core_catalog.add_contents(coll['contents'])

    def get_new_output_collections(self):
        """
        Get new output collections
        """
        coll_status = [CollectionStatus.Updated]
        colls = core_catalog.get_collections_by_status(status=coll_status,
                                                       relations_type=CollectionRelationType.Output,
                                                       time_period=1800)
        self.logger.info("Main thread get %s [Updated] output collections to process" % len(colls))
        return colls

    def register_contents(self, scope, name, contents):
        if 'contents_register' not in self.plugins:
            raise AgentPluginError('Plugin contents_register is required')
        return self.plugins['contents_register'](scope, name, contents)

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

        coll['coll_status'] = CollectionStatus.Updated1
        return coll

    def finish_processing_output_collections(self):
        while not self.processed_output_queue.empty():
            coll = self.processed_output_queue.get()
            self.logger.info("Main thread finished processing output collection(%s) with number of contents: %s" % (coll['coll_id']))
            parameters = {'status': coll['coll_status']}
            core_catalog.update_collection(coll_id=coll['coll_id'], parameters=parameters)

    def prepare_finish_tasks(self):
        """
        Prepare tasks and finished tasks
        """
        self.finish_processing_input_collections()
        self.finish_processing_output_collections()

        colls = self.get_new_input_collections()
        for coll in colls:
            self.submit_task(self.process_input_collection, self.processed_input_queue, coll)

        colls = self.get_new_output_collections()
        for coll in colls:
            self.submit_task(self.process_output_collection, self.processed_output_queue, coll)

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
