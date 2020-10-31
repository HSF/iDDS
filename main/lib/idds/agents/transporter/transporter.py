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
import datetime
import traceback
try:
    # python 3
    from queue import Queue
except ImportError:
    # Python 2
    from Queue import Queue

from idds.common.constants import (Sections, CollectionRelationType, CollectionStatus,
                                   CollectionLocking, CollectionType, ContentType, ContentStatus,
                                   TransformType, ProcessingStatus,
                                   MessageType, MessageStatus, MessageSource)
from idds.common.exceptions import AgentPluginError
from idds.common.utils import setup_logging
from idds.core import (catalog as core_catalog, transforms as core_transforms, processings as core_processings)
from idds.agents.common.baseagent import BaseAgent

setup_logging(__name__)


class Transporter(BaseAgent):
    """
    Transporter works to list collections from DDM and register contents to DDM.
    """

    def __init__(self, num_threads=1, poll_time_period=10, retrieve_bulk_size=None, poll_input_time_period=None, poll_output_time_period=None, **kwargs):
        super(Transporter, self).__init__(num_threads=num_threads, **kwargs)
        self.poll_time_period = int(poll_time_period)
        if poll_input_time_period is None:
            self.poll_input_time_period = self.poll_time_period
        else:
            self.poll_input_time_period = int(poll_input_time_period)
        if poll_output_time_period is None:
            self.poll_output_time_period = self.poll_time_period
        else:
            self.poll_output_time_period = int(poll_output_time_period)

        self.retrieve_bulk_size = int(retrieve_bulk_size)
        self.config_section = Sections.Transporter

        self.new_input_queue = Queue()
        self.processed_input_queue = Queue()
        self.new_output_queue = Queue()
        self.processed_output_queue = Queue()

    def init(self):
        status = [CollectionStatus.New, CollectionStatus.Open,
                  CollectionStatus.Updated, CollectionStatus.Processing]
        core_catalog.clean_next_poll_at(status)

    def get_new_input_collections(self):
        """
        Get new input collections
        """
        coll_status = [CollectionStatus.Open]
        colls_open = core_catalog.get_collections_by_status(status=coll_status,
                                                            relation_type=CollectionRelationType.Input,
                                                            # time_period=self.poll_input_time_period,
                                                            locking=True,
                                                            bulk_size=self.retrieve_bulk_size)

        self.logger.debug("Main thread get %s [open] input collections to process" % len(colls_open))
        if colls_open:
            self.logger.info("Main thread get %s [open] input collections to process" % len(colls_open))

        coll_status = [CollectionStatus.New]
        colls_new = core_catalog.get_collections_by_status(status=coll_status,
                                                           relation_type=CollectionRelationType.Input,
                                                           locking=True,
                                                           bulk_size=self.retrieve_bulk_size)

        self.logger.debug("Main thread get %s [new] input collections to process" % len(colls_new))
        if colls_new:
            self.logger.info("Main thread get %s [new] input collections to process" % len(colls_new))

        colls = colls_open + colls_new
        self.logger.debug("Main thread get totally %s [open + new] collections to process" % len(colls))
        if colls:
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
        if coll['coll_type'] in [CollectionType.PseudoDataset, CollectionType.PseudoDataset.value]:
            pseudo_content = {'coll_id': coll['coll_id'],
                              'scope': coll['scope'],
                              'name': 'pseudo_%s' % coll['coll_id'],
                              'min_id': 0,
                              'max_id': 0,
                              'content_type': ContentType.PseudoContent,
                              'status': ContentStatus.Available,
                              'bytes': 0,
                              'md5': None,
                              'adler32': None,
                              'expired_at': coll['expired_at']}
            new_coll = {'coll': coll,
                        'bytes': 0,
                        'status': CollectionStatus.Open,
                        'total_files': 1,
                        'new_files': 0,
                        'processed_files': 0,
                        'coll_metadata': {'availability': True,
                                          'events': 0,
                                          'is_open': False,
                                          'status': CollectionStatus.Closed.name},
                        'contents': [pseudo_content]}
        elif coll['coll_metadata'] and coll['coll_metadata']['status'] in [CollectionStatus.Closed, CollectionStatus.Closed.name, CollectionStatus.Closed.value]:
            new_coll = {'coll': coll, 'status': coll['status'], 'contents': []}
        else:
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
                               'status': ContentStatus.Available,
                               'bytes': content['bytes'],
                               'md5': content['md5'] if 'md5' in content else None,
                               'adler32': content['adler32'] if 'adler32' in content else None,
                               'expired_at': coll['expired_at']}
                new_contents.append(new_content)

            coll['bytes'] = coll_metadata['bytes']
            coll['total_files'] = coll_metadata['total_files']
            new_coll = {'coll': coll,
                        'bytes': coll_metadata['bytes'],
                        'status': CollectionStatus.Open,
                        'total_files': coll_metadata['total_files'],
                        'coll_metadata': {'availability': coll_metadata['availability'],
                                          'events': coll_metadata['events'],
                                          'is_open': coll_metadata['is_open'],
                                          'status': coll_metadata['status'].name,
                                          'run_number': coll_metadata['run_number']},
                        'contents': new_contents}
        return new_coll

    def process_input_collections(self):
        ret = []
        while not self.new_input_queue.empty():
            try:
                coll = self.new_input_queue.get()
                if coll:
                    self.logger.info("Main thread processing input collection: %s" % coll)
                    ret_coll = self.process_input_collection(coll)
                    if ret_coll:
                        ret.append(ret_coll)
            except Exception as ex:
                self.logger.error(ex)
                self.logger.error(traceback.format_exc())
        return ret

    def finish_processing_input_collections(self):
        while not self.processed_input_queue.empty():
            coll = self.processed_input_queue.get()
            self.logger.info("Main thread finished processing intput collection(%s) with number of contents: %s" % (coll['coll']['coll_id'], len(coll['contents'])))
            parameters = copy.deepcopy(coll)
            if parameters['coll']['coll_type'] in [CollectionType.PseudoDataset, CollectionType.PseudoDataset.value]:
                pass
            else:
                parameters['next_poll_at'] = datetime.datetime.utcnow() + datetime.timedelta(seconds=self.poll_input_time_period)
            del parameters['contents']
            del parameters['coll']
            parameters['locking'] = CollectionLocking.Idle
            core_catalog.update_input_collection_with_contents(coll=coll['coll'],
                                                               parameters=parameters,
                                                               contents=coll['contents'])

    def get_new_output_collections(self):
        """
        Get new output collections
        """
        coll_status = [CollectionStatus.Updated, CollectionStatus.Processing]
        colls = core_catalog.get_collections_by_status(status=coll_status,
                                                       relation_type=CollectionRelationType.Output,
                                                       # time_period=self.poll_output_time_period,
                                                       locking=True,
                                                       bulk_size=self.retrieve_bulk_size)
        self.logger.debug("Main thread get %s [Updated + Processing] output collections to process" % len(colls))
        if colls:
            self.logger.info("Main thread get %s [Updated + Processing] output collections to process" % len(colls))
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

    def is_all_processings_finished(self, transform_id):
        last_processing = None
        processings = core_processings.get_processings_by_transform_id(transform_id=transform_id)
        if processings:
            last_processing = processings[-1]

        for processing in processings:
            if not processing['status'] in [ProcessingStatus.Finished, ProcessingStatus.Failed, ProcessingStatus.Lost,
                                            # ProcessingStatus.Cancel, ProcessingStatus.FinishedOnStep, ProcessingStatus.FinishedOnExec,
                                            ProcessingStatus.Cancel, ProcessingStatus.FinishedOnStep,
                                            ProcessingStatus.TimeOut,
                                            ProcessingStatus.Finished.value, ProcessingStatus.Failed.value, ProcessingStatus.Lost.value,
                                            # ProcessingStatus.Cancel.value, ProcessingStatus.FinishedOnStep.value, ProcessingStatus.FinishedOnExec.value,
                                            ProcessingStatus.Cancel.value, ProcessingStatus.FinishedOnStep.value,
                                            ProcessingStatus.TimeOut.value]:
                return False, last_processing
        return True, last_processing

    def process_output_collection(self, coll):
        """
        Process output collection
        """
        if coll['coll_metadata'] and 'to_register' in coll['coll_metadata'] and coll['coll_metadata']['to_register'] is True:
            contents = core_catalog.get_contents(coll_id=coll['coll_id'])
            registered_contents = self.get_contents(coll['scope'], coll['name'])
            registered_content_names = [con['name'] for con in registered_contents]
            new_contents = []
            for content in contents:
                if content['name'] not in registered_content_names:
                    new_contents.append(content)
            if new_contents:
                self.register_contents(coll['scope'], coll['name'], new_contents)

        is_all_processings_finished, last_processing = self.is_all_processings_finished(coll['transform_id'])

        is_input_collection_all_processed = False
        if coll['coll_metadata'] and 'input_collections' in coll['coll_metadata'] and coll['coll_metadata']['input_collections']:
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
        # output_metadata = None
        if ContentStatus.Available in contents_statistics:
            processed_files += contents_statistics[ContentStatus.Available]
        if ContentStatus.Available.value in contents_statistics:
            processed_files += contents_statistics[ContentStatus.Available.value]
        if ContentStatus.New in contents_statistics:
            new_files += contents_statistics[ContentStatus.New]
        if ContentStatus.New.value in contents_statistics:
            new_files += contents_statistics[ContentStatus.New.value]

        coll_msg = None
        if not is_input_collection_all_processed or not is_all_processings_finished:
            coll_status = CollectionStatus.Processing
        elif content_status_keys == [ContentStatus.Available] or content_status_keys == [ContentStatus.Available.value]:
            coll_status = CollectionStatus.Closed

            transform = core_transforms.get_transform(coll['coll_metadata']['transform_id'])
            # if 'processing_id' in transform['transform_metadata']:
            #    processing = core_processings.get_processing(transform['transform_metadata']['processing_id'])
            #    output_metadata = processing['output_metadata']
            output_ret = {}
            if last_processing:
                output_ret = {'status': last_processing['status'].name,
                              'msg': last_processing['processing_metadata']['final_errors'] if 'final_errors' in last_processing['processing_metadata'] else None}
            if transform['transform_type'] in [TransformType.StageIn, TransformType.StageIn.value]:
                msg_type = 'collection_stagein'
                msg_type_c = MessageType.StageInCollection
            elif transform['transform_type'] in [TransformType.ActiveLearning, TransformType.ActiveLearning.value]:
                msg_type = 'collection_activelearning'
                msg_type_c = MessageType.ActiveLearningCollection
            elif transform['transform_type'] in [TransformType.HyperParameterOpt, TransformType.HyperParameterOpt.value]:
                msg_type = 'collection_hyperparameteropt'
                msg_type_c = MessageType.HyperParameterOptCollection
            else:
                msg_type = 'collection_unknown'
                msg_type_c = MessageType.UnknownCollection

            msg_content = {'msg_type': msg_type,
                           'workload_id': coll['coll_metadata']['workload_id'] if 'workload_id' in coll['coll_metadata'] else None,
                           'collections': [{'scope': coll['scope'],
                                            'name': coll['name'],
                                            'status': 'Available'}],
                           'output': output_ret}
            coll_msg = {'msg_type': msg_type_c,
                        'status': MessageStatus.New,
                        'source': MessageSource.Transporter,
                        'transform_id': coll['coll_metadata']['transform_id'] if 'transform_id' in coll['coll_metadata'] else None,
                        'num_contents': 1,
                        'msg_content': msg_content}

        elif content_status_keys == [ContentStatus.FinalFailed] or content_status_keys == [ContentStatus.FinalFailed.value]:
            coll_status = CollectionStatus.Failed
        elif (len(content_status_keys) == 2                                                                                   # noqa: W503
            and (ContentStatus.FinalFailed in content_status_keys or ContentStatus.FinalFailed.value in content_status_keys)  # noqa: W503
            and (ContentStatus.Available in content_status_keys or ContentStatus.Available.value in content_status_keys)):    # noqa: W503
            coll_status = CollectionStatus.SubClosed
        elif (ContentStatus.New in content_status_keys or ContentStatus.New.value in content_status_keys            # noqa: W503
            or ContentStatus.Failed in content_status_keys or ContentStatus.Failed.value in content_status_keys):   # noqa: W503
            # coll_status = CollectionStatus.Processing
            coll_status = CollectionStatus.Failed
        else:
            # coll_status = CollectionStatus.Processing
            coll_status = CollectionStatus.Failed

        coll_metadata = coll['coll_metadata']
        if not coll_metadata:
            coll_metadata = {}
        coll_metadata['status_statistics'] = contents_statistics_with_name
        # coll_metadata['output_metadata'] = output_metadata
        ret_coll = {'coll_id': coll['coll_id'],
                    'total_files': total_files,
                    'status': coll_status,
                    'new_files': new_files,
                    'processing_files': 0,
                    'processed_files': processed_files,
                    'coll_metadata': coll_metadata,
                    'coll_msg': coll_msg}

        if coll['coll_type'] in [CollectionType.PseudoDataset, CollectionType.PseudoDataset.value]:
            pass
        else:
            ret_coll['next_poll_at'] = datetime.datetime.utcnow() + datetime.timedelta(seconds=self.poll_output_time_period)

        return ret_coll

    def process_output_collections(self):
        ret = []
        while not self.new_output_queue.empty():
            try:
                coll = self.new_output_queue.get()
                if coll:
                    self.logger.info("Main thread processing output collection: %s" % coll)
                    ret_coll = self.process_output_collection(coll)
                    if ret_coll:
                        ret.append(ret_coll)
            except Exception as ex:
                self.logger.error(ex)
                self.logger.error(traceback.format_exc())
        return ret

    def finish_processing_output_collections(self):
        while not self.processed_output_queue.empty():
            coll = self.processed_output_queue.get()
            self.logger.info("Main thread finished processing output collection(%s) with number of processed contents: %s" % (coll['coll_id'], coll['processed_files']))
            coll_id = coll['coll_id']
            coll_msg = coll['coll_msg']
            parameters = coll
            del parameters['coll_id']
            del coll['coll_msg']
            parameters['locking'] = CollectionLocking.Idle
            core_catalog.update_collection(coll_id=coll_id, parameters=parameters, msg=coll_msg)

    def clean_locks(self):
        self.logger.info("clean locking")
        core_catalog.clean_locking()

    def run(self):
        """
        Main run function.
        """
        try:
            self.logger.info("Starting main thread")

            self.load_plugins()
            self.init()

            self.add_default_tasks()

            task = self.create_task(task_func=self.get_new_input_collections, task_output_queue=self.new_input_queue, task_args=tuple(), task_kwargs={}, delay_time=1, priority=1)
            self.add_task(task)
            task = self.create_task(task_func=self.process_input_collections, task_output_queue=self.processed_input_queue, task_args=tuple(), task_kwargs={}, delay_time=1, priority=1)
            self.add_task(task)
            task = self.create_task(task_func=self.finish_processing_input_collections, task_output_queue=None, task_args=tuple(), task_kwargs={}, delay_time=1, priority=1)
            self.add_task(task)

            task = self.create_task(task_func=self.get_new_output_collections, task_output_queue=self.new_output_queue, task_args=tuple(), task_kwargs={}, delay_time=1, priority=1)
            self.add_task(task)
            task = self.create_task(task_func=self.process_output_collections, task_output_queue=self.processed_output_queue, task_args=tuple(), task_kwargs={}, delay_time=1, priority=1)
            self.add_task(task)
            task = self.create_task(task_func=self.finish_processing_output_collections, task_output_queue=None, task_args=tuple(), task_kwargs={}, delay_time=1, priority=1)
            self.add_task(task)

            task = self.create_task(task_func=self.clean_locks, task_output_queue=None, task_args=tuple(), task_kwargs={}, delay_time=1800, priority=1)
            self.add_task(task)

            self.execute()
        except KeyboardInterrupt:
            self.stop()


if __name__ == '__main__':
    agent = Transporter()
    agent()
