#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019 - 2020

import copy
import traceback
try:
    # python 3
    from queue import Queue
except ImportError:
    # Python 2
    from Queue import Queue


from idds.common.constants import (Sections, TransformStatus, TransformLocking,
                                   CollectionRelationType, CollectionStatus,
                                   CollectionType, ContentType, ContentStatus,
                                   ProcessingStatus)
from idds.common.utils import setup_logging
# from idds.core import (transforms as core_transforms, processings as core_processings)
from idds.core import (transforms as core_transforms)
from idds.agents.common.baseagent import BaseAgent

setup_logging(__name__)


class Transformer(BaseAgent):
    """
    Transformer works to process transforms.
    """

    def __init__(self, num_threads=1, poll_time_period=1800, retrieve_bulk_size=10,
                 message_bulk_size=1000, **kwargs):
        super(Transformer, self).__init__(num_threads=num_threads, **kwargs)
        self.config_section = Sections.Transformer
        self.poll_time_period = int(poll_time_period)
        self.retrieve_bulk_size = int(retrieve_bulk_size)
        self.message_bulk_size = int(message_bulk_size)

        self.new_task_queue = Queue()
        self.new_output_queue = Queue()
        self.running_task_queue = Queue()
        self.running_output_queue = Queue()

    def get_new_transforms(self):
        """
        Get new transforms to process
        """

        transform_status = [TransformStatus.New, TransformStatus.Ready, TransformStatus.Extend]
        transforms_new = core_transforms.get_transforms_by_status(status=transform_status, locking=True, bulk_size=self.retrieve_bulk_size)

        self.logger.debug("Main thread get %s New+Ready+Extend transforms to process" % len(transforms_new))
        if transforms_new:
            self.logger.info("Main thread get %s New+Ready+Extend transforms to process" % len(transforms_new))
        return transforms_new

    def generate_collection_model(self, transform, collection, relation_type=CollectionRelationType.Input):
        if 'coll_metadata' in collection:
            coll_metadata = collection['coll_metadata']
        else:
            coll_metadata = {}

        if 'did_type' in coll_metadata:
            if coll_metadata['did_type'] == 'DATASET':
                coll_type = CollectionType.Dataset
            elif coll_metadata['did_type'] == 'CONTAINER':
                coll_type = CollectionType.Container
            else:
                coll_type = CollectionType.File
        else:
            coll_type = CollectionType.Dataset

        if 'is_open' in coll_metadata and not coll_metadata['is_open']:
            coll_status = CollectionStatus.Closed
        else:
            coll_status = CollectionStatus.Open

        coll = {'transform_id': transform['transform_id'],
                'coll_type': coll_type,
                'scope': collection['scope'],
                'name': collection['name'],
                'relation_type': relation_type,
                'bytes': coll_metadata['bytes'] if 'bytes' in coll_metadata else 0,
                'total_files': coll_metadata['total_files'] if 'total_files' in coll_metadata else 0,
                'new_files': coll_metadata['new_files'] if 'new_files' in coll_metadata else 0,
                'processed_files': 0,
                'processing_files': 0,
                'coll_metadata': coll_metadata,
                'status': coll_status,
                'expired_at': transform['expired_at']}
        return coll

    def get_new_contents(self, transform, new_input_output_maps):
        new_contents = []
        for map_id in new_input_output_maps:
            inputs = new_input_output_maps[map_id]['inputs']
            outputs = new_input_output_maps[map_id]['outputs']

            for input_content in inputs:
                content = {'transform_id': transform['transform_id'],
                           'coll_id': input_content['coll_id'],
                           'map_id': map_id,
                           'scope': input_content['scope'],
                           'name': input_content['name'],
                           'min_id': input_content['min_id'] if 'min_id' in input_content else 0,
                           'max_id': input_content['max_id'] if 'max_id' in input_content else 0,
                           'status': ContentStatus.New,
                           'substatus': ContentStatus.New,
                           'path': None,
                           'content_type': input_content['content_type'] if 'content_type' in input_content else ContentType.File,
                           'bytes': input_content['bytes'],
                           'adler32': input_content['adler32'],
                           'content_metadata': input_content['content_metadata']}
                new_contents.append(content)
            for output_content in outputs:
                content = {'transform_id': transform['transform_id'],
                           'coll_id': output_content['coll_id'],
                           'map_id': map_id,
                           'scope': output_content['scope'],
                           'name': output_content['name'],
                           'min_id': output_content['min_id'] if 'min_id' in output_content else 0,
                           'max_id': output_content['max_id'] if 'max_id' in output_content else 0,
                           'status': ContentStatus.New,
                           'substatus': ContentStatus.New,
                           'path': None,
                           'content_type': output_content['content_type'] if 'content_type' in output_content else ContentType.File,
                           'bytes': output_content['bytes'],
                           'adler32': output_content['adler32'],
                           'content_metadata': input_content['content_metadata']}
                new_contents.append(content)
        return new_contents

    def get_updated_contents(self, transform, registered_input_output_maps):
        updated_contents = []
        for map_id in registered_input_output_maps:
            outputs = registered_input_output_maps[map_id]['outputs']

            for content in outputs:
                if content['status'] != content['substatus']:
                    updated_content = {'content_id': content['content_id'],
                                       'status': content['substatus']}
                    updated_contents.append(updated_content)
        return updated_contents

    def process_new_transform(self, transform):
        """
        Process new transform
        """
        # self.logger.info("process_new_transform: transform_id: %s" % transform['transform_id'])
        work = transform['transform_metadata']['work']
        input_collections = work.get_input_collections()
        output_collections = work.get_output_collections()
        log_collections = work.get_log_collections()

        input_colls, output_colls, log_colls = [], [], []
        for input_coll in input_collections:
            in_coll = self.generate_collection_model(transform, input_coll, relation_type=CollectionRelationType.Input)
            input_colls.append(in_coll)
        for output_coll in output_collections:
            out_coll = self.generate_collection_model(transform, output_coll, relation_type=CollectionRelationType.Output)
            output_colls.append(out_coll)
        for log_coll in log_collections:
            l_coll = self.generate_collection_model(transform, log_coll, relation_type=CollectionRelationType.Log)
            log_colls.append(l_coll)

        # new_input_output_maps = work.get_new_input_output_maps()
        # new_contents = self.get_new_contents(new_input_output_maps)

        # file_msgs = []
        # if input_output_maps:
        #     file_msg = self.generate_file_message(transform, input_output_maps)
        #     file_msgs.append(file_msg)

        # processing = self.get_processing(transform, input_colls, output_colls, log_colls, input_output_maps)

        transform['locking'] = TransformLocking.Idle
        transform['status'] = TransformStatus.Transforming
        # ret = {'transform': transform, 'input_collections': input_colls, 'output_collections': output_colls,
        #        'log_collections': log_colls, 'new_input_output_maps': input_output_maps, 'messages': file_msgs,
        #        'new_processing': processing}
        ret = {'transform': transform, 'input_collections': input_colls, 'output_collections': output_colls,
               'log_collections': log_colls}
        return ret

    def process_new_transforms(self):
        ret = []
        while not self.new_task_queue.empty():
            try:
                transform = self.new_task_queue.get()
                if transform:
                    self.logger.info("Main thread processing new transform: %s" % transform)
                    ret_transform = self.process_new_transform(transform)
                    if ret_transform:
                        ret.append(ret_transform)
            except Exception as ex:
                self.logger.error(ex)
                self.logger.error(traceback.format_exc())
        return ret

    def finish_new_transforms(self):
        while not self.new_output_queue.empty():
            try:
                ret = self.new_output_queue.get()
                self.logger.info("Main thread finishing processing transform: %s" % ret['transform'])
                if ret:
                    # self.logger.debug("wen: %s" % str(ret['output_contents']))
                    core_transforms.add_transform_outputs(transform=ret['transform'],
                                                          input_collections=ret.get('input_collections', None),
                                                          output_collections=ret.get('output_collections', None),
                                                          log_collections=ret.get('log_collections', None),
                                                          new_contents=ret.get('new_contents', None),
                                                          update_input_collections=ret.get('update_input_collections', None),
                                                          update_output_collections=ret.get('update_output_collections', None),
                                                          update_log_collections=ret.get('update_log_collections', None),
                                                          update_contents=ret.get('update_contents', None),
                                                          messages=ret.get('messages', None),
                                                          new_processing=ret.get('new_processing', None),
                                                          message_bulk_size=self.message_bulk_size)
            except Exception as ex:
                self.logger.error(ex)
                self.logger.error(traceback.format_exc())

    def get_running_transforms(self):
        """
        Get running transforms
        """
        transform_status = [TransformStatus.Transforming]
        transforms = core_transforms.get_transforms_by_status(status=transform_status,
                                                              period=self.poll_time_period,
                                                              locking=True,
                                                              bulk_size=self.retrieve_bulk_size)

        self.logger.debug("Main thread get %s transforming transforms to process" % len(transforms))
        if transforms:
            self.logger.info("Main thread get %s transforming transforms to process" % len(transforms))
        return transforms

    def get_collection_ids(self, collections):
        coll_ids = []
        for coll in collections:
            coll_ids.append(coll['coll_id'])
        return coll_ids

    def process_running_transform(self, transform):
        """
        process running transforms
        """
        self.logger.info("process_running_transform: transform_id: %s" % transform['transform_id'])
        work = transform['transform_metadata']['work']

        input_collections = work.get_input_collections()
        output_collections = work.get_output_collections()
        log_collections = work.get_log_collections()

        input_coll_ids = self.get_collection_ids(input_collections)
        output_coll_ids = self.get_collection_ids(output_collections)
        log_coll_ids = self.get_collection_ids(log_collections)

        registered_input_output_maps = core_transforms.get_transform_input_output_maps(transform['transform_id'],
                                                                                       input_coll_ids=input_coll_ids,
                                                                                       output_coll_ids=output_coll_ids,
                                                                                       log_coll_ids=log_coll_ids)
        # update_input_output_maps = self.get_update_input_output_maps(registered_input_output_maps)
        update_contents = self.get_updated_contents(transform, registered_input_output_maps)
        if work.has_new_inputs():
            new_input_output_maps = work.get_new_input_output_maps(registered_input_output_maps)
        else:
            new_input_output_maps = {}
        new_contents = self.get_new_contents(transform, new_input_output_maps)

        # new_input_output_maps = work.get_new_input_output_maps()
        # new_contents = self.get_new_contents(new_input_output_maps)

        file_msgs = []
        """
        if new_contents:
            file_msg = self.generate_file_message(transform, new_contents)
            file_msgs.append(file_msg)
        if updated_contents:
            file_msg = self.generate_file_message(transform, updated_contents)
            file_msgs.append(file_msg)
        """

        # processing = self.get_processing(transform, input_colls, output_colls, log_colls, new_input_output_maps)
        processing = work.get_processing(new_input_output_maps)
        new_processing = None
        if not processing:
            new_processing = work.create_processing(new_input_output_maps)
            new_processing_model = copy.deepcopy(new_processing)
            new_processing_model['transform_id'] = transform['transform_id']
            new_processing_model['status'] = ProcessingStatus.New
            if 'processing_metadata' not in new_processing:
                new_processing['processing_metadata'] = {}
            if 'processing_metadata' not in new_processing_model:
                new_processing_model['processing_metadata'] = {}
            new_processing_model['processing_metadata']['work'] = work

        transform['locking'] = TransformLocking.Idle
        # status_statistics = work.get_status_statistics(registered_input_output_maps)
        work.syn_work_status(registered_input_output_maps)
        if work.is_finished():
            transform['status'] = TransformStatus.Finished
        elif work.is_subfinished():
            transform['status'] = TransformStatus.SubFinished
        elif work.is_failed():
            transform['status'] = TransformStatus.Failed
        else:
            transform['status'] = TransformStatus.Transforming

        ret = {'transform': transform,
               'update_input_collections': copy.deepcopy(input_collections) if input_collections else input_collections,
               'update_output_collections': copy.deepcopy(output_collections) if output_collections else output_collections,
               'update_log_collections': copy.deepcopy(log_collections) if log_collections else log_collections,
               'new_contents': new_contents,
               'update_contents': update_contents,
               'messages': file_msgs,
               'new_processing': new_processing_model}
        return ret

    def process_running_transforms(self):
        ret = []
        while not self.running_task_queue.empty():
            try:
                transform = self.running_task_queue.get()
                if transform:
                    self.logger.info("Main thread processing running transform: %s" % transform)
                    ret_transform = self.process_running_transform(transform)
                    if ret_transform:
                        ret.append(ret_transform)
            except Exception as ex:
                self.logger.error(ex)
                self.logger.error(traceback.format_exc())
        return ret

    def finish_running_transforms(self):
        while not self.running_output_queue.empty():
            try:
                ret = self.running_output_queue.get()
                self.logger.info("Main thread finishing running transform: %s" % ret['transform'])
                if ret:
                    # self.logger.debug("wen: %s" % str(ret['output_contents']))
                    core_transforms.add_transform_outputs(transform=ret['transform'],
                                                          input_collections=ret.get('input_collections', None),
                                                          output_collections=ret.get('output_collections', None),
                                                          log_collections=ret.get('log_collections', None),
                                                          new_contents=ret.get('new_contents', None),
                                                          update_input_collections=ret.get('update_input_collections', None),
                                                          update_output_collections=ret.get('update_output_collections', None),
                                                          update_log_collections=ret.get('update_log_collections', None),
                                                          update_contents=ret.get('update_contents', None),
                                                          messages=ret.get('messages', None),
                                                          new_processing=ret.get('new_processing', None),
                                                          message_bulk_size=self.message_bulk_size)

            except Exception as ex:
                self.logger.error(ex)
                self.logger.error(traceback.format_exc())

    def clean_locks(self):
        self.logger.info("clean locking")
        core_transforms.clean_locking()

    def run(self):
        """
        Main run function.
        """
        try:
            self.logger.info("Starting main thread")

            self.load_plugins()

            task = self.create_task(task_func=self.get_new_transforms, task_output_queue=self.new_task_queue, task_args=tuple(), task_kwargs={}, delay_time=1, priority=1)
            self.add_task(task)
            task = self.create_task(task_func=self.process_new_transforms, task_output_queue=self.new_output_queue, task_args=tuple(), task_kwargs={}, delay_time=1, priority=1)
            self.add_task(task)
            task = self.create_task(task_func=self.finish_new_transforms, task_output_queue=None, task_args=tuple(), task_kwargs={}, delay_time=2, priority=1)
            self.add_task(task)

            task = self.create_task(task_func=self.get_running_transforms, task_output_queue=self.running_task_queue, task_args=tuple(), task_kwargs={}, delay_time=1, priority=1)
            self.add_task(task)
            task = self.create_task(task_func=self.process_running_transforms, task_output_queue=self.running_output_queue, task_args=tuple(), task_kwargs={}, delay_time=1, priority=1)
            self.add_task(task)
            task = self.create_task(task_func=self.finish_running_transforms, task_output_queue=None, task_args=tuple(), task_kwargs={}, delay_time=1, priority=1)
            self.add_task(task)

            task = self.create_task(task_func=self.clean_locks, task_output_queue=None, task_args=tuple(), task_kwargs={}, delay_time=1800, priority=1)
            self.add_task(task)

            self.execute()
        except KeyboardInterrupt:
            self.stop()


if __name__ == '__main__':
    agent = Transformer()
    agent()
