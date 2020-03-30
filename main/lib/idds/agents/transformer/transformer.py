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


from idds.common.constants import (Sections, TransformStatus, TransformLocking,
                                   TransformType, CollectionRelationType, CollectionStatus,
                                   ContentStatus, ProcessingStatus)
from idds.common.exceptions import AgentPluginError
from idds.common.utils import setup_logging
from idds.core import (transforms as core_transforms, catalog as core_catalog, processings as core_processings)
from idds.agents.common.baseagent import BaseAgent

setup_logging(__name__)


class Transformer(BaseAgent):
    """
    Transformer works to process transforms.
    """

    def __init__(self, num_threads=1, poll_time_period=1800, retrieve_bulk_size=None, **kwargs):
        super(Transformer, self).__init__(num_threads=num_threads, **kwargs)
        self.config_section = Sections.Transformer
        self.poll_time_period = int(poll_time_period)
        self.retrieve_bulk_size = int(retrieve_bulk_size)

        self.new_task_queue = Queue()
        self.new_output_queue = Queue()
        self.monitor_task_queue = Queue()
        self.monitor_output_queue = Queue()

    def get_new_transforms(self):
        """
        Get new transforms to process
        """

        transform_status = [TransformStatus.New, TransformStatus.Ready, TransformStatus.Extend]
        transforms_new = core_transforms.get_transforms_by_status(status=transform_status, locking=True, bulk_size=self.retrieve_bulk_size)
        self.logger.info("Main thread get %s New+Ready+Extend transforms to process" % len(transforms_new))
        return transforms_new

    def generate_transform_output_contents(self, transform, input_collection, output_collection, contents):
        self.logger.debug("generate_transform_output_contents: transform: %s, number of input_contents: %s" % (transform, len(contents)))
        if transform['transform_type'] == TransformType.StageIn:
            if 'stagein_transformer' not in self.plugins:
                raise AgentPluginError('Plugin stagein_transformer is required')
            return self.plugins['stagein_transformer'](transform, input_collection, output_collection, contents)
        if transform['transform_type'] == TransformType.ActiveLearning:
            if 'activelearning_transformer' not in self.plugins:
                raise AgentPluginError('Plugin activelearning_transformer is required')
            return self.plugins['activelearning_transformer'](transform, input_collection, output_collection, contents)

        return []

    def generate_transform_outputs(self, transform, collections):
        self.logger.debug("Generating transform outputs: transform: %s, collections: %s" % (transform, collections))
        input_collection = None
        output_collection = None
        for collection in collections:
            if collection['relation_type'] == CollectionRelationType.Input:
                input_collection = collection
            if collection['relation_type'] == CollectionRelationType.Output:
                output_collection = collection

        status = [ContentStatus.New, ContentStatus.Failed]
        contents = core_catalog.get_contents_by_coll_id_status(coll_id=input_collection['coll_id'], status=status)
        output_contents = self.generate_transform_output_contents(transform,
                                                                  input_collection,
                                                                  output_collection,
                                                                  contents)

        self.logger.debug("Generating transform number of output contents: %s" % len(output_contents))

        to_cancel_processing = []
        if transform['status'] == TransformStatus.Extend:
            processings = core_processings.get_processings_by_transform_id(transform['transform_id'])
            for processing in processings:
                to_cancel_processing.append(processing['processing_id'])

        new_processing = None
        if output_contents or transform['status'] == TransformStatus.Extend:
            processing_metadata = {'transform_id': transform['transform_id'],
                                   'input_collection': input_collection['coll_id'],
                                   'output_collection': output_collection['coll_id']}
            for key in transform['transform_metadata']:
                processing_metadata[key] = transform['transform_metadata'][key]

            new_processing = {'transform_id': transform['transform_id'],
                              'status': ProcessingStatus.New,
                              'processing_metadata': processing_metadata}
            self.logger.debug("Generating transform output processing: %s" % new_processing)

        return {'transform': transform, 'input_collection': input_collection, 'output_collection': output_collection,
                'input_contents': contents, 'output_contents': output_contents, 'processing': new_processing,
                'to_cancel_processing': to_cancel_processing}

    def process_new_transform(self, transform):
        """
        Process new transform
        """
        self.logger.debug("process_new_transform: transform_id: %s" % transform['transform_id'])
        ret_collections = core_catalog.get_collections_by_request_transform_id(transform_id=transform['transform_id'])
        self.logger.debug("Processing transform(%s): ret_collections: %s" % (transform['transform_id'], ret_collections))

        collections = []
        ret_transform = None
        for request_id in ret_collections:
            for transform_id in ret_collections[request_id]:
                if transform_id == transform['transform_id']:
                    collections = ret_collections[request_id][transform_id]
                    ret_transform = transform
        self.logger.debug("Processing transform(%s): transform: %s, collections: %s" % (transform['transform_id'],
                                                                                        ret_transform,
                                                                                        collections))

        input_collection = None
        for collection in collections:
            if collection['relation_type'] == CollectionRelationType.Input:
                input_collection = collection

        if ret_transform is not None \
            and (ret_transform['status'] == TransformStatus.Extend                                                                  # noqa: W503
            or (input_collection and input_collection['status'] not in [CollectionStatus.Closed, CollectionStatus.Closed.value])):  # noqa: W503
            ret = self.generate_transform_outputs(ret_transform, collections)
            ret['transform']['locking'] = TransformLocking.Idle
            ret['transform']['status'] = TransformStatus.Transforming
            return ret
        else:
            transform['locking'] = TransformLocking.Idle
            return {'transform': transform, 'input_collection': None, 'output_collection': None,
                    'input_contents': None, 'output_contents': None, 'processing': None, 'to_cancel_processing': []}

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
                self.logger.debug("Main thread finishing processing transform: %s" % ret['transform'])
                if ret:
                    # self.logger.debug("wen: %s" % str(ret['output_contents']))
                    core_transforms.add_transform_outputs(transform=ret['transform'],
                                                          input_collection=ret['input_collection'],
                                                          output_collection=ret['output_collection'],
                                                          input_contents=ret['input_contents'],
                                                          output_contents=ret['output_contents'],
                                                          processing=ret['processing'],
                                                          to_cancel_processing=ret['to_cancel_processing'])
            except Exception as ex:
                self.logger.error(ex)
                self.logger.error(traceback.format_exc())

    def get_monitor_transforms(self):
        """
        Get transforms to monitor
        """
        transform_status = [TransformStatus.Transforming]
        transforms = core_transforms.get_transforms_by_status(status=transform_status,
                                                              period=self.poll_time_period,
                                                              locking=True,
                                                              bulk_size=self.retrieve_bulk_size)
        self.logger.info("Main thread get %s transforming transforms to process" % len(transforms))
        return transforms

    def process_transform_outputs(self, transform, output_collection):
        transform_metadata = transform['transform_metadata']
        if not transform_metadata:
            transform_metadata = {}
        transform_metadata['output_collection'] = output_collection['coll_metadata']
        if output_collection['status'] == CollectionStatus.Closed:
            ret = {'transform_id': transform['transform_id'],
                   'status': TransformStatus.Finished,
                   'transform_metadata': transform_metadata}
        elif output_collection['status'] == CollectionStatus.SubClosed:
            ret = {'transform_id': transform['transform_id'],
                   'status': TransformStatus.SubFinished,
                   'transform_metadata': transform_metadata}
        elif output_collection['status'] == CollectionStatus.Failed:
            ret = {'transform_id': transform['transform_id'],
                   'status': TransformStatus.Failed,
                   'transform_metadata': transform_metadata}
        elif output_collection['status'] == CollectionStatus.Deleted:
            ret = {'transform_id': transform['transform_id'],
                   'status': TransformStatus.Deleted,
                   'transform_metadata': transform_metadata}
        else:
            ret = {'transform_id': transform['transform_id'],
                   'status': TransformStatus.Transforming,
                   'transform_metadata': transform_metadata}
        return ret

    def process_monitor_transform(self, transform):
        """
        process monitor transforms
        """
        self.logger.debug("process_monitor_transform: transform_id: %s" % transform['transform_id'])
        ret_collections = core_catalog.get_collections_by_request_transform_id(transform_id=transform['transform_id'])

        collections = []
        ret_transform = None
        for request_id in ret_collections:
            for transform_id in ret_collections[request_id]:
                if transform_id == transform['transform_id']:
                    collections = ret_collections[request_id][transform_id]
                    ret_transform = transform

        input_collection = None
        output_collection = None
        for collection in collections:
            if collection['relation_type'] == CollectionRelationType.Input:
                input_collection = collection
            if collection['relation_type'] == CollectionRelationType.Output:
                output_collection = collection

        transform_input, transform_output = None, None
        if ret_transform and input_collection and input_collection['status'] not in [CollectionStatus.Closed, CollectionStatus.Closed.value]:
            transform_input = self.generate_transform_outputs(ret_transform, collections)
            transform_input['transform']['locking'] = TransformLocking.Idle
            transform_input['transform']['status'] = TransformStatus.Transforming

        if ret_transform and output_collection:
            transform_output = self.process_transform_outputs(ret_transform, output_collection)
            transform_output['locking'] = TransformLocking.Idle
        else:
            transform_output = {'transform_id': transform['transform_id'],
                                'locking': TransformLocking.Idle}

        ret = {'transform_input': transform_input,
               'transform_output': transform_output}
        return ret

    def process_monitor_transforms(self):
        ret = []
        while not self.monitor_task_queue.empty():
            try:
                transform = self.monitor_task_queue.get()
                if transform:
                    self.logger.info("Main thread processing monitor transform: %s" % transform)
                    ret_transform = self.process_monitor_transform(transform)
                    if ret_transform:
                        ret.append(ret_transform)
            except Exception as ex:
                self.logger.error(ex)
                self.logger.error(traceback.format_exc())
        return ret

    def finish_monitor_transforms(self):
        while not self.monitor_output_queue.empty():
            ret = self.monitor_output_queue.get()
            transform_input = ret['transform_input']
            transform_output = ret['transform_output']
            if transform_input:
                # combine the output changes into the same session
                if transform_output:
                    # status and locking should use the items from transform_input
                    transform_input['transform']['transform_metadata']['output_collection'] = transform_output['transform_metadata']['output_collection']
                # self.logger.debug("wen: %s" % str(transform_input['output_contents']))
                core_transforms.add_transform_outputs(transform=transform_input['transform'],
                                                      input_collection=transform_input['input_collection'],
                                                      output_collection=transform_input['output_collection'],
                                                      input_contents=transform_input['input_contents'],
                                                      output_contents=transform_input['output_contents'],
                                                      processing=transform_input['processing'])
            elif transform_output:
                transform_id = transform_output['transform_id']
                del transform_output['transform_id']
                core_transforms.update_transform(transform_id=transform_id, parameters=transform_output)

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

            task = self.create_task(task_func=self.get_new_transforms, task_output_queue=self.new_task_queue, task_args=tuple(), task_kwargs={}, delay_time=5, priority=1)
            self.add_task(task)
            task = self.create_task(task_func=self.process_new_transforms, task_output_queue=self.new_output_queue, task_args=tuple(), task_kwargs={}, delay_time=2, priority=1)
            self.add_task(task)
            task = self.create_task(task_func=self.finish_new_transforms, task_output_queue=None, task_args=tuple(), task_kwargs={}, delay_time=2, priority=1)
            self.add_task(task)

            task = self.create_task(task_func=self.get_monitor_transforms, task_output_queue=self.monitor_task_queue, task_args=tuple(), task_kwargs={}, delay_time=5, priority=1)
            self.add_task(task)
            task = self.create_task(task_func=self.process_monitor_transforms, task_output_queue=self.monitor_output_queue, task_args=tuple(), task_kwargs={}, delay_time=2, priority=1)
            self.add_task(task)
            task = self.create_task(task_func=self.finish_monitor_transforms, task_output_queue=None, task_args=tuple(), task_kwargs={}, delay_time=2, priority=1)
            self.add_task(task)

            task = self.create_task(task_func=self.clean_locks, task_output_queue=None, task_args=tuple(), task_kwargs={}, delay_time=1800, priority=1)
            self.add_task(task)

            self.execute()
        except KeyboardInterrupt:
            self.stop()


if __name__ == '__main__':
    agent = Transformer()
    agent()
