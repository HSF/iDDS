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
import Queue


from idds.common.constants import (Sections, TransformType, TransformStatus,
                                   CollectionType, CollectionRelationType, CollectionStatus,
                                   ContentStatus)
from idds.common.exceptions import AgentPluginError, IDDSException
from idds.common.utils import setup_logging
from idds.core import (transforms as core_transforms, collections as core_collections,
                       contents as core_contents)
from idds.agents.common.baseagent import BaseAgent

setup_logging(__name__)


class Transformer(BaseAgent):
    """
    Transformer works to process transforms.
    """

    def __init__(self, num_threads=1, **kwargs):
        super(Transformer, self).__init__(num_threads=num_threads, **kwargs)
        self.config_section = Sections.Transformer
        self.new_queue = Queue.Queue()
        self.monitor_queue = Queue.Queue()

    def get_new_transforms(self):
        """
        Get new transforms to process
        """

        transform_status = [TransformStatus.New]
        transforms_new = core_transforms.get_transforms_by_status(status=transform_status)
        self.logger.info("Main thread get %s New transforms to process" % len(transforms_new))
        return transforms_new

    def get_output_collection(self, input_collections, transform_type, transform_tag):
        if transform_type in [TransformType.StageIn, TransformType.StageIn.value]:
            return None

        collection = input_collections[0]
        output_collection = copy.deepcopy(collection)
        output_collection['name'] = collection['name'] + '_iDDS.%s.%s' % (transform_type.name, transform_tag),
        output_collection['coll_type'] = CollectionType.Dataset
        output_collection['relation_type'] = CollectionRelationType.Output
        output_collection['status'] = CollectionStatus.New
        return output_collection

    def get_log_collection(self, input_collections, transform_type, transform_tag):
        if transform_type in [TransformType.StageIn, TransformType.StageIn.value]:
            return None

        collection = input_collections[0]
        output_collection = copy.deepcopy(collection)
        output_collection['name'] = collection['name'] + '_iDDS.%s.%s.log' % (transform_type.name, transform_tag),
        output_collection['coll_type'] = CollectionType.Dataset
        output_collection['relation_type'] = CollectionRelationType.Log
        output_collection['status'] = CollectionStatus.New
        return output_collection

    def get_collections(self, scope, name):
        if 'collection_lister' not in self.plugins:
            raise AgentPluginError('Plugin collection_lister is required')
        return self.plugins['collection_lister'](scope, name)

    def process_new_transform(self, transform):
        """
        Process new transform
        """
        collections = core_collections.get_collections_by_request_transform_id(transform_id=transform['transform_id'])
        input_collections = []
        for coll in collections:
            if coll['relation_type'] == CollectionRelationType.Input:
                input_collections.append(coll)
        output_collection = self.get_output_collection(input_collections)
        log_collection = self.get_log_collection(input_collections)

        return [output_collection, log_collection]

    def finish_new_transforms(self):
        while not self.new_output_queue.empty():
            transform_id, output_collection, log_collection = self.new_output_queue.get()
            self.logger.info("Main thread finishing processing transform: %s" % transform_id)
            core_collections.add_collection(output_collection)
            core_collections.add_collection(log_collection)
            core_transforms.update_transform(transform_id, {'status': TransformStatus.Transforming})

    def get_monitor_transforms(self):
        """
        Get transforms to monitor
        """
        transform_status = [TransformStatus.Transforming]
        transforms = core_transforms.get_transforms_by_status(status=transform_status, period=3600)
        self.logger.info("Main thread get %s transforming transforms to process" % len(transforms))
        return transforms

    def get_new_input_contents(self, input_collection):
        pass

    def get_new_output_contents(self, transform, input_collection, output_collection):
        # new_input_contents = self.get_new_input_contents(input_collection)
        pass

    def fill_new_output_contents(self, transform, input_collections, output_collections, log_collections):
        if len(input_collections) > 1 or len(output_collections) > 1 or len(log_collections) > 1:
            raise IDDSException("IDDS currently doesn't support transforms with more than one input or output")
        input_collection = input_collections[0]
        output_collection = output_collections[0]
        # log_collection = log_collections[0]

        if input_collection['coll_status'] == CollectionStatus.Update:
            # split files
            new_out_contents = self.get_new_output_contents(transform, input_collection, output_collection)

            # register output contents
            core_contents.add_contents(new_out_contents)

            # update input collection status
            if input_collection['coll_metadata']['ddm_status'] == 'closed':
                parameters = {'coll_status': CollectionStatus.Closed}
            else:
                parameters = {'coll_status': CollectionStatus.Open}
            core_collections.update_collection(input_collection['coll_id'], parameters)

            # update output collection status
            core_collections.update_collection(output_collection['coll_id'], {'coll_status': CollectionStatus.Updated})

    def generate_processing(self, transform, input_collections, output_collections, log_collections):
        pass

    def check_output_contents(self, transform, input_collections, output_collections, log_collections):
        if len(input_collections) > 1 or len(output_collections) > 1 or len(log_collections) > 1:
            raise IDDSException("IDDS currently doesn't support transforms with more than one input or output")
        output_collection = output_collections[0]

        contents = core_contents.get_content_status_statistics(coll_id=output_collection['coll_id'])
        content_status_keys = list(contents.keys())
        if content_status_keys == [ContentStatus.Available] or content_status_keys == [ContentStatus.Available.value]:
            core_collections.update_collection(output_collection['coll_id'], {'coll_status': CollectionStatus.Closed})
            transform['status'] = TransformStatus.Finished
            transform['transform_metadata']['status_statistics'] = contents
        elif content_status_keys == [ContentStatus.FinalFailed] or content_status_keys == [ContentStatus.FinalFailed.value]:
            core_collections.update_collection(output_collection['coll_id'], {'coll_status': CollectionStatus.Failed})
            transform['status'] = TransformStatus.Failed
            transform['transform_metadata']['status_statistics'] = contents
        elif (len(content_status_keys) == 2                                                                                   # noqa: W503
            and (ContentStatus.FinalFailed in content_status_keys or ContentStatus.FinalFailed.value in content_status_keys)  # noqa: W503
            and (ContentStatus.Available in content_status_keys or ContentStatus.Available.value in content_status_keys)):    # noqa: W503
            core_collections.update_collection(output_collection['coll_id'], {'coll_status': CollectionStatus.SubClosed})
            transform['status'] = TransformStatus.SubFinished
            transform['transform_metadata']['status_statistics'] = contents
        elif (ContentStatus.New in content_status_keys or ContentStatus.New.value in content_status_keys            # noqa: W503
            or ContentStatus.Failed in content_status_keys or ContentStatus.Failed.value in content_status_keys):   # noqa: W503
            self.generate_processing(transform, input_collections, output_collections, log_collections)
            transform['status'] = TransformStatus.Transforming
            transform['transform_metadata']['status_statistics'] = contents
        return transform

    def process_monitor_transform(self, transform):
        """
        process monitor transforms
        """
        collections = core_collections.get_collections_by_request_transform_id(transform_id=transform['transform_id'])
        input_collections, output_collections, log_collections = [], [], []
        for coll in collections:
            if coll['relation_type'] == CollectionRelationType.Input:
                input_collections.append(coll)
            if coll['relation_type'] == CollectionRelationType.Output:
                output_collections.append(coll)
            if coll['relation_type'] == CollectionRelationType.Log:
                log_collections.append(coll)

        for coll in input_collections:
            if coll['coll_status'] == CollectionStatus.Updated:
                self.fill_new_output_contents(transform, input_collections, output_collections, log_collections)

        for coll in output_collections:
            if coll['coll_status'] == CollectionStatus.Updated1:
                return self.check_output_contents(transform, input_collections, output_collections, log_collections)

    def finish_monitor_transforms(self):
        while not self.monitor_output_queue.empty():
            transform = self.monitor_output_queue.get()
            parameter = {}
            for key in ['status', 'transform_metadata']:
                if key in transform:
                    parameter[key] = transform[key]
            core_transforms.update_transform(transform['transform_id'], parameter)

    def prepare_finish_tasks(self):
        """
        Prepare tasks and finished tasks
        """
        # finish tasks
        self.finish_new_transforms()
        self.finish_monitor_transforms()

        # prepare tasks
        transforms = self.get_new_transforms()
        for transform in transforms:
            self.submit_task(self.process_new_transform, self.new_output_queue, transform)

        transforms = self.get_monitor_transforms()
        for transform in transforms:
            self.submit_task(self.process_monitor_transform, self.monitor_output_queue, transform)

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
    agent = Transformer()
    agent()
