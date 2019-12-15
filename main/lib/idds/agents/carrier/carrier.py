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
import Queue


from idds.common.constants import (Sections, CollectionRelationType, CollectionStatus,
                                   ContentType, ContentStatus)
from idds.common.exceptions import AgentPluginError
from idds.common.utils import setup_logging
from idds.core import collections as core_collections, contents as core_contents
from idds.agents.common.baseagent import BaseAgent

setup_logging(__name__)


class Carrier(BaseAgent):
    """
    Carrier works to submit and monitor tasks to WFMS.
    """

    def __init__(self, num_threads=1, **kwargs):
        super(Carrier, self).__init__(num_threads=num_threads, **kwargs)
        self.config_section = Sections.Carrier
        self.processed_queue = Queue.Queue()

    def get_new_processing(self):
        """
        Get new processing
        """
        processing_status = [ProcessingStatus.New]
        processings = core_processings.get_processings_by_status(status=processing_status)
        self.logger.info("Main thread get %s [new] processings to process" % len(processings))
        return processings

    def process_new_processings(self, processing):
        pass
        processing['status'] = ProcessingStatus.Submitted
        return processing

    def finish_new_processings(self):
        while not self.new_output_queue.empty():
            processing = self.new_output_queue.get()
            self.logger.info("Main thread submitted new processing: %s" % (processing['processing_id']))
            parameters = {'status': processing['status']}
            core_processing.update_processing(processing['processing_id'], parameters=parameters)

    def get_monitor_processing(self):
        """
        Get monitor processing
        """
        processing_status = [ProcessingStatus.Submitted, ProcessingStatus.Running]
        processings = core_processings.get_processings_by_status(status=processing_status, time_period=1800)
        self.logger.info("Main thread get %s [submitted + running] processings to process" % len(processings))
        return processings

    def process_monitor_processings(self, processing):
        pass
        # processing['status'] = ProcessingStatus.Submitted
        return processing

    def finish_monitor_processings(self):
        while not self.monitor_output_queue.empty():
            processing = self.monitor_output_queue.get()
            self.logger.info("Main thread processing %s status changed to %s" % (processing['processing_id'], processing['status']))
            parameters = {'status': processing['status']}
            core_processing.update_processing(processing['processing_id'], parameters=parameters)

    def prepare_finish_tasks(self):
        """
        Prepare tasks and finished tasks
        """
        self.finish_new_processings()
        self.finish_monitor_prcessings()

        processings = self.get_new_processings()
        for processing in processings:
            self.submit_task(self.process_new_processings, self.new_output_queue, processing)

        processings = self.get_monitor_processings()
        for processing in processings:
            self.submit_task(self.process_monitor_processings, self.monitor_output_queue, processing)

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
