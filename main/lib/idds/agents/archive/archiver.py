#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019 - 2023

import traceback

from idds.common.constants import Sections, RequestStatus
from idds.common.utils import setup_logging
from idds.core import (requests as core_requests,
                       messages as core_messages)
from idds.agents.common.baseagent import BaseAgent


setup_logging(__name__)


class Archiver(BaseAgent):
    """
    Archiver works to archive data
    """

    def __init__(self, num_threads=1, poll_period=7, older_than=30, **kwargs):
        self.set_max_workers()
        num_threads = self.max_number_workers
        super(Archiver, self).__init__(num_threads=num_threads, name='Archive', **kwargs)
        if not poll_period:
            poll_period = 7            # days
        self.poll_period = int(poll_period) * 3600 * 24
        if not older_than:
            older_than = 30
        self.older_than = int(older_than)      # days
        self.config_section = Sections.Archiver

    def clean_messages(self):
        try:
            status = [RequestStatus.Finished, RequestStatus.SubFinished,
                      RequestStatus.Failed, RequestStatus.Cancelled,
                      RequestStatus.Suspended, RequestStatus.Expired]
            request_id = core_requests.get_last_request_id(older_than=self.older_than, status=status)
            if request_id:
                self.logger.info("cleaning old mesages older than request id %s" % request_id)
                core_messages.clean_old_messages(request_id=request_id)
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())

    def run(self):
        """
        Main run function.
        """
        try:
            self.logger.info("Starting main thread")
            self.init_thread_info()

            self.add_default_tasks()

            self.logger.info("poll period: %s seconds" % self.poll_period)
            self.logger.info("older_than: %s days" % self.older_than)

            task = self.create_task(task_func=self.clean_messages, task_output_queue=None,
                                    task_args=tuple(), task_kwargs={}, delay_time=self.poll_period, priority=1)
            self.add_task(task)

            self.execute()
        except KeyboardInterrupt:
            self.stop()


if __name__ == '__main__':
    agent = Archiver()
    agent()
