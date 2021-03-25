#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2020

import traceback
try:
    # python 3
    from queue import Queue
except ImportError:
    # Python 2
    from Queue import Queue


from idds.common.constants import (Sections, WorkprogressStatus, WorkprogressLocking, TransformStatus)
from idds.common.utils import setup_logging
from idds.core import (workprogress as core_workprogress, transforms as core_transforms)
from idds.workflow.work import WorkStatus
from idds.agents.common.baseagent import BaseAgent

setup_logging(__name__)


class Marshaller(BaseAgent):
    """
    Marshaller works to organize workflow, activate the current works in a workflow to workprogresses..
    """

    def __init__(self, num_threads=1, poll_time_period=1800, retrieve_bulk_size=10, **kwargs):
        super(Marshaller, self).__init__(num_threads=num_threads, **kwargs)
        self.config_section = Sections.Marshaller
        self.poll_time_period = int(poll_time_period)
        self.retrieve_bulk_size = int(retrieve_bulk_size)

        self.new_task_queue = Queue()
        self.new_output_queue = Queue()
        self.running_task_queue = Queue()
        self.running_output_queue = Queue()

    def get_new_workprogresses(self):
        """
        Get new workprogress to process
        """

        workprogress_status = [WorkprogressStatus.New, WorkprogressStatus.Ready, WorkprogressStatus.Extend]
        workprogresses_new = core_workprogress.get_workprogresses_by_status(status=workprogress_status,
                                                                            locking=True,
                                                                            bulk_size=self.retrieve_bulk_size)

        self.logger.debug("Main thread get %s New+Ready+Extend workprogresses to process" % len(workprogresses_new))
        if workprogresses_new:
            self.logger.info("Main thread get %s New+Ready+Extend workprogresses to process" % len(workprogresses_new))
        return workprogresses_new

    def process_new_workprogress(self, workprogress):
        """
        Process new workprogress
        """
        wf = workprogress['workprogress_metadata']['workflow']
        works = wf.get_new_works()

        transforms = []
        for work in works:
            new_work = work.copy()
            new_work.add_proxy(wf.get_proxy())
            transform = {'workprogress_id': workprogress['workprogress_id'],
                         'request_id': workprogress['request_id'],
                         'workload_id': workprogress['workload_id'],
                         'transform_type': work.get_work_type(),
                         'transform_tag': work.get_work_tag(),
                         'priority': workprogress['priority'],
                         'status': TransformStatus.New,
                         'retries': 0,
                         'expired_at': workprogress['expired_at'],
                         'transform_metadata': {'orginal_work': work, 'work': new_work}
                         # 'collections': related_collections
                         }
            transforms.append(transform)

        self.logger.info("Processing workprogress(%s): new transforms: %s" % (workprogress['workprogress_id'],
                                                                              transforms))

        workprogress['locking'] = WorkprogressLocking.Idle
        workprogress['status'] = WorkprogressStatus.Transforming
        ret_wp = {'workprogress': workprogress, 'new_transforms': transforms}
        return ret_wp

    def process_new_workprogresses(self):
        ret = []
        while not self.new_task_queue.empty():
            try:
                workprogress = self.new_task_queue.get()
                if workprogress:
                    self.logger.info("Main thread processing new workprogress: %s" % workprogress)
                    ret_workprogress = self.process_new_workprogress(workprogress)
                    if ret_workprogress:
                        ret.append(ret_workprogress)
            except Exception as ex:
                self.logger.error(ex)
                self.logger.error(traceback.format_exc())
        return ret

    def finish_new_workprogresses(self):
        while not self.new_output_queue.empty():
            try:
                ret = self.new_output_queue.get()
                self.logger.info("Main thread finishing new workprogress: %s" % ret['workprogress'])
                if ret:
                    wp = ret['workprogress']
                    tfs = ret['new_transforms']
                    wp_parameters = {'status': wp['status'],
                                     'locking': wp['locking'],
                                     'workprogress_metadata': wp['workprogress_metadata']}
                    core_workprogress.update_workprogress(workprogress_id=wp['workprogress_id'],
                                                          parameters=wp_parameters,
                                                          new_transforms=tfs)
            except Exception as ex:
                self.logger.error(ex)
                self.logger.error(traceback.format_exc())

    def get_running_workprogresses(self):
        """
        Get workprogresses to running
        """
        workprogress_status = [WorkprogressStatus.Transforming, WorkprogressStatus.ToCancel,
                               WorkprogressStatus.Cancelling]
        workprogresses = core_workprogress.get_workprogresses_by_status(status=workprogress_status,
                                                                        period=self.poll_time_period,
                                                                        locking=True,
                                                                        bulk_size=self.retrieve_bulk_size)

        self.logger.debug("Main thread get %s progressing workprogresses to process" % len(workprogresses))
        if workprogresses:
            self.logger.info("Main thread get %s progressing workprogresses to process" % len(workprogresses))
        return workprogresses

    def process_running_workprogress(self, workprogress):
        """
        process running workprogresses
        """
        self.logger.info("process_running_workprogress: workprogress_id: %s" % workprogress['workprogress_id'])
        workprogress_metadata = workprogress['workprogress_metadata']
        wf = workprogress_metadata['workflow']

        new_transforms = []
        if workprogress['status'] in [WorkprogressStatus.Transforming]:
            # new works
            works = wf.get_new_works()
            for work in works:
                new_work = work.copy()
                new_work.add_proxy(wf.get_proxy())
                new_transform = {'workprogress_id': workprogress['workprogress_id'],
                                 'request_id': workprogress['request_id'],
                                 'workload_id': workprogress['workload_id'],
                                 'transform_type': work.get_work_type(),
                                 'transform_tag': work.get_work_tag(),
                                 'priority': workprogress['priority'],
                                 'status': TransformStatus.New,
                                 'retries': 0,
                                 'expired_at': workprogress['expired_at'],
                                 'transform_metadata': {'orginal_work': work, 'work': new_work}
                                 # 'collections': related_collections
                                 }
                new_transforms.append(new_transform)
            self.logger.info("Processing workprogress(%s): new transforms: %s" % (workprogress['workprogress_id'],
                                                                                  new_transforms))

        update_transforms = {}
        if workprogress['status'] in [WorkprogressStatus.ToCancel]:
            # current works
            works = wf.get_current_works()
            # print(works)
            for work in works:
                if work.get_status() not in [WorkStatus.Finished, WorkStatus.SubFinished,
                                             WorkStatus.Failed, WorkStatus.Cancelling,
                                             WorkStatus.Cancelled]:
                    update_transforms[work.get_work_id()] = {'status': TransformStatus.ToCancel}

        # current works
        works = wf.get_current_works()
        # print(works)
        for work in works:
            # print(work.get_work_id())
            tf = core_transforms.get_transform(transform_id=work.get_work_id())
            work_status = WorkStatus(tf['status'].value)
            work.set_status(work_status)
            work.set_terminated_msg(msg=None)   # TODO

        if wf.is_terminated():
            if wf.is_finished():
                wp_status = WorkprogressStatus.Finished
            elif wf.is_subfinished():
                wp_status = WorkprogressStatus.SubFinished
            elif wf.is_failed():
                wp_status = WorkprogressStatus.Failed
            elif wf.is_cancelled():
                wp_status = WorkprogressStatus.Cancelled
            else:
                wp_status = WorkprogressStatus.Failed
            wp_msg = wf.get_terminated_msg()
        else:
            wp_status = WorkprogressStatus.Transforming
            wp_msg = None
        parameters = {'status': wp_status,
                      'locking': WorkprogressLocking.Idle,
                      'workprogress_metadata': workprogress_metadata,
                      'errors': {'msg': wp_msg}}
        ret = {'workprogress_id': workprogress['workprogress_id'],
               'parameters': parameters,
               'new_transforms': new_transforms,
               'update_transforms': update_transforms}
        return ret

    def process_running_workprogresses(self):
        ret = []
        while not self.running_task_queue.empty():
            try:
                workprogress = self.running_task_queue.get()
                if workprogress:
                    self.logger.info("Main thread processing running workprogress: %s" % workprogress)
                    ret_workprogress = self.process_running_workprogress(workprogress)
                    if ret_workprogress:
                        ret.append(ret_workprogress)
            except Exception as ex:
                self.logger.error(ex)
                self.logger.error(traceback.format_exc())
        return ret

    def finish_running_workprogresses(self):
        while not self.running_output_queue.empty():
            try:
                ret = self.running_output_queue.get()
                self.logger.info("Main thread finishing processing workprogress: %s" % ret)

                wp_id = ret['workprogress_id']
                parameters = ret['parameters']
                new_transforms = ret['new_transforms']
                update_transforms = ret['update_transforms']
                core_workprogress.update_workprogress(workprogress_id=wp_id,
                                                      parameters=parameters,
                                                      new_transforms=new_transforms,
                                                      update_transforms=update_transforms)
            except Exception as ex:
                self.logger.error(ex)
                self.logger.error(traceback.format_exc())

    def clean_locks(self):
        self.logger.info("clean locking")
        core_workprogress.clean_locking()

    def run(self):
        """
        Main run function.
        """
        try:
            self.logger.info("Starting main thread")

            self.load_plugins()

            self.add_default_tasks()

            task = self.create_task(task_func=self.get_new_workprogresses, task_output_queue=self.new_task_queue, task_args=tuple(), task_kwargs={}, delay_time=1, priority=1)
            self.add_task(task)
            for _ in range(self.num_threads):
                task = self.create_task(task_func=self.process_new_workprogresses, task_output_queue=self.new_output_queue, task_args=tuple(), task_kwargs={}, delay_time=1, priority=1)
                self.add_task(task)
            task = self.create_task(task_func=self.finish_new_workprogresses, task_output_queue=None, task_args=tuple(), task_kwargs={}, delay_time=2, priority=1)
            self.add_task(task)

            task = self.create_task(task_func=self.get_running_workprogresses, task_output_queue=self.running_task_queue, task_args=tuple(), task_kwargs={}, delay_time=1, priority=1)
            self.add_task(task)
            for _ in range(self.num_threads):
                task = self.create_task(task_func=self.process_running_workprogresses, task_output_queue=self.running_output_queue, task_args=tuple(), task_kwargs={}, delay_time=1, priority=1)
                self.add_task(task)
            task = self.create_task(task_func=self.finish_running_workprogresses, task_output_queue=None, task_args=tuple(), task_kwargs={}, delay_time=1, priority=1)
            self.add_task(task)

            task = self.create_task(task_func=self.clean_locks, task_output_queue=None, task_args=tuple(), task_kwargs={}, delay_time=1800, priority=1)
            self.add_task(task)

            self.execute()
        except KeyboardInterrupt:
            self.stop()


if __name__ == '__main__':
    agent = Marshaller()
    agent()
