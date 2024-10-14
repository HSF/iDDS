#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019 - 2024

import time
import traceback

from idds.common import exceptions
from idds.common.constants import (Sections, ReturnCode, ProcessingType,
                                   ProcessingStatus, ProcessingLocking,
                                   Terminated_processing_status)
from idds.common.utils import setup_logging, truncate_string
from idds.core import processings as core_processings
from idds.agents.common.baseagent import BaseAgent
from idds.agents.common.eventbus.event import (EventType,
                                               UpdateProcessingEvent,
                                               SyncProcessingEvent,
                                               TerminatedProcessingEvent,
                                               UpdateTransformEvent)


from .utils import (handle_abort_processing,
                    handle_resume_processing,
                    # is_process_terminated,
                    sync_processing)
from .iutils import sync_iprocessing, handle_abort_iprocessing, handle_resume_iprocessing
from .poller import Poller

setup_logging(__name__)


class Finisher(Poller):
    """
    Finisher works to submit and running tasks to WFMS.
    """

    def __init__(self, num_threads=1, finisher_max_number_workers=None, max_number_workers=3, poll_time_period=10, retries=3, retrieve_bulk_size=2,
                 message_bulk_size=1000, **kwargs):
        if finisher_max_number_workers:
            self.max_number_workers = int(finisher_max_number_workers)
        else:
            self.max_number_workers = int(max_number_workers)
        self.set_max_workers()

        num_threads = int(self.max_number_workers)

        super(Finisher, self).__init__(num_threads=num_threads, max_number_workers=self.max_number_workers,
                                       name='Finisher',
                                       poll_time_period=poll_time_period, retries=retries,
                                       retrieve_bulk_size=retrieve_bulk_size,
                                       message_bulk_size=message_bulk_size, **kwargs)
        self.logger.info("num_threads: %s" % num_threads)

        self.config_section = Sections.Carrier
        self.poll_time_period = int(poll_time_period)
        self.retries = int(retries)

        if hasattr(self, 'finisher_max_number_workers'):
            self.max_number_workers = int(self.finisher_max_number_workers)

        self.show_queue_size_time = None

    def show_queue_size(self):
        if self.show_queue_size_time is None or time.time() - self.show_queue_size_time >= 600:
            self.show_queue_size_time = time.time()
            q_str = "number of processings: %s, max number of processings: %s" % (self.number_workers, self.max_number_workers)
            self.logger.debug(q_str)

    def get_finishing_processings(self):
        """
        Get finishing processing
        """
        try:
            if not self.is_ok_to_run_more_processings():
                return []

            self.show_queue_size()

            if BaseAgent.min_request_id is None:
                return []

            processing_status = [ProcessingStatus.Terminating, ProcessingStatus.Synchronizing]
            # next_poll_at = datetime.datetime.utcnow() + datetime.timedelta(seconds=self.poll_period)
            processings = core_processings.get_processings_by_status(status=processing_status,
                                                                     locking=True, update_poll=True,
                                                                     not_lock=True,
                                                                     # only_return_id=True,
                                                                     min_request_id=BaseAgent.min_request_id,
                                                                     bulk_size=self.retrieve_bulk_size)

            # self.logger.debug("Main thread get %s [submitting + submitted + running] processings to process" % (len(processings)))
            if processings:
                self.logger.info("Main thread get terminating/synchronizing processings to process: %s" % (str(processings)))

            events, pr_ids = [], []
            for pr in processings:
                pr_id = pr['processing_id']
                pr_ids.append(pr_id)
                pr_status = pr['status']
                if pr_status in [ProcessingStatus.Terminating]:
                    self.logger.info("TerminatedProcessingEvent(processing_id: %s)" % pr_id)
                    event = TerminatedProcessingEvent(publisher_id=self.id, processing_id=pr_id)
                    events.append(event)
                elif pr_status in [ProcessingStatus.Synchronizing]:
                    self.logger.info("SyncProcessingEvent(processing_id: %s)" % pr_id)
                    event = SyncProcessingEvent(publisher_id=self.id, processing_id=pr_id)
                    events.append(event)
            self.event_bus.send_bulk(events)

            return pr_ids
        except exceptions.DatabaseException as ex:
            if 'ORA-00060' in str(ex):
                self.logger.warn("(cx_Oracle.DatabaseError) ORA-00060: deadlock detected while waiting for resource")
            else:
                # raise ex
                self.logger.error(ex)
                self.logger.error(traceback.format_exc())
        return []

    def handle_sync_processing(self, processing, log_prefix=""):
        """
        process terminated processing
        """
        try:
            processing, update_collections, messages = sync_processing(processing, self.agent_attributes, logger=self.logger, log_prefix=log_prefix)

            update_processing = {'processing_id': processing['processing_id'],
                                 'parameters': {'status': processing['status'],
                                                'locking': ProcessingLocking.Idle}}
            ret = {'update_processing': update_processing,
                   'update_collections': update_collections,
                   'messages': messages}
            return ret
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            error = {'sync_err': {'msg': truncate_string('%s' % (ex), length=200)}}
            update_processing = {'processing_id': processing['processing_id'],
                                 'parameters': {'status': ProcessingStatus.Running,
                                                'locking': ProcessingLocking.Idle,
                                                'errors': processing['errors'] if processing['errors'] else {}}}
            update_processing['parameters']['errors'].update(error)
            ret = {'update_processing': update_processing}
            return ret
        return None

    def handle_sync_iprocessing(self, processing, log_prefix=""):
        """
        process terminated processing
        """
        try:
            processing, update_collections, messages = sync_iprocessing(processing, self.agent_attributes, logger=self.logger, log_prefix=log_prefix)

            update_processing = {'processing_id': processing['processing_id'],
                                 'parameters': {'status': processing['status'],
                                                'locking': ProcessingLocking.Idle}}
            ret = {'update_processing': update_processing,
                   'update_collections': update_collections,
                   'messages': messages}
            return ret
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            error = {'sync_err': {'msg': truncate_string('%s' % (ex), length=200)}}
            update_processing = {'processing_id': processing['processing_id'],
                                 'parameters': {'status': ProcessingStatus.Running,
                                                'locking': ProcessingLocking.Idle,
                                                'errors': processing['errors'] if processing['errors'] else {}}}
            update_processing['parameters']['errors'].update(error)
            ret = {'update_processing': update_processing}
            return ret
        return None

    def process_sync_processing(self, event):
        self.number_workers += 1
        pro_ret = ReturnCode.Ok.value
        try:
            if event:
                self.logger.info("process_sync_processing: event: %s" % event)
                pr = self.get_processing(processing_id=event._processing_id, locking=True)
                if not pr:
                    self.logger.error("Cannot find processing for event: %s" % str(event))
                    # pro_ret = ReturnCode.Locked.value
                    pro_ret = ReturnCode.Ok.value
                elif pr['status'] in Terminated_processing_status:
                    parameters = {'locking': ProcessingLocking.Idle}
                    update_processing = {'processing_id': pr['processing_id'],
                                         'parameters': parameters}
                    ret = {'update_processing': update_processing,
                           'update_contents': []}
                    self.update_processing(ret, pr)
                    pro_ret = ReturnCode.Ok.value
                else:
                    log_pre = self.get_log_prefix(pr)

                    self.logger.info(log_pre + "process_sync_processing")
                    if pr['processing_type'] and pr['processing_type'] in [ProcessingType.iWorkflow, ProcessingType.iWork]:
                        ret = self.handle_sync_iprocessing(pr, log_prefix=log_pre)
                    else:
                        ret = self.handle_sync_processing(pr, log_prefix=log_pre)
                    ret_copy = {}
                    for ret_key in ret:
                        if ret_key != 'messages':
                            ret_copy[ret_key] = ret[ret_key]
                    self.logger.info(log_pre + "process_sync_processing result: %s" % str(ret_copy))

                    self.update_processing(ret, pr)

                    # no need to update transform
                    # self.logger.info(log_pre + "UpdateTransformEvent(transform_id: %s)" % pr['transform_id'])
                    # event = UpdateTransformEvent(publisher_id=self.id, transform_id=pr['transform_id'])
                    # self.event_bus.send(event)
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            pro_ret = ReturnCode.Failed.value
        self.number_workers -= 1
        return pro_ret

    def handle_terminated_processing(self, processing, log_prefix=""):
        """
        process terminated processing
        """
        try:
            processing, update_collections, messages = sync_processing(processing, self.agent_attributes, terminate=True, logger=self.logger, log_prefix=log_prefix)

            update_processing = {'processing_id': processing['processing_id'],
                                 'parameters': {'status': processing['status'],
                                                'locking': ProcessingLocking.Idle}}
            ret = {'update_processing': update_processing,
                   'update_collections': update_collections,
                   'messages': messages}

            return ret
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            error = {'term_err': {'msg': truncate_string('%s' % (ex), length=200)}}
            update_processing = {'processing_id': processing['processing_id'],
                                 'parameters': {'status': ProcessingStatus.Running,
                                                'locking': ProcessingLocking.Idle,
                                                'errors': processing['errors'] if processing['errors'] else {}}}
            update_processing['parameters']['errors'].update(error)
            ret = {'update_processing': update_processing}
            return ret
        return None

    def handle_terminated_iprocessing(self, processing, log_prefix=""):
        """
        process terminated processing
        """
        try:
            processing, update_collections, messages = sync_iprocessing(processing, self.agent_attributes, terminate=True, logger=self.logger, log_prefix=log_prefix)

            update_processing = {'processing_id': processing['processing_id'],
                                 'parameters': {'status': processing['status'],
                                                'locking': ProcessingLocking.Idle}}
            ret = {'update_processing': update_processing,
                   'update_collections': update_collections,
                   'messages': messages}

            return ret
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            error = {'term_err': {'msg': truncate_string('%s' % (ex), length=200)}}
            update_processing = {'processing_id': processing['processing_id'],
                                 'parameters': {'status': ProcessingStatus.Running,
                                                'locking': ProcessingLocking.Idle,
                                                'errors': processing['errors'] if processing['errors'] else {}}}
            update_processing['parameters']['errors'].update(error)
            ret = {'update_processing': update_processing}
            return ret
        return None

    def process_terminated_processing(self, event):
        self.number_workers += 1
        pro_ret = ReturnCode.Ok.value
        try:
            if event:
                if event._counter > 3:
                    self.logger.warn("Event counter is bigger than 3, skip event: %s" % str(event))
                else:
                    original_event = event
                    pr = self.get_processing(processing_id=event._processing_id, locking=True)
                    if not pr:
                        self.logger.error("Cannot find processing for event: %s" % str(event))
                        # pro_ret = ReturnCode.Locked.value
                        pro_ret = ReturnCode.Ok.value
                    elif pr['status'] in Terminated_processing_status:
                        parameters = {'locking': ProcessingLocking.Idle}
                        update_processing = {'processing_id': pr['processing_id'],
                                             'parameters': parameters}
                        ret = {'update_processing': update_processing,
                               'update_contents': []}
                        self.update_processing(ret, pr)
                        pro_ret = ReturnCode.Ok.value
                    else:
                        log_pre = self.get_log_prefix(pr)

                        self.logger.info(log_pre + "process_terminated_processing")
                        if pr['processing_type'] and pr['processing_type'] in [ProcessingType.iWorkflow, ProcessingType.iWork]:
                            ret = self.handle_terminated_iprocessing(pr, log_prefix=log_pre)
                        else:
                            ret = self.handle_terminated_processing(pr, log_prefix=log_pre)
                        ret_copy = {}
                        for ret_key in ret:
                            if ret_key != 'messages':
                                ret_copy[ret_key] = ret[ret_key]
                        self.logger.info(log_pre + "process_terminated_processing result: %s" % str(ret_copy))

                        self.update_processing(ret, pr)
                        self.logger.info(log_pre + "UpdateTransformEvent(transform_id: %s)" % pr['transform_id'])
                        event = UpdateTransformEvent(publisher_id=self.id, transform_id=pr['transform_id'])
                        self.event_bus.send(event)

                        if pr['processing_type'] and pr['processing_type'] in [ProcessingType.iWorkflow, ProcessingType.iWork]:
                            pass
                        else:
                            if pr['status'] not in [ProcessingStatus.Finished, ProcessingStatus.Failed, ProcessingStatus.SubFinished, ProcessingStatus.Broken]:
                                # some files are missing, poll it.
                                self.logger.info(log_pre + "UpdateProcessingEvent(processing_id: %s)" % pr['processing_id'])
                                event = UpdateProcessingEvent(publisher_id=self.id, processing_id=pr['processing_id'], counter=original_event._counter + 1)
                                event.set_terminating()
                                self.event_bus.send(event)
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            pro_ret = ReturnCode.Failed.value
        self.number_workers -= 1
        return pro_ret

    def handle_abort_processing(self, processing, log_prefix=""):
        """
        process abort processing
        """
        try:
            processing, update_collections, update_contents, messages = handle_abort_processing(processing, self.agent_attributes, logger=self.logger, log_prefix=log_prefix)

            update_processing = {'processing_id': processing['processing_id'],
                                 'parameters': {'status': processing['status'],
                                                'locking': ProcessingLocking.Idle}}
            ret = {'update_processing': update_processing,
                   'update_collections': update_collections,
                   'update_contents': update_contents,
                   'messages': messages
                   }
            return ret
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            error = {'abort_err': {'msg': truncate_string('%s' % (ex), length=200)}}
            update_processing = {'processing_id': processing['processing_id'],
                                 'parameters': {'status': ProcessingStatus.ToCancel,
                                                'locking': ProcessingLocking.Idle,
                                                'errors': processing['errors'] if processing['errors'] else {}}}
            update_processing['parameters']['errors'].update(error)
            ret = {'update_processing': update_processing}
            return ret
        return None

    def handle_abort_iprocessing(self, processing, log_prefix=""):
        """
        process abort processing
        """
        try:
            plugin = None
            if processing['processing_type']:
                plugin_name = processing['processing_type'].name.lower() + '_poller'
                plugin = self.get_plugin(plugin_name)
            else:
                raise exceptions.ProcessSubmitFailed('No corresponding poller plugins for %s' % processing['processing_type'])

            processing_status, update_collections, update_contents, messages = handle_abort_iprocessing(processing, self.agent_attributes, plugin=plugin, logger=self.logger, log_prefix=log_prefix)

            update_processing = {'processing_id': processing['processing_id'],
                                 'parameters': {'status': processing_status,
                                                'substatus': ProcessingStatus.ToCancel,
                                                'locking': ProcessingLocking.Idle}}
            ret = {'update_processing': update_processing,
                   'update_collections': update_collections,
                   'update_contents': update_contents,
                   'messages': messages
                   }
            return ret
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            error = {'abort_err': {'msg': truncate_string('%s' % (ex), length=200)}}
            update_processing = {'processing_id': processing['processing_id'],
                                 'parameters': {'status': ProcessingStatus.ToCancel,
                                                'locking': ProcessingLocking.Idle,
                                                'errors': processing['errors'] if processing['errors'] else {}}}
            update_processing['parameters']['errors'].update(error)
            ret = {'update_processing': update_processing}
            return ret
        return None

    def process_abort_processing(self, event):
        self.number_workers += 1
        pro_ret = ReturnCode.Ok.value
        try:
            if event:
                processing_status = [ProcessingStatus.Finished, ProcessingStatus.Failed,
                                     ProcessingStatus.Lost, ProcessingStatus.Cancelled,
                                     ProcessingStatus.Suspended, ProcessingStatus.Expired,
                                     ProcessingStatus.Broken]

                pr = self.get_processing(processing_id=event._processing_id, locking=True)

                if not pr:
                    self.logger.error("Cannot find processing for event: %s" % str(event))
                    pro_ret = ReturnCode.Locked.value
                else:
                    log_pre = self.get_log_prefix(pr)
                    self.logger.info(log_pre + "process_abort_processing")

                    if pr and pr['status'] in processing_status:
                        update_processing = {'processing_id': pr['processing_id'],
                                             'parameters': {'locking': ProcessingLocking.Idle,
                                                            'errors': {'abort_err': {'msg': truncate_string("Processing is already terminated. Cannot be aborted", length=200)}}}}
                        ret = {'update_processing': update_processing}
                        self.logger.info(log_pre + "process_abort_processing result: %s" % str(ret))
                        self.update_processing(ret, pr)
                    elif pr:
                        if pr['processing_type'] and pr['processing_type'] in [ProcessingType.iWorkflow, ProcessingType.iWork]:
                            ret = self.handle_abort_iprocessing(pr, log_prefix=log_pre)
                            self.logger.info(log_pre + "process_abort_processing result: %s" % str(ret))

                            self.update_processing(ret, pr, use_bulk_update_mappings=False)

                            self.logger.info(log_pre + "UpdateTransformEvent(transform_id: %s)" % pr['transform_id'])
                            event = UpdateTransformEvent(publisher_id=self.id, transform_id=pr['transform_id'], content=event._content)
                            self.event_bus.send(event)
                        else:
                            ret = self.handle_abort_processing(pr, log_prefix=log_pre)
                            ret_copy = {}
                            for ret_key in ret:
                                if ret_key != 'messages':
                                    ret_copy[ret_key] = ret[ret_key]
                            self.logger.info(log_pre + "process_abort_processing result: %s" % str(ret_copy))

                            self.update_processing(ret, pr)
                            self.logger.info(log_pre + "UpdateTransformEvent(transform_id: %s)" % pr['transform_id'])
                            event = UpdateTransformEvent(publisher_id=self.id, transform_id=pr['transform_id'], content=event._content)
                            self.event_bus.send(event)
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            pro_ret = ReturnCode.Failed.value
        self.number_workers -= 1
        return pro_ret

    def handle_resume_processing(self, processing, log_prefix=""):
        """
        process resume processing
        """
        try:
            processing, update_collections, update_contents = handle_resume_processing(processing, self.agent_attributes, logger=self.logger, log_prefix=log_prefix)

            update_processing = {'processing_id': processing['processing_id'],
                                 'parameters': {'status': processing['status'],
                                                'locking': ProcessingLocking.Idle}}
            ret = {'update_processing': update_processing,
                   'update_collections': update_collections,
                   'update_contents': update_contents,
                   }
            return ret
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            error = {'resume_err': {'msg': truncate_string('%s' % (ex), length=200)}}
            update_processing = {'processing_id': processing['processing_id'],
                                 'parameters': {'status': ProcessingStatus.ToResume,
                                                'locking': ProcessingLocking.Idle,
                                                'errors': processing['errors'] if processing['errors'] else {}}}
            update_processing['parameters']['errors'].update(error)
            ret = {'update_processing': update_processing}
            return ret
        return None

    def handle_resume_iprocessing(self, processing, log_prefix=""):
        """
        process resume processing
        """
        try:
            plugin = None
            if processing['processing_type']:
                plugin_name = processing['processing_type'].name.lower() + '_poller'
                plugin = self.get_plugin(plugin_name)
            else:
                raise exceptions.ProcessSubmitFailed('No corresponding poller plugins for %s' % processing['processing_type'])

            processing_status, update_collections, update_contents = handle_resume_iprocessing(processing, self.agent_attributes, plugin=plugin, logger=self.logger, log_prefix=log_prefix)

            update_processing = {'processing_id': processing['processing_id'],
                                 'parameters': {'status': processing_status,
                                                'substatus': ProcessingStatus.ToResume,
                                                'locking': ProcessingLocking.Idle}}
            ret = {'update_processing': update_processing,
                   'update_collections': update_collections,
                   'update_contents': update_contents,
                   }
            return ret
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            error = {'resume_err': {'msg': truncate_string('%s' % (ex), length=200)}}
            update_processing = {'processing_id': processing['processing_id'],
                                 'parameters': {'status': ProcessingStatus.ToResume,
                                                'locking': ProcessingLocking.Idle,
                                                'errors': processing['errors'] if processing['errors'] else {}}}
            update_processing['parameters']['errors'].update(error)
            ret = {'update_processing': update_processing}
            return ret
        return None

    def process_resume_processing(self, event):
        self.number_workers += 1
        pro_ret = ReturnCode.Ok.value
        try:
            if event:
                processing_status = [ProcessingStatus.Finished]

                pr = self.get_processing(processing_id=event._processing_id, locking=True)

                if not pr:
                    self.logger.error("Cannot find processing for event: %s" % str(event))
                    pro_ret = ReturnCode.Locked.value
                else:
                    log_pre = self.get_log_prefix(pr)
                    self.logger.info(log_pre + "process_resume_processing")

                    if pr and pr['status'] in processing_status:
                        update_processing = {'processing_id': pr['processing_id'],
                                             'parameters': {'locking': ProcessingLocking.Idle,
                                                            'errors': {'abort_err': {'msg': truncate_string("Processing has already finished. Cannot be resumed", length=200)}}}}
                        ret = {'update_processing': update_processing}

                        self.logger.info(log_pre + "process_resume_processing result: %s" % str(ret))

                        self.update_processing(ret, pr)
                    elif pr:
                        if pr['processing_type'] and pr['processing_type'] in [ProcessingType.iWorkflow, ProcessingType.iWork]:
                            ret = self.handle_resume_iprocessing(pr, log_prefix=log_pre)
                            self.logger.info(log_pre + "process_resume_processing result: %s" % str(ret))

                            self.update_processing(ret, pr, use_bulk_update_mappings=False)

                            self.logger.info(log_pre + "UpdateTransformEvent(transform_id: %s)" % pr['transform_id'])
                            event = UpdateTransformEvent(publisher_id=self.id, transform_id=pr['transform_id'], content=event._content)
                            self.event_bus.send(event)
                        else:
                            ret = self.handle_resume_processing(pr, log_prefix=log_pre)
                            self.logger.info(log_pre + "process_resume_processing result: %s" % str(ret))

                            self.update_processing(ret, pr, use_bulk_update_mappings=False)

                            self.logger.info(log_pre + "UpdateTransformEvent(transform_id: %s)" % pr['transform_id'])
                            event = UpdateTransformEvent(publisher_id=self.id, transform_id=pr['transform_id'], content=event._content)
                            self.event_bus.send(event)
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            pro_ret = ReturnCode.Failed.value
        self.number_workers -= 1
        return pro_ret

    def init_event_function_map(self):
        self.event_func_map = {
            EventType.SyncProcessing: {
                'pre_check': self.is_ok_to_run_more_processings,
                'exec_func': self.process_sync_processing
            },
            EventType.TerminatedProcessing: {
                'pre_check': self.is_ok_to_run_more_processings,
                'exec_func': self.process_terminated_processing
            },
            EventType.AbortProcessing: {
                'pre_check': self.is_ok_to_run_more_processings,
                'exec_func': self.process_abort_processing
            },
            EventType.ResumeProcessing: {
                'pre_check': self.is_ok_to_run_more_processings,
                'exec_func': self.process_resume_processing
            }
        }

    def run(self):
        """
        Main run function.
        """
        try:
            self.logger.info("Starting main thread")
            self.init_thread_info()

            self.load_plugins()
            self.init()

            self.add_default_tasks()

            self.init_event_function_map()

            task = self.create_task(task_func=self.get_finishing_processings, task_output_queue=None, task_args=tuple(), task_kwargs={}, delay_time=10, priority=1)
            self.add_task(task)

            self.execute()
        except KeyboardInterrupt:
            self.stop()


if __name__ == '__main__':
    agent = Finisher()
    agent()
