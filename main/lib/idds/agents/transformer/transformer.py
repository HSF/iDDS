#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019 - 2024

import copy
import datetime
import random
import time
import traceback

from idds.common import exceptions
from idds.common.constants import (Sections, ReturnCode, TransformType,
                                   TransformStatus, TransformLocking,
                                   CollectionType, CollectionStatus,
                                   CollectionRelationType,
                                   CommandType, ProcessingStatus, WorkflowType,
                                   get_processing_type_from_transform_type,
                                   get_transform_status_from_processing_status)
from idds.common.utils import setup_logging, truncate_string
from idds.core import (transforms as core_transforms,
                       processings as core_processings)
from idds.agents.common.baseagent import BaseAgent
from idds.agents.common.eventbus.event import (EventType,
                                               NewTransformEvent,
                                               UpdateTransformEvent,
                                               AbortProcessingEvent,
                                               ResumeProcessingEvent,
                                               UpdateRequestEvent,
                                               NewProcessingEvent,
                                               UpdateProcessingEvent)

setup_logging(__name__)


class Transformer(BaseAgent):
    """
    Transformer works to process transforms.
    """

    def __init__(self, num_threads=1, max_number_workers=8, poll_period=1800, retries=3, retrieve_bulk_size=10,
                 message_bulk_size=10000, **kwargs):
        self.max_number_workers = max_number_workers
        self.set_max_workers()
        num_threads = self.max_number_workers
        super(Transformer, self).__init__(num_threads=num_threads, name='Transformer', **kwargs)
        self.config_section = Sections.Transformer
        self.poll_period = int(poll_period)
        self.retries = int(retries)
        self.retrieve_bulk_size = int(retrieve_bulk_size)
        self.message_bulk_size = int(message_bulk_size)

        if not hasattr(self, 'new_poll_period') or not self.new_poll_period:
            self.new_poll_period = self.poll_period
        else:
            self.new_poll_period = int(self.new_poll_period)
        if not hasattr(self, 'update_poll_period') or not self.update_poll_period:
            self.update_poll_period = self.poll_period
        else:
            self.update_poll_period = int(self.update_poll_period)

        if not hasattr(self, 'new_poll_period') or not self.new_poll_period:
            self.new_poll_period = self.poll_period
        else:
            self.new_poll_period = int(self.new_poll_period)
        if not hasattr(self, 'update_poll_period') or not self.update_poll_period:
            self.update_poll_period = self.poll_period
        else:
            self.update_poll_period = int(self.update_poll_period)

        if hasattr(self, 'poll_period_increase_rate'):
            self.poll_period_increase_rate = float(self.poll_period_increase_rate)
        else:
            self.poll_period_increase_rate = 2

        if hasattr(self, 'max_new_poll_period'):
            self.max_new_poll_period = int(self.max_new_poll_period)
        else:
            self.max_new_poll_period = 3600 * 6
        if hasattr(self, 'max_update_poll_period'):
            self.max_update_poll_period = int(self.max_update_poll_period)
        else:
            self.max_update_poll_period = 3600 * 6

        self.number_workers = 0
        if not hasattr(self, 'max_number_workers') or not self.max_number_workers:
            self.max_number_workers = 3
        else:
            self.max_number_workers = int(self.max_number_workers)

        self.show_queue_size_time = None

    def is_ok_to_run_more_transforms(self):
        if self.number_workers >= self.max_number_workers:
            return False
        return True

    def show_queue_size(self):
        if self.show_queue_size_time is None or time.time() - self.show_queue_size_time >= 600:
            self.show_queue_size_time = time.time()
            q_str = "number of transforms: %s, max number of transforms: %s" % (self.number_workers, self.max_number_workers)
            self.logger.debug(q_str)

    def get_new_transforms(self):
        """
        Get new transforms to process
        """
        try:
            if not self.is_ok_to_run_more_transforms():
                return []

            self.show_queue_size()

            if BaseAgent.min_request_id is None:
                return []

            transform_status = [TransformStatus.New, TransformStatus.Ready, TransformStatus.Extend]
            # next_poll_at = datetime.datetime.utcnow() + datetime.timedelta(seconds=self.poll_period)
            transforms_new = core_transforms.get_transforms_by_status(status=transform_status, locking=True,
                                                                      not_lock=True,
                                                                      new_poll=True, only_return_id=True,
                                                                      min_request_id=BaseAgent.min_request_id,
                                                                      bulk_size=self.retrieve_bulk_size)

            # self.logger.debug("Main thread get %s New+Ready+Extend transforms to process" % len(transforms_new))
            if transforms_new:
                self.logger.info("Main thread get New+Ready+Extend transforms to process: %s" % str(transforms_new))

            events = []
            for tf_id in transforms_new:
                event = NewTransformEvent(publisher_id=self.id, transform_id=tf_id)
                events.append(event)
            self.event_bus.send_bulk(events)

            return transforms_new
        except exceptions.DatabaseException as ex:
            if 'ORA-00060' in str(ex):
                self.logger.warn("(cx_Oracle.DatabaseError) ORA-00060: deadlock detected while waiting for resource")
            else:
                # raise ex
                self.logger.error(ex)
                self.logger.error(traceback.format_exc())
        return []

    def get_running_transforms(self):
        """
        Get running transforms
        """
        try:
            if not self.is_ok_to_run_more_transforms():
                return []

            self.show_queue_size()

            if BaseAgent.min_request_id is None:
                return []

            transform_status = [TransformStatus.Transforming,
                                TransformStatus.ToCancel, TransformStatus.Cancelling,
                                TransformStatus.ToSuspend, TransformStatus.Suspending,
                                TransformStatus.ToExpire, TransformStatus.Expiring,
                                TransformStatus.ToResume, TransformStatus.Resuming,
                                TransformStatus.ToFinish, TransformStatus.ToForceFinish]
            transforms = core_transforms.get_transforms_by_status(status=transform_status,
                                                                  period=None,
                                                                  locking=True,
                                                                  not_lock=True,
                                                                  min_request_id=BaseAgent.min_request_id,
                                                                  update_poll=True, only_return_id=True,
                                                                  bulk_size=self.retrieve_bulk_size)

            # self.logger.debug("Main thread get %s transforming transforms to process" % len(transforms))
            if transforms:
                self.logger.info("Main thread get transforming transforms to process: %s" % str(transforms))

            events = []
            for tf_id in transforms:
                event = UpdateTransformEvent(publisher_id=self.id, transform_id=tf_id)
                events.append(event)
            self.event_bus.send_bulk(events)

            return transforms
        except exceptions.DatabaseException as ex:
            if 'ORA-00060' in str(ex):
                self.logger.warn("(cx_Oracle.DatabaseError) ORA-00060: deadlock detected while waiting for resource")
            else:
                self.logger.error(ex)
                self.logger.error(traceback.format_exc())
        return []

    def get_transform(self, transform_id, status=None, locking=False):
        try:
            return core_transforms.get_transform_by_id_status(transform_id=transform_id, status=status, locking=locking)
        except exceptions.DatabaseException as ex:
            if 'ORA-00060' in str(ex):
                self.logger.warn("(cx_Oracle.DatabaseError) ORA-00060: deadlock detected while waiting for resource")
            else:
                # raise ex
                self.logger.error(ex)
                self.logger.error(traceback.format_exc())
        return None

    def load_poll_period(self, transform, parameters):
        if self.new_poll_period and transform['new_poll_period'] != self.new_poll_period:
            parameters['new_poll_period'] = self.new_poll_period
        if self.update_poll_period and transform['update_poll_period'] != self.update_poll_period:
            parameters['update_poll_period'] = self.update_poll_period
        return parameters

    def generate_processing_model(self, transform):
        new_processing_model = {}
        new_processing_model['transform_id'] = transform['transform_id']
        new_processing_model['request_id'] = transform['request_id']
        new_processing_model['workload_id'] = transform['workload_id']
        new_processing_model['status'] = ProcessingStatus.New
        # new_processing_model['expired_at'] = work.get_expired_at(None)
        new_processing_model['expired_at'] = transform['expired_at']

        new_processing_model['processing_type'] = get_processing_type_from_transform_type(transform['transform_type'])
        new_processing_model['new_poll_period'] = transform['new_poll_period']
        new_processing_model['update_poll_period'] = transform['update_poll_period']
        new_processing_model['max_new_retries'] = transform['max_new_retries']
        new_processing_model['max_update_retries'] = transform['max_update_retries']
        return new_processing_model

    def get_log_prefix(self, transform):
        if transform:
            return "<request_id=%s,transform_id=%s>" % (transform['request_id'], transform['transform_id'])
        self.logger.error("get_log_prefix transform is empty: %s" % str(transform))
        return ""

    def handle_new_transform_real(self, transform):
        """
        Process new transform
        """
        log_pre = self.get_log_prefix(transform)
        self.logger.info(log_pre + "handle_new_transform: transform_id: %s" % transform['transform_id'])

        work = transform['transform_metadata']['work']
        work.set_work_id(transform['transform_id'])
        work.set_agent_attributes(self.agent_attributes, transform)

        work_name_to_coll_map = core_transforms.get_work_name_to_coll_map(request_id=transform['request_id'])
        work.set_work_name_to_coll_map(work_name_to_coll_map)

        # create processing
        new_processing_model = None
        processing = work.get_processing(input_output_maps=[], without_creating=False)
        self.logger.debug(log_pre + "work get_processing with creating: %s" % processing)
        if processing and not processing.processing_id:
            new_processing_model = self.generate_processing_model(transform)

            proc_work = copy.deepcopy(work)
            proc_work.clean_work()
            processing.work = proc_work
            new_processing_model['processing_metadata'] = {'processing': processing}

        transform_parameters = {'status': TransformStatus.Transforming,
                                'locking': TransformLocking.Idle,
                                'workload_id': transform['workload_id'],
                                'transform_metadata': transform['transform_metadata']}

        transform_parameters = self.load_poll_period(transform, transform_parameters)

        if new_processing_model is not None:
            if 'new_poll_period' in transform_parameters:
                new_processing_model['new_poll_period'] = transform_parameters['new_poll_period']
            if 'update_poll_period' in transform_parameters:
                new_processing_model['update_poll_period'] = transform_parameters['update_poll_period']
            if 'max_new_retries' in transform_parameters:
                new_processing_model['max_new_retries'] = transform_parameters['max_new_retries']
            if 'max_update_retries' in transform_parameters:
                new_processing_model['max_update_retries'] = transform_parameters['max_update_retries']

        ret = {'transform': transform,
               'transform_parameters': transform_parameters,
               'new_processing': new_processing_model
               }
        return ret

    def handle_new_transform(self, transform):
        """
        Process new transform
        """
        try:
            log_pre = self.get_log_prefix(transform)
            ret = self.handle_new_transform_real(transform)
            self.logger.info(log_pre + "handle_new_transform result: %s" % str(ret))
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            retries = transform['new_retries'] + 1
            if not transform['max_new_retries'] or retries < transform['max_new_retries']:
                tf_status = transform['status']
            else:
                tf_status = TransformStatus.Failed

            # increase poll period
            new_poll_period = int(transform['new_poll_period'].total_seconds() * self.poll_period_increase_rate)
            if new_poll_period > self.max_new_poll_period:
                new_poll_period = self.max_new_poll_period

            error = {'submit_err': {'msg': truncate_string('%s' % (ex), length=200)}}

            transform_parameters = {'status': tf_status,
                                    'new_retries': retries,
                                    'new_poll_period': new_poll_period,
                                    'errors': transform['errors'] if transform['errors'] else {},
                                    'locking': TransformLocking.Idle}
            transform_parameters['errors'].update(error)
            ret = {'transform': transform, 'transform_parameters': transform_parameters}
            self.logger.info(log_pre + "handle_new_transform exception result: %s" % str(ret))
        return ret

    def generate_collection(self, transform, collection, relation_type=CollectionRelationType.Input):
        coll = {'transform_id': transform['transform_id'],
                'request_id': transform['request_id'],
                'workload_id': transform['workload_id'],
                'coll_type': CollectionType.Dataset,
                'scope': collection['scope'],
                'name': collection['name'][:254],
                'relation_type': relation_type,
                'bytes': 0,
                'total_files': 0,
                'new_files': 0,
                'processed_files': 0,
                'processing_files': 0,
                'coll_metadata': None,
                'status': CollectionStatus.Open,
                'expired_at': transform['expired_at']}
        return coll

    def handle_new_itransform_real(self, transform):
        """
        Process new transform
        """
        log_pre = self.get_log_prefix(transform)
        self.logger.info(log_pre + "handle_new_itransform: transform_id: %s" % transform['transform_id'])

        work = transform['transform_metadata']['work']
        if work.type in [WorkflowType.iWork]:
            work.transform_id = transform['transform_id']

        # create processing
        new_processing_model = self.generate_processing_model(transform)
        new_processing_model['processing_metadata'] = {'work': work}

        transform_parameters = {'status': TransformStatus.Transforming,
                                'locking': TransformLocking.Idle,
                                'workload_id': transform['workload_id']}

        transform_parameters = self.load_poll_period(transform, transform_parameters)

        if new_processing_model is not None:
            if 'new_poll_period' in transform_parameters:
                new_processing_model['new_poll_period'] = transform_parameters['new_poll_period']
            if 'update_poll_period' in transform_parameters:
                new_processing_model['update_poll_period'] = transform_parameters['update_poll_period']
            if 'max_new_retries' in transform_parameters:
                new_processing_model['max_new_retries'] = transform_parameters['max_new_retries']
            if 'max_update_retries' in transform_parameters:
                new_processing_model['max_update_retries'] = transform_parameters['max_update_retries']

        func_name = work.get_func_name()
        func_name = func_name.split(':')[-1]
        input_coll = {'scope': 'pseudo_dataset', 'name': 'pseudo_input_%s' % func_name}
        output_coll = {'scope': 'pseudo_dataset', 'name': 'pseudo_output_%s' % func_name}

        input_collection = self.generate_collection(transform, input_coll, relation_type=CollectionRelationType.Input)
        output_collection = self.generate_collection(transform, output_coll, relation_type=CollectionRelationType.Output)

        ret = {'transform': transform,
               'transform_parameters': transform_parameters,
               'new_processing': new_processing_model,
               'input_collections': [input_collection],
               'output_collections': [output_collection]
               }
        return ret

    def handle_new_itransform(self, transform):
        """
        Process new transform
        """
        try:
            log_pre = self.get_log_prefix(transform)
            ret = self.handle_new_itransform_real(transform)
            self.logger.info(log_pre + "handle_new_itransform result: %s" % str(ret))
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            retries = transform['new_retries'] + 1
            if not transform['max_new_retries'] or retries < transform['max_new_retries']:
                tf_status = transform['status']
            else:
                tf_status = TransformStatus.Failed

            # increase poll period
            new_poll_period = int(transform['new_poll_period'].total_seconds() * self.poll_period_increase_rate)
            if new_poll_period > self.max_new_poll_period:
                new_poll_period = self.max_new_poll_period

            error = {'submit_err': {'msg': truncate_string('%s' % (ex), length=200)}}

            transform_parameters = {'status': tf_status,
                                    'new_retries': retries,
                                    'new_poll_period': new_poll_period,
                                    'errors': transform['errors'] if transform['errors'] else {},
                                    'locking': TransformLocking.Idle}
            transform_parameters['errors'].update(error)
            ret = {'transform': transform, 'transform_parameters': transform_parameters}
            self.logger.info(log_pre + "handle_new_itransform exception result: %s" % str(ret))
        return ret

    def update_transform(self, ret):
        new_pr_ids, update_pr_ids = [], []
        try:
            if ret:
                log_pre = self.get_log_prefix(ret['transform'])
                self.logger.info(log_pre + "Update transform: %s" % str(ret))

                ret['transform_parameters']['locking'] = TransformLocking.Idle
                ret['transform_parameters']['updated_at'] = datetime.datetime.utcnow()

                retry = True
                retry_num = 0
                while retry:
                    retry = False
                    retry_num += 1
                    try:
                        # self.logger.debug("wen: %s" % str(ret['output_contents']))
                        new_pr_ids, update_pr_ids = core_transforms.add_transform_outputs(transform=ret['transform'],
                                                                                          transform_parameters=ret['transform_parameters'],
                                                                                          input_collections=ret.get('input_collections', None),
                                                                                          output_collections=ret.get('output_collections', None),
                                                                                          log_collections=ret.get('log_collections', None),
                                                                                          new_contents=ret.get('new_contents', None),
                                                                                          update_input_collections=ret.get('update_input_collections', None),
                                                                                          update_output_collections=ret.get('update_output_collections', None),
                                                                                          update_log_collections=ret.get('update_log_collections', None),
                                                                                          update_contents=ret.get('update_contents', None),
                                                                                          messages=ret.get('messages', None),
                                                                                          update_messages=ret.get('update_messages', None),
                                                                                          new_processing=ret.get('new_processing', None),
                                                                                          update_processing=ret.get('update_processing', None),
                                                                                          message_bulk_size=self.message_bulk_size)
                    except exceptions.DatabaseException as ex:
                        if 'ORA-00060' in str(ex):
                            self.logger.warn("(cx_Oracle.DatabaseError) ORA-00060: deadlock detected while waiting for resource")
                            if retry_num < 5:
                                retry = True
                                if retry_num <= 1:
                                    random_sleep = random.randint(1, 10)
                                elif retry_num <= 2:
                                    random_sleep = random.randint(1, 60)
                                else:
                                    random_sleep = random.randint(1, 120)
                                time.sleep(random_sleep)
                            else:
                                raise ex
                        else:
                            raise ex
                            # self.logger.error(ex)
                            # self.logger.error(traceback.format_exc())
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            try:
                transform_parameters = {'status': TransformStatus.Transforming,
                                        'locking': TransformLocking.Idle}
                if 'new_retries' in ret['transform_parameters']:
                    transform_parameters['new_retries'] = ret['transform_parameters']['new_retries']
                if 'update_retries' in ret['transform_parameters']:
                    transform_parameters['update_retries'] = ret['transform_parameters']['update_retries']
                if 'errors' in ret['transform_parameters']:
                    transform_parameters['errors'] = ret['transform_parameters']['errors']

                log_pre = self.get_log_prefix(ret['transform'])
                self.logger.warn(log_pre + "update transform exception result: %s" % str(transform_parameters))

                new_pr_ids, update_pr_ids = core_transforms.add_transform_outputs(transform=ret['transform'],
                                                                                  transform_parameters=transform_parameters)
            except Exception as ex:
                self.logger.error(ex)
                self.logger.error(traceback.format_exc())
        return new_pr_ids, update_pr_ids

    def process_new_transform(self, event):
        self.number_workers += 1
        try:
            if event:
                tf_status = [TransformStatus.New, TransformStatus.Ready, TransformStatus.Extend]
                tf = self.get_transform(transform_id=event._transform_id, status=tf_status, locking=True)
                if not tf:
                    self.logger.error("Cannot find transform for event: %s" % str(event))
                else:
                    log_pre = self.get_log_prefix(tf)
                    self.logger.info(log_pre + "process_new_transform")
                    if tf['transform_type'] in [TransformType.iWorkflow, TransformType.iWork]:
                        ret = self.handle_new_itransform(tf)
                    else:
                        ret = self.handle_new_transform(tf)
                    self.logger.info(log_pre + "process_new_transform result: %s" % str(ret))

                    new_pr_ids, update_pr_ids = self.update_transform(ret)
                    for pr_id in new_pr_ids:
                        self.logger.info(log_pre + "NewProcessingEvent(processing_id: %s)" % pr_id)
                        event = NewProcessingEvent(publisher_id=self.id, processing_id=pr_id, content=event._content)
                        self.event_bus.send(event)
                    for pr_id in update_pr_ids:
                        self.logger.info(log_pre + "UpdateProcessingEvent(processing_id: %s)" % pr_id)
                        event = UpdateProcessingEvent(publisher_id=self.id, processing_id=pr_id, content=event._content)
                        self.event_bus.send(event)
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
        self.number_workers -= 1

    def handle_update_transform_real(self, transform, event):
        """
        process running transforms
        """
        log_pre = self.get_log_prefix(transform)

        self.logger.info(log_pre + "handle_update_transform: transform_id: %s" % transform['transform_id'])

        is_terminated = False
        to_abort = False
        if (event and event._content and 'cmd_type' in event._content and event._content['cmd_type']
            and event._content['cmd_type'] in [CommandType.AbortRequest, CommandType.ExpireRequest]):      # noqa W503
            to_abort = True
            self.logger.info(log_pre + "to_abort %s" % to_abort)

        work = transform['transform_metadata']['work']
        work.set_work_id(transform['transform_id'])
        work.set_agent_attributes(self.agent_attributes, transform)

        work_name_to_coll_map = core_transforms.get_work_name_to_coll_map(request_id=transform['request_id'])
        work.set_work_name_to_coll_map(work_name_to_coll_map)

        # link processings
        new_processing_model, processing_model = None, None
        ret_processing_id = None

        processing = work.get_processing(input_output_maps=[], without_creating=True)
        self.logger.debug(log_pre + "work get_processing: %s" % processing)
        if processing and processing.processing_id:
            ret_processing_id = processing.processing_id
            processing_model = core_processings.get_processing(processing_id=processing.processing_id)
            work.sync_processing(processing, processing_model)
            proc = processing_model['processing_metadata']['processing']
            work.sync_work_data(status=processing_model['status'], substatus=processing_model['substatus'],
                                work=proc.work, output_data=processing_model['output_metadata'], processing=proc)
            # processing_metadata = processing_model['processing_metadata']
            if processing_model['errors']:
                work.set_terminated_msg(processing_model['errors'])
            # work.set_processing_output_metadata(processing, processing_model['output_metadata'])
            work.set_output_data(processing.output_data)
            transform['workload_id'] = processing_model['workload_id']
        else:
            if not processing:
                processing = work.get_processing(input_output_maps=[], without_creating=False)
                self.logger.debug(log_pre + "work get_processing with creating: %s" % processing)
            new_processing_model = self.generate_processing_model(transform)

            proc_work = copy.deepcopy(work)
            proc_work.clean_work()
            processing.work = proc_work
            new_processing_model['processing_metadata'] = {'processing': processing}

        self.logger.info(log_pre + "syn_work_status: %s, transform status: %s" % (transform['transform_id'], transform['status']))
        if work.is_terminated():
            is_terminated = True
            self.logger.info(log_pre + "Transform(%s) work is terminated: work status: %s" % (transform['transform_id'], work.get_status()))
            if work.is_finished():
                transform['status'] = TransformStatus.Finished
            else:
                if to_abort:
                    transform['status'] = TransformStatus.Cancelled
                elif work.is_subfinished():
                    transform['status'] = TransformStatus.SubFinished
                elif work.is_failed():
                    transform['status'] = TransformStatus.Failed
                else:
                    transform['status'] = TransformStatus.Failed

        transform_parameters = {'status': transform['status'],
                                'locking': TransformLocking.Idle,
                                'workload_id': transform['workload_id'],
                                'transform_metadata': transform['transform_metadata']}
        transform_parameters = self.load_poll_period(transform, transform_parameters)

        if new_processing_model is not None:
            if 'new_poll_period' in transform_parameters:
                new_processing_model['new_poll_period'] = transform_parameters['new_poll_period']
            if 'update_poll_period' in transform_parameters:
                new_processing_model['update_poll_period'] = transform_parameters['update_poll_period']
            if 'max_new_retries' in transform_parameters:
                new_processing_model['max_new_retries'] = transform_parameters['max_new_retries']
            if 'max_update_retries' in transform_parameters:
                new_processing_model['max_update_retries'] = transform_parameters['max_update_retries']

        ret = {'transform': transform,
               'transform_parameters': transform_parameters,
               'new_processing': new_processing_model}
        return ret, is_terminated, ret_processing_id

    def handle_update_transform(self, transform, event):
        """
        Process running transform
        """
        try:
            log_pre = self.get_log_prefix(transform)

            self.logger.info(log_pre + "handle_update_transform: %s" % transform)
            ret, is_terminated, ret_processing_id = self.handle_update_transform_real(transform, event)
            self.logger.info(log_pre + "handle_update_transform result: %s" % str(ret))
            return ret, is_terminated, ret_processing_id
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())

            retries = transform['update_retries'] + 1
            if not transform['max_update_retries'] or retries < transform['max_update_retries']:
                tf_status = transform['status']
            else:
                tf_status = TransformStatus.Failed
            error = {'submit_err': {'msg': truncate_string('%s' % (ex), length=200)}}

            # increase poll period
            update_poll_period = int(transform['update_poll_period'].total_seconds() * self.poll_period_increase_rate)
            if update_poll_period > self.max_update_poll_period:
                update_poll_period = self.max_update_poll_period

            transform_parameters = {'status': tf_status,
                                    'update_retries': retries,
                                    'update_poll_period': update_poll_period,
                                    'errors': transform['errors'] if transform['errors'] else {},
                                    'locking': TransformLocking.Idle}
            transform_parameters['errors'].update(error)

            ret = {'transform': transform, 'transform_parameters': transform_parameters}
            self.logger.warn(log_pre + "handle_update_transform exception result: %s" % str(ret))
        return ret, False, None

    def handle_update_itransform_real(self, transform, event):
        """
        process running transforms
        """
        log_pre = self.get_log_prefix(transform)

        self.logger.info(log_pre + "handle_update_itransform: transform_id: %s" % transform['transform_id'])

        # work = transform['transform_metadata']['work']

        prs = core_processings.get_processings(transform_id=transform['transform_id'])
        pr = None
        for pr in prs:
            if pr['processing_id'] == transform['current_processing_id']:
                transform['workload_id'] = pr['workload_id']
                break

        errors = None
        if pr:
            transform['status'] = get_transform_status_from_processing_status(pr['status'])
            log_msg = log_pre + "transform id: %s, transform status: %s" % (transform['transform_id'], transform['status'])
            log_msg = log_msg + ", processing id: %s, processing status: %s" % (pr['processing_id'], pr['status'])
            self.logger.info(log_msg)
        else:
            transform['status'] = TransformStatus.Failed
            log_msg = log_pre + "transform id: %s, transform status: %s" % (transform['transform_id'], transform['status'])
            log_msg = log_msg + ", no attached processings."
            self.logger.error(log_msg)
            errors = {'submit_err': 'no attached processings'}

        is_terminated = False
        if transform['status'] in [TransformStatus.Finished, TransformStatus.Failed, TransformStatus.Cancelled,
                                   TransformStatus.SubFinished, TransformStatus.Suspended, TransformStatus.Expired]:
            is_terminated = True

        transform_parameters = {'status': transform['status'],
                                'locking': TransformLocking.Idle,
                                'workload_id': transform['workload_id']}
        transform_parameters = self.load_poll_period(transform, transform_parameters)
        if errors:
            transform_parameters['errors'] = errors

        ret = {'transform': transform,
               'transform_parameters': transform_parameters}
        return ret, is_terminated, None

    def handle_update_itransform(self, transform, event):
        """
        Process running transform
        """
        try:
            log_pre = self.get_log_prefix(transform)

            self.logger.info(log_pre + "handle_update_itransform: %s" % transform)
            ret, is_terminated, ret_processing_id = self.handle_update_itransform_real(transform, event)
            self.logger.info(log_pre + "handle_update_itransform result: %s" % str(ret))
            return ret, is_terminated, ret_processing_id
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())

            retries = transform['update_retries'] + 1
            if not transform['max_update_retries'] or retries < transform['max_update_retries']:
                tf_status = transform['status']
            else:
                tf_status = TransformStatus.Failed
            error = {'submit_err': {'msg': truncate_string('%s' % (ex), length=200)}}

            # increase poll period
            update_poll_period = int(transform['update_poll_period'].total_seconds() * self.poll_period_increase_rate)
            if update_poll_period > self.max_update_poll_period:
                update_poll_period = self.max_update_poll_period

            transform_parameters = {'status': tf_status,
                                    'update_retries': retries,
                                    'update_poll_period': update_poll_period,
                                    'errors': transform['errors'] if transform['errors'] else {},
                                    'locking': TransformLocking.Idle}
            transform_parameters['errors'].update(error)

            ret = {'transform': transform, 'transform_parameters': transform_parameters}
            self.logger.warn(log_pre + "handle_update_itransform exception result: %s" % str(ret))
        return ret, False, None

    def process_update_transform(self, event):
        self.number_workers += 1
        pro_ret = ReturnCode.Ok.value
        try:
            if event:
                # tf_status = [TransformStatus.Transforming,
                #              TransformStatus.ToCancel, TransformStatus.Cancelling,
                #              TransformStatus.ToSuspend, TransformStatus.Suspending,
                #              TransformStatus.ToExpire, TransformStatus.Expiring,
                #              TransformStatus.ToResume, TransformStatus.Resuming,
                #              TransformStatus.ToFinish, TransformStatus.ToForceFinish]
                # tf = self.get_transform(transform_id=event._transform_id, status=tf_status, locking=True)
                tf = self.get_transform(transform_id=event._transform_id, locking=True)
                if not tf:
                    self.logger.error("Cannot find transform for event: %s" % str(event))
                    pro_ret = ReturnCode.Locked.value
                else:
                    log_pre = self.get_log_prefix(tf)

                    if tf['transform_type'] in [TransformType.iWorkflow, TransformType.iWork]:
                        ret, is_terminated, ret_processing_id = self.handle_update_itransform(tf, event)
                    else:
                        ret, is_terminated, ret_processing_id = self.handle_update_transform(tf, event)
                    new_pr_ids, update_pr_ids = self.update_transform(ret)

                    if is_terminated or (event._content and 'event' in event._content and event._content['event'] == 'submitted'):
                        self.logger.info(log_pre + "UpdateRequestEvent(request_id: %s)" % tf['request_id'])
                        event = UpdateRequestEvent(publisher_id=self.id, request_id=tf['request_id'], content=event._content)
                        self.event_bus.send(event)
                    for pr_id in new_pr_ids:
                        self.logger.info(log_pre + "NewProcessingEvent(processing_id: %s)" % pr_id)
                        event = NewProcessingEvent(publisher_id=self.id, processing_id=pr_id, content=event._content)
                        self.event_bus.send(event)
                    for pr_id in update_pr_ids:
                        self.logger.info(log_pre + "UpdateProcessingEvent(processing_id: %s)" % pr_id)
                        event = UpdateProcessingEvent(publisher_id=self.id, processing_id=pr_id, content=event._content)
                        self.event_bus.send(event)
                    if ret_processing_id and (event._content and 'event' in event._content and event._content['event'] == 'Trigger'):
                        self.logger.info(log_pre + "UpdateProcessingEvent(processing_id: %s)" % ret_processing_id)
                        event = UpdateProcessingEvent(publisher_id=self.id, processing_id=ret_processing_id)
                        self.event_bus.send(event)
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            pro_ret = ReturnCode.Failed.value
        self.number_workers -= 1
        return pro_ret

    def handle_abort_transform(self, transform):
        """
        process abort transform
        """
        try:
            work = transform['transform_metadata']['work']
            work.set_agent_attributes(self.agent_attributes, transform)

            # save old status for retry
            oldstatus = transform['status']

            processing = work.get_processing(input_output_maps=[], without_creating=True)
            if processing and processing.processing_id:
                tf_status = TransformStatus.Cancelling
            else:
                tf_status = TransformStatus.Cancelled

            transform_parameters = {'status': tf_status,
                                    'oldstatus': oldstatus,
                                    'locking': TransformLocking.Idle,
                                    'transform_metadata': transform['transform_metadata']}
            ret = {'transform': transform, 'transform_parameters': transform_parameters}
            return ret
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            error = {'abort_err': {'msg': truncate_string('%s' % (ex), length=200)}}
            transform_parameters = {'status': tf_status,
                                    'locking': TransformLocking.Idle,
                                    'errors': transform['errors'] if transform['errors'] else {}}
            transform_parameters['errors'].update(error)
            ret = {'transform': transform, 'transform_parameters': transform_parameters}
            return ret
        return None

    def process_abort_transform(self, event):
        self.number_workers += 1
        pro_ret = ReturnCode.Ok.value
        try:
            if event:
                self.logger.info("process_abort_transform: event: %s" % event)
                tf = self.get_transform(transform_id=event._transform_id, locking=True)
                if not tf:
                    self.logger.error("Cannot find transform for event: %s" % str(event))
                    pro_ret = ReturnCode.Locked.value
                else:
                    log_pre = self.get_log_prefix(tf)
                    self.logger.info(log_pre + "process_abort_transform")

                    if tf['status'] in [TransformStatus.Finished, TransformStatus.SubFinished,
                                        TransformStatus.Failed, TransformStatus.Cancelled,
                                        TransformStatus.Suspended, TransformStatus.Expired]:
                        ret = {'transform': tf,
                               'transform_parameters': {'locking': TransformLocking.Idle,
                                                        'errors': {'extra_msg': "Transform is already terminated. Cannot be aborted"}}}
                        if tf['errors'] and 'msg' in tf['errors']:
                            ret['parameters']['errors']['msg'] = tf['errors']['msg']

                        self.logger.info(log_pre + "process_abort_transform result: %s" % str(ret))

                        self.update_transform(ret)
                    else:
                        ret = self.handle_abort_transform(tf)
                        self.logger.info(log_pre + "process_abort_transform result: %s" % str(ret))
                        if ret:
                            self.update_transform(ret)

                        work = tf['transform_metadata']['work']
                        work.set_work_id(tf['transform_id'])
                        work.set_agent_attributes(self.agent_attributes, tf)

                        processing = work.get_processing(input_output_maps=[], without_creating=True)
                        if processing and processing.processing_id:
                            self.logger.info(log_pre + "AbortProcessingEvent(processing_id: %s)" % processing.processing_id)
                            event = AbortProcessingEvent(publisher_id=self.id, processing_id=processing.processing_id, content=event._content)
                            self.event_bus.send(event)
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            pro_ret = ReturnCode.Failed.value
        self.number_workers -= 1
        return pro_ret

    def handle_resume_transform(self, transform):
        """
        process resume transform
        """
        try:
            work = transform['transform_metadata']['work']
            work.set_agent_attributes(self.agent_attributes, transform)

            tf_status = transform['oldstatus']

            transform_parameters = {'status': tf_status,
                                    'locking': TransformLocking.Idle}

            ret = {'transform': transform,
                   'transform_parameters': transform_parameters}
            return ret
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            error = {'resume_err': {'msg': truncate_string('%s' % (ex), length=200)}}
            transform_parameters = {'status': tf_status,
                                    'locking': TransformLocking.Idle,
                                    'errors': transform['errors'] if transform['errors'] else {}}
            transform_parameters['errors'].update(error)
            ret = {'transform': transform, 'transform_parameters': transform_parameters}
            return ret
        return None

    def process_resume_transform(self, event):
        self.number_workers += 1
        pro_ret = ReturnCode.Ok.value
        try:
            if event:
                self.logger.info("process_resume_transform: event: %s" % event)
                tf = self.get_transform(transform_id=event._transform_id, locking=True)
                if not tf:
                    self.logger.error("Cannot find transform for event: %s" % str(event))
                    pro_ret = ReturnCode.Locked.value
                else:
                    log_pre = self.get_log_prefix(tf)

                    if tf['status'] in [TransformStatus.Finished]:
                        ret = {'transform': tf,
                               'transform_parameters': {'locking': TransformLocking.Idle,
                                                        'errors': {'extra_msg': "Transform is already finished. Cannot be resumed"}}}
                        if tf['errors'] and 'msg' in tf['errors']:
                            ret['parameters']['errors']['msg'] = tf['errors']['msg']

                        self.logger.info(log_pre + "process_resume_transform result: %s" % str(ret))
                        self.update_transform(ret)
                    else:
                        ret = self.handle_resume_transform(tf)
                        self.logger.info(log_pre + "process_resume_transform result: %s" % str(ret))
                        if ret:
                            self.update_transform(ret)

                        work = tf['transform_metadata']['work']
                        work.set_agent_attributes(self.agent_attributes, tf)

                        processing = work.get_processing(input_output_maps=[], without_creating=True)
                        if processing and processing.processing_id:
                            self.logger.info(log_pre + "ResumeProcessingEvent(processing_id: %s)" % processing.processing_id)
                            event = ResumeProcessingEvent(publisher_id=self.id,
                                                          processing_id=processing.processing_id,
                                                          content=event._content)
                            self.event_bus.send(event)
                        else:
                            self.logger.info(log_pre + "UpdateTransformEvent(transform_id: %s)" % tf['transform_id'])
                            event = UpdateTransformEvent(publisher_id=self.id,
                                                         transform_id=tf['transform_id'],
                                                         content=event._content)
                            self.event_bus.send(event)
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            pro_ret = ReturnCode.Failed.value
        self.number_workers -= 1
        return pro_ret

    def clean_locks(self):
        self.logger.info("clean locking")
        core_transforms.clean_locking()

    def init_event_function_map(self):
        self.event_func_map = {
            EventType.NewTransform: {
                'pre_check': self.is_ok_to_run_more_transforms,
                'exec_func': self.process_new_transform
            },
            EventType.UpdateTransform: {
                'pre_check': self.is_ok_to_run_more_transforms,
                'exec_func': self.process_update_transform
            },
            EventType.AbortTransform: {
                'pre_check': self.is_ok_to_run_more_transforms,
                'exec_func': self.process_abort_transform
            },
            EventType.ResumeTransform: {
                'pre_check': self.is_ok_to_run_more_transforms,
                'exec_func': self.process_resume_transform
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

            self.add_default_tasks()

            self.init_event_function_map()

            task = self.create_task(task_func=self.get_new_transforms, task_output_queue=None, task_args=tuple(), task_kwargs={}, delay_time=10, priority=1)
            self.add_task(task)
            task = self.create_task(task_func=self.get_running_transforms, task_output_queue=None, task_args=tuple(), task_kwargs={}, delay_time=10, priority=1)
            self.add_task(task)
            task = self.create_task(task_func=self.clean_locks, task_output_queue=None, task_args=tuple(), task_kwargs={}, delay_time=1800, priority=1)
            self.add_task(task)

            self.execute()
        except KeyboardInterrupt:
            self.stop()


if __name__ == '__main__':
    agent = Transformer()
    agent()
