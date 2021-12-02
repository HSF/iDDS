#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2020 - 2021


import json
import os
import traceback

# from rucio.client.client import Client as RucioClient
from rucio.common.exception import (CannotAuthenticate as RucioCannotAuthenticate)

from idds.common import exceptions
from idds.common.constants import (ContentStatus,
                                   ProcessingStatus, WorkStatus)
from idds.common.utils import extract_scope_atlas
# from idds.workflowv2.work import Work, Processing
# from idds.workflowv2.workflow import Condition
from .atlaspandawork import ATLASPandaWork


class ATLASLocalPandaWork(ATLASPandaWork):
    def __init__(self, task_parameters=None,
                 work_tag='atlas', exec_type='panda', work_id=None,
                 primary_input_collection=None, other_input_collections=None,
                 input_collections=None,
                 primary_output_collection=None, other_output_collections=None,
                 output_collections=None, log_collections=None,
                 logger=None,
                 num_retries=5,
                 ):

        super(ATLASLocalPandaWork, self).__init__(task_parameters=task_parameters,
                                                  work_tag=work_tag,
                                                  exec_type=exec_type,
                                                  work_id=work_id,
                                                  primary_input_collection=primary_input_collection,
                                                  other_input_collections=other_input_collections,
                                                  input_collections=input_collections,
                                                  primary_output_collection=primary_output_collection,
                                                  other_output_collections=other_output_collections,
                                                  output_collections=output_collections,
                                                  log_collections=log_collections,
                                                  logger=logger,
                                                  num_retries=num_retries)
        self.work_dir = "/tmp"
        self.output_files = []

    def set_agent_attributes(self, attrs, req_attributes=None):
        if self.class_name not in attrs or 'life_time' not in attrs[self.class_name] or int(attrs[self.class_name]['life_time']) <= 0:
            attrs['life_time'] = None
        super(ATLASLocalPandaWork, self).set_agent_attributes(attrs)
        if self.agent_attributes and 'num_retries' in self.agent_attributes and self.agent_attributes['num_retries']:
            self.num_retries = int(self.agent_attributes['num_retries'])
        if self.agent_attributes and 'work_dir' in self.agent_attributes and self.agent_attributes['work_dir']:
            self.work_dir = int(self.agent_attributes['work_dir'])

    def parse_task_parameters(self, task_parameters):
        super(ATLASLocalPandaWork, self).parse_task_parameters(task_parameters)

        try:

            if 'jobParameters' in self.task_parameters:
                jobParameters = self.task_parameters['jobParameters']
                for jobP in jobParameters:
                    if type(jobP) in [dict]:
                        if 'dataset' in jobP and 'param_type' in jobP:
                            if jobP['param_type'] == 'output' and 'value' in jobP:
                                output_c = jobP['dataset']
                                scope, name = extract_scope_atlas(output_c, scopes=[])
                                # output_coll = {'scope': scope, 'name': name}
                                output_f = jobP['value']
                                output_file = {'scope': scope, 'name': name, 'file': output_f}
                                self.output_files.append(output_file)
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            # raise exceptions.IDDSException('%s: %s' % (str(ex), traceback.format_exc()))
            self.add_errors(str(ex))

    def renew_parameters_from_attributes(self):
        if not self.task_parameters:
            return

        try:
            if 'taskName' in self.task_parameters:
                self.task_name = self.task_parameters['taskName']
                self.task_name = self.renew_parameter(self.task_name)
                self.set_work_name(self.task_name)

            if 'prodSourceLabel' in self.task_parameters:
                self.task_type = self.task_parameters['prodSourceLabel']

            if 'jobParameters' in self.task_parameters:
                jobParameters = self.task_parameters['jobParameters']
                for jobP in jobParameters:
                    if type(jobP) in [dict]:
                        for key in jobP:
                            if jobP[key] and type(jobP[key]) in [str]:
                                jobP[key] = self.renew_parameter(jobP[key])
            for coll_id in self.collections:
                coll_name = self.collections[coll_id].name
                self.collections[coll_id].name = self.renew_parameter(coll_name)

            output_files = self.output_files
            self.output_files = []
            for output_file in output_files:
                output_file['name'] = self.renew_parameter(output_file['name'])
                output_file['file'] = self.renew_parameter(output_file['file'])
                self.output_files.append(output_file)
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            # raise exceptions.IDDSException('%s: %s' % (str(ex), traceback.format_exc()))
            self.add_errors(str(ex))

    def set_output_data(self, data):
        self.output_data = data
        if type(data) in [dict]:
            for key in data:
                new_key = "user_" + str(key)
                setattr(self, new_key, data[key])

    def ping_output_files(self):
        try:
            rucio_client = self.get_rucio_client()
            for output_i in range(len(self.output_files)):
                output_file = self.output_files[output_i]
                f_parts = output_file['file'].split(".")
                pos = output_file['file'].find("$")
                begin_part = output_file['file'][:pos]
                i = 0
                for i in range(len(f_parts) - 1, -1, -1):
                    if "$" in f_parts[i]:
                        break
                end_parts = f_parts[i + 1:]
                end_part = '.'.join(end_parts)

                files = rucio_client.list_files(scope=output_file['scope'],
                                                name=output_file['name'])
                for f in files:
                    if ((not begin_part) or f['name'].startswith(begin_part)) and ((not end_part) or f['name'].endswith(end_part)):
                        self.output_files[output_i]['lfn'] = f['name']
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            raise exceptions.IDDSException('%s: %s' % (str(ex), traceback.format_exc()))

    def download_output_file_rucio(self, file_items):
        try:
            client = self.get_rucio_download_client()
            outputs = client.download_dids(file_items)
            ret = {}
            for output in outputs:
                if 'dest_file_paths' in output and output['dest_file_paths']:
                    ret[output['name']] = output['dest_file_paths'][0]
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            raise exceptions.IDDSException('%s: %s' % (str(ex), traceback.format_exc()))

    def download_output_files(self, processing):
        try:
            file_items = []
            for output_i in range(len(self.output_files)):
                if 'lfn' in self.output_files[output_i]:
                    self.logger.debug("download_output_files, Processing (%s) lfn for %s: %s" % (processing['processing_id'],
                                                                                                 self.output_files[output_i]['file'],
                                                                                                 self.output_files[output_i]['lfn']))
                    file_items.append("%s:%s" % (self.output_files[output_i]['scope'], self.output_files[output_i]['lfn']))
                else:
                    self.logger.warn("download_output_files, Processing (%s) lfn for %s not found" % (processing['processing_id'],
                                                                                                      self.output_files[output_i]['file']))

            pfn_items = self.download_output_files_rucio(file_items)
            for output_i in range(len(self.output_files)):
                if 'lfn' in self.output_files[output_i]:
                    pfn = pfn_items[self.output_files[output_i]['lfn']]
                    self.output_files[output_i]['pfn'] = pfn
                    self.logger.info("download_output_files, Processing (%s) pfn for %s: %s" % (processing['processing_id'],
                                                                                                self.output_files[output_i]['file'],
                                                                                                self.output_files[output_i]['pfn']))
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            raise exceptions.IDDSException('%s: %s' % (str(ex), traceback.format_exc()))

    def parse_output_file(self, pfn):
        try:
            if not os.path.exists(pfn):
                self.logger.warn("%s doesn't exist" % pfn)
                return {}
            else:
                with open(pfn, 'r') as f:
                    data = f.read()
                outputs = json.loads(data)
                return outputs
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            raise exceptions.IDDSException('%s: %s' % (str(ex), traceback.format_exc()))
        return {}

    def parse_output_files(self, processing):
        try:
            output_data = {}
            for output_i in range(len(self.output_files)):
                if 'pfn' in self.output_files[output_i]:
                    data = self.parse_output_file(self.output_files[output_i]['pfn'])
                    if type(data) in [dict]:
                        output_data.update(data)
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            raise exceptions.IDDSException('%s: %s' % (str(ex), traceback.format_exc()))

    def process_outputs(self, processing):
        self.download_output_files(processing)
        output_data = self.parse_output_files(processing)
        return output_data

    def get_rucio_download_client(self):
        try:
            from rucio.client.downloadclient import DownloadClient
            client = DownloadClient()
        except RucioCannotAuthenticate as error:
            self.logger.error(error)
            self.logger.error(traceback.format_exc())
            raise exceptions.IDDSException('%s: %s' % (str(error), traceback.format_exc()))
        return client

    def poll_panda_task_output(self, processing=None, input_output_maps=None):
        task_id = None
        try:
            from pandatools import Client

            if processing:
                output_metadata = {}
                proc = processing['processing_metadata']['processing']
                task_id = proc.workload_id
                if task_id is None:
                    task_id = self.get_panda_task_id(processing)

                if task_id:
                    # ret_ids = Client.getPandaIDsWithTaskID(task_id, verbose=False)
                    task_info = Client.getJediTaskDetails({'jediTaskID': task_id}, True, True, verbose=False)
                    self.logger.info("poll_panda_task, task_info: %s" % str(task_info))
                    if task_info[0] != 0:
                        self.logger.warn("poll_panda_task %s, error getting task status, task_info: %s" % (task_id, str(task_info)))
                        return ProcessingStatus.Submitting, [], {}

                    task_info = task_info[1]

                    processing_status = self.get_processing_status_from_panda_status(task_info["status"])

                    if processing_status in [ProcessingStatus.SubFinished]:
                        if self.retry_number < self.num_retries:
                            self.reactivate_processing(processing)
                            processing_status = ProcessingStatus.Submitted
                            self.retry_number += 1
                    if processing_status in [ProcessingStatus.SubFinished, ProcessingStatus.Finished]:
                        output_metdata = self.process_outputs(processing)

                    return processing_status, [], {}, output_metdata
                else:
                    return ProcessingStatus.Failed, [], {}, output_metadata
        except Exception as ex:
            msg = "Failed to check the processing (%s) status: %s" % (str(processing['processing_id']), str(ex))
            self.logger.error(msg)
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            # raise exceptions.IDDSException(msg)
        return ProcessingStatus.Submitting, [], {}, {}

    def poll_processing_updates(self, processing, input_output_maps):
        """
        *** Function called by Carrier agent.
        """
        updated_contents = []
        update_processing = {}
        reset_expired_at = False
        reactive_contents = []
        # self.logger.debug("poll_processing_updates, input_output_maps: %s" % str(input_output_maps))

        if processing:
            proc = processing['processing_metadata']['processing']
            if proc.tocancel:
                self.logger.info("Cancelling processing (processing id: %s, jediTaskId: %s)" % (processing['processing_id'], proc.workload_id))
                self.kill_processing_force(processing)
                # self.kill_processing(processing)
                proc.tocancel = False
                proc.polling_retries = 0
            elif proc.tosuspend:
                self.logger.info("Suspending processing (processing id: %s, jediTaskId: %s)" % (processing['processing_id'], proc.workload_id))
                self.kill_processing_force(processing)
                # self.kill_processing(processing)
                proc.tosuspend = False
                proc.polling_retries = 0
            elif proc.toresume:
                self.logger.info("Resuming processing (processing id: %s, jediTaskId: %s)" % (processing['processing_id'], proc.workload_id))
                self.reactivate_processing(processing)
                reset_expired_at = True
                proc.toresume = False
                proc.polling_retries = 0
                proc.has_new_updates()
                # reactive_contents = self.reactive_contents(input_output_maps)
            # elif self.is_processing_expired(processing):
            elif proc.toexpire:
                self.logger.info("Expiring processing (processing id: %s, jediTaskId: %s)" % (processing['processing_id'], proc.workload_id))
                self.kill_processing(processing)
                proc.toexpire = False
                proc.polling_retries = 0
            elif proc.tofinish or proc.toforcefinish:
                self.logger.info("Finishing processing (processing id: %s, jediTaskId: %s)" % (processing['processing_id'], proc.workload_id))
                self.kill_processing(processing)
                proc.tofinish = False
                proc.toforcefinish = False
                proc.polling_retries = 0

            processing_status, poll_updated_contents, new_input_output_maps, output_metadata = self.poll_panda_task_output(processing=processing, input_output_maps=input_output_maps)
            self.logger.debug("poll_processing_updates, processing_status: %s" % str(processing_status))
            self.logger.debug("poll_processing_updates, update_contents: %s" % str(poll_updated_contents))
            self.logger.debug("poll_processing_updates, output_metadata: %s" % str(output_metadata))

            if poll_updated_contents:
                proc.has_new_updates()
            for content in poll_updated_contents:
                updated_content = {'content_id': content['content_id'],
                                   'substatus': content['substatus'],
                                   'content_metadata': content['content_metadata']}
                updated_contents.append(updated_content)

            content_substatus = {'finished': 0, 'unfinished': 0}
            for map_id in input_output_maps:
                outputs = input_output_maps[map_id]['outputs']
                for content in outputs:
                    if content.get('substatus', ContentStatus.New) != ContentStatus.Available:
                        content_substatus['unfinished'] += 1
                    else:
                        content_substatus['finished'] += 1

            if processing_status in [ProcessingStatus.SubFinished, ProcessingStatus.Finished, ProcessingStatus.Failed] and updated_contents:
                self.logger.info("Processing %s is terminated, but there are still contents to be flushed. Waiting." % (proc.workload_id))
                # there are still polling contents, should not terminate the task.
                processing_status = ProcessingStatus.Running

            if processing_status in [ProcessingStatus.SubFinished] and content_substatus['finished'] > 0 and content_substatus['unfinished'] == 0:
                # found that a 'done' panda task has got a 'finished' status. Maybe in this case 'finished' is a transparent status.
                if proc.polling_retries is None:
                    proc.polling_retries = 0

            if processing_status in [ProcessingStatus.SubFinished, ProcessingStatus.Finished, ProcessingStatus.Failed]:
                if proc.polling_retries is not None and proc.polling_retries < 3:
                    self.logger.info("processing %s polling_retries(%s) < 3, keep running" % (processing['processing_id'], proc.polling_retries))
                    processing_status = ProcessingStatus.Running
                    proc.polling_retries += 1
            else:
                proc.polling_retries = 0

            if proc.in_operation_time():
                processing_status = ProcessingStatus.Running

            update_processing = {'processing_id': processing['processing_id'],
                                 'parameters': {'status': processing_status,
                                                'output_metadata': output_metadata}}

            if reset_expired_at:
                processing['expired_at'] = None
                update_processing['parameters']['expired_at'] = None
                proc.polling_retries = 0
                # if (processing_status in [ProcessingStatus.SubFinished, ProcessingStatus.Finished, ProcessingStatus.Failed]
                #     or processing['status'] in [ProcessingStatus.Resuming]):   # noqa W503
                # using polling_retries to poll it again when panda may update the status in a delay(when issuing retryTask, panda will not update it without any delay).
                update_processing['parameters']['status'] = ProcessingStatus.Resuming
            proc.status = update_processing['parameters']['status']

        self.logger.debug("poll_processing_updates, task: %s, update_processing: %s" %
                          (proc.workload_id, str(update_processing)))
        self.logger.debug("poll_processing_updates, task: %s, updated_contents: %s" %
                          (proc.workload_id, str(updated_contents)))
        self.logger.debug("poll_processing_updates, task: %s, reactive_contents: %s" %
                          (proc.workload_id, str(reactive_contents)))
        return update_processing, updated_contents + reactive_contents, new_input_output_maps

    def syn_work_status(self, registered_input_output_maps, all_updates_flushed=True, output_statistics={}, to_release_input_contents=[]):
        super(ATLASLocalPandaWork, self).syn_work_status(registered_input_output_maps, all_updates_flushed, output_statistics, to_release_input_contents)
        # self.get_status_statistics(registered_input_output_maps)
        self.status_statistics = output_statistics

        self.logger.debug("syn_work_status, self.active_processings: %s" % str(self.active_processings))
        self.logger.debug("syn_work_status, self.has_new_inputs(): %s" % str(self.has_new_inputs))
        self.logger.debug("syn_work_status, coll_metadata_is_open: %s" %
                          str(self.collections[self._primary_input_collection].coll_metadata['is_open']))
        self.logger.debug("syn_work_status, primary_input_collection_status: %s" %
                          str(self.collections[self._primary_input_collection].status))

        self.logger.debug("syn_work_status(%s): is_processings_terminated: %s" % (str(self.get_processing_ids()), str(self.is_processings_terminated())))
        self.logger.debug("syn_work_status(%s): is_input_collections_closed: %s" % (str(self.get_processing_ids()), str(self.is_input_collections_closed())))
        self.logger.debug("syn_work_status(%s): has_new_inputs: %s" % (str(self.get_processing_ids()), str(self.has_new_inputs)))
        self.logger.debug("syn_work_status(%s): has_to_release_inputs: %s" % (str(self.get_processing_ids()), str(self.has_to_release_inputs())))
        self.logger.debug("syn_work_status(%s): to_release_input_contents: %s" % (str(self.get_processing_ids()), str(to_release_input_contents)))

        if self.is_processings_terminated() and self.is_input_collections_closed() and not self.has_new_inputs and not self.has_to_release_inputs() and not to_release_input_contents:
            # if not self.is_all_outputs_flushed(registered_input_output_maps):
            if not all_updates_flushed:
                self.logger.warn("The work processings %s is terminated. but not all outputs are flushed. Wait to flush the outputs then finish the transform" % str(self.get_processing_ids()))
                return

            if self.is_processings_finished():
                self.status = WorkStatus.Finished
            elif self.is_processings_subfinished():
                self.status = WorkStatus.SubFinished
            elif self.is_processings_failed():
                self.status = WorkStatus.Failed
            elif self.is_processings_expired():
                self.status = WorkStatus.Expired
            elif self.is_processings_cancelled():
                self.status = WorkStatus.Cancelled
            elif self.is_processings_suspended():
                self.status = WorkStatus.Suspended
        elif self.is_processings_running():
            self.status = WorkStatus.Running
        else:
            self.status = WorkStatus.Transforming

        if self.is_processings_terminated() or self.is_processings_running() or self.is_processings_started():
            self.started = True