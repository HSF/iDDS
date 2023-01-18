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
import re
import traceback

# from rucio.client.client import Client as RucioClient
from rucio.common.exception import (CannotAuthenticate as RucioCannotAuthenticate)

from idds.common import exceptions
from idds.common.constants import (ProcessingStatus, WorkStatus)
from idds.common.utils import extract_scope_atlas
# from idds.workflow.work import Work, Processing
# from idds.workflow.workflow import Condition
from .atlaspandawork import ATLASPandaWork


class ATLASLocalPandaWork(ATLASPandaWork):
    def __init__(self, task_parameters=None,
                 work_tag='atlaslocalpanda', exec_type='panda', work_id=None,
                 primary_input_collection=None, other_input_collections=None,
                 input_collections=None,
                 primary_output_collection=None, other_output_collections=None,
                 output_collections=None, log_collections=None,
                 logger=None,
                 num_retries=5,
                 ):

        self.work_dir = "/tmp"
        self.output_files = []

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

    def set_agent_attributes(self, attrs, req_attributes=None):
        if self.class_name not in attrs or 'life_time' not in attrs[self.class_name] or int(attrs[self.class_name]['life_time']) <= 0:
            attrs['life_time'] = None
        super(ATLASLocalPandaWork, self).set_agent_attributes(attrs)
        if self.agent_attributes and 'num_retries' in self.agent_attributes and self.agent_attributes['num_retries']:
            self.num_retries = int(self.agent_attributes['num_retries'])
        if self.agent_attributes and 'work_dir' in self.agent_attributes and self.agent_attributes['work_dir']:
            self.work_dir = self.agent_attributes['work_dir']

    def parse_task_parameters(self, task_parameters):
        super(ATLASLocalPandaWork, self).parse_task_parameters(task_parameters)

        try:
            if self.task_parameters and 'jobParameters' in self.task_parameters:
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
        super(ATLASLocalPandaWork, self).renew_parameters_from_attributes()
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
        if data and type(data) in [dict]:
            for key in data:
                new_key = "user_" + str(key)
                setattr(self, new_key, data[key])

    def match_pattern_file(self, pattern, lfn):
        pattern1 = "\\$[_a-zA-Z0-9]+"
        pattern2 = "\\$\\{[_a-zA-Z0-9\\/]+\\}"
        while True:
            m = re.search(pattern1, pattern)
            if m:
                pattern = pattern.replace(m.group(0), "*")
            else:
                break
        while True:
            m = re.search(pattern2, pattern)
            if m:
                pattern = pattern.replace(m.group(0), "*")
            else:
                break

        pattern = pattern.replace(".", "\\.")
        pattern = pattern.replace("*", ".*")
        pattern = pattern + "$"

        m = re.search(pattern, lfn)
        if m:
            return True
        return False

    def ping_output_files(self):
        try:
            rucio_client = self.get_rucio_client()
            for output_i in range(len(self.output_files)):
                output_file = self.output_files[output_i]
                files = rucio_client.list_files(scope=output_file['scope'],
                                                name=output_file['name'])
                files = [f for f in files]
                self.logger.debug("ping_output_files found files for dataset(%s:%s): %s" % (output_file['scope'],
                                                                                            output_file['name'],
                                                                                            str([f['name'] for f in files])))
                for f in files:
                    if self.match_pattern_file(output_file['file'], f['name']):
                        self.output_files[output_i]['lfn'] = f['name']
            return True
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            # raise exceptions.IDDSException('%s: %s' % (str(ex), traceback.format_exc()))
        return False

    def download_output_files_rucio(self, file_items):
        try:
            ret = {}
            self.logger.debug("download_output_files_rucio: %s" % str(file_items))
            if file_items:
                client = self.get_rucio_download_client()
                outputs = client.download_dids(file_items)
                for output in outputs:
                    if 'dest_file_paths' in output and output['dest_file_paths']:
                        ret[output['name']] = output['dest_file_paths'][0]
            return ret
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            # raise exceptions.IDDSException('%s: %s' % (str(ex), traceback.format_exc()))
        return {}

    def get_download_dir(self, processing):
        req_dir = 'request_%s_%s/transform_%s' % (processing['request_id'],
                                                  processing['workload_id'],
                                                  processing['transform_id'])
        d_dir = os.path.join(self.work_dir, req_dir)
        if not os.path.exists(d_dir):
            os.makedirs(d_dir)
        return d_dir

    def download_output_files(self, processing):
        try:
            failed_items = []
            file_items = []
            for output_i in range(len(self.output_files)):
                if 'lfn' in self.output_files[output_i]:
                    self.logger.debug("download_output_files, Processing (%s) lfn for %s: %s" % (processing['processing_id'],
                                                                                                 self.output_files[output_i]['file'],
                                                                                                 self.output_files[output_i]['lfn']))
                    file_item = {'did': "%s:%s" % (self.output_files[output_i]['scope'], self.output_files[output_i]['lfn']),
                                 'base_dir': self.get_download_dir(processing),
                                 'no_subdir': True}
                    file_items.append(file_item)
                else:
                    self.logger.warn("download_output_files, Processing (%s) lfn for %s not found" % (processing['processing_id'],
                                                                                                      self.output_files[output_i]['file']))

            pfn_items = self.download_output_files_rucio(file_items)
            for output_i in range(len(self.output_files)):
                if 'lfn' in self.output_files[output_i]:
                    if self.output_files[output_i]['lfn'] in pfn_items:
                        pfn = pfn_items[self.output_files[output_i]['lfn']]
                        self.output_files[output_i]['pfn'] = pfn
                        self.logger.info("download_output_files, Processing (%s) pfn for %s: %s" % (processing['processing_id'],
                                                                                                    self.output_files[output_i]['file'],
                                                                                                    self.output_files[output_i]['pfn']))
                    else:
                        self.logger.info("download_output_files, Processing (%s) pfn cannot be found for %s" % (processing['processing_id'],
                                                                                                                self.output_files[output_i]['file']))
                        failed_items.append(self.output_files[output_i])
            return failed_items
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            # raise exceptions.IDDSException('%s: %s' % (str(ex), traceback.format_exc()))
        return []

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
            # self.logger.error(ex)
            # self.logger.error(traceback.format_exc())
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
            return True, output_data
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            # raise exceptions.IDDSException('%s: %s' % (str(ex), traceback.format_exc()))
        return False, output_data

    def process_outputs(self, processing):
        ping_status = self.ping_output_files()
        self.logger.debug("ping_output_files(Processing_id: %s), status: %s" % (processing['processing_id'], ping_status))

        failed_items = self.download_output_files(processing)
        self.logger.debug("download_output_files(Processing_id: %s), failed_items: %s" % (processing['processing_id'], str(failed_items)))

        parse_status, output_data = self.parse_output_files(processing)
        self.logger.debug("parse_output_files(Processing_id: %s), parse_status: %s, output_data: %s" % (processing['processing_id'], parse_status, str(output_data)))

        if ping_status and not failed_items and parse_status:
            return True, output_data
        return False, output_data

    def get_rucio_download_client(self):
        try:
            from rucio.client.downloadclient import DownloadClient
            client = DownloadClient()
        except RucioCannotAuthenticate as error:
            self.logger.error(error)
            self.logger.error(traceback.format_exc())
            raise exceptions.IDDSException('%s: %s' % (str(error), traceback.format_exc()))
        return client

    def poll_panda_task_output(self, processing=None, input_output_maps=None, log_prefix=''):
        task_id = None
        try:
            from pandaclient import Client

            if processing:
                output_metadata = {}
                proc = processing['processing_metadata']['processing']
                task_id = proc.workload_id
                if task_id is None:
                    task_id = self.get_panda_task_id(processing)

                if task_id:
                    # ret_ids = Client.getPandaIDsWithTaskID(task_id, verbose=False)
                    task_info = Client.getJediTaskDetails({'jediTaskID': task_id}, True, True, verbose=False)
                    self.logger.info(log_prefix + "poll_panda_task, task_info: %s" % str(task_info))
                    if task_info[0] != 0:
                        self.logger.warn(log_prefix + "poll_panda_task %s, error getting task status, task_info: %s" % (task_id, str(task_info)))
                        return ProcessingStatus.Submitting, [], {}, {}

                    task_info = task_info[1]

                    processing_status = self.get_processing_status_from_panda_status(task_info["status"])

                    if processing_status in [ProcessingStatus.SubFinished, ProcessingStatus.Finished]:
                        output_status, output_metadata = self.process_outputs(processing)
                        if not output_status:
                            err = "Failed to process processing(processing_id: %s, task_id: %s) outputs" % (processing['processing_id'], task_id)
                            self.logger.error(log_prefix + err)
                            self.add_errors(err)
                            processing_status = ProcessingStatus.Failed

                    return processing_status, [], {}, output_metadata
                else:
                    return ProcessingStatus.Failed, [], {}, output_metadata
        except Exception as ex:
            msg = "Failed to check the processing (%s) status: %s" % (str(processing['processing_id']), str(ex))
            self.logger.error(log_prefix + msg)
            self.logger.error(log_prefix + ex)
            self.logger.error(traceback.format_exc())
            # raise exceptions.IDDSException(msg)
        return ProcessingStatus.Submitting, [], {}, {}

    def poll_processing_updates(self, processing, input_output_maps, log_prefix=''):
        """
        *** Function called by Carrier agent.
        """
        updated_contents = []
        update_contents_full = []
        parameters = {}
        # self.logger.debug("poll_processing_updates, input_output_maps: %s" % str(input_output_maps))

        if processing:
            proc = processing['processing_metadata']['processing']

            processing_status, poll_updated_contents, new_input_output_maps, output_metadata = self.poll_panda_task_output(processing=processing,
                                                                                                                           input_output_maps=input_output_maps,
                                                                                                                           log_prefix=log_prefix)
            self.logger.debug(log_prefix + "poll_processing_updates, output_metadata: %s" % str(output_metadata))

            if poll_updated_contents:
                proc.has_new_updates()
            for content in poll_updated_contents:
                updated_content = {'content_id': content['content_id'],
                                   'status': content['status'],
                                   'substatus': content['substatus'],
                                   'content_metadata': content['content_metadata']}
                updated_contents.append(updated_content)

            if output_metadata:
                parameters = {'output_metadata': output_metadata}

        return processing_status, updated_contents, new_input_output_maps, update_contents_full, parameters

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

        # if self.is_processings_terminated() and self.is_input_collections_closed() and not self.has_new_inputs and not self.has_to_release_inputs() and not to_release_input_contents:
        if self.is_processings_terminated():
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
