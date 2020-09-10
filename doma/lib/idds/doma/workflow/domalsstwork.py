#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2020

try:
    import ConfigParser
except ImportError:
    import configparser as ConfigParser

import copy
import json
import os
import traceback
import uuid
import urllib

from pandatools import Client

from idds.common import exceptions
from idds.common.constants import (TransformType, CollectionStatus, ContentStatus, ContentType,
                                   ProcessingStatus, WorkStatus)
from idds.workflow.work import Work
from idds.workflow.workflow import Condition


class DomaCondition(Condition):
    def __init__(self, cond, current_work, true_work, false_work=None):
        super(DomaCondition, self).__init__(cond=cond, current_work=current_work,
                                            true_work=true_work, false_work=false_work)


class DomaLSSTWork(Work):
    def __init__(self, executable=None, arguments=None, parameters=None, setup=None,
                 work_tag='lsst', exec_type='panda', sandbox=None, work_id=None,
                 primary_input_collection=None, other_input_collections=None,
                 output_collections=None, log_collections=None,
                 logger=None):
        """
        Init a work/task/transformation.

        :param setup: A string to setup the executable enviroment, it can be None.
        :param executable: The executable.
        :param arguments: The arguments.
        :param parameters: A dict with arguments needed to be replaced.
        :param work_type: The work type like data carousel, hyperparameteroptimization and so on.
        :param exec_type: The exec type like 'local', 'remote'(with remote_package set), 'docker' and so on.
        :param sandbox: The sandbox.
        :param work_id: The work/task id.
        :param primary_input_collection: The primary input collection.
        :param other_input_collections: List of the input collections.
        :param output_collections: List of the output collections.
        # :param workflow: The workflow the current work belongs to.
        """
        super(DomaLSSTWork, self).__init__(executable=executable, arguments=arguments,
                                           parameters=parameters, setup=setup, work_type=TransformType.Workflow,
                                           work_tag=work_tag, exec_type=exec_type, sandbox=sandbox, work_id=work_id,
                                           primary_input_collection=primary_input_collection,
                                           other_input_collections=other_input_collections,
                                           output_collections=output_collections,
                                           log_collections=log_collections,
                                           logger=logger)

    def my_condition(self):
        if self.is_finished():
            return True
        return False

    def load_panda_config(self):
        panda_config = ConfigParser.SafeConfigParser()
        if os.environ.get('IDDS_PANDA_CONFIG', None):
            configfile = os.environ['IDDS_PANDA_CONFIG']
            if panda_config.read(configfile) == [configfile]:
                return panda_config

        configfiles = ['%s/etc/idds/panda.cfg' % os.environ.get('IDDS_HOME', ''),
                       '/etc/idds/panda.cfg',
                       '%s/etc/idds/panda.cfg' % os.environ.get('VIRTUAL_ENV', '')]
        for configfile in configfiles:
            if panda_config.read(configfile) == [configfile]:
                return panda_config
        return panda_config

    def load_panda_server(self):
        panda_config = self.load_panda_config()
        if panda_config.has_section('panda'):
            if panda_config.has_option('panda', 'pandaserver'):
                pandaserver = panda_config.get('panda', 'pandaserver')
                return pandaserver
        return None

    def poll_external_collection(self, coll):
        try:
            if 'coll_metadata' in coll and 'is_open' in coll['coll_metadata'] and not coll['coll_metadata']['is_open']:
                return coll
            else:
                new_coll = {}
                new_coll = {'scope': coll['scope'],
                            'name': coll['name'],
                            'coll_metadata': {
                                'bytes': 1,
                                'total_files': 1,
                                'availability': 1,
                                'events': 1,
                                'is_open': False,
                                'run_number': 1,
                                'did_type': 'DATASET',   # DATASET, CONTAINER or FILE (rucio did types)
                                'list_all_files': True}
                            }
                return new_coll
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            raise exceptions.IDDSException('%s: %s' % (str(ex), traceback.format_exc()))

    def get_input_collections(self):
        """
        *** Function called by Transformer agent.
        """
        colls = [self.primary_input_collection] + self.other_input_collections
        for coll_int_id in colls:
            coll = self.collections[coll_int_id]
            coll = self.poll_external_collection(coll)
            self.collections[coll_int_id] = coll
        return super(self).get_input_collections()

    def get_input_contents(self):
        """
        Get all input contents from DDM.
        """
        try:
            ret_files = []
            coll = self.collections[self.primary_input_collection]
            files = ['fake']
            for file in files:
                ret_file = {'coll_id': coll['coll_id'],
                            'scope': coll['scope'],
                            'name': coll['name'],  # or a different file name from the dataset name
                            'bytes': 1,
                            'adler32': '12345678',
                            'min_id': 0,
                            'max_id': 1,
                            'content_type': ContentType.File,
                            'content_metadata': {'events': 1}}  # here events is all events for eventservice, not used here.
                ret_files.append(ret_file)
            return ret_files
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            raise exceptions.IDDSException('%s: %s' % (str(ex), traceback.format_exc()))

    def get_mapped_inputs(self, mapped_input_output_maps):
        ret = []
        for map_id in mapped_input_output_maps:
            inputs = mapped_input_output_maps[map_id]['inputs']

            # if 'primary' is not set, the first one is the primary input.
            primary_input = inputs[0]
            for ip in inputs:
                if 'primary' in ip['content_metadata'] and ip['content_metadata']['primary']:
                    primary_input = ip
            ret.append(primary_input)
        return ret

    def get_new_input_output_maps(self, mapped_input_output_maps={}):
        """
        *** Function called by Transformer agent.
        New inputs which are not yet mapped to outputs.

        :param mapped_input_output_maps: Inputs that are already mapped.
        """
        inputs = self.get_input_contents()
        mapped_inputs = self.get_mapped_inputs(mapped_input_output_maps)
        mapped_inputs_scope_name = [ip['scope'] + ":" + ip['name'] for ip in mapped_inputs]

        new_inputs = []
        new_input_output_maps = {}
        for ip in inputs:
            ip_scope_name = ip['scope'] + ":" + ip['name']
            if ip_scope_name not in mapped_inputs_scope_name:
                new_inputs.append(ip)

        # to avoid cheking new inputs if there are no new inputs anymore
        if not new_inputs and self.collections[self.primary_input_collection]['status'] in [CollectionStatus.Closed]:
            self.set_has_new_inputs(False)
        else:
            mapped_keys = mapped_input_output_maps.keys()
            if mapped_keys:
                next_key = max(mapped_keys) + 1
            else:
                next_key = 1
            for ip in new_inputs:
                out_ip = copy.deepcopy(ip)
                out_ip['coll_id'] = self.collections[self.output_collections[0]]['coll_id']
                new_input_output_maps[next_key] = {'inputs': [ip],
                                                   'outputs': [out_ip]}
                next_key += 1

        return new_input_output_maps

    def get_processing(self, input_output_maps):
        """
        *** Function called by Transformer agent.

        If there is already an active processing for this work, will do nothing.
        If there is no active processings, create_processing will be called.
        """
        if self.active_processings:
            return self.processings[self.active_processings[0]]
        else:
            return None

    def create_processing(self, input_output_maps):
        """
        *** Function called by Transformer agent.

        :param input_output_maps: new maps from inputs to outputs.
        """
        in_files = []
        for map_id in input_output_maps:
            # one map is a job which transform the inputs to outputs.
            inputs = input_output_maps['map_id']['inputs']
            # outputs = input_output_maps['map_id']['outputs']
            for ip in inputs:
                in_files.append(ip['scope'] + ":" + ip['name'])

        taskParamMap = {}
        taskParamMap['vo'] = 'wlcg'
        taskParamMap['site'] = 'BNL_OSG_1'
        taskParamMap['workingGroup'] = 'lsst'
        taskParamMap['nFilesPerJob'] = 1
        taskParamMap['nFiles'] = len(in_files)
        taskParamMap['noInput'] = True
        taskParamMap['pfnList'] = in_files
        taskParamMap['taskName'] = "user.taskname"
        taskParamMap['userName'] = 'Siarhei Padolski'
        taskParamMap['taskPriority'] = 900
        taskParamMap['architecture'] = ''
        taskParamMap['transUses'] = ''
        taskParamMap['transHome'] = None
        taskParamMap['transPath'] = 'https://atlpan.web.cern.ch/atlpan/bash-c'
        taskParamMap['processingType'] = 'workflow'
        taskParamMap['prodSourceLabel'] = 'test'
        taskParamMap['taskType'] = 'test'
        taskParamMap['coreCount'] = 1
        taskParamMap['ramCount'] = 4000
        taskParamMap['skipScout'] = True
        taskParamMap['cloud'] = 'US'
        taskParamMap['jobParameters'] = [
            {'type': 'constant',
             'value': "singularity exec --no-home --cleanenv -w docker://spodolsky/lsst.w.2020.23:6 /opt/lsst/lsst_runner ${IN/L} testrun_s/output4 calib/hsc,raw/hsc,masks/hsc,ref_cats,skymaps,shared/ci_hsc https://storage.googleapis.com GOOG2KTG3NTEB2HDFQFIU3O2 0IkpSF0nGFYYTdH/5msrTpzMB7mEslz9fWK9spUs s3://testbutl_w_2020_23",  # noqa: E501
             },
        ]

        proc = {'processing_metadata': {'internal_id': str(uuid.uuid1()),
                                        'panda_id': None,
                                        'task_param': taskParamMap}}
        self.add_processing_to_processings(proc)
        self.active_processings.append(proc['processing_metadata']['internal_id'])
        return proc

    def submit_panda_task(self, processing):
        try:
            task_param = processing['processing_metadata']['task_param']
            return_code = Client.insertTaskParams(task_param, verbose=True)
            if return_code[0] == 0:
                return return_code[1][1]
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            # raise exceptions.AgentPluginError('%s: %s' % (str(ex), traceback.format_exc()))
        return None

    def submit_processing(self, processing):
        """
        *** Function called by Carrier agent.
        """
        if 'panda_id' in processing['processing_metadata'] and processing['processing_metadata']['panda_id']:
            pass
        else:
            panda_id = self.submit_panda_task(processing)
            processing['processing_metadata']['panda_id'] = panda_id

    def download_payload_json(self, task_url):
        response = None
        try:
            req = urllib.request.Request(task_url)
            response = urllib.request.urlopen(req, timeout=180).read()
            response = json.loads(response)
        except Exception or urllib.request.error as e:
            raise e
        return response

    def poll_panda_tasks(self, processing):
        try:
            panda_id = processing['processing_metadata']['panda_id']
            task_url = self.pandaserver + '|'.join(map(str, [panda_id]))
            jobs_list = self.download_payload_json(task_url)
            for job_info in jobs_list['jobs']:
                # if one job is one output, just need to check the job status
                pass
            task_status = 'Running'
            outputs_status = []
            return processing, task_status, outputs_status
        except Exception as ex:
            msg = "Failed to check the panda task(%s) status: %s" % (str(panda_id), str(ex))
            raise exceptions.IDDSException(msg)

    def poll_processing(self, processing):
        task_status, outputs_status = self.poll_panda_task(processing)
        return processing, task_status, outputs_status

    def poll_processing_updates(self, processing, input_output_maps):
        """
        *** Function called by Carrier agent.
        """

        processing, task_status, outputs_status = self.poll_processing(processing)

        updated_contents = []
        update_processing = {}

        if processing:
            content_substatus = {'finished': 0, 'unfinished': 0}
            for map_id in input_output_maps:
                outputs = input_output_maps[map_id]['outputs']
                for content in outputs:
                    key = '%s:%s' % (content['scope'], content['name'])
                    if key in outputs_status:
                        if content['substatus'] != outputs_status[key]['substatus']:
                            updated_content = {'content_id': content['content_id'],
                                               'substatus': outputs_status[key]['substatus']}
                            updated_contents.append(updated_content)
                            content['substatus'] = outputs_status[key]['substatus']
                    if content['substatus'] == ContentStatus.Available:
                        content_substatus['finished'] += 1
                    else:
                        content_substatus['unfinished'] += 1

            if task_status == 'Finished' and content_substatus['finished'] > 0 and content_substatus['unfinished'] == 0:
                update_processing = {'processing_id': processing['processing_id'],
                                     'parameters': {'status': ProcessingStatus.Finished}}
        return update_processing, updated_contents

    def get_status_statistics(self, registered_input_output_maps):
        status_statistics = {}
        for map_id in registered_input_output_maps:
            outputs = registered_input_output_maps['map_id']['outputs']

            for content in outputs:
                if content['status'].name not in status_statistics:
                    status_statistics[content['status'].name] = 0
                status_statistics[content['status'].name] += 1
        self.status_statistics = status_statistics
        return status_statistics

    def syn_work_status(self, registered_input_output_maps):
        self.get_status_statistics(registered_input_output_maps)

        if not self.active_processings and not self.has_new_inputs():
            keys = self.status_statistics.keys()
            if ContentStatus.New.name in keys or ContentStatus.Processing.name in keys:
                pass
            else:
                if len(keys) == 1:
                    if ContentStatus.Available.name in keys:
                        self.status = WorkStatus.Finished
                    else:
                        self.status = WorkStatus.Failed
                else:
                    self.status = WorkStatus.SubFinished
