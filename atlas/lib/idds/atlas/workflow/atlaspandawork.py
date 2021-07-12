#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2020 - 2021

try:
    import ConfigParser
except ImportError:
    import configparser as ConfigParser

# try:
#     from urllib import quote
# except ImportError:
#     from urllib.parse import quote

import copy
import os
import re
import traceback

from idds.common import exceptions
from idds.common.constants import (TransformType, CollectionType, CollectionStatus,
                                   ProcessingStatus, WorkStatus, ContentStatus)
from idds.workflow.work import Work, Processing
from idds.workflow.workflow import Condition


class PandaCondition(Condition):
    def __init__(self, cond=None, current_work=None, true_work=None, false_work=None):
        super(PandaCondition, self).__init__(cond=cond, current_work=current_work,
                                             true_work=true_work, false_work=false_work)


class ATLASPandaWork(Work):
    def __init__(self, executable=None, arguments=None, parameters=None, setup=None,
                 work_tag='activelearning', exec_type='panda', sandbox=None, work_id=None,
                 primary_input_collection=None, other_input_collections=None,
                 output_collections=None, log_collections=None,
                 logger=None, dependency_map=None, task_name="",
                 panda_task_paramsmap=None):
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
        # self.cmd_to_arguments = cmd_to_arguments
        self.panda_task_paramsmap = panda_task_paramsmap
        self.output_dataset_name = None
        super(ATLASPandaWork, self).__init__(executable=executable, arguments=arguments,
                                             parameters=parameters, setup=setup, work_type=TransformType.Processing,
                                             work_tag=work_tag, exec_type=exec_type, sandbox=sandbox, work_id=work_id,
                                             primary_input_collection=primary_input_collection,
                                             other_input_collections=other_input_collections,
                                             output_collections=output_collections,
                                             log_collections=log_collections,
                                             logger=logger)
        self.panda_url = None
        self.panda_url_ssl = None
        self.panda_monitor = None
        self.load_panda_urls()

        # from pandatools import Client
        # Client.getTaskParamsMap(23752996)
        # (0, '{"buildSpec": {"jobParameters": "-i ${IN} -o ${OUT} --sourceURL ${SURL} -r . ", "archiveName": "sources.0ca6a2fb-4ad0-42d0-979d-aa7c284f1ff7.tar.gz", "prodSourceLabel": "panda"}, "sourceURL": "https://aipanda048.cern.ch:25443", "cliParams": "prun --exec \\"python simplescript.py 0.5 0.5 200 output.json\\" --outDS user.wguan.altest1234 --outputs output.json --nJobs=10", "site": null, "vo": "atlas", "respectSplitRule": true, "osInfo": "Linux-3.10.0-1127.19.1.el7.x86_64-x86_64-with-centos-7.9.2009-Core", "log": {"type": "template", "param_type": "log", "container": "user.wguan.altest1234.log/", "value": "user.wguan.altest1234.log.$JEDITASKID.${SN}.log.tgz", "dataset": "user.wguan.altest1234.log/"}, "transUses": "", "excludedSite": [], "nMaxFilesPerJob": 200, "uniqueTaskName": true, "noInput": true, "taskName": "user.wguan.altest1234/", "transHome": null, "includedSite": null, "nEvents": 10, "nEventsPerJob": 1, "jobParameters": [{"type": "constant", "value": "-j \\"\\" --sourceURL ${SURL}"}, {"type": "constant", "value": "-r ."}, {"padding": false, "type": "constant", "value": "-p \\""}, {"padding": false, "type": "constant", "value": "python%20simplescript.py%200.5%200.5%20200%20output.json"}, {"type": "constant", "value": "\\""}, {"type": "constant", "value": "-l ${LIB}"}, {"container": "user.wguan.altest1234_output.json/", "value": "user.wguan.$JEDITASKID._${SN/P}.output.json", "dataset": "user.wguan.altest1234_output.json/", "param_type": "output", "hidden": true, "type": "template"}, {"type": "constant", "value": "-o \\"{\'output.json\': \'user.wguan.$JEDITASKID._${SN/P}.output.json\'}\\""}], "prodSourceLabel": "user", "processingType": "panda-client-1.4.47-jedi-run", "architecture": "@centos7", "userName": "Wen Guan", "taskType": "anal", "taskPriority": 1000, "countryGroup": "us"}')  # noqa E501

        self.panda_task_id = None
        self.init_panda_task_info()

    def initialize_work(self):
        if not self.is_initialized():
            self.init_new_panda_task_info()
            super(ATLASPandaWork, self).initialize_work()

    def get_scope_name(self, dataset):
        if dataset.startswith("user"):
            scope = "user." + dataset.split('.')[1]
        elif dataset.startswith("group"):
            scope = "group." + dataset.split('.')[1]
        else:
            scope = dataset.split('.')[0]
        return scope

    def get_output_dataset_name_from_task_paramsmap(self):
        if self.panda_task_paramsmap:
            cliParams = self.panda_task_paramsmap['cliParams']
            output_dataset_name = cliParams.split("--outDS")[1].strip().split(" ")[0]
            return output_dataset_name
        return None

    def init_panda_task_info(self):
        if self.panda_task_paramsmap:
            self.output_dataset_name = self.get_output_dataset_name_from_task_paramsmap()
            self.sandbox = os.path.join(self.panda_task_paramsmap['sourceURL'], 'cache/' + self.panda_task_paramsmap['buildSpec']['archiveName'])
            for p in self.panda_task_paramsmap["jobParameters"]:
                if 'param_type' in p and p['param_type'] == 'output':
                    output_dataset = p['dataset']
                    output_dataset = output_dataset.replace("/", "")
                    scope = self.get_scope_name(output_dataset)
                    primary_input_collection = {'scope': scope, 'name': output_dataset}
                    output_collection = {'scope': scope, 'name': output_dataset}
                    self.set_primary_input_collection(primary_input_collection)
                    self.add_output_collections([output_collection])
                if 'log' in p:
                    log_dataset = p['dataset']
                    log_dataset = log_dataset.replace("/", "")
                    scope = self.get_scope_name(log_dataset)
                    log_collection = {'scope': scope, 'name': log_dataset}
                    self.add_log_collections([log_collection])

    def init_new_panda_task_info(self):
        if not self.panda_task_paramsmap:
            return

        # generate new dataset name
        # self.padding = self.sequence_in_workflow
        new_dataset_name = self.output_dataset_name + "_" + str(self.sequence_id)
        for coll_id in self.collections:
            coll = self.collections[coll_id]
            coll['name'] = coll['name'].replace(self.output_dataset_name, new_dataset_name)

        self.panda_task_paramsmap['cliParams'] = \
            self.panda_task_paramsmap['cliParams'].replace(self.output_dataset_name, new_dataset_name)

        self.panda_task_paramsmap['taskName'] = \
            self.panda_task_paramsmap['taskName'].replace(self.output_dataset_name, new_dataset_name)

        jobParameters = self.panda_task_paramsmap['jobParameters']
        for p in jobParameters:
            if 'container' in p:
                p['container'] = p['container'].replace(self.output_dataset_name, new_dataset_name)
            if 'dataset' in p:
                p['dataset'] = p['dataset'].replace(self.output_dataset_name, new_dataset_name)

        log = self.panda_task_paramsmap['log']
        if 'value' in log:
            log['value'] = log['value'].replace(self.output_dataset_name, new_dataset_name)
        if 'container' in log:
            log['container'] = log['container'].replace(self.output_dataset_name, new_dataset_name)
        if 'dataset' in log:
            log['dataset'] = log['dataset'].replace(self.output_dataset_name, new_dataset_name)

        self.parse_arguments()

    def parse_arguments(self):
        try:
            # arguments = self.get_arguments()
            # parameters = self.get_parameters()
            new_parameters = self.get_parameters()

            if new_parameters:
                self.panda_task_paramsmap['cliParams'] = self.panda_task_paramsmap['cliParams'].format(**new_parameters)

            # todo
            # jobParameters = self.panda_task_paramsmap['jobParameters']
            # for p in jobParameters:
            #     if 'value' in p:
            #         p['value'] = p['value'].replace(quote(arguments), quote(new_arguments))

            # return new_arguments
        except Exception as ex:
            self.add_errors(str(ex))

    def generate_work_from_template(self):
        new_work = super(ATLASPandaWork, self).generate_work_from_template()
        # new_work.unset_initialized()
        # new_work.panda_task_id = None
        return new_work

    def set_parameters(self, parameters):
        self.parameters = parameters
        # trigger to submit new tasks
        self.unset_initialized()
        self.panda_task_id = None

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

        configfiles = ['%s/etc/panda/panda.cfg' % os.environ.get('IDDS_HOME', ''),
                       '/etc/panda/panda.cfg', '/opt/idds/etc/panda/panda.cfg',
                       '%s/etc/panda/panda.cfg' % os.environ.get('VIRTUAL_ENV', '')]
        for configfile in configfiles:
            if panda_config.read(configfile) == [configfile]:
                return panda_config
        return panda_config

    def load_panda_urls(self):
        panda_config = self.load_panda_config()
        self.logger.debug("panda config: %s" % panda_config)
        self.panda_url = None
        self.panda_url_ssl = None
        self.panda_monitor = None

        if panda_config.has_section('panda'):
            if panda_config.has_option('panda', 'panda_monitor_url'):
                self.panda_monitor = panda_config.get('panda', 'panda_monitor_url')
                os.environ['PANDA_MONITOR_URL'] = self.panda_monitor
                self.logger.debug("Panda monitor url: %s" % str(self.panda_monitor))
            if panda_config.has_option('panda', 'panda_url'):
                self.panda_url = panda_config.get('panda', 'panda_url')
                os.environ['PANDA_URL'] = self.panda_url
                self.logger.debug("Panda url: %s" % str(self.panda_url))
            if panda_config.has_option('panda', 'panda_url_ssl'):
                self.panda_url_ssl = panda_config.get('panda', 'panda_url_ssl')
                os.environ['PANDA_URL_SSL'] = self.panda_url_ssl
                self.logger.debug("Panda url ssl: %s" % str(self.panda_url_ssl))

        if not self.panda_monitor and 'PANDA_MONITOR_URL' in os.environ and os.environ['PANDA_MONITOR_URL']:
            self.panda_monitor = os.environ['PANDA_MONITOR_URL']
            self.logger.debug("Panda monitor url: %s" % str(self.panda_monitor))
        if not self.panda_url and 'PANDA_URL' in os.environ and os.environ['PANDA_URL']:
            self.panda_url = os.environ['PANDA_URL']
            self.logger.debug("Panda url: %s" % str(self.panda_url))
        if not self.panda_url_ssl and 'PANDA_URL_SSL' in os.environ and os.environ['PANDA_URL_SSL']:
            self.panda_url_ssl = os.environ['PANDA_URL_SSL']
            self.logger.debug("Panda url ssl: %s" % str(self.panda_url_ssl))

    def poll_external_collection(self, coll):
        try:
            # if 'coll_metadata' in coll and 'is_open' in coll['coll_metadata'] and not coll['coll_metadata']['is_open']:
            if coll.status in [CollectionStatus.Closed]:
                return coll
            else:
                # client = self.get_rucio_client()
                # did_meta = client.get_metadata(scope=coll['scope'], name=coll['name'])
                coll.coll_metadata['bytes'] = 0
                coll.coll_metadata['total_files'] = 0
                coll.coll_metadata['availability'] = True
                coll.coll_metadata['events'] = 0
                coll.coll_metadata['is_open'] = False
                coll.coll_metadata['run_number'] = None
                coll.coll_metadata['did_type'] = 'DATASET'
                coll.coll_metadata['list_all_files'] = False

                if 'is_open' in coll.coll_metadata and not coll.coll_metadata['is_open']:
                    coll_status = CollectionStatus.Closed
                else:
                    coll_status = CollectionStatus.Open
                coll.status = coll_status

                if 'did_type' in coll.coll_metadata:
                    if coll.coll_metadata['did_type'] == 'DATASET':
                        coll_type = CollectionType.Dataset
                    elif coll.coll_metadata['did_type'] == 'CONTAINER':
                        coll_type = CollectionType.Container
                    else:
                        coll_type = CollectionType.File
                else:
                    coll_type = CollectionType.Dataset
                coll.coll_metadata['coll_type'] = coll_type

                return coll
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
        return super(ATLASPandaWork, self).get_input_collections()

    def get_input_contents(self):
        """
        Get all input contents from DDM.
        """
        try:
            ret_files = []
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
        if (not new_inputs and 'status' in self.collections[self.primary_input_collection]
           and self.collections[self.primary_input_collection]['status'] in [CollectionStatus.Closed]):  # noqa: W503
            self.set_has_new_inputs(False)
        else:
            mapped_keys = mapped_input_output_maps.keys()
            if mapped_keys:
                next_key = max(mapped_keys) + 1
            else:
                next_key = 1
            for ip in new_inputs:
                out_ip = copy.deepcopy(ip)
                ip['status'] = ContentStatus.Available
                ip['substatus'] = ContentStatus.Available
                out_ip['coll_id'] = self.collections[self.output_collections[0]]['coll_id']
                new_input_output_maps[next_key] = {'inputs': [ip],
                                                   'outputs': [out_ip],
                                                   'inputs_dependency': [],
                                                   'logs': []}
                next_key += 1

        return new_input_output_maps

    def get_processing(self, input_output_maps, without_creating=False):
        """
        *** Function called by Transformer agent.

        If there is already an active processing for this work, will do nothing.
        If there is no active processings, create_processing will be called.
        """
        if self.active_processings:
            return self.processings[self.active_processings[0]]
        else:
            if not without_creating:
                return self.create_processing(input_output_maps)
        return None

    def create_processing(self, input_output_maps=[]):
        """
        *** Function called by Transformer agent.

        :param input_output_maps: new maps from inputs to outputs.
        """
        processing_metadata = {'panda_task_id': self.panda_task_id}
        proc = Processing(processing_metadata=processing_metadata)
        proc.workload_id = self.panda_task_id
        self.add_processing_to_processings(proc)
        self.active_processings.append(proc.internal_id)
        return proc

    def submit_panda_task(self, processing):
        try:
            from pandatools import Client

            status, tmpOut = Client.insertTaskParams(self.panda_task_paramsmap, False, True)
            if status == 0:
                tmp_status, tmp_output = tmpOut
                m = re.search("jediTaskID=(\d+)", tmp_output)  # noqa W605
                task_id = int(m.group(1))
                processing.workload_id = task_id
            else:
                self.add_errors(tmpOut)
                raise Exception(tmpOut)
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            raise exceptions.IDDSException('%s: %s' % (str(ex), traceback.format_exc()))

    def submit_processing(self, processing):
        """
        *** Function called by Carrier agent.
        """
        if 'panda_task_id' in processing['processing_metadata'] and processing['processing_metadata']['panda_task_id']:
            pass
        else:
            self.set_user_proxy()
            self.submit_panda_task(processing)
            self.unset_user_proxy()

    def poll_panda_task(self, processing):
        if 'panda_task_id' in processing['processing_metadata']:
            from pandatools import Client

            status, task_status = Client.getTaskStatus(processing.workload_id)
            if status == 0:
                return task_status
        else:
            return 'failed'
        return None

    def kill_processing(self, processing):
        try:
            if processing:
                from pandatools import Client
                task_id = processing.workload_id
                Client.killTask(task_id)
        except Exception as ex:
            msg = "Failed to check the processing (%s) status: %s" % (str(processing['processing_id']), str(ex))
            raise exceptions.IDDSException(msg)

    def reactivate_processing(self, processing):
        try:
            if processing:
                from pandatools import Client
                task_id = processing.workload_id
                Client.retryTask(task_id)
                # Client.reactivateTask(task_id)
                # Client.resumeTask(task_id)
        except Exception as ex:
            msg = "Failed to check the processing (%s) status: %s" % (str(processing['processing_id']), str(ex))
            raise exceptions.IDDSException(msg)

    def poll_processing_updates(self, processing, input_output_maps):
        """
        *** Function called by Carrier agent.
        """
        updated_contents = []
        update_processing = {}
        reset_expired_at = False

        if processing:
            if self.tocancel:
                self.logger.info("Cancelling processing (processing id: %s, jediTaskId: %s)" % (processing['processing_id'], processing['processing_metadata']['task_id']))
                self.kill_processing(processing)
                self.tocancel = False
            elif self.tosuspend:
                self.logger.info("Suspending processing (processing id: %s, jediTaskId: %s)" % (processing['processing_id'], processing['processing_metadata']['task_id']))
                self.kill_processing(processing)
                self.tosuspend = False
            elif self.toresume:
                self.logger.info("Resuming processing (processing id: %s, jediTaskId: %s)" % (processing['processing_id'], processing['processing_metadata']['task_id']))
                self.reactivate_processing(processing)
                self.toresume = False
                reset_expired_at = True
            elif self.toexpire:
                self.logger.info("Expiring processing (processing id: %s, jediTaskId: %s)" % (processing['processing_id'], processing['processing_metadata']['task_id']))
                self.kill_processing(processing)

            task_status = self.poll_panda_task(processing)
            if task_status:
                if task_status in ['registered', 'defined']:
                    processing_status = ProcessingStatus.Submitted
                elif task_status in ['assigning', 'ready', 'pending', 'scouting', 'scouted', 'running', 'prepared']:
                    processing_status = ProcessingStatus.Running
                elif task_status in ['done']:
                    # finished, finishing, waiting it to be done
                    processing_status = ProcessingStatus.Finished
                elif task_status in ['failed', 'aborted', 'broken', 'exhausted']:
                    processing_status = ProcessingStatus.Failed
                else:
                    # finished, finishing, aborting, topreprocess, preprocessing, tobroken
                    # toretry, toincexec, rerefine, paused, throttled, passed
                    processing_status = ProcessingStatus.Running

                update_processing = {'processing_id': processing['processing_id'],
                                     'parameters': {'status': processing_status}}
                if reset_expired_at:
                    update_processing['parameters']['expired_at'] = None
                    processing['expired_at'] = None
                    if (processing_status in [ProcessingStatus.SubFinished, ProcessingStatus.Finished, ProcessingStatus.Failed]
                        or processing['status'] in [ProcessingStatus.Resuming]):   # noqa W503
                        update_processing['parameters']['status'] = ProcessingStatus.Resuming

        return update_processing, updated_contents

    def syn_work_status(self, registered_input_output_maps, all_updates_flushed=True, output_statistics={}):
        # self.syn_collection_status()

        if self.is_processings_terminated() and not self.has_new_inputs():
            if not self.is_all_outputs_flushed(registered_input_output_maps):
                self.logger.warn("The processing is terminated. but not all outputs are flushed. Wait to flush the outputs then finish the transform")
                return

            if self.is_processings_finished():
                self.status = WorkStatus.Finished
            elif self.is_processings_failed():
                self.status = WorkStatus.Failed
            elif self.is_processings_subfinished():
                self.status = WorkStatus.SubFinished
