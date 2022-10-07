#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2020 - 2021

import copy
import json
import os
import traceback

from rucio.client.client import Client as RucioClient
from rucio.common.exception import (CannotAuthenticate as RucioCannotAuthenticate)

from idds.common import exceptions
from idds.common.constants import (TransformType, CollectionType, CollectionStatus,
                                   ContentStatus, ContentType,
                                   ProcessingStatus, WorkStatus)
from idds.common.utils import run_command
# from idds.workflow.work import Work
from idds.workflow.work import Processing
from idds.atlas.workflow.atlascondorwork import ATLASCondorWork


class ATLASActuatorWork(ATLASCondorWork):
    def __init__(self, executable=None, arguments=None, parameters=None, setup=None,
                 work_tag='actuating', exec_type='local', sandbox=None, work_id=None,
                 name=None,
                 primary_input_collection=None, other_input_collections=None, input_collections=None,
                 primary_output_collection=None, other_output_collections=None,
                 output_collections=None, log_collections=None,
                 logger=None,
                 workload_id=None,
                 agent_attributes=None,
                 output_json=None):
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
        :param sandbox: The sandbox to be uploaded or the container path.
        :param executable: The executable command.
        :param arguments: The arguments for the executable.
        """

        super(ATLASActuatorWork, self).__init__(executable=executable, arguments=arguments, work_tag=work_tag,
                                                parameters=parameters, setup=setup, work_type=TransformType.Actuating,
                                                exec_type=exec_type, sandbox=sandbox, work_id=work_id,
                                                primary_input_collection=primary_input_collection,
                                                other_input_collections=other_input_collections,
                                                primary_output_collection=primary_output_collection,
                                                other_output_collections=other_output_collections,
                                                input_collections=input_collections,
                                                output_collections=output_collections,
                                                log_collections=log_collections,
                                                logger=logger,
                                                agent_attributes=agent_attributes)

        self.output_json = output_json

        self.terminated = False
        self.tocancel = False

        # if self.agent_attributes and 'atlashpowork' in self.agent_attributes:
        #    self.agent_attributes = self.agent_attributes['atlashpowork']
        # self.logger.info("agent_attributes: %s" % self.agent_attributes)

        # if self.agent_attributes and 'workdir' in self.agent_attributes and self.agent_attributes['workdir']:
        #     self.set_workdir(self.agent_attributes['workdir'])
        # self.logger.info("workdir: %s" % self.get_workdir())

        if agent_attributes:
            self.set_agent_attributes(agent_attributes)

    def set_agent_attributes(self, attrs, req_attributes=None):
        super(ATLASActuatorWork, self).set_agent_attributes(attrs)

        if self.agent_attributes and 'workdir' in self.agent_attributes and self.agent_attributes['workdir']:
            if req_attributes and 'request_id' in req_attributes and 'workload_id' in req_attributes and 'transform_id' in req_attributes:
                req_dir = 'request_%s_%s/transform_%s' % (req_attributes['request_id'],
                                                          req_attributes['workload_id'],
                                                          req_attributes['transform_id'])
                self.set_workdir(os.path.join(self.agent_attributes['workdir'], req_dir))
        self.logger.info("workdir: %s" % self.get_workdir())

    ##########################################   # noqa E266
    def generate_new_task(self):
        self.logger.info("Work %s parameters for next task: %s" % (self.internal_id, str(self.get_parameters_for_next_task())))
        if self.get_parameters_for_next_task():
            return True
        else:
            return False

    ####### functions for transformer ########   # noqa E266
    ######################################       # noqa E266

    def set_output_data(self, data):
        # overwrite to transfer the output of current task to next task
        super(ATLASActuatorWork, self).set_output_data(data)
        super(ATLASActuatorWork, self).set_parameters_for_next_task(data)

    def get_rucio_client(self):
        try:
            client = RucioClient()
        except RucioCannotAuthenticate as error:
            self.logger.error(error)
            self.logger.error(traceback.format_exc())
            raise exceptions.IDDSException('%s: %s' % (str(error), traceback.format_exc()))
        return client

    def poll_external_collection(self, coll):
        try:
            if coll.status in [CollectionStatus.Closed]:
                return coll
            else:
                client = self.get_rucio_client()
                did_meta = client.get_metadata(scope=coll.scope, name=coll.name)
                coll.coll_metadata['bytes'] = did_meta['bytes']
                coll.coll_metadata['total_files'] = did_meta['length']
                coll.coll_metadata['availability'] = did_meta['availability']
                coll.coll_metadata['events'] = did_meta['events']
                coll.coll_metadata['is_open'] = did_meta['is_open']
                coll.coll_metadata['run_number'] = did_meta['run_number']
                coll.coll_metadata['did_type'] = did_meta['did_type']
                coll.coll_metadata['list_all_files'] = False

                if (('is_open' in coll.coll_metadata and not coll.coll_metadata['is_open'])
                   or ('force_close' in coll.coll_metadata and coll.coll_metadata['force_close'])):  # noqa: W503
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
        # return [self.primary_input_collection] + self.other_input_collections
        colls = [self._primary_input_collection] + self._other_input_collections
        for coll_int_id in colls:
            coll = self.collections[coll_int_id]
            coll = self.poll_external_collection(coll)
            self.collections[coll_int_id] = coll
        return super(ATLASActuatorWork, self).get_input_collections()

    def get_input_contents(self):
        """
        Get all input contents from DDM.
        """
        try:
            ret_files = []
            coll = self.collections[self._primary_input_collection]
            ret_file = {'coll_id': coll['coll_id'],
                        'scope': coll['scope'],
                        'name': coll['name'],
                        'bytes': coll.coll_metadata['bytes'],
                        'adler32': None,
                        'min_id': 0,
                        'max_id': coll.coll_metadata['total_files'],
                        'content_type': ContentType.File,
                        'content_metadata': {'total_files': coll['coll_metadata']['total_files']}
                        }
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
        if (not new_inputs and 'status' in self.collections[self._primary_input_collection]
           and self.collections[self._primary_input_collection]['status'] in [CollectionStatus.Closed]):  # noqa: W503
            self.set_has_new_inputs(False)
        else:
            mapped_keys = mapped_input_output_maps.keys()
            if mapped_keys:
                next_key = max(mapped_keys) + 1
            else:
                next_key = 1
            for ip in new_inputs:
                out_ip = copy.deepcopy(ip)
                out_ip['coll_id'] = self.collections[self._primary_output_collection]['coll_id']
                new_input_output_maps[next_key] = {'inputs': [ip],
                                                   'outputs': [out_ip]}
                next_key += 1

        self.unfinished_points = 1

        return new_input_output_maps

    def get_processing(self, input_output_maps, without_creating=False):
        if self.active_processings:
            return self.processings[self.active_processings[0]]
        else:
            if not without_creating:
                return self.create_processing(input_output_maps)
        return None

    def create_processing(self, input_output_maps):
        processing_metadata = {}
        proc = Processing(processing_metadata=processing_metadata)
        self.add_processing_to_processings(proc)
        self.active_processings.append(proc.internal_id)
        return proc

    def get_status_statistics(self, registered_input_output_maps):
        status_statistics = {}

        self.total_output_files = 0
        self.processed_output_file = 0

        for map_id in registered_input_output_maps:
            outputs = registered_input_output_maps[map_id]['outputs']

            self.total_output_files += 1

            for content in outputs:
                if content['status'].name not in status_statistics:
                    status_statistics[content['status'].name] = 0
                status_statistics[content['status'].name] += 1

                if content['status'] == ContentStatus.Available:
                    self.processed_output_file += 1

        self.status_statistics = status_statistics
        return status_statistics

    def syn_collection_status(self):
        input_collections = self.get_input_collections()
        output_collections = self.get_output_collections()
        # log_collections = self.get_log_collections()

        for input_collection in input_collections:
            input_collection['total_files'] = 1
            input_collection['processed_files'] = 1

        for output_collection in output_collections:
            output_collection['total_files'] = self.total_output_files
            output_collection['processed_files'] = self.processed_output_file

    def syn_work_status(self, registered_input_output_maps):
        self.get_status_statistics(registered_input_output_maps)

        self.syn_collection_status()

        if self.is_processings_terminated() and not self.has_new_inputs:
            if self.is_processings_finished():
                self.status = WorkStatus.Finished
            elif self.is_processings_failed():
                self.status = WorkStatus.Failed
            elif self.is_processings_subfinished():
                self.status = WorkStatus.SubFinished
        else:
            self.status = WorkStatus.Transforming

    ####### functions for carrier ########     # noqa E266
    ######################################     # noqa E266

    def get_rucio_setup_env(self):
        script = "export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase\n"
        script += "source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh\n"
        script += "export RUCIO_ACCOUNT=pilot\n"
        script += "localSetupRucioClients\n"
        return script

    def generate_processing_script_sandbox(self, processing):
        arguments = self.parse_arguments()

        script = "#!/bin/bash\n\n"
        script += self.get_rucio_setup_env()
        script += "\n"

        script += "sandbox=%s\n" % str(self.sandbox)
        script += "executable=%s\n" % str(self.executable)
        script += "arguments=%s\n" % str(arguments)
        script += "output_json=%s\n" % str(self.output_json)
        script += "\n"

        script += "env\n"
        script += "echo $X509_USER_PROXY\n"
        script += "\n"

        script += "echo 'user id:'\n"
        script += "id\n"
        script += "\n"

        script += "wget $sandbox\n"
        script += 'base_sandbox="$(basename -- $sandbox)"\n'
        script += 'tar xzf $base_sandbox\n'

        dataset = self.collections[self._primary_input_collection]
        script += 'rucio download %s:%s\n' % (dataset['scope'], dataset['name'])
        script += 'chmod +x %s\n' % str(self.executable)
        script += "echo '%s' '%s'\n" % (str(self.executable), str(arguments))
        script += '%s %s\n' % (str(self.executable), str(arguments))

        script += 'ls\n\n'

        long_id = self.get_long_id(processing)
        script_name = 'processing_%s.sh' % long_id
        script_name = os.path.join(self.get_working_dir(processing), script_name)
        with open(script_name, 'w') as f:
            f.write(script)
        run_command("chmod +x %s" % script_name)
        return script_name

    def get_output_json(self, processing):
        # job_dir = self.get_working_dir(processing)
        if self.output_json:
            return self.output_json
        elif 'output_json' in self.agent_attributes and self.agent_attributes['output_json']:
            output_json = self.agent_attributes['output_json']
        else:
            output_json = 'idds_output.json'
        return output_json

    def generate_processing_script(self, processing):
        self.output_json = self.get_output_json(processing)

        script_name = self.generate_processing_script_sandbox(processing)
        return script_name, None

    def get_output_files(self, processing):
        return [self.output_json]

    def submit_processing(self, processing):
        if 'job_id' in processing['processing_metadata']:
            pass
        else:
            job_id, errors = self.submit_condor_processing(processing)
            if errors:
                self.add_errors(errors)
            processing['processing_metadata']['job_id'] = job_id
            processing['processing_metadata']['errors'] = str(self.get_errors())

    def abort_processing(self, processing):
        self.tocancel = True

    def parse_processing_outputs(self, processing):
        request_id = processing['request_id']
        workload_id = processing['workload_id']
        processing_id = processing['processing_id']

        if not self.output_json:
            return None, 'Request(%s)_workload(%s)_processing(%s) output_json(%s) is not defined' % (request_id, workload_id,
                                                                                                     processing_id, self.output_json)

        job_dir = self.get_working_dir(processing)
        full_output_json = os.path.join(job_dir, self.output_json)
        if not os.path.exists(full_output_json):
            return None, '%s is not created' % str(full_output_json)
        else:
            try:
                with open(full_output_json, 'r') as f:
                    data = f.read()
                outputs = json.loads(data)
                if not outputs:
                    return outputs, "No points generated: the outputs is empty"
                return outputs, None
            except Exception as ex:
                return None, 'Failed to load the content of %s: %s' % (str(full_output_json), str(ex))

    def poll_processing(self, processing):
        job_status, job_err_msg = self.poll_condor_job_status(processing, processing['processing_metadata']['job_id'])
        processing_outputs = None
        if job_status in [ProcessingStatus.Finished]:
            job_outputs, parser_errors = self.parse_processing_outputs(processing)
            if job_outputs:
                processing_status = ProcessingStatus.Finished
                processing_err = None
                processing_outputs = job_outputs
            else:
                processing_status = ProcessingStatus.Failed
                processing_err = parser_errors
        elif self.tocancel:
            processing_status = ProcessingStatus.Cancelled
            processing_outputs = None
            processing_err = None
        else:
            processing_status = job_status
            processing_err = job_err_msg
        return processing_status, processing_outputs, processing_err

    def poll_processing_updates(self, processing, input_output_maps):
        processing_status, processing_outputs, processing_err = self.poll_processing(processing)

        processing_metadata = processing['processing_metadata']
        if not processing_metadata:
            processing_metadata = {}
        if processing_err:
            processing_err = processing_err.strip()
            if processing_err:
                self.add_errors(processing_err)
        processing_metadata['errors'] = str(self.get_errors())

        update_processing = {'processing_id': processing['processing_id'],
                             'parameters': {'status': processing_status,
                                            'processing_metadata': processing_metadata,
                                            'output_metadata': processing_outputs}}

        updated_contents = []
        return update_processing, updated_contents, {}
