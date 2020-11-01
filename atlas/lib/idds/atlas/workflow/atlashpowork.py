#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2020

import copy
import datetime
import json
import random
import os
import traceback
import uuid

from idds.common import exceptions
from idds.common.constants import (TransformType, CollectionType, CollectionStatus,
                                   ContentStatus, ContentType,
                                   ProcessingStatus, WorkStatus)
# from idds.common.utils import run_command
# from idds.workflow.work import Work
from idds.atlas.workflow.atlascondorwork import ATLASCondorWork


class ATLASHPOWork(ATLASCondorWork):
    def __init__(self, executable=None, arguments=None, parameters=None, setup=None,
                 work_tag='hpo', exec_type='local', sandbox=None, work_id=None,
                 name=None,
                 # primary_input_collection=None, other_input_collections=None,
                 # output_collections=None, log_collections=None,
                 logger=None,
                 agent_attributes=None,
                 method=None,
                 container_workdir=None,
                 opt_space=None, initial_points=None,
                 max_points=None, num_points_per_iteration=10):
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
        :param method: The HPO methd to use. It can be 'nevergrad', 'container' or 'sandbox'.
        :param sandbox: The sandbox to be uploaded or the container path.
        :param executable: The executable command.
        :param arguments: The arguments for the executable.
        :param container_workdir: The working directory for container.
        :param opt_space: The optimization space.
        :param initial_points: The initial points.
        :param max_points: The maximum number of points.
        :param number_points_per_iteration: The number of points to be generated per iteration.
        """
        if not name:
            name = 'hpo.' + datetime.datetime.utcnow().strftime("%Y_%m_%d_%H_%M_%S_%f") + str(random.randint(1, 1000))

        primary_input_collection = {'scope': 'HPO', 'name': name}
        other_input_collections = None
        output_collections = [{'scope': 'HPO', 'name': name + 'output'}]
        log_collections = None

        super(ATLASHPOWork, self).__init__(executable=executable, arguments=arguments,
                                           parameters=parameters, setup=setup, work_type=TransformType.HyperParameterOpt,
                                           exec_type=exec_type, sandbox=sandbox, work_id=work_id,
                                           primary_input_collection=primary_input_collection,
                                           other_input_collections=other_input_collections,
                                           output_collections=output_collections,
                                           log_collections=log_collections,
                                           logger=logger,
                                           agent_attributes=agent_attributes)
        self.method = method
        self.sandbox = sandbox
        self.executable = executable
        self.arguments = arguments
        self.container_workdir = container_workdir
        self.opt_space = opt_space
        self.initial_points = initial_points
        self.max_points = max_points
        self.num_points_per_iteration = num_points_per_iteration

        self.input_json = None
        self.output_json = None

        self.finished_points = 0
        self.points_to_generate = self.num_points_per_iteration
        self.point_index = 0
        self.terminated = False

        if not self.num_points_per_iteration or self.num_points_per_iteration < 0:
            raise exceptions.IDDSException("num_points_per_iteration must be integer bigger than 0")
        self.num_points_per_iteration = int(self.num_points_per_iteration)

    ####### functions for transformer ########   # noqa E266
    ######################################       # noqa E266

    def poll_external_collection(self, coll):
        try:
            if 'status' in coll and coll['status'] in [CollectionStatus.Closed]:
                return coll
            else:
                if 'coll_metadata' not in coll:
                    coll['coll_metadata'] = {}
                coll['coll_metadata']['bytes'] = 0
                coll['coll_metadata']['total_files'] = 0
                coll['coll_metadata']['availability'] = True
                coll['coll_metadata']['events'] = 0
                coll['coll_metadata']['is_open'] = True
                coll['coll_metadata']['run_number'] = None
                coll['coll_metadata']['did_type'] = 'DATASET'
                coll['coll_metadata']['list_all_files'] = False

                if self.terminated:
                    coll['coll_metadata']['is_open'] = False

                if 'is_open' in coll['coll_metadata'] and not coll['coll_metadata']['is_open']:
                    coll_status = CollectionStatus.Closed
                else:
                    coll_status = CollectionStatus.Open
                coll['status'] = coll_status

                if 'did_type' in coll['coll_metadata']:
                    if coll['coll_metadata']['did_type'] == 'DATASET':
                        coll_type = CollectionType.Dataset
                    elif coll['coll_metadata']['did_type'] == 'CONTAINER':
                        coll_type = CollectionType.Container
                    else:
                        coll_type = CollectionType.File
                else:
                    coll_type = CollectionType.Dataset
                coll['coll_type'] = coll_type

                return coll
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            raise exceptions.IDDSException('%s: %s' % (str(ex), traceback.format_exc()))

    def get_input_collections(self):
        # return [self.primary_input_collection] + self.other_input_collections
        colls = [self.primary_input_collection] + self.other_input_collections
        for coll_int_id in colls:
            coll = self.collections[coll_int_id]
            coll = self.poll_external_collection(coll)
            self.collections[coll_int_id] = coll
        return super(ATLASHPOWork, self).get_input_collections()

    def get_input_contents(self):
        """
        Get all input contents from DDM.
        """
        try:
            if self.terminated:
                return []

            ret_files = []
            coll = self.collections[self.primary_input_collection]

            if self.max_points and (self.max_points - self.finished_points < self.num_points_per_iteration):
                self.points_to_generate = self.max_points - self.finished_points

            # call external processing to generate points
            points = self.generate_points()

            for point in points:
                ret_file = {'coll_id': coll['coll_id'],
                            'scope': coll['scope'],
                            'name': str(self.point_index),
                            'bytes': 0,
                            'adler32': None,
                            'min_id': 0,
                            'max_id': 0,
                            'path': json.dumps(point),
                            'content_type': ContentType.File,
                            'content_metadata': {'events': 0}}
                ret_files.append(ret_file)
                self.point_index += 1
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
                out_ip['coll_id'] = self.collections[self.output_collections[0]]['coll_id']
                new_input_output_maps[next_key] = {'inputs': [ip],
                                                   'outputs': [out_ip]}
                next_key += 1

        return new_input_output_maps

    def generate_points(self):
        active_processing = self.get_processing(None)
        if not active_processing:
            if self.points_to_generate > 0:
                active_processing = self.create_processing(None)
                self.terminated = True
                self.set_terminated_msg("Failed to create processing")
                return []
            else:
                self.terminated = True
                self.set_terminated_msg("Number of points is enough(points_to_generate: %s)" % self.points_to_generate)
                return []

        if self.is_processing_terminated(active_processing):
            self.reap_processing(active_processing)
            output_metadata = active_processing['output_metadata']
            if output_metadata:
                return output_metadata
            else:
                self.terminated = True
                processing_metadata = active_processing['processing_metadata']
                errors = None
                if 'errors' in processing_metadata:
                    errors = processing_metadata['errors']
                self.set_terminated_msg("No points generated. Terminating the Work/Transformation. Detailed errors: %s" % errors)
                return []
        return []

    def get_processing(self, input_output_maps):
        if self.active_processings:
            return self.processings[self.active_processings[0]]
        else:
            return None

    def create_processing(self, input_output_maps):
        proc = {'processing_metadata': {'internal_id': str(uuid.uuid1()),
                                        'points_to_generate': self.points_to_generate}}
        self.add_processing_to_processings(proc)
        self.active_processings.append(proc['processing_metadata']['internal_id'])
        return proc

    def get_status_statistics(self, registered_input_output_maps):
        status_statistics = {}
        for map_id in registered_input_output_maps:
            outputs = registered_input_output_maps[map_id]['outputs']

            for content in outputs:
                if content['status'].name not in status_statistics:
                    status_statistics[content['status'].name] = 0
                status_statistics[content['status'].name] += 1
        self.status_statistics = status_statistics
        return status_statistics

    def syn_work_status(self, registered_input_output_maps):
        self.get_status_statistics(registered_input_output_maps)

        if self.is_processings_terminated() and not self.has_new_inputs():
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

    ####### functions for carrier ########     # noqa E266
    ######################################     # noqa E266

    def generate_submit_script_nevergrad(self, processing):
        executable = self.agent_attributes['nevergrad']['executable']
        arguments = self.agent_attributes['nevergrad']['arguments']

        script = "#!/bin/bash\n\n"
        script += "executable=%s\n" % os.path.basename(executable)
        script += "arguments='%s'\n" % str(arguments)
        script += "input_json=%s\n" % str(self.input_json)
        script += "output_json=%s\n" % str(self.agent_attributes['nevergrad']['output_json'])
        script += "\n"

        script += "env\n"
        script += "echo $X509_USER_PROXY\n"
        script += "\n"

        script += "echo 'user id:'\n"
        script += "id\n"
        script += "\n"

        script += 'chmod +x %s\n' % os.path.basename(executable)
        script += "echo '%s' '%s'\n" % (os.path.basename(executable), str(arguments))
        script += './%s %s\n' % (os.path.basename(executable), str(arguments))

        script += '\n'

        long_id = self.get_long_id(processing)
        script_name = 'processing_%s.sh' % long_id
        script_name = os.path.join(self.get_working_dir(processing), script_name)
        with open(script_name, 'w') as f:
            f.write(script)
        return script_name

    def generate_submit_script_container(self, processing):
        script = "#!/bin/bash\n\n"
        script += "executable=%s\n" % str(self.executable)
        script += "arguments=%s\n" % str(self.arguments)
        script += "input_json=%s\n" % str(self.input_json)
        script += "output_json=%s\n" % str(self.agent_attributes['nevergrad']['output_json'])
        script += "\n"

        script += "env\n"
        script += "echo $X509_USER_PROXY\n"
        script += "\n"

        script += "echo 'user id:'\n"
        script += "id\n"
        script += "\n"

        script += "echo '%s' '%s'\n" % (str(self.executable), str(self.arguments))
        script += '%s %s\n' % (str(self.executable), str(self.arguments))

        script += '\n'

        long_id = self.get_long_id(processing)
        script_name = 'processing_%s.sh' % long_id
        script_name = os.path.join(self.get_working_dir(processing), script_name)
        with open(script_name, 'w') as f:
            f.write(script)
        return script_name

    def generate_submit_script_sandbox(self, processing):
        script = "#!/bin/bash\n\n"
        script += "sandbox=%s\n" % str(self.sandbox)
        script += "executable=%s\n" % str(self.executable)
        script += "arguments=%s\n" % str(self.arguments)
        script += "input_json=%s\n" % str(self.input_json)
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

        script += 'chmod +x %s\n' % str(self.executable)
        script += "echo '%s' '%s'\n" % (str(self.executable), str(self.arguments))
        script += '%s %s\n' % (str(self.executable), str(self.arguments))

        script += '\n'

        long_id = self.get_long_id(processing)
        script_name = 'processing_%s.sh' % long_id
        script_name = os.path.join(self.get_working_dir(processing), script_name)
        with open(script_name, 'w') as f:
            f.write(script)
        return script_name

    def generate_processing_script(self, processing):
        if not self.method:
            err_msg = "Processing %s HPO method(%s) is not defined" % (processing['processing_id'], self.method)
            self.logger.error(err_msg)
            self.set_terminated_msg(err_msg)
            self.terminated = True
            return None, err_msg

        self.input_json = self.generate_input_json(processing)
        self.output_json = self.get_output_json(processing)

        if self.method == 'nevergrad':
            return self.generate_processing_script_nevergrad(processing)
        elif self.method == 'container':
            return self.generate_processing_script_container(processing)
        elif self.method == 'sandbox':
            return self.generate_processing_script_sandbox(processing)
        else:
            err_msg = "Processing %s not supported HPO method: %s" % (processing['processing_id'], self.method)
            self.logger.error(err_msg)
            self.set_terminated_msg(err_msg)
            self.terminated = True
            return None, err_msg

    def get_input_files(self, processing):
        return [self.input_json]

    def get_output_files(self, processing):
        return [self.output_json]

    def submit_processing(self, processing):
        if 'job_id' in processing['processing_metadata']:
            pass
        else:
            job_id = self.submit_condor_processing(processing)
            processing['processing_metadata']['job_id'] = job_id

    def parse_processing_outputs(self, processing):
        request_id = processing['request_id']
        workload_id = processing['workload_id']
        processing_id = processing['processing_id']
        long_id = '%s_%s_%s' % (request_id, workload_id, processing_id)

        if not self.output_json:
            return None, 'Request(%s)_workload(%s)_processing(%s) output_json(%s) is not defined' % (request_id, workload_id,
                                                                                                     processing_id, self.output_json)

        job_dir = self.get_job_dir(long_id)
        full_output_json = os.path.join(job_dir, self.output_json)
        if not os.path.exists(full_output_json):
            return None, '%s is not created' % str(full_output_json)
        else:
            try:
                with open(full_output_json, 'r') as f:
                    data = f.read()
                outputs = json.loads(data)
                return outputs, None
            except Exception as ex:
                return None, 'Failed to load the content of %s: %s' % (str(full_output_json), str(ex))

    def poll_processing(self, processing):
        job_status, job_err_msg = self.poll_condor_job_status(processing['processing_id'], processing['processing_metadata']['job_id'])
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
        else:
            processing_status = job_status
            processing_err = job_err_msg
        return processing_status, processing_outputs, processing_err

    def poll_processing_updates(self, processing, input_output_maps):
        processing_status, processing_outputs, processing_err = self.poll_processing(processing)

        processing_metadata = processing['processing_metadata']
        if not processing_metadata:
            processing_metadata = {}
        processing_metadata['errors'] = processing_err

        update_processing = {'processing_id': processing['processing_id'],
                             'parameters': {'status': processing_status,
                                            'processing_metadata': processing_metadata,
                                            'output_metadata': processing_outputs}}

        updated_contents = []
        return update_processing, updated_contents
