#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2020 - 2023

import copy
import datetime
import json
import random
import os
import traceback

from idds.common import exceptions
from idds.common.constants import (TransformType, CollectionType, CollectionStatus,
                                   ContentStatus, ContentType,
                                   ProcessingStatus, WorkStatus)
from idds.common.utils import run_command
from idds.common.utils import replace_parameters_with_values
# from idds.workflow.work import Work
from idds.workflow.work import Processing
from idds.atlas.workflow.atlascondorwork import ATLASCondorWork
# from idds.core import (catalog as core_catalog)


class ATLASHPOWork(ATLASCondorWork):
    def __init__(self, executable=None, arguments=None, parameters=None, setup=None,
                 work_tag='hpo', exec_type='local', sandbox=None, work_id=None,
                 name=None,
                 # primary_input_collection=None, other_input_collections=None,
                 # output_collections=None, log_collections=None,
                 logger=None,
                 workload_id=None,
                 agent_attributes=None,
                 method=None,
                 container_workdir=None,
                 output_json=None,
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
            if workload_id:
                name = 'hpo.' + str(workload_id) + "." + datetime.datetime.utcnow().strftime("%Y_%m_%d_%H_%M_%S_%f") + str(random.randint(1, 1000))
            else:
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
        self.unfinished_points = 0

        self.input_json = None
        self.output_json = output_json

        self.finished_points = 0
        self.points_to_generate = self.num_points_per_iteration
        self.point_index = 0
        self.terminated = False
        self.polling_retries = 0

        if not self.num_points_per_iteration or self.num_points_per_iteration < 0:
            raise exceptions.IDDSException("num_points_per_iteration must be integer bigger than 0")
        self.num_points_per_iteration = int(self.num_points_per_iteration)

        if not self.method and self.executable and 'docker' in self.executable:
            self.method = 'docker'

        # if self.agent_attributes and 'atlashpowork' in self.agent_attributes:
        #    self.agent_attributes = self.agent_attributes['atlashpowork']
        # self.logger.info("agent_attributes: %s" % self.agent_attributes)

        # if self.agent_attributes and 'workdir' in self.agent_attributes and self.agent_attributes['workdir']:
        #     self.set_workdir(self.agent_attributes['workdir'])
        # self.logger.info("workdir: %s" % self.get_workdir())

        if agent_attributes:
            self.set_agent_attributes(agent_attributes)

        self.logger = self.get_logger()

    def generating_new_inputs(self):
        return True

    def set_agent_attributes(self, attrs, req_attributes=None):
        self.agent_attributes = attrs

        if self.agent_attributes and 'atlashpowork' in self.agent_attributes:
            self.agent_attributes = self.agent_attributes['atlashpowork']
        self.logger.info("agent_attributes: %s" % self.agent_attributes)

        if self.agent_attributes and 'workdir' in self.agent_attributes and self.agent_attributes['workdir']:
            if req_attributes and 'request_id' in req_attributes and 'workload_id' in req_attributes and 'transform_id' in req_attributes:
                req_dir = 'request_%s_%s/transform_%s' % (req_attributes['request_id'],
                                                          req_attributes['workload_id'],
                                                          req_attributes['transform_id'])
                self.set_workdir(os.path.join(self.agent_attributes['workdir'], req_dir))
        self.logger.info("workdir: %s" % self.get_workdir())

    ####### functions for transformer ########   # noqa E266
    ######################################       # noqa E266

    def poll_external_collection(self, coll):
        try:
            if coll.status in [CollectionStatus.Closed]:
                if not self.terminated:
                    self.logger.info("Work is not terminated, reopen collection")
                    coll.coll_metadata['is_open'] = True
                    coll.status = CollectionStatus.Open
                return coll
            else:
                coll.coll_metadata['bytes'] = 0
                coll.coll_metadata['total_files'] = 0
                coll.coll_metadata['availability'] = True
                coll.coll_metadata['events'] = 0
                coll.coll_metadata['is_open'] = True
                coll.coll_metadata['run_number'] = None
                coll.coll_metadata['did_type'] = 'DATASET'
                coll.coll_metadata['list_all_files'] = False

                if self.terminated:
                    self.logger.info("Work is terminated. Closing input dataset.")
                    coll.coll_metadata['is_open'] = False

                if self.points_to_generate <= 0:
                    self.logger.info("points_to_generate(%s) is equal or smaller than 0. Closing input dataset." % self.points_to_generate)
                    coll.coll_metadata['is_open'] = False

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

    def get_input_collections(self, poll_externel=False):
        # return [self.primary_input_collection] + self.other_input_collections
        colls = [self._primary_input_collection] + self._other_input_collections
        for coll_int_id in colls:
            coll = self.collections[coll_int_id]
            if poll_externel:
                coll = self.poll_external_collection(coll)
            self.collections[coll_int_id] = coll
        return super(ATLASHPOWork, self).get_input_collections()

    def get_input_contents(self, point_index=1):
        """
        Get all input contents from DDM.
        """
        try:
            if self.terminated:
                return []

            if self.unfinished_points > 0:
                return []

            ret_files = []
            coll = self.collections[self._primary_input_collection]

            if self.max_points and (self.max_points - self.finished_points < self.num_points_per_iteration):
                self.points_to_generate = self.max_points - self.finished_points

            # call external processing to generate points
            points = self.generate_points()
            self.logger.info("points generated: %s" % str(points))

            loss = None
            for point in points:
                ret_file = {'coll_id': coll.coll_id,
                            'scope': coll.scope,
                            'name': str(point_index),
                            'bytes': 0,
                            'adler32': None,
                            'min_id': 0,
                            'max_id': 0,
                            'path': json.dumps((point, loss)),
                            'content_type': ContentType.File,
                            'content_metadata': {'events': 0}}
                ret_files.append(ret_file)
                point_index += 1
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

    def get_unfinished_points(self, mapped_input_output_maps):
        counts = 0
        count_finished = 0
        for map_id in mapped_input_output_maps:
            outputs = mapped_input_output_maps[map_id]['outputs']

            for op in outputs:
                if op['status'] in [ContentStatus.New]:
                    counts += 1
                if op['status'] in [ContentStatus.Available]:
                    count_finished += 1
        self.finished_points = count_finished
        return counts

    def get_new_input_output_maps(self, mapped_input_output_maps={}):
        """
        New inputs which are not yet mapped to outputs.

        :param mapped_input_output_maps: Inputs that are already mapped.

        :returns new_input_output_maps  as dict.
        """

        return {}

    def generate_points(self):
        active_processing = self.get_processing(None, without_creating=True)
        if not active_processing:
            if self.points_to_generate > 0:
                active_processing = self.create_processing(None)
                log_str = "max_points: %s, finished_points: %s, points_to_generate: %s, new processing: %s" % (self.max_points,
                                                                                                               self.finished_points,
                                                                                                               self.points_to_generate,
                                                                                                               active_processing)
                self.logger.info(log_str)
                if active_processing:
                    return []
                else:
                    self.polling_retries += 1
                    if self.polling_retries > 3:
                        self.terminated = True
                        self.set_terminated_msg("Failed to create processing")
                    return []
            else:
                self.polling_retries += 1
                if self.polling_retries > 3:
                    self.terminated = True
                self.set_terminated_msg("Number of points is enough(points_to_generate: %s)" % self.points_to_generate)
                return []

        if active_processing and self.is_processing_terminated(active_processing):
            self.logger.info("processing terminated: %s" % active_processing)
            self.reap_processing(active_processing)
            # output_metadata = active_processing['output_metadata']
            output_metadata = active_processing.output_data
            if output_metadata:
                self.polling_retries = 0
                if self.max_points and self.max_points > self.finished_points:
                    self.terminated = False
                return output_metadata
            else:
                self.polling_retries += 1
                if self.polling_retries > 3:
                    self.terminated = True
                # processing_metadata = active_processing['processing_metadata']
                # errors = None
                # if 'errors' in processing_metadata:
                #     errors = processing_metadata['errors']
                errors = active_processing.errors
                self.set_terminated_msg("No points generated. Terminating the Work/Transformation. Detailed errors: %s" % errors)
                return []
        return []

    def get_processing(self, input_output_maps, without_creating=False):
        if self.active_processings:
            return self.processings[self.active_processings[0]]
        else:
            if not without_creating:
                return self.create_processing(input_output_maps)
            pass
        return None

    def create_processing(self, input_output_maps=[]):
        processing_metadata = {'points_to_generate': self.points_to_generate}
        proc = Processing(processing_metadata=processing_metadata)
        self.add_processing_to_processings(proc)
        self.active_processings.append(proc.internal_id)
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

    def syn_work_status(self, registered_input_output_maps, all_updates_flushed=True, output_statistics={}, to_release_input_contents=[]):
        super(ATLASHPOWork, self).syn_work_status(registered_input_output_maps)
        self.get_status_statistics(registered_input_output_maps)

        # self.syn_collection_status()

        if self.is_processings_terminated() and not self.has_new_inputs:
            if not self.is_all_outputs_flushed(registered_input_output_maps):
                self.logger.warn("The processing is terminated. but not all outputs are flushed. Wait to flush the outputs then finish the transform")
                return

            keys = self.status_statistics.keys()
            if len(keys) == 1:
                if ContentStatus.Available.name in keys:
                    self.status = WorkStatus.Finished
                else:
                    self.status = WorkStatus.Failed
            else:
                self.status = WorkStatus.SubFinished
        else:
            self.status = WorkStatus.Transforming

    ####### functions for carrier ########     # noqa E266
    ######################################     # noqa E266

    def generate_processing_script_nevergrad(self, processing):
        executable = self.agent_attributes['nevergrad']['executable']
        arguments = self.agent_attributes['nevergrad']['arguments']

        param_values = {'MAX_POINTS': self.max_points,
                        'NUM_POINTS': self.points_to_generate,
                        'IN': self.input_json,
                        'OUT': self.output_json}
        if 'X509_USER_PROXY' in os.environ and os.environ['X509_USER_PROXY']:
            proxy_filename = os.path.basename(os.environ['X509_USER_PROXY'])
            param_values['X509_USER_PROXY_FULLNAME'] = os.environ['X509_USER_PROXY']
            param_values['X509_USER_PROXY_BASENAME'] = proxy_filename

        arguments = replace_parameters_with_values(arguments, param_values)

        script = "#!/bin/bash\n\n"
        script += "executable=%s\n" % os.path.basename(executable)
        script += "arguments='%s'\n" % str(arguments)
        script += "input_json=%s\n" % str(self.input_json)
        script += "output_json=%s\n" % str(self.output_json)
        script += "\n"

        script += "env\n"
        script += "echo $X509_USER_PROXY\n"
        script += "\n"

        script += "echo 'user id:'\n"
        script += "id\n"
        script += "\n"

        script += "echo '%s' '%s'\n" % (os.path.basename(executable), str(arguments))
        script += '%s %s\n' % (os.path.basename(executable), str(arguments))

        script += '\n'

        long_id = self.get_long_id(processing)
        script_name = 'processing_%s.sh' % long_id
        script_name = os.path.join(self.get_working_dir(processing), script_name)
        with open(script_name, 'w') as f:
            f.write(script)
        run_command("chmod +x %s" % script_name)
        return script_name

    def generate_processing_script_container(self, processing):
        param_values = {'MAX_POINTS': self.max_points,
                        'NUM_POINTS': self.points_to_generate,
                        'IN': self.input_json,
                        'OUT': self.output_json}
        proxy_filename = 'x509up'
        if 'X509_USER_PROXY' in os.environ and os.environ['X509_USER_PROXY']:
            proxy_filename = os.path.basename(os.environ['X509_USER_PROXY'])
            param_values['X509_USER_PROXY_FULLNAME'] = os.environ['X509_USER_PROXY']
            param_values['X509_USER_PROXY_BASENAME'] = proxy_filename

        executable = replace_parameters_with_values(self.executable, param_values)
        arguments = replace_parameters_with_values(self.arguments, param_values)

        script = "#!/bin/bash\n\n"
        script += "executable=%s\n" % str(executable)
        script += "arguments=%s\n" % str(arguments)
        script += "input_json=%s\n" % str(self.input_json)
        script += "output_json=%s\n" % str(self.output_json)
        script += "\n"

        script += "env\n"
        script += "echo $X509_USER_PROXY\n"
        script += "\n"

        script += "echo 'user id:'\n"
        script += "id\n"
        script += "\n"

        if self.sandbox and 'docker' in executable:
            arguments = 'run --rm -v $(pwd):%s -v /cvmfs:/cvmfs -e X509_USER_PROXY=%s/%s %s ' % (self.container_workdir, self.container_workdir, proxy_filename, self.sandbox) + arguments

        script += "echo '%s' '%s'\n" % (str(executable), str(arguments))
        script += '%s %s\n' % (str(executable), str(arguments))

        if self.sandbox and 'docker' in executable:
            script += 'docker image rm -f %s\n' % self.sandbox

        script += '\n'

        long_id = self.get_long_id(processing)
        script_name = 'processing_%s.sh' % long_id
        script_name = os.path.join(self.get_working_dir(processing), script_name)
        with open(script_name, 'w') as f:
            f.write(script)
        run_command("chmod +x %s" % script_name)
        return script_name

    def generate_processing_script_sandbox(self, processing):
        param_values = {'MAX_POINTS': self.max_points,
                        'NUM_POINTS': self.points_to_generate,
                        'IN': self.input_json,
                        'OUT': self.output_json}
        if 'X509_USER_PROXY' in os.environ and os.environ['X509_USER_PROXY']:
            proxy_filename = os.path.basename(os.environ['X509_USER_PROXY'])
            param_values['X509_USER_PROXY_FULLNAME'] = os.environ['X509_USER_PROXY']
            param_values['X509_USER_PROXY_BASENAME'] = proxy_filename

        executable = replace_parameters_with_values(self.executable, param_values)
        arguments = replace_parameters_with_values(self.arguments, param_values)

        script = "#!/bin/bash\n\n"
        script += "sandbox=%s\n" % str(self.sandbox)
        script += "executable=%s\n" % str(executable)
        script += "arguments=%s\n" % str(arguments)
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

        script += 'chmod +x %s\n' % str(executable)
        script += "echo '%s' '%s'\n" % (str(executable), str(arguments))
        script += '%s %s\n' % (str(executable), str(arguments))

        script += '\n'

        long_id = self.get_long_id(processing)
        script_name = 'processing_%s.sh' % long_id
        script_name = os.path.join(self.get_working_dir(processing), script_name)
        with open(script_name, 'w') as f:
            f.write(script)
        run_command("chmod +x %s" % script_name)
        return script_name

    def generate_input_json(self, processing):
        try:
            from idds.core import (catalog as core_catalog)

            output_collection = self.get_output_collections()[0]
            contents = core_catalog.get_contents_by_coll_id_status(coll_id=output_collection.coll_id)
            points = []
            for content in contents:
                # point = content['content_metadata']['point']
                point = json.loads(content['path'])
                points.append(point)

            job_dir = self.get_working_dir(processing)
            if 'input_json' in self.agent_attributes and self.agent_attributes['input_json']:
                input_json = self.agent_attributes['input_json']
            else:
                input_json = 'idds_input.json'
            opt_points = {'points': points, 'opt_space': self.opt_space}
            with open(os.path.join(job_dir, input_json), 'w') as f:
                json.dump(opt_points, f)
            return input_json
        except Exception as e:
            raise Exception("Failed to generate idds inputs for HPO: %s" % str(e))

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
        if not self.method:
            err_msg = "Processing %s HPO method(%s) is not defined" % (processing['processing_id'], self.method)
            self.logger.error(err_msg)
            self.set_terminated_msg(err_msg)
            self.terminated = True
            return None, err_msg

        self.input_json = self.generate_input_json(processing)
        self.output_json = self.get_output_json(processing)

        if self.method == 'nevergrad':
            script_name = self.generate_processing_script_nevergrad(processing)
            return script_name, None
        elif self.method in ['container', 'docker']:
            script_name = self.generate_processing_script_container(processing)
            return script_name, None
        elif self.method == 'sandbox':
            script_name = self.generate_processing_script_sandbox(processing)
            return script_name, None
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
        proc = processing['processing_metadata']['processing']
        if proc.external_id:
            # if 'job_id' in processing['processing_metadata']:
            pass
            return True, None, None
        else:
            job_id, errors = self.submit_condor_processing(processing)
            # processing['processing_metadata']['job_id'] = job_id
            # processing['processing_metadata']['errors'] = errors
            if job_id:
                proc.external_id = job_id
                proc.submitted_at = datetime.datetime.utcnow()
                return True, None, None
            else:
                proc.errors = errors
                return False, None, errors

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
        try:
            proc = processing['processing_metadata']['processing']
            job_status, job_err_msg = self.poll_condor_job_status(processing, proc.external_id)
            self.logger.info("poll_condor_job_status: (status: %s, error: %s" % (str(job_status), str(job_err_msg)))
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
            elif job_status in [ProcessingStatus.Failed]:
                processing_status = job_status
                processing_status = job_status
                processing_err = job_err_msg
            return processing_status, processing_outputs, processing_err
        except Exception as ex:
            self.logger.error("processing_id %s exception: %s, %s" % (processing['processing_id'], str(ex), traceback.format_exc()))
            proc.retries += 1
            if proc.retries > 10:
                processing_status = ProcessingStatus.Failed
            else:
                processing_status = ProcessingStatus.Running
            return processing_status, None, None

    def generate_new_input_output_maps(self, input_output_maps, points):
        unfinished_mapped = self.get_unfinished_points(input_output_maps)
        self.unfinished_points = unfinished_mapped

        mapped_keys = input_output_maps.keys()
        if mapped_keys:
            next_key = max(mapped_keys) + 1
        else:
            next_key = 1

        coll = self.collections[self._primary_input_collection]
        new_input_output_maps = {}
        loss = None
        for point in points:
            content = {'coll_id': coll.coll_id,
                       'scope': coll.scope,
                       'name': str(next_key),
                       'bytes': 0,
                       'adler32': None,
                       'min_id': 0,
                       'max_id': 0,
                       'path': json.dumps((point, loss)),
                       'content_type': ContentType.File,
                       'content_metadata': {'events': 0}}
            out_content = copy.deepcopy(content)
            content['status'] = ContentStatus.Available
            content['substatus'] = ContentStatus.Available
            out_content['coll_id'] = self.collections[self._primary_output_collection].coll_id
            new_input_output_maps[next_key] = {'inputs': [content],
                                               'outputs': [out_content],
                                               'inputs_dependency': [],
                                               'logs': []}
            next_key += 1
        return new_input_output_maps

    def poll_processing_updates(self, processing, input_output_maps, log_prefix=''):
        proc = processing['processing_metadata']['processing']
        new_input_output_maps = {}
        updated_contents = []
        if proc.external_id:
            processing_status, processing_outputs, processing_err = self.poll_processing(processing)

            proc.errors = processing_err

            if processing_status in [ProcessingStatus.Finished]:
                proc.external_id = None
                if processing_outputs:
                    self.logger.info(log_prefix + "Processing finished with outputs")
                    points = processing_outputs
                    new_input_output_maps = self.generate_new_input_output_maps(input_output_maps, points)
                    proc.old_external_id.append(proc.external_id)
                    processing_status = ProcessingStatus.Running
                else:
                    self.status = WorkStatus.Finished
                    self.logger.info(log_prefix + "Processing finished and output is empty (output: %s)" % str(processing_outputs))
            elif processing_status in [ProcessingStatus.Failed, ProcessingStatus.Cancelled]:
                proc.external_id = None
                proc.retries += 1
                if proc.retries <= 3:
                    self.logger.warn(log_prefix + "Processing terminated (status: %s, retries: %s), retries <=3, new status: %s" % (processing_status,
                                                                                                                                    proc.retries,
                                                                                                                                    ProcessingStatus.Running))
                    processing_status = ProcessingStatus.Running
                else:
                    self.status = WorkStatus.Failed
                    self.logger.warn(log_prefix + "Processing terminated (status: %s, retries: %s)" % (processing_status, proc.retries))
        else:
            unfinished_mapped = self.get_unfinished_points(input_output_maps)
            self.unfinished_points = unfinished_mapped

            if self.unfinished_points > 0:
                processing_status = ProcessingStatus.Running
            else:
                self.logger.warn(log_prefix + "max_points: %s, finished_points: %s, unfinished_points: %s, num_points_per_iteration: %s" % (self.max_points,
                                                                                                                                            self.finished_points,
                                                                                                                                            self.unfinished_points,
                                                                                                                                            self.num_points_per_iteration))
                if self.max_points and (self.max_points - self.finished_points < self.num_points_per_iteration):
                    self.points_to_generate = self.max_points - self.finished_points
                if self.points_to_generate <= 0:
                    processing_status = ProcessingStatus.Finished
                    self.status = WorkStatus.Finished
                else:
                    status, workload_id, error = self.submit_processing(processing)
                    processing_status = ProcessingStatus.Running

        return processing_status, updated_contents, new_input_output_maps, [], {}

    def abort_processing(self, processing, log_prefix=''):
        self.logger.info(log_prefix + "abort processing")

    def resume_processing(self, processing, log_prefix=''):
        self.logger.info(log_prefix + "resume processing")
