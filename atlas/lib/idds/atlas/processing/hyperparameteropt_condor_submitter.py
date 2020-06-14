#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2020


"""
Class of activelearning condor plubin
"""
import datetime
import os
import json
import traceback


from idds.common import exceptions
from idds.common.constants import ContentStatus, ProcessingStatus
from idds.common.utils import replace_parameters_with_values
from idds.atlas.processing.condor_submitter import CondorSubmitter
from idds.core import (catalog as core_catalog)


class HyperParameterOptCondorSubmitter(CondorSubmitter):
    def __init__(self, workdir, **kwargs):
        super(HyperParameterOptCondorSubmitter, self).__init__(workdir, **kwargs)
        if not hasattr(self, 'max_unevaluated_points'):
            self.max_unevaluated_points = 10
        else:
            self.max_unevaluated_points = int(self.max_unevaluated_points)
        if not hasattr(self, 'min_unevaluated_points'):
            self.min_unevaluated_points = 1
        else:
            self.min_unevaluated_points = int(self.min_unevaluated_points)
        if not hasattr(self, 'max_points'):
            self.max_points = 1000000000
        else:
            self.max_points = int(self.max_points)

    def get_max_points(self, transform_metadata):
        if 'max_points' in transform_metadata:
            max_points = transform_metadata['max_points']
        else:
            max_points = self.max_points
        return max_points

    def get_executable_arguments_for_method(self, transform_metadata, input_json, unevaluated_points):
        method = transform_metadata['method']
        method_executable = '%s.executable' % method
        method_arguments = '%s.arguments' % method
        method_output_json = '%s.output_json' % method
        method_should_transfer_executable = '%s.should_transfer_executable' % method
        if not hasattr(self, method_executable) or not hasattr(self, method_arguments) or not hasattr(self, method_output_json):
            return -1, "method %s is not support" % method, None, None, None, None, None
        else:
            executable = getattr(self, method_executable)
            arguments = getattr(self, method_arguments)
            output_json = getattr(self, method_output_json)

            should_transfer_executable = False
            if hasattr(self, method_should_transfer_executable):
                should_transfer_executable = getattr(self, method_should_transfer_executable)
                if type(should_transfer_executable) in [str]:
                    should_transfer_executable = should_transfer_executable.upper()
            if should_transfer_executable in [True, 'TRUE']:
                should_transfer_executable = True
            else:
                should_transfer_executable = False

            max_points = self.get_max_points(transform_metadata)
            if 'num_points_per_generation' in transform_metadata:
                num_points = transform_metadata['num_points_per_generation']
            else:
                num_points = self.max_unevaluated_points - unevaluated_points

            param_values = {'MAX_POINTS': max_points,
                            'NUM_POINTS': num_points,
                            'IN': input_json,
                            'OUT': output_json}

            executable = replace_parameters_with_values(executable, param_values)
            arguments = replace_parameters_with_values(arguments, param_values)
        return 0, None, None, executable, arguments, input_json, output_json, should_transfer_executable

    def get_executable_arguments_for_sandbox(self, transform_metadata, input_json, unevaluated_points):
        sandbox = None
        if 'sandbox' in transform_metadata:
            sandbox = transform_metadata['sandbox']
        executable = transform_metadata['executable'].strip()
        arguments = transform_metadata['arguments'].strip()

        if executable == 'docker' and sandbox:
            if 'workdir' in transform_metadata:
                docker_workdir = transform_metadata['workdir']
            else:
                docker_workdir = None

            arg_pre = 'run --rm -v $(pwd):%s %s ' % (docker_workdir, sandbox)
            arguments = arg_pre + arguments
            sandbox = None

        output_json = None
        if 'output_json' in transform_metadata:
            output_json = transform_metadata['output_json']
        max_points = self.get_max_points(transform_metadata)
        if 'num_points_per_generation' in transform_metadata:
            num_points = transform_metadata['num_points_per_generation']
        else:
            num_points = self.max_unevaluated_points - unevaluated_points

        param_values = {'MAX_POINTS': max_points,
                        'NUM_POINTS': num_points,
                        'IN': input_json,
                        'OUT': output_json}

        executable = replace_parameters_with_values(executable, param_values)
        arguments = replace_parameters_with_values(arguments, param_values)
        return 0, None, sandbox, executable, arguments, input_json, output_json, False

    def __call__(self, processing, transform, input_collection, output_collection):
        try:
            contents = core_catalog.get_contents_by_coll_id_status(coll_id=output_collection['coll_id'])
            points = []
            unevaluated_points = 0
            for content in contents:
                # point = content['content_metadata']['point']
                point = json.loads(content['path'])
                points.append(point)
                if not content['status'] == ContentStatus.Available:
                    unevaluated_points += 1

            job_dir = self.get_job_dir(processing['processing_id'])
            input_json = 'idds_input.json'
            opt_space = None
            opt_points = {'points': points}
            if 'opt_space' in transform['transform_metadata']:
                opt_space = transform['transform_metadata']['opt_space']
                opt_points['opt_space'] = opt_space
            else:
                opt_points['opt_space'] = None
            with open(os.path.join(job_dir, input_json), 'w') as f:
                json.dump(opt_points, f)

            if 'method' in transform['transform_metadata'] and transform['transform_metadata']['method']:
                status, errors, sandbox, executable, arguments, input_json, output_json, should_transfer_executable = self.get_executable_arguments_for_method(transform['transform_metadata'], input_json, unevaluated_points)
            else:
                status, errors, sandbox, executable, arguments, input_json, output_json, should_transfer_executable = self.get_executable_arguments_for_sandbox(transform['transform_metadata'], input_json, unevaluated_points)

            if status != 0:
                processing_metadata = processing['processing_metadata']
                processing_metadata['job_id'] = None
                processing_metadata['submitter'] = self.name
                processing_metadata['submit_errors'] = errors
                processing_metadata['output_json'] = output_json
                processing_metadata['max_points'] = self.get_max_points(transform['transform_metadata'])
                # processing_metadata['job_dir'] = job_dir
                ret = {'processing_id': processing['processing_id'],
                       'status': ProcessingStatus.Submitted,
                       'processing_metadata': processing_metadata}
            else:
                input_list = None
                job_id, outputs = self.submit_job(processing['processing_id'], sandbox, executable, arguments, input_list, input_json, output_json, should_transfer_executable)

                processing_metadata = processing['processing_metadata']
                processing_metadata['job_id'] = job_id
                processing_metadata['submitter'] = self.name
                processing_metadata['output_json'] = output_json
                processing_metadata['max_points'] = self.get_max_points(transform['transform_metadata'])
                # processing_metadata['job_dir'] = job_dir
                if not job_id:
                    processing_metadata['submit_errors'] = outputs
                else:
                    processing_metadata['submit_errors'] = None

                ret = {'processing_id': processing['processing_id'],
                       'status': ProcessingStatus.Submitted,
                       'next_poll_at': datetime.datetime.utcnow() + datetime.timedelta(seconds=self.poll_time_period),
                       'processing_metadata': processing_metadata}
            return ret
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            raise exceptions.AgentPluginError('%s: %s' % (str(ex), traceback.format_exc()))
