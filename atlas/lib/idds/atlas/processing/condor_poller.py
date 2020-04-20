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
import os
import json


from idds.common import exceptions
from idds.common.constants import ProcessingStatus
from idds.common.utils import run_command
from idds.atlas.processing.base_plugin import ProcessingPluginBase


class CondorPoller(ProcessingPluginBase):
    def __init__(self, workdir, **kwargs):
        super(CondorPoller, self).__init__(**kwargs)
        self.workdir = workdir
        self.name = 'condor'

    def __call__(self, processing, transform, input_collection, output_collection, output_contents):
        raise exceptions.AgentPluginError('NotImplemented')

    def get_job_dir(self, processing_id):
        job_dir = 'processing_%s' % processing_id
        job_dir = os.path.join(self.workdir, job_dir)
        if not os.path.exists(job_dir):
            os.makedirs(job_dir)
        return job_dir

    def poll_job_status(self, processing_id, job_id):
        # 0 Unexpanded     U
        # 1 Idle           I
        # 2 Running        R
        # 3 Removed        X
        # 4 Completed      C
        # 5 Held           H
        # 6 Submission_err E
        cmd = "condor_q -format '%s' ClusterId  -format ' %s' Processing_id -format ' %s' JobStatus " + str(job_id)
        status, output, error = run_command(cmd)
        self.logger.debug("poll job status: %s" % cmd)
        self.logger.debug("status: %s, output: %s, error: %s" % (status, output, error))
        if status == 0 and len(output) == 0:
            cmd = "condor_history -format '%s' ClusterId  -format ' %s' Processing_id -format ' %s' JobStatus " + str(job_id)
            status, output, error = run_command(cmd)
            self.logger.debug("poll job status: %s" % cmd)
            self.logger.debug("status: %s, output: %s, error: %s" % (status, output, error))

        ret_err = None
        if status == 0:
            lines = output.split('\n')
            for line in lines:
                c_job_id, c_processing_id, c_job_status = line.split(' ')
                if str(c_job_id) != str(job_id):
                    continue

                c_processing_id = int(c_processing_id)
                c_job_status = int(c_job_status)
                if c_processing_id != processing_id:
                    final_job_status = ProcessingStatus.Failed
                    ret_err = 'jobid and the processing_id mismatched'
                else:
                    job_status = c_job_status
                    if job_status < 2:
                        final_job_status = ProcessingStatus.Submitted
                    elif job_status == 2:
                        final_job_status = ProcessingStatus.Running
                    elif job_status == 3:
                        final_job_status = ProcessingStatus.Cancel
                    elif job_status == 4:
                        final_job_status = ProcessingStatus.Finished
                    else:
                        final_job_status = ProcessingStatus.Failed
        else:
            final_job_status = ProcessingStatus.Submitted
        return final_job_status, ret_err

    def parse_job_outputs(self, processing_id, output_json):
        job_dir = self.get_job_dir(processing_id)
        full_output_json = os.path.join(job_dir, output_json)
        if not os.path.exists(full_output_json):
            return None, '%s is not created' % str(output_json)
        else:
            try:
                with open(full_output_json, 'r') as f:
                    data = f.read()
                outputs = json.loads(data)
                return outputs, None
            except Exception as ex:
                return None, 'Failed to load the content of %s: %s' % (str(output_json), str(ex))
