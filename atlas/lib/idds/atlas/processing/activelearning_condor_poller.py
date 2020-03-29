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
import traceback


from idds.common import exceptions
from idds.common.constants import ContentStatus, ProcessingStatus
from idds.common.utils import run_command
from idds.atlas.processing.base_plugin import ProcessingPluginBase


class ActiveLearningCondorPoller(ProcessingPluginBase):
    def __init__(self, workdir, **kwargs):
        super(ActiveLearningCondorPoller, self).__init__(**kwargs)
        self.workdir = workdir
        self.name = 'condor'

    def __call__(self, processing, transform, input_collection, output_collection, output_contents):
        try:
            # if 'result_parser' in transform['transform_metadata'] and transform['transform_metadata']['result_parser']

            processing_metadata = processing['processing_metadata']
            output_metadata = None
            if 'submitter' in processing_metadata and processing_metadata['submitter'] == self.name:
                job_id = processing_metadata['job_id']
                if job_id:
                    job_status, job_err_msg = self.poll_job_status(processing['processing_id'], job_id)
                else:
                    job_status = ProcessingStatus.Failed
                    job_err_msg = 'job_id is cannot be found in the processing metadata.'

                processing_status = ProcessingStatus.Running
                if job_status in [ProcessingStatus.Finished, ProcessingStatus.Finished.value]:
                    if 'output_json' in transform['transform_metadata']:
                        job_outputs, parser_errors = self.parse_job_outputs(processing['processing_id'], transform['transform_metadata']['output_json'])
                        if job_outputs:
                            processing_status = ProcessingStatus.Finished
                            processing_metadata['job_status'] = job_status.name
                            processing_metadata['final_error'] = None
                            # processing_metadata['final_outputs'] = job_outputs
                            output_metadata = job_outputs
                        else:
                            processing_status = ProcessingStatus.Failed
                            processing_metadata['job_status'] = job_status.name
                            err_msg = 'Failed to parse outputs: %s' % str(parser_errors)
                            processing_metadata['final_errors'] = err_msg
                    else:
                        processing_status = ProcessingStatus.Failed
                        processing_metadata['job_status'] = job_status.name
                        err_msg = 'Failed to parse outputs: "output_json" file is not defined and it is the only way currently supported to parse the results'
                        processing_metadata['final_errors'] = err_msg
                else:
                    if job_status in [ProcessingStatus.Failed, ProcessingStatus.Cancel]:
                        processing_status = ProcessingStatus.Failed
                        processing_metadata['job_status'] = job_status.name
                        err_msg = 'The job failed: %s' % job_err_msg
                        processing_metadata['final_errors'] = err_msg
                    else:
                        processing_status = ProcessingStatus.Failed
                        processing_metadata['job_status'] = job_status.name
                        err_msg = 'The job failed with unknown error: %s' % job_err_msg
                        processing_metadata['final_errors'] = err_msg

                updated_files = []
                if processing_status in [ProcessingStatus.Finished, ProcessingStatus.Failed]:
                    if processing_status == ProcessingStatus.Finished:
                        file_status = ContentStatus.Available
                        # content_metadata = processing_metadata['final_outputs']
                        content_metadata = output_metadata
                    if processing_status == ProcessingStatus.Failed:
                        file_status = ContentStatus.Failed
                        content_metadata = None
                    for file in output_contents:
                        updated_file = {'content_id': file['content_id'],
                                        'status': file_status,
                                        'scope': file['scope'],
                                        'name': file['name'],
                                        'path': None,
                                        'content_metadata': content_metadata}
                        updated_files.append(updated_file)
                processing_updates = {'status': processing_status,
                                      'processing_metadata': processing_metadata}
                if output_metadata:
                    processing_updates['output_metadata'] = output_metadata

            return {'updated_files': updated_files, 'processing_updates': processing_updates}
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            raise exceptions.AgentPluginError('%s: %s' % (str(ex), traceback.format_exc()))

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
