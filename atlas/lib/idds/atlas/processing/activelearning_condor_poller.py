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
import traceback


from idds.common import exceptions
from idds.common.constants import ContentStatus, ProcessingStatus
from idds.atlas.processing.condor_poller import CondorPoller


class ActiveLearningCondorPoller(CondorPoller):
    def __init__(self, workdir, **kwargs):
        super(ActiveLearningCondorPoller, self).__init__(workdir, **kwargs)

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
                                      'next_poll_at': datetime.datetime.utcnow() + datetime.timedelta(seconds=self.poll_time_period),
                                      'processing_metadata': processing_metadata}
                if output_metadata:
                    processing_updates['output_metadata'] = output_metadata

            return {'updated_files': updated_files, 'processing_updates': processing_updates}
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            raise exceptions.AgentPluginError('%s: %s' % (str(ex), traceback.format_exc()))
