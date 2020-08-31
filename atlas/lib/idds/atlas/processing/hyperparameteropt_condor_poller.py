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
import copy
import datetime
import json
import traceback


from idds.common import exceptions
from idds.common.constants import ContentStatus, ContentType, ProcessingStatus
from idds.atlas.processing.condor_poller import CondorPoller
from idds.core import (catalog as core_catalog)


class HyperParameterOptCondorPoller(CondorPoller):
    def __init__(self, workdir, max_life_time=14 * 24 * 3600, **kwargs):
        super(HyperParameterOptCondorPoller, self).__init__(workdir, **kwargs)
        if not hasattr(self, 'max_unevaluated_points'):
            self.max_unevaluated_points = None
        else:
            self.max_unevaluated_points = int(self.max_unevaluated_points)
        if not hasattr(self, 'min_unevaluated_points'):
            self.min_unevaluated_points = None
        else:
            self.min_unevaluated_points = int(self.min_unevaluated_points)

        self.max_life_time = int(max_life_time)

    def generate_new_contents(self, transform, input_collection, output_collection, points):
        if not isinstance(points, (tuple, list)):
            points = [points]
        avail_points = core_catalog.get_contents_by_coll_id_status(coll_id=output_collection['coll_id'])

        output_contents = []
        i = len(avail_points)
        for point in points:
            content_metadata = {'input_collection_id': input_collection['coll_id']
                                }
            content = {'coll_id': output_collection['coll_id'],
                       # 'scope': output_collection['scope'],
                       'scope': 'hpo',
                       'name': str(i),
                       'min_id': 0,
                       'max_id': 0,
                       'path': json.dumps((point, None)),
                       'status': ContentStatus.New,
                       'content_type': ContentType.PseudoContent,
                       'content_metadata': content_metadata}
            output_contents.append(content)
            i += 1
        return output_contents

    def create_new_processing(self, processing):
        new_processing = {'transform_id': processing['transform_id'],
                          'status': ProcessingStatus.New,
                          'granularity': processing['granularity'],
                          'granularity_type': processing['granularity_type'],
                          'expired_at': processing['expired_at'],
                          'processing_metadata': copy.deepcopy(processing['processing_metadata'])}

        if 'job_status' in new_processing['processing_metadata']:
            del new_processing['processing_metadata']['job_status']
        if 'final_errors' in new_processing['processing_metadata']:
            del new_processing['processing_metadata']['final_errors']
        if 'unevaluated_points' in new_processing['processing_metadata']:
            del new_processing['processing_metadata']['unevaluated_points']
        if 'job_id' in new_processing['processing_metadata']:
            del new_processing['processing_metadata']['job_id']
        if 'submitter' in new_processing['processing_metadata']:
            del new_processing['processing_metadata']['submitter']
        if 'submit_errors' in new_processing['processing_metadata']:
            del new_processing['processing_metadata']['submit_errors']
        if 'output_json' in new_processing['processing_metadata']:
            del new_processing['processing_metadata']['output_json']
        return new_processing

    def get_max_points(self, processing):
        processing_metadata = processing['processing_metadata']
        if 'max_points' in processing_metadata and processing_metadata['max_points']:
            return processing_metadata['max_points']
        return None

    def get_last_touch_time(self, processing, output_contents, updated_files):
        if updated_files:
            last_touch_time = datetime.datetime.utcnow()
        else:
            processing_created_at = processing['created_at']
            last_touch_time = None
            for file in output_contents:
                if not last_touch_time or file['updated_at'] > last_touch_time:
                    last_touch_time = file['updated_at']
            if processing_created_at > last_touch_time:
                last_touch_time = processing_created_at
        return last_touch_time

    def __call__(self, processing, transform, input_collection, output_collection, output_contents):
        try:
            # if 'result_parser' in transform['transform_metadata'] and transform['transform_metadata']['result_parser']

            # The exec part is already finished. No need to poll the job. Here we just need to poll the results.
            if processing['status'] in [ProcessingStatus.FinishedOnExec, ProcessingStatus.FinishedOnExec.value]:
                updated_files = []
                unevaluated_points = 0
                processing_status = processing['status']
                processing_substatus = processing['substatus']
                for file in output_contents:
                    if file['status'] not in [ContentStatus.Available, ContentStatus.Available.value]:
                        path = file['path']
                        point, loss = json.loads(path)
                        if loss is not None:
                            file_status = ContentStatus.Available
                            updated_file = {'content_id': file['content_id'],
                                            'status': file_status,
                                            'scope': file['scope'],
                                            'name': file['name'],
                                            'path': path,
                                            'content_metadata': file['content_metadata']}
                            updated_files.append(updated_file)
                        else:
                            unevaluated_points += 1

                if unevaluated_points == 0:
                    if processing_substatus in [ProcessingStatus.FinishedTerm, ProcessingStatus.FinishedTerm.value]:
                        processing_status = ProcessingStatus.Finished
                    elif processing_substatus in [ProcessingStatus.Failed, ProcessingStatus.Failed.value]:
                        processing_status = ProcessingStatus.Failed
                    elif processing_substatus in [ProcessingStatus.Timeout, ProcessingStatus.Timeout.value]:
                        processing_status = ProcessingStatus.Timeout
                    else:
                        # check whether max_points reached
                        max_points = self.get_max_points(processing)
                        if output_contents is None:
                            output_contents = []
                        if (max_points and len(output_contents) >= max_points):
                            processing_status = ProcessingStatus.Finished
                        else:
                            processing_status = ProcessingStatus.FinishedOnStep
                else:
                    processing_metadata = processing['processing_metadata']
                    # fail processes if it waits too long time
                    current_time = datetime.datetime.utcnow()
                    last_touch_time = self.get_last_touch_time(processing, output_contents, updated_files)
                    life_diff = current_time - last_touch_time
                    life_time = life_diff.total_seconds()
                    if life_time > self.max_life_time:
                        processing_status = ProcessingStatus.TimeOut
                        if processing_metadata['final_errors']:
                            processing_metadata['final_errors'] = "Timeout(%s seconds) to wait evaluation reports" % self.max_life_time + processing_metadata['final_errors']
                        else:
                            processing_metadata['final_errors'] = "Timeout(%s seconds) to wait evaluation reports" % self.max_life_time
                    else:
                        if self.min_unevaluated_points and unevaluated_points >= self.min_unevaluated_points:
                            pass
                        else:
                            # check whether max_points reached
                            max_points = self.get_max_points(processing)
                            if output_contents is None:
                                output_contents = []
                            if (max_points and len(output_contents) >= max_points):
                                pass
                            else:
                                processing_status = ProcessingStatus.FinishedOnStep

                new_processing = None
                if processing_status == ProcessingStatus.FinishedOnStep:
                    new_processing = self.create_new_processing(processing)

                processing_updates = {'status': processing_status,
                                      'substatus': processing['substatus'],
                                      'next_poll_at': datetime.datetime.utcnow() + datetime.timedelta(seconds=self.poll_time_period),
                                      'processing_metadata': processing['processing_metadata']}

                return {'updated_files': updated_files, 'processing_updates': processing_updates,
                        'new_processing': new_processing, 'new_files': []}

            processing_metadata = processing['processing_metadata']
            output_metadata = None
            if 'submitter' in processing_metadata and processing_metadata['submitter'] == self.name:
                job_id = processing_metadata['job_id']
                if job_id:
                    job_status, job_err_msg, std_out_msg, std_err_msg = self.poll_job_status(processing['processing_id'], job_id)
                else:
                    job_status = ProcessingStatus.Failed
                    job_err_msg = 'job_id is cannot be found in the processing metadata.'
                    std_out_msg = None
                    std_err_msg = None

                if std_out_msg:
                    std_out_msg = std_out_msg[-2000:]
                if std_err_msg:
                    std_err_msg = std_err_msg[-2000:]

                new_files = []
                processing_status = ProcessingStatus.Running
                processing_substatus = ProcessingStatus.Running
                if job_status in [ProcessingStatus.Finished, ProcessingStatus.Finished.value]:
                    if 'output_json' in processing_metadata:
                        job_outputs, parser_errors = self.parse_job_outputs(processing['processing_id'], processing_metadata['output_json'])
                        if job_outputs:
                            # processing_status = ProcessingStatus.FinishedOnStep
                            processing_status = ProcessingStatus.FinishedOnExec
                            processing_substatus = ProcessingStatus.Finished
                            processing_metadata['job_status'] = job_status.name
                            processing_metadata['final_errors'] = None
                            # processing_metadata['final_outputs'] = job_outputs
                            output_metadata = job_outputs
                            new_files = self.generate_new_contents(transform, input_collection, output_collection, job_outputs)
                        elif job_outputs is not None and type(job_outputs) in [list] and len(job_outputs) == 0:
                            processing_status = ProcessingStatus.FinishedOnExec
                            processing_substatus = ProcessingStatus.FinishedTerm
                            processing_metadata['job_status'] = job_status.name
                            processing_metadata['final_errors'] = "No new hyperparameters are created." + " stderr: (%s), stdout: (%s)" % (std_out_msg, std_err_msg)
                            # processing_metadata['final_outputs'] = job_outputs
                            output_metadata = job_outputs
                            new_files = self.generate_new_contents(transform, input_collection, output_collection, job_outputs)
                        else:
                            processing_status = ProcessingStatus.FinishedOnExec
                            processing_substatus = ProcessingStatus.Failed
                            processing_metadata['job_status'] = job_status.name
                            err_msg = 'Failed to parse outputs: %s' % str(parser_errors)
                            processing_metadata['final_errors'] = err_msg + " stderr: (%s), stdout: (%s)" % (std_out_msg, std_err_msg)
                    else:
                        processing_status = ProcessingStatus.FinishedOnExec
                        processing_substatus = ProcessingStatus.Failed
                        processing_metadata['job_status'] = job_status.name
                        err_msg = 'Failed to parse outputs: "output_json" file is not defined and it is the only way currently supported to parse the results'
                        processing_metadata['final_errors'] = err_msg + " stderr: (%s), stdout: (%s)" % (std_out_msg, std_err_msg)
                else:
                    if job_status in [ProcessingStatus.Failed, ProcessingStatus.Cancel]:
                        processing_status = ProcessingStatus.FinishedOnExec
                        processing_substatus = ProcessingStatus.Failed
                        processing_metadata['job_status'] = job_status.name
                        err_msg = 'The job failed: %s' % job_err_msg
                        processing_metadata['final_errors'] = err_msg + " stderr: (%s), stdout: (%s)" % (std_out_msg, std_err_msg)

                if processing_status == ProcessingStatus.FinishedOnExec:
                    job_dir = self.get_job_dir(processing['processing_id'])
                    tar_file_name = self.tar_job_logs(job_dir)
                    processing_metadata['job_logs_tar'] = tar_file_name

                updated_files = []
                for file in output_contents:
                    if file['status'] not in [ContentStatus.Available, ContentStatus.Available.value]:
                        path = file['path']
                        point, loss = json.loads(path)
                        if loss is not None:
                            file_status = ContentStatus.Available
                            updated_file = {'content_id': file['content_id'],
                                            'status': file_status,
                                            'scope': file['scope'],
                                            'name': file['name'],
                                            'path': path,
                                            'content_metadata': file['content_metadata']}
                            updated_files.append(updated_file)

                processing_updates = {'status': processing_status,
                                      'substatus': processing_substatus,
                                      'next_poll_at': datetime.datetime.utcnow() + datetime.timedelta(seconds=self.poll_time_period),
                                      'processing_metadata': processing_metadata}
                if output_metadata is not None:
                    processing_updates['output_metadata'] = output_metadata

                # check whether max_points reached
                max_points = self.get_max_points(processing)
                if output_contents is None:
                    output_contents = []
                if max_points and new_files and len(output_contents) + len(new_files) >= max_points:
                    left_points = max_points - len(output_contents)
                    if left_points <= 0:
                        left_points = 0
                    new_files = new_files[:left_points]

            return {'updated_files': updated_files, 'processing_updates': processing_updates,
                    'new_processing': None, 'new_files': new_files}
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            raise exceptions.AgentPluginError('%s: %s' % (str(ex), traceback.format_exc()))
