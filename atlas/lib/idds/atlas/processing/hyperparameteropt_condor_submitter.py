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
from idds.common.utils import replace_parameters_with_values
from idds.atlas.processing.condor_submitter import CondorSubmitter
from idds.core import (catalog as core_catalog)


class HyperParameterOptCondorSubmitter(CondorSubmitter):
    def __init__(self, workdir, **kwargs):
        super(HyperParameterOptCondorSubmitter, self).__init__(workdir, **kwargs)
        if not hasattr(self, 'max_unevaluated_points'):
            self.max_unevaluated_points = 1
        if not hasattr(self, 'min_unevaluated_points'):
            self.min_unevaluated_points = 1

    def __call__(self, processing, transform, input_collection, output_collection):
        try:
            contents = core_catalog.get_contents_by_coll_id_status(coll_id=output_collection['coll_id'])
            points = []
            unevaluated_points = 0
            for content in contents:
                point = content['content_metadata']['point']
                points.append(point)
                if not content['status'] == ContentStatus.Available:
                    unevaluated_points += 1

            if unevaluated_points >= self.min_unevaluated_points:
                # not submit the job
                processing_metadata = processing['processing_metadata']
                processing_metadata['unevaluated_points'] = unevaluated_points
                ret = {'processing_id': processing['processing_id'],
                       'status': ProcessingStatus.New,
                       'processing_metadata': processing_metadata}
                return ret

            job_dir = self.get_job_dir(processing['processing_id'])
            input_json = 'idds_input.json'
            with open(os.path.join(job_dir, input_json), 'w') as f:
                json.dump(points, f)

            sandbox = None
            if 'sandbox' in transform['transform_metadata']:
                sandbox = transform['transform_metadata']['sandbox']
            executable = transform['transform_metadata']['executable']
            arguments = transform['transform_metadata']['arguments']
            output_json = None
            if 'output_json' in transform['transform_metadata']:
                output_json = transform['transform_metadata']['output_json']

            param_values = {'NUM_POINTS': self.max_unevaluated_points - unevaluated_points,
                            'IN': 'input_json',
                            'OUT': output_json}

            executable = replace_parameters_with_values(executable, param_values)
            arguments = replace_parameters_with_values(arguments, param_values)

            input_list = None
            job_id, outputs = self.submit_job(processing['processing_id'], sandbox, executable, arguments, input_list, input_json, output_json)

            processing_metadata = processing['processing_metadata']
            processing_metadata['job_id'] = job_id
            processing_metadata['submitter'] = self.name
            if not job_id:
                processing_metadata['submit_errors'] = outputs
            else:
                processing_metadata['submit_errors'] = None

            ret = {'processing_id': processing['processing_id'],
                   'status': ProcessingStatus.Submitted,
                   'processing_metadata': processing_metadata}
            return ret
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            raise exceptions.AgentPluginError('%s: %s' % (str(ex), traceback.format_exc()))
