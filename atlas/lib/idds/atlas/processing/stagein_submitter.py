#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019 - 2020


"""
Class of collection lister plubin
"""

import datetime
import traceback


from idds.common import exceptions
from idds.common.constants import ProcessingStatus
from idds.atlas.processing.base_plugin import ProcessingPluginBase


class StageInSubmitter(ProcessingPluginBase):
    def __init__(self, **kwargs):
        super(StageInSubmitter, self).__init__(**kwargs)

    def __call__(self, processing, transform, input_collection, output_collection):
        try:
            processing_metadata = processing['processing_metadata']
            if 'rule_id' in processing_metadata:
                ret = {'processing_id': processing['processing_id'],
                       'status': ProcessingStatus.Submitted}
            else:
                if 'rule_submitter' not in self.plugins:
                    raise exceptions.AgentPluginError('Plugin rule_submitter is required')
                rule_id = self.plugins['rule_submitter'](processing, transform, input_collection)
                processing_metadata['rule_id'] = rule_id
                ret = {'processing_id': processing['processing_id'],
                       'status': ProcessingStatus.Submitted,
                       'next_poll_at': datetime.datetime.utcnow() + datetime.timedelta(seconds=self.poll_time_period),
                       'processing_metadata': processing_metadata}
            return ret
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            raise exceptions.AgentPluginError('%s: %s' % (str(ex), traceback.format_exc()))
