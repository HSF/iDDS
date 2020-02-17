#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019


"""
Class of collection lister plubin
"""

import traceback


from idds.common import exceptions
from idds.common.constants import ContentStatus, ProcessingStatus
from idds.atlas.processing.base_plugin import ProcessingPluginBase


class StageInPoller(ProcessingPluginBase):
    def __init__(self, **kwargs):
        super(StageInPoller, self).__init__(**kwargs)

    def __call__(self, processing, transform, input_collection, output_collection, output_contents):
        try:
            processing_metadata = processing['processing_metadata']
            rule_id = processing_metadata['rule_id']

            if 'rule_poller' not in self.plugins:
                raise exceptions.AgentPluginError('Plugin rule_poller is required')
            rule, replicases_status = self.plugins['rule_poller'](rule_id)

            updated_files = []
            processing_updates = {}
            if replicases_status:
                file_status_statistics = {}
                for file in output_contents:
                    file_key = '%s:%s' % (file['scope'], file['name'])
                    if file_key in replicases_status:
                        new_file_status = replicases_status[file_key]
                        if not new_file_status == file['status']:
                            file['status'] = new_file_status

                            updated_file = {'content_id': file['content_id'],
                                            'status': new_file_status,
                                            'scope': file['scope'],
                                            'name': file['name'],
                                            'path': None}
                            updated_files.append(updated_file)

                    if file['status'] not in file_status_statistics:
                        file_status_statistics[file['status']] = 0
                    file_status_statistics[file['status']] += 1

                file_status_keys = list(file_status_statistics.keys())
                if len(file_status_keys) == 1:
                    if file_status_keys == [ContentStatus.Available] and rule['state'] == 'OK':
                        processing_status = ProcessingStatus.Finished
                    elif file_status_keys == [ContentStatus.Failed]:
                        processing_status = ProcessingStatus.Failed
                else:
                    processing_status = ProcessingStatus.Running

                file_statusvalue_statistics = {}
                for key in file_status_statistics:
                    file_statusvalue_statistics[key.name] = file_status_statistics[key]

                processing_metadata['content_status_statistics'] = file_statusvalue_statistics

                processing_updates = {'status': processing_status,
                                      'processing_metadata': processing_metadata}

            return {'updated_files': updated_files, 'processing_updates': processing_updates}
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            raise exceptions.AgentPluginError('%s: %s' % (str(ex), traceback.format_exc()))
