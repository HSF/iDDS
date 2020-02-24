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
from idds.common.constants import CollectionStatus
from idds.atlas.rucio.base_plugin import RucioPluginBase


class CollectionMetadataReader(RucioPluginBase):
    def __init__(self, **kwargs):
        super(CollectionMetadataReader, self).__init__(**kwargs)

    def __call__(self, scope, name):
        try:
            meta = {}
            did_meta = self.client.get_metadata(scope=scope, name=name)
            meta = {'bytes': did_meta['bytes'],
                    'availability': did_meta['availability'],
                    'events': did_meta['events'],
                    'is_open': did_meta['is_open'],
                    'run_number': did_meta['run_number'],
                    'status': CollectionStatus.Open if did_meta['is_open'] else CollectionStatus.Closed,
                    'total_files': did_meta['length']}
            return meta
        except Exception as error:
            self.logger.error(error)
            self.logger.error(traceback.format_exc())
            raise exceptions.AgentPluginError('%s: %s' % (str(error), traceback.format_exc()))
