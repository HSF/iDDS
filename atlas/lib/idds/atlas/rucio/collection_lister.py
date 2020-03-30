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
from idds.common.constants import CollectionType
from idds.atlas.rucio.base_plugin import RucioPluginBase


class CollectionLister(RucioPluginBase):
    def __init__(self, **kwargs):
        super(CollectionLister, self).__init__(**kwargs)

    def __call__(self, scope, name):
        try:
            did_infos = self.client.list_dids(scope, {'name': name}, type='collection', long=True, recursive=False)
            collections = []
            for did_info in did_infos:
                if did_info['did_type'] == 'DATASET':
                    coll_type = CollectionType.Dataset
                elif did_info['did_type'] == 'CONTAINER':
                    coll_type = CollectionType.Container
                else:
                    coll_type = CollectionType.File

                collection = {'scope': did_info['scope'],
                              'name': did_info['name'],
                              'total_files': did_info['length'],
                              'bytes': did_info['bytes'],
                              'coll_type': coll_type}
                collections.append(collection)
            return collections
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            raise exceptions.AgentPluginError('%s: %s' % (str(ex), traceback.format_exc()))
