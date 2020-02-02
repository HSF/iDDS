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

from rucio.common.exception import DataIdentifierNotFound

from idds.common import exceptions
from idds.atlas.rucio.base_plugin import RucioPluginBase


class ContentsRegister(RucioPluginBase):
    def __init__(self, **kwargs):
        super(ContentsRegister, self).__init__(**kwargs)

    def __call__(self, coll_scope, coll_name, account=None, lifetime=7 * 24 * 3600, rse=None, files=[]):
        try:
            try:
                self.client.get_did(scope=coll_scope, name=coll_name)
            except DataIdentifierNotFound:
                self.client.add_dataset(scope=coll_scope,
                                        name=coll_name,
                                        rules=[{'account': account,
                                                'copies': 1,
                                                'rse_expression': rse,
                                                'grouping': 'DATASET',
                                                'lifetime': lifetime}])

            new_files = []
            for file in files:
                new_file = {'scope': file['scope'],
                            'name': file['name'],
                            'state': 'A',   # available
                            'bytes': file['content_size'],
                            'adler32': file['adler32'],
                            'pfn': file['pfn']}
                new_files.append(new_file)

            self.client.add_replicas(rse=rse, files=new_files)
            self.client.add_files_to_dataset(scope=coll_scope,
                                             name=coll_name,
                                             files=[new_files],
                                             rse=rse)
        except Exception as error:
            self.logger.error(error)
            self.logger.error(traceback.format_exc())
            raise exceptions.AgentPluginError('%s: %s' % (str(error), traceback.format_exc()))
