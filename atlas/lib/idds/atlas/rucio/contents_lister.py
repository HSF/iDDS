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
from idds.atlas.rucio.base_plugin import RucioPluginBase


class ContentsLister(RucioPluginBase):
    def __init__(self, **kwargs):
        super(ContentsLister, self).__init__(**kwargs)

    def __call__(self, scope, name):
        try:
            ret_files = []
            files = self.client.list_files(scope=scope, name=name)
            for file in files:
                ret_file = {'scope': file['scope'],
                            'name': file['name'],
                            'bytes': file['bytes'],
                            'events': file['events'],
                            'adler32': file['adler32']}
                ret_files.append(ret_file)
            return ret_files
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            raise exceptions.AgentPluginError('%s: %s' % (str(ex), traceback.format_exc()))
