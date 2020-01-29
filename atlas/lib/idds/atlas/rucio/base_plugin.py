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
Class of base plugin
"""

import traceback

from rucio.client.client import Client
from rucio.common.exception import CannotAuthenticate

from idds.common import exceptions
from idds.common.plugin.plugin_base import PluginBase


class RucioPluginBase(PluginBase):
    def __init__(self, **kwargs):
        super(RucioPluginBase, self).__init__(**kwargs)
        self.client = self.get_rucio_client()

    def get_rucio_client(self):
        try:
            client = Client()
        except CannotAuthenticate as error:
            self.logger.error(error)
            self.logger.error(traceback.format_exc())
            raise exceptions.AgentPluginError('%s: %s' % (str(error), traceback.format_exc()))
        return client

    def __call__(self, **kwargs):
        return exceptions.NotImplementedException(self.get_class_name())
