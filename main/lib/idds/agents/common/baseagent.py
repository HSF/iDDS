#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019


from idds.common.constants import (Sections, TransformType,
                                   MessageType, MessageStatus, MessageSource)
from idds.common.plugin.plugin_base import PluginBase
from idds.common.plugin.plugin_utils import load_plugins, load_plugin_sequence
from idds.common.utils import setup_logging
from idds.agents.common.timerscheduler import TimerScheduler


setup_logging(__name__)


class BaseAgent(TimerScheduler, PluginBase):
    """
    The base IDDS agent class
    """

    def __init__(self, num_threads=1, **kwargs):
        super(BaseAgent, self).__init__(num_threads)
        self.name = self.__class__.__name__
        self.logger = None
        self.setup_logger()
        self.set_logger(self.logger)

        self.config_section = Sections.Common

        for key in kwargs:
            setattr(self, key, kwargs[key])

        self.plugins = {}
        self.plugin_sequence = []

    def load_plugin_sequence(self):
        self.plugin_sequence = load_plugin_sequence(self.config_section)

    def load_plugins(self):
        self.plugins = load_plugins(self.config_section)
        """
        for plugin_name in self.plugin_sequence:
            if plugin_name not in self.plugins:
                raise AgentPluginError("Plugin %s is defined in plugin_sequence but no plugin is defined with this name")
        for plugin_name in self.plugins:
            if plugin_name not in self.plugin_sequence:
                raise AgentPluginError("Plugin %s is defined but it is not defined in plugin_sequence" % plugin_name)
        """

    def run(self):
        """
        Main run function.
        """
        try:
            self.logger.info("Starting main thread")

            self.load_plugins()

            self.execute()
        except KeyboardInterrupt:
            self.stop()

    def __call__(self):
        self.run()

    def terminate(self):
        self.stop()

    def generate_file_message(self, transform, files):
        if not files:
            return None

        updated_files_message = []
        for file in files:
            updated_file_message = {'scope': file['scope'],
                                    'name': file['name'],
                                    'path': file['path'],
                                    'status': file['status'].name}
            updated_files_message.append(updated_file_message)

        workload_id = None
        if 'workload_id' in transform['transform_metadata']:
            workload_id = transform['transform_metadata']['workload_id']

        if transform['transform_type'] in [TransformType.StageIn, TransformType.StageIn.value]:
            msg_type = 'file_stagein'
            msg_type_c = MessageType.StageInFile
        elif transform['transform_type'] in [TransformType.ActiveLearning, TransformType.ActiveLearning.value]:
            msg_type = 'file_activelearning'
            msg_type_c = MessageType.ActiveLearningFile
        elif transform['transform_type'] in [TransformType.HyperParameterOpt, TransformType.HyperParameterOpt.value]:
            msg_type = 'file_hyperparameteropt'
            msg_type_c = MessageType.HyperParameterOptFile
        else:
            msg_type = 'file_unknown'
            msg_type_c = MessageType.UnknownFile

        msg_content = {'msg_type': msg_type,
                       'workload_id': workload_id,
                       'files': updated_files_message}
        file_msg_content = {'msg_type': msg_type_c,
                            'status': MessageStatus.New,
                            'source': MessageSource.Carrier,
                            'transform_id': transform['transform_id'],
                            'num_contents': len(updated_files_message),
                            'msg_content': msg_content}
        return file_msg_content


if __name__ == '__main__':
    agent = BaseAgent()
    agent()
