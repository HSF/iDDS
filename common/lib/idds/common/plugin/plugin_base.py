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

import logging

from idds.common import exceptions


class PluginBase(object):
    def __init__(self, **kwargs):
        for key in kwargs:
            setattr(self, key, kwargs[key])

        self.logger = None
        self.setup_logger()
        self.plugins = self.load_plugins(kwargs)

    def get_class_name(self):
        return self.__class__.__name__

    def setup_logger(self):
        """
        Setup logger
        """
        self.logger = logging.getLogger(self.get_class_name())

    def __call__(self, **kwargs):
        return exceptions.NotImplementedException(self.get_class_name())

    def load_plugin_attributes(self, name, plugin, kwargs):
        """
        Load plugin attributes
        """
        attrs = {}
        for option, value in kwargs.items():
            plugin_prefix = 'plugin.%s.' % name
            if option.startswith(plugin_prefix):
                attr_name = option.replace(plugin_prefix, '')
                if isinstance(value, str) and value.lower() == 'true':
                    value = True
                if isinstance(value, str) and value.lower() == 'false':
                    value = False
                attrs[attr_name] = value
        return attrs

    def load_plugin(self, name, plugin, kwargs):
        """
        Load plugin
        """
        attrs = self.load_plugin_attributes(name, plugin, kwargs)
        k = plugin.rfind('.')
        plugin_modules = plugin[:k]
        plugin_class = plugin[k + 1:]
        module = __import__(plugin_modules, fromlist=[None])
        cls = getattr(module, plugin_class)
        impl = cls(**attrs)
        return impl

    def load_plugins(self, kwargs):
        if not kwargs:
            return {}

        plugins = {}
        for key, value in kwargs.items():
            if key.startswith('plugin.'):
                if key.count('.') == 1:
                    plugin_name = key.replace('plugin.', '').strip()
                    plugins[plugin_name] = self.load_plugin(plugin_name, value, kwargs)
        return plugins
