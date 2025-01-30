#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019 - 2022


"""
plugin utils

plugin configuration structure:

[section_name]
plugin_sequence = <plugin_name1>,<plugin_name2>
plugin.<plugin_name> = <plugin.lib.path.ClassName>
plugin.<plugin_name>.<attr1> = <value1>
plugin.<plugin_name>.<attr2> = <value2>
"""


import traceback
from idds.common.config import (config_has_section, config_has_option,
                                config_list_options, config_get)


def load_plugin_sequence(config_section, config_option='plugin_sequence'):
    """
    load plugin sequence
    """
    plugin_sequence = []
    if config_has_section(config_section) and config_has_option(config_section, config_option):
        plugin_sequence = config_get(config_section, config_option)
        plugin_sequence = plugin_sequence.split(",")
        plugin_sequence = [plugin.strip() for plugin in plugin_sequence]
    return plugin_sequence


def load_plugin_attributes(config_section, name, plugin):
    """
    Load plugin attributes
    """
    attrs = {}
    if config_has_section(config_section):
        options = config_list_options(config_section)
        for option, value in options:
            plugin_prefix = 'plugin.%s.' % name
            if option.startswith(plugin_prefix):
                attr_name = option.replace(plugin_prefix, '')
                if isinstance(value, str) and value.lower() == 'true':
                    value = True
                if isinstance(value, str) and value.lower() == 'false':
                    value = False
                attrs[attr_name] = value
    return attrs


def load_plugin(config_section, name, plugin, logger=None):
    """
    Load plugin attributes
    """
    attrs = load_plugin_attributes(config_section, name, plugin)
    k = plugin.rfind('.')
    plugin_modules = plugin[:k]
    plugin_class = plugin[k + 1:]
    module = __import__(plugin_modules, fromlist=[None])
    cls = getattr(module, plugin_class)
    attrs['logger'] = logger
    impl = cls(**attrs)
    return impl


def load_plugins(config_section, logger=None):
    """
    Load plugins
    """
    plugins = {}
    if config_has_section(config_section):
        options = config_list_options(config_section)
        for option, value in options:
            if option.startswith('plugin.'):
                if option.count('.') == 1:
                    plugin_name = option.replace('plugin.', '').strip()
                    try:
                        plugins[plugin_name] = load_plugin(config_section, plugin_name, value, logger=logger)
                    except Exception as ex:
                        print(f"Failed to load plugin {plugin_name}: {ex}")
                        print(traceback.format_exc())
    return plugins
