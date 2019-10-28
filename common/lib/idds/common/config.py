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
Configurations.

configuration file looking for path:
    1. $IDDS_CONFIG
    2. $IDDS_HOME/etc/idds/idds.cfg
    3. /etc/idds/idds.cfg
    4. $VIRTUAL_ENV/etc/idds/idds.cfg
"""


import os

try:
    import ConfigParser
except ImportError:
    import configparser as ConfigParser


def config_has_section(section):
    """
    Return where there is a section

    :param section: the named section.
.
    :returns: True/False.
    """
    return __CONFIG.has_section(section)


def config_has_option(section, option):
    """
    Return where there is an option for a given option in a section

    :param section: the named section.
    :param option: the named option.
.
    :returns: True/False.
    """
    return __CONFIG.has_option(section, option)


def config_list_options(section):
    """
    Return list of (name, value) for a given option in a section

    :param section: the named section.
.
    :returns: list of (name, value).
    """
    return __CONFIG.items(section)


def config_get(section, option):
    """
    Return the string value for a given option in a section
    :param section: the named section.
    :param option: the named option.
.
    :returns: the configuration value.
    """
    return __CONFIG.get(section, option)


def config_get_int(section, option):
    """
    Return the integer value for a given option in a section
    :param section: the named section.
    :param option: the named option.
.
    :returns: the integer configuration value.
    """
    return __CONFIG.getint(section, option)


def config_get_float(section, option):
    """
    Return the float value for a given option in a section
    :param section: the named section.
    :param option: the named option.
.
    :returns: the float configuration value.
    """
    return __CONFIG.getfloat(section, option)


def config_get_bool(section, option):
    """
    Return the boolean value for a given option in a section
    :param section: the named section.
    :param option: the named option.
.
    :returns: the boolean configuration value.
    """
    return __CONFIG.getboolean(section, option)


__CONFIG = ConfigParser.SafeConfigParser()

__HAS_CONFIG = False
if os.environ.get('IDDS_CONFIG', None):
    configfile = os.environ['IDDS_CONFIG']
    if not __CONFIG.read(configfile) == [configfile]:
        raise Exception('IDDS_CONFIG is defined as %s, ' % configfile,
                        'but could not load configurations from it.')
    __HAS_CONFIG = True
else:
    configfiles = ['%s/etc/idds/idds.cfg' % os.environ.get('IDDS_HOME', ''),
                   '/etc/idds/idds.cfg',
                   '%s/etc/idds/idds.cfg' % os.environ.get('VIRTUAL_ENV', '')]

    for configfile in configfiles:
        if __CONFIG.read(configfile) == [configfile]:
            __HAS_CONFIG = True
            # print("Configuration file %s is used" % configfile)
            break

if not __HAS_CONFIG:
    raise Exception("Could not load configuration file."
                    "IDDS looks for a configuration file, in order:"
                    "\n\t${IDDS_CONFIG}"
                    "\n\t${IDDS_HOME}/etc/idds/idds.cfg"
                    "\n\t/etc/idds/idds.cfg"
                    "\n\t${VIRTUAL_ENV}/etc/idds/idds.cfg")
