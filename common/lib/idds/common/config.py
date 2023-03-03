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


def is_client():
    if 'IDDS_CLIENT_MODE' in os.environ and os.environ['IDDS_CLIENT_MODE']:
        client_mode = True
    else:
        client_mode = False
        try:
            import idds.agents.common                # noqa F401
        except ModuleNotFoundError as ex:            # noqa F841
            client_mode = True

    return client_mode


def config_has_section(section):
    """
    Return where there is a section

    :param section: the named section.
.
    :returns: True/False.
    """
    __CONFIG = get_config()
    return __CONFIG.has_section(section)


def config_has_option(section, option):
    """
    Return where there is an option for a given option in a section

    :param section: the named section.
    :param option: the named option.
.
    :returns: True/False.
    """
    __CONFIG = get_config()
    return __CONFIG.has_option(section, option)


def config_list_options(section):
    """
    Return list of (name, value) for a given option in a section

    :param section: the named section.
.
    :returns: list of (name, value).
    """
    __CONFIG = get_config()
    return __CONFIG.items(section)


def config_get(section, option):
    """
    Return the string value for a given option in a section
    :param section: the named section.
    :param option: the named option.
.
    :returns: the configuration value.
    """
    __CONFIG = get_config()
    return __CONFIG.get(section, option)


def config_get_int(section, option):
    """
    Return the integer value for a given option in a section
    :param section: the named section.
    :param option: the named option.
.
    :returns: the integer configuration value.
    """
    __CONFIG = get_config()
    return __CONFIG.getint(section, option)


def config_get_float(section, option):
    """
    Return the float value for a given option in a section
    :param section: the named section.
    :param option: the named option.
.
    :returns: the float configuration value.
    """
    __CONFIG = get_config()
    return __CONFIG.getfloat(section, option)


def config_get_bool(section, option):
    """
    Return the boolean value for a given option in a section
    :param section: the named section.
    :param option: the named option.
.
    :returns: the boolean configuration value.
    """
    __CONFIG = get_config()
    return __CONFIG.getboolean(section, option)


def get_local_config_root(local_config_root=None):
    if 'IDDS_LOCAL_CONFIG_ROOT' in os.environ and os.environ['IDDS_LOCAL_CONFIG_ROOT']:
        if local_config_root is None:
            # print("IDDS_LOCAL_CONFIG_ROOT is set. Will use it.")
            local_config_root = os.environ['IDDS_LOCAL_CONFIG_ROOT']
        else:
            # print("local_config_root is set to %s. Ignore IDDS_LOCAL_CONFIG_ROOT" % local_config_root)
            pass

    if local_config_root is None:
        # local_config_root = "~/.idds"
        home_dir = os.path.expanduser("~")
        if os.access(home_dir, os.W_OK):
            local_config_root = os.path.join(home_dir, ".idds")
        else:
            # username = os.getlogin()
            # if username == 'root':
            #     local_config_root = os.path.join('/tmp', ".idds")
            # else:
            #     local_config_root = os.path.join(os.path.join("/tmp", username), ".idds")
            pass

    if local_config_root and not os.path.exists(local_config_root):
        try:
            os.makedirs(local_config_root, exist_ok=True)
        except Exception as ex:
            print("Failed to create %s: %s", local_config_root, str(ex))
            local_config_root = None
    if local_config_root and not os.access(local_config_root, os.W_OK):
        print("No write permission on local config root %s, set it to None" % (local_config_root))
        local_config_root = None
    return local_config_root


def get_local_cfg_file(local_config_root=None):
    local_config_root = get_local_config_root(local_config_root)
    if local_config_root:
        local_cfg = os.path.join(local_config_root, 'idds_local.cfg')
        return local_cfg
    return None


def get_local_config_value(configuration, section, name, current, default):
    value = None
    if configuration and configuration.has_section(section) and configuration.has_option(section, name):
        if name in ['oidc_refresh_lifetime']:
            value = configuration.getint(section, name)
        elif name in ['oidc_auto', 'oidc_polling']:
            value = configuration.getboolean(section, name)
        else:
            value = configuration.get(section, name)
    if current is not None:
        value = current
    elif value is None:
        value = default
    return value


def get_main_config_file():
    __CONFIG = ConfigParser.ConfigParser()
    if os.environ.get('IDDS_CONFIG', None):
        configfile = os.environ['IDDS_CONFIG']
        if __CONFIG.read(configfile) == [configfile]:
            return configfile
    else:
        configfiles = ['%s/etc/idds/idds.cfg' % os.environ.get('IDDS_HOME', ''),
                       '/etc/idds/idds.cfg',
                       '%s/etc/idds/idds.cfg' % os.environ.get('VIRTUAL_ENV', '')]
        for configfile in configfiles:
            if __CONFIG.read(configfile) == [configfile]:
                return configfile
    return None


def get_main_config():
    __CONFIG = ConfigParser.ConfigParser()

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
    return __CONFIG, __HAS_CONFIG


def get_config():
    __CONFIG, __HAS_CONFIG = get_main_config()

    if not __HAS_CONFIG:
        local_cfg = get_local_cfg_file()
        if local_cfg and os.path.exists(local_cfg):
            __CONFIG.read(local_cfg)
            __HAS_CONFIG = True
        else:
            if not is_client():
                raise Exception("Could not load configuration file."
                                "For iDDS client, please run 'idds setup' to create local config file."
                                "For an iDDS server, IDDS looks for a configuration file, in order:"
                                "\n\t${IDDS_CONFIG}"
                                "\n\t${IDDS_HOME}/etc/idds/idds.cfg"
                                "\n\t/etc/idds/idds.cfg"
                                "\n\t${VIRTUAL_ENV}/etc/idds/idds.cfg")
    return __CONFIG
