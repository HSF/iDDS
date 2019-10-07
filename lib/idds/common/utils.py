#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019


import datetime
import logging
import os
import requests
import subprocess
import sys

from idds.common.config import (config_has_section, config_has_option,
                                config_get, config_get_bool)


# RFC 1123
DATE_FORMAT = '%a, %d %b %Y %H:%M:%S UTC'


def setup_logging(name):
    """
    Setup logging
    """
    if config_has_section('common') and config_has_option('common', 'loglevel'):
        loglevel = getattr(logging, config_get('common', 'loglevel').upper())
    else:
        loglevel = logging.INFO

    if config_has_section('common') and config_has_option('common', 'logdir'):
        logging.basicConfig(filename=os.path.join(config_get('common', 'logdir'), name),
                            level=loglevel,
                            format='%(asctime)s\t%(threadName)s\t%(levelname)s\t%(message)s')
    else:
        logging.basicConfig(stream=sys.stdout, level=loglevel,
                            format='%(asctime)s\t%(threadName)s\t%(levelname)s\t%(message)s')


def get_rest_url_prefix():
    if config_has_section('rest') and config_has_option('rest', 'url_prefix'):
        url_prefix = config_get('rest', 'url_prefix')
    else:
        url_prefix = None
    if url_prefix:
        while url_prefix.startswith('/'):
            url_prefix = url_prefix[1:]
        while url_prefix.endswith('/'):
            url_prefix = url_prefix[:-1]
        url_prefix = '/' + url_prefix
    return url_prefix


def get_rest_debug():
    if config_has_section('rest') and config_has_option('rest', 'debug'):
        url_prefix = config_get_bool('rest', 'debug')
    return False


def str_to_date(string):
    """
    Converts a string to the corresponding datetime value.

    :param string: the string to convert to datetime value.
    """
    return datetime.datetime.strptime(string, DATE_FORMAT) if string else None


def date_to_str(date):
    """
    Converts a datetime value to a string.

    :param date: the datetime value to convert.
    """
    return datetime.datetime.strftime(date, DATE_FORMAT) if date else None


def has_config():
    """
    check whether there is a config file
    """
    if os.environ.get('IDDS_CONFIG', None):
        configfile = os.environ.get('IDDS_CONFIG', None)
        if configfile and os.path.exists(configfile):
            return True
    else:
        configfiles = ['%s/etc/idds/idds.cfg' % os.environ.get('IDDS_HOME', ''),
                       '/etc/idds/idds.cfg',
                       '%s/etc/idds/idds.cfg' % os.environ.get('VIRTUAL_ENV', '')]

        for configfile in configfiles:
            if configfile and os.path.exists(configfile):
                return True
    return False


def check_rest_host():
    """
    Function to check whether rest host is defined in config.
    To be used to decide whether to skip some test functions.

    :returns True: if rest host is available. Otherwise False.
    """
    if config_has_option('rest', 'host'):
        host = config_get('rest', 'host')
        if host:
            return True
    return False


def get_rest_host():
    """
    Function to get rest host
    """
    host =  config_get('rest', 'host')
    url_prefix = get_rest_url_prefix()
    while host.endswith("/"):
        host = host[:-1]
    if url_prefix:
        host = ''.join([host, url_prefix])
    return host


def check_user_proxy():
    """
    Check whether there is a user proxy.
    """
    if 'X509_USER_PROXY' in os.environ:
        client_proxy = os.environ['X509_USER_PROXY']
    else:
        client_proxy = '/tmp/x509up_u%d' % os.geteuid()

    if not os.path.exists(client_proxy):
        return False
    else:
        return True


def check_database():
    """
    Function to check whether database is defined in config.
    To be used to decide whether to skip some test functions.

    :returns True: if database.default is available. Otherwise False.
    """
    if config_has_option('database', 'default'):
        database = config_get('database', 'default')
        if database:
            return True
    return False


def run_process(cmd, stdout=None, stderr=None):
    """
    Runs a command in an out-of-procees shell.
    """
    if stdout and stderr:
        process = subprocess.Popen(cmd, shell=True, stdout=stdout, stderr=stderr, preexec_fn=os.setsid)
    else:
        process = subprocess.Popen(cmd, shell=True)
    return process


def get_space_from_string(space_str):
    """
    Convert space with P, T, G, M to int
    """
    M = 1024
    G = 1024 * M
    T = 1024 * G
    P = 1024 * T

    if 'M' in space_str:
        return int(float(space_str.split('M')[0]) * M)
    elif 'G' in space_str:
        return int(float(space_str.split('G')[0]) * G)
    elif 'T' in space_str:
        return int(float(space_str.split('T')[0]) * T)
    elif 'P' in space_str:
        return int(float(space_str.split('P')[0]) * P)
    else:
        return int(space_str)


def urlretrieve(url, dest, timeout=300):
    """
    Download a file.

    :param url: The url of the source file.
    :param dest: destination file path.
    """
    with open(dest, 'wb') as f:
        r = requests.get(url, allow_redirects=True, timeout=timeout)
        if r.status_code == 200:
            f.write(r.content)
            return 0
        else:
            return -1
