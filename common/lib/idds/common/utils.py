#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019 - 2020


import datetime
import logging
import json
import os
import re
import requests
import subprocess
import sys
import tarfile
import traceback

from enum import Enum
from functools import wraps

from idds.common.config import (config_has_section, config_has_option,
                                config_get, config_get_bool)
from idds.common.constants import (IDDSEnum, RequestType, RequestStatus,
                                   TransformType, TransformStatus,
                                   CollectionType, CollectionRelationType, CollectionStatus,
                                   ContentType, ContentStatus,
                                   GranularityType, ProcessingStatus)
from idds.common.dict_class import DictClass
from idds.common.exceptions import IDDSException


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
                            format='%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s')
    else:
        logging.basicConfig(stream=sys.stdout, level=loglevel,
                            format='%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s')


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
        return config_get_bool('rest', 'debug')
    return False


def get_rest_cacher_dir():
    cacher_dir = None
    if config_has_section('rest') and config_has_option('rest', 'cacher_dir'):
        cacher_dir = config_get('rest', 'cacher_dir')
    if cacher_dir and os.path.exists(cacher_dir):
        return cacher_dir
    raise Exception("cacher_dir is not defined or it doesn't exist")


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
    host = config_get('rest', 'host')
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


def run_command(cmd):
    """
    Runs a command in an out-of-procees shell.
    """
    process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, preexec_fn=os.setsid)
    stdout, stderr = process.communicate()
    if stdout is not None and type(stdout) in [bytes]:
        stdout = stdout.decode()
    if stderr is not None and type(stderr) in [bytes]:
        stderr = stderr.decode()
    status = process.returncode
    return status, stdout, stderr


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


def convert_nojsontype_to_value(params):
    """
    Convert enum to its value

    :param params: dict of parameters.

    :returns: dict of parameters.
    """
    if isinstance(params, list):
        new_params = []
        for v in params:
            if v is not None:
                if isinstance(v, Enum):
                    v = v.value
                if isinstance(v, datetime.datetime):
                    v = date_to_str(v)
                if isinstance(v, (list, dict)):
                    v = convert_nojsontype_to_value(v)
            new_params.append(v)
        params = new_params
    elif isinstance(params, dict):
        for key in params:
            if params[key] is not None:
                if isinstance(params[key], Enum):
                    params[key] = params[key].value
                if isinstance(params[key], datetime.datetime):
                    params[key] = date_to_str(params[key])
                if isinstance(params[key], (list, dict)):
                    params[key] = convert_nojsontype_to_value(params[key])
    return params


def convert_value_to_nojsontype(params):
    """
    Convert value to enum

    :param params: dict of parameters.

    :returns: dict of parameters.
    """
    req_keys = {'request_type': RequestType, 'status': RequestStatus}
    transform_keys = {'transform_type': TransformType, 'status': TransformStatus}
    coll_keys = {'coll_type': CollectionType, 'relation_type': CollectionRelationType, 'coll_status': CollectionStatus}
    content_keys = {'content_type': ContentType, 'status': ContentStatus}
    process_keys = {'granularity_type': GranularityType, 'status': ProcessingStatus}

    if 'request_type' in params:
        keys = req_keys
    elif 'transform_type' in params:
        keys = transform_keys
    elif 'coll_type' in params:
        keys = coll_keys
    elif 'content_type' in params:
        keys = content_keys
    elif 'granularity_type' in params:
        keys = process_keys

    if isinstance(params, list):
        new_params = []
        for v in params:
            if v is not None and isinstance(v, (list, dict)):
                v = convert_value_to_nojsontype(v)
            new_params.append(v)
        params = new_params
    elif isinstance(params, dict):
        keys = []
        if 'request_type' in params:
            keys = req_keys
        elif 'transform_type' in params:
            keys = transform_keys
        elif 'coll_type' in params:
            keys = coll_keys
        elif 'content_type' in params:
            keys = content_keys
        elif 'granularity_type' in params:
            keys = process_keys

        for key in keys.keys():
            if key in params and params[key] is not None and isinstance(params[key], int):
                params[key] = keys[key](params[key])

        for key in params:
            if params[key] is not None:
                if isinstance(params[key], (list, dict)):
                    params[key] = convert_value_to_nojsontype(params[key])

    return params


def convert_request_type_to_transform_type(request_type):
    if isinstance(request_type, RequestType):
        request_type = request_type.value
    return TransformType(request_type)


class DictClassEncoder(json.JSONEncoder):
    def default(self, obj):
        # print(obj)
        if isinstance(obj, IDDSEnum) or isinstance(obj, DictClass):
            return obj.to_dict()
        elif isinstance(obj, datetime.datetime):
            return date_to_str(obj)
        # elif isinstance(obj, (datetime.time, datetime.date)):
        #     return obj.isoformat()
        # elif isinstance(obj, datetime.timedelta):
        #     return obj.days * 24 * 60 * 60 + obj.seconds

        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)


def as_has_dict(dct):
    if DictClass.is_class(dct):
        return DictClass.from_dict(dct)
    return dct


def json_dumps(obj, indent=None, sort_keys=False):
    return json.dumps(obj, indent=indent, sort_keys=sort_keys, cls=DictClassEncoder)


def json_loads(obj):
    return json.loads(obj, object_hook=as_has_dict)


def get_parameters_from_string(text):
    """
    Find all strings starting with '%'. For example, for this string below, it should return ['NUM_POINTS', 'IN', 'OUT']
    'run --rm -it -v "$(pwd)":/payload gitlab-registry.cern.ch/zhangruihpc/endpointcontainer:latest /bin/bash -c "echo "--num_points %NUM_POINTS"; /bin/cat /payload/%IN>/payload/%OUT"'
    """
    ret = re.findall(r"[%]\w+", text)
    ret = [r.replace('%', '') for r in ret]
    # remove dumplications
    ret = list(set(ret))
    return ret


def replace_parameters_with_values(text, values):
    """
    Replace all strings starting with '%'. For example, for this string below, it should replace ['%NUM_POINTS', '%IN', '%OUT']
    'run --rm -it -v "$(pwd)":/payload gitlab-registry.cern.ch/zhangruihpc/endpointcontainer:latest /bin/bash -c "echo "--num_points %NUM_POINTS"; /bin/cat /payload/%IN>/payload/%OUT"'

    :param text
    :param values: parameter values, for example {'NUM_POINTS': 5, 'IN': 'input.json', 'OUT': 'output.json'}
    """
    for key in values:
        key1 = '%' + key
        text = re.sub(key1, str(values[key]), text)
    return text


def tar_zip_files(output_dir, output_filename, files):
    output_filename = os.path.join(output_dir, output_filename)
    with tarfile.open(output_filename, "w:gz") as tar:
        for file in files:
            tar.add(file, arcname=os.path.basename(file))


def exception_handler(function):
    @wraps(function)
    def new_funct(*args, **kwargs):
        try:
            return function(*args, **kwargs)
        except IDDSException as ex:
            logging.error(ex)
            print(traceback.format_exc())
            return str(ex)
        except Exception as ex:
            logging.error(ex)
            print(traceback.format_exc())
            return str(ex)
    return new_funct


def is_sub(a, b):
    if not a:
        return True

    for i in a:
        if i not in b:
            return False
    return True


def get_proxy_path():
    try:
        if 'X509_USER_PROXY' in os.environ:
            proxy = os.environ['X509_USER_PROXY']
            if os.path.exists(proxy) and os.access(proxy, os.R_OK):
                return proxy
        proxy = '/tmp/x509up_u%s' % os.getuid()
        if os.path.exists(proxy) and os.access(proxy, os.R_OK):
            return proxy
    except Exception as ex:
        raise IDDSException("Cannot find User proxy: %s" % str(ex))
    return None


def get_proxy():
    try:
        proxy = get_proxy_path()
        if not proxy:
            return proxy
        with open(proxy, 'r') as fp:
            data = fp.read()
        return data
    except Exception as ex:
        raise IDDSException("Cannot find User proxy: %s" % str(ex))
    return None


def is_new_version(version1, version2):
    return version1 > version2
