#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019 - 2024


import base64
import errno
import datetime
import importlib
import hashlib
import logging
import json
import os
import re
import requests
import signal
import subprocess
import sys
import tarfile
import time
# import traceback

from enum import Enum
from functools import wraps
from itertools import groupby
from operator import itemgetter
from packaging import version as packaging_version
from typing import Any, Callable

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


def get_log_dir():
    if config_has_section('common') and config_has_option('common', 'logdir'):
        return config_get('common', 'logdir')
    return "/var/log/idds"


def setup_logging(name, stream=None, loglevel=None):
    """
    Setup logging
    """
    if loglevel is None:
        if config_has_section('common') and config_has_option('common', 'loglevel'):
            loglevel = getattr(logging, config_get('common', 'loglevel').upper())
        else:
            loglevel = logging.INFO

        if os.environ.get('IDDS_LOG_LEVEL', None):
            idds_log_level = os.environ.get('IDDS_LOG_LEVEL', None)
            idds_log_level = idds_log_level.upper()
            if idds_log_level in ["DEBUG", "CRITICAL", "ERROR", "WARNING", "INFO"]:
                loglevel = getattr(logging, idds_log_level)

    if type(loglevel) in [str]:
        loglevel = loglevel.upper()
        loglevel = getattr(logging, loglevel)

    if stream is None:
        if config_has_section('common') and config_has_option('common', 'logdir'):
            logging.basicConfig(filename=os.path.join(config_get('common', 'logdir'), name),
                                level=loglevel,
                                format='%(asctime)s\t%(threadName)s\t%(name)s\t%(levelname)s\t%(message)s')
        else:
            logging.basicConfig(stream=sys.stdout, level=loglevel,
                                format='%(asctime)s\t%(threadName)s\t%(name)s\t%(levelname)s\t%(message)s')
    else:
        logging.basicConfig(stream=stream, level=loglevel,
                            format='%(asctime)s\t%(threadName)s\t%(name)s\t%(levelname)s\t%(message)s')
    logging.Formatter.converter = time.gmtime


def get_logger(name, filename=None, loglevel=None):
    """
    Setup logging
    """
    if loglevel is None:
        if config_has_section('common') and config_has_option('common', 'loglevel'):
            loglevel = getattr(logging, config_get('common', 'loglevel').upper())
        else:
            loglevel = logging.INFO

    if filename is None:
        filename = name + ".log"
    if not filename.startswith("/"):
        logdir = None
        if config_has_section('common') and config_has_option('common', 'logdir'):
            logdir = config_get('common', 'logdir')
        if not logdir:
            logdir = '/var/log/idds'
        filename = os.path.join(logdir, filename)

    formatter = logging.Formatter('%(asctime)s\t%(threadName)s\t%(name)s\t%(levelname)s\t%(message)s')

    handler = logging.FileHandler(filename)
    handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.setLevel(loglevel)
    logger.addHandler(handler)
    logger.propagate = False
    return logger


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
    if "IDDS_HOST" in os.environ:
        return os.environ.get("IDDS_HOST")
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


def kill_process_group(pgrp, nap=10):
    """
    Kill the process group.
    DO NOT MOVE TO PROCESSES.PY - will lead to circular import since execute() needs it as well.
    :param pgrp: process group id (int).
    :param nap: napping time between kill signals in seconds (int)
    :return: boolean (True if SIGTERM followed by SIGKILL signalling was successful)
    """

    status = False
    _sleep = True

    # kill the process gracefully
    print(f"killing group process {pgrp}")
    try:
        os.killpg(pgrp, signal.SIGTERM)
    except Exception as error:
        print(f"exception thrown when killing child group process under SIGTERM: {error}")
        _sleep = False
    else:
        print(f"SIGTERM sent to process group {pgrp}")

    if _sleep:
        print(f"sleeping {nap} s to allow processes to exit")
        time.sleep(nap)

    try:
        os.killpg(pgrp, signal.SIGKILL)
    except Exception as error:
        print(f"exception thrown when killing child group process with SIGKILL: {error}")
    else:
        print(f"SIGKILL sent to process group {pgrp}")
        status = True

    return status


def kill_all(process: Any) -> str:
    """
    Kill all processes after a time-out exception in process.communication().

    :param process: process object
    :return: stderr (str).
    """

    stderr = ''
    try:
        print('killing lingering subprocess and process group')
        time.sleep(1)
        # process.kill()
        kill_process_group(os.getpgid(process.pid))
    except ProcessLookupError as exc:
        stderr += f'\n(kill process group) ProcessLookupError={exc}'
    except Exception as exc:
        stderr += f'\n(kill_all 1) exception caught: {exc}'
    try:
        print('killing lingering process')
        time.sleep(1)
        os.kill(process.pid, signal.SIGTERM)
        print('sleeping a bit before sending SIGKILL')
        time.sleep(10)
        os.kill(process.pid, signal.SIGKILL)
    except ProcessLookupError as exc:
        stderr += f'\n(kill process) ProcessLookupError={exc}'
    except Exception as exc:
        stderr += f'\n(kill_all 2) exception caught: {exc}'
    print(f'sent soft kill signals - final stderr: {stderr}')
    return stderr


def run_process(cmd, stdout=None, stderr=None, wait=False, timeout=7 * 24 * 3600):
    """
    Runs a command in an out-of-procees shell.
    """
    print(f"To run command: {cmd}")
    if stdout and stderr:
        process = subprocess.Popen(cmd, shell=True, stdout=stdout, stderr=stderr, preexec_fn=os.setsid, encoding='utf-8')
    else:
        process = subprocess.Popen(cmd, shell=True, preexec_fn=os.setsid, encoding='utf-8')
    if not wait:
        return process

    try:
        print(f'subprocess.communicate() will use timeout={timeout} s')
        process.communicate(timeout=timeout)
    except subprocess.TimeoutExpired as ex:
        stderr = f'subprocess communicate sent TimeoutExpired: {ex}'
        print(stderr)
        stderr = kill_all(process)
        print(f'Killing process: {stderr}')
        exit_code = -1
    except Exception as ex:
        stderr = f'subprocess has an exception: {ex}'
        print(stderr)
        stderr = kill_all(process)
        print(f'Killing process: {stderr}')
        exit_code = -1
    else:
        exit_code = process.poll()

    try:
        process.wait(timeout=60)
    except subprocess.TimeoutExpired:
        print("process did not complete within the timeout of 60s - terminating")
        process.terminate()
    return exit_code


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
        elif isinstance(obj, datetime.timedelta):
            return str(obj)
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
            # print(traceback.format_exc())
            return str(ex)
        except Exception as ex:
            logging.error(ex)
            # print(traceback.format_exc())
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
    return packaging_version.parse(version1) > packaging_version.parse(version2)


def extract_scope_atlas(did, scopes):
    # Try to extract the scope from the DSN
    if did.find(':') > -1:
        if len(did.split(':')) > 2:
            raise IDDSException('Too many colons. Cannot extract scope and name')
        scope, name = did.split(':')[0], did.split(':')[1]
        if name.endswith('/'):
            name = name[:-1]
        return scope, name
    else:
        scope = did.split('.')[0]
        if did.startswith('user') or did.startswith('group'):
            scope = ".".join(did.split('.')[0:2])
        if did.endswith('/'):
            did = did[:-1]
        return scope, did


def truncate_string(string, length=800):
    string = (string[:length] + '...') if string and len(string) > length else string
    return string


def merge_dict(dict1, dict2):
    keys = list(dict1.keys())
    for key in list(dict2.keys()):
        if key not in keys:
            keys.append(key)
    for key in keys:
        if key in dict2:
            if key not in dict1 or dict1[key] is None:
                dict1[key] = dict2[key]
            else:
                if dict2[key] is None:
                    continue
                elif not isinstance(dict1[key], type(dict2[key])):
                    raise Exception("type of %s is different from %s, cannot merge" % (type(dict1[key]), type(dict2[key])))
                elif dict1[key] == dict2[key]:
                    continue
                elif type(dict1[key]) in (list, tuple, str):
                    dict1[key] = dict1[key] + dict2[key]
                elif type(dict1[key]) in (int, float, complex):
                    dict1[key] = dict1[key] + dict2[key]
                elif type(dict1[key]) in (bool, bool):
                    dict1[key] = True
                elif type(dict1[key]) in (dict, dict):
                    dict1[key] = merge_dict(dict1[key], dict2[key])
    return dict1


def pid_exists(pid):
    """
    Check whether pid exists in the current process table.
    UNIX only.
    """
    if pid < 0:
        return False
    if pid == 0:
        # According to "man 2 kill" PID 0 refers to every process
        # in the process group of the calling process.
        # On certain systems 0 is a valid PID but we have no way
        # to know that in a portable fashion.
        raise ValueError('invalid PID 0')
    try:
        os.kill(pid, 0)
    except OSError as err:
        if err.errno == errno.ESRCH:
            # ESRCH == No such process
            return False
        elif err.errno == errno.EPERM:
            # EPERM clearly means there's a process to deny access to
            return True
        else:
            # According to "man 2 kill" possible error values are
            # (EINVAL, EPERM, ESRCH)
            raise
    else:
        return True


def get_list_chunks(full_list, bulk_size=2000):
    chunks = [full_list[i:i + bulk_size] for i in range(0, len(full_list), bulk_size)]
    return chunks


def report_availability(availability):
    try:
        log_dir = get_log_dir()
        if log_dir:
            filename = os.path.join(log_dir, 'idds_availability')
            with open(filename, 'w') as f:
                json.dump(availability, f)
        else:
            print("availability: %s" % str(availability))
    except Exception as ex:
        error = "Failed to report availablity: %s" % str(ex)
        print(error)
        logging.debug(error)


def split_chunks_not_continous(data):
    rets = []
    for k, g in groupby(enumerate(data), lambda i_x: i_x[0] - i_x[1]):
        rets.append(list(map(itemgetter(1), g)))
    return rets


def group_list(input_list, key):
    update_groups = {}
    for item in input_list:
        item_key = item[key]
        del item[key]
        item_tuple = str(tuple(sorted(item.items())))
        if item_tuple not in update_groups:
            update_groups[item_tuple] = {'keys': [], 'items': item}
        update_groups[item_tuple]['keys'].append(item_key)
    return update_groups


def import_func(name: str) -> Callable[..., Any]:
    """Returns a function from a dotted path name. Example: `path.to.module:func`.

    When the attribute we look for is a staticmethod, module name in its
    dotted path is not the last-before-end word

    E.g.: package_a.package_b.module_a:ClassA.my_static_method

    Thus we remove the bits from the end of the name until we can import it

    Args:
        name (str): The name (reference) to the path.

    Raises:
        ValueError: If no module is found or invalid attribute name.

    Returns:
        Any: An attribute (normally a Callable)
    """
    name_bits = name.split(':')
    module_name_bits, attribute_bits = name_bits[:-1], [name_bits[-1]]
    module_name_bits = module_name_bits.split('.')
    attribute_bits = attribute_bits.split('.')
    module = None
    while len(module_name_bits):
        try:
            module_name = '.'.join(module_name_bits)
            module = importlib.import_module(module_name)
            break
        except ImportError:
            attribute_bits.insert(0, module_name_bits.pop())

    if module is None:
        # maybe it's a builtin
        try:
            return __builtins__[name]
        except KeyError:
            raise ValueError('Invalid attribute name: %s' % name)

    attribute_name = '.'.join(attribute_bits)
    if hasattr(module, attribute_name):
        return getattr(module, attribute_name)
    # staticmethods
    attribute_name = attribute_bits.pop()
    attribute_owner_name = '.'.join(attribute_bits)
    try:
        attribute_owner = getattr(module, attribute_owner_name)
    except:  # noqa
        raise ValueError('Invalid attribute name: %s' % attribute_name)

    if not hasattr(attribute_owner, attribute_name):
        raise ValueError('Invalid attribute name: %s' % name)
    return getattr(attribute_owner, attribute_name)


def import_attribute(name: str) -> Callable[..., Any]:
    """Returns an attribute from a dotted path name. Example: `path.to.func`.

    When the attribute we look for is a staticmethod, module name in its
    dotted path is not the last-before-end word

    E.g.: package_a.package_b.module_a.ClassA.my_static_method

    Thus we remove the bits from the end of the name until we can import it

    Args:
        name (str): The name (reference) to the path.

    Raises:
        ValueError: If no module is found or invalid attribute name.

    Returns:
        Any: An attribute (normally a Callable)
    """
    name_bits = name.split('.')
    module_name_bits, attribute_bits = name_bits[:-1], [name_bits[-1]]
    module = None
    while len(module_name_bits):
        try:
            module_name = '.'.join(module_name_bits)
            module = importlib.import_module(module_name)
            break
        except ImportError:
            attribute_bits.insert(0, module_name_bits.pop())

    if module is None:
        # maybe it's a builtin
        try:
            return __builtins__[name]
        except KeyError:
            raise ValueError('Invalid attribute name: %s' % name)

    attribute_name = '.'.join(attribute_bits)
    if hasattr(module, attribute_name):
        return getattr(module, attribute_name)
    # staticmethods
    attribute_name = attribute_bits.pop()
    attribute_owner_name = '.'.join(attribute_bits)
    try:
        attribute_owner = getattr(module, attribute_owner_name)
    except:  # noqa
        raise ValueError('Invalid attribute name: %s' % attribute_name)

    if not hasattr(attribute_owner, attribute_name):
        raise ValueError('Invalid attribute name: %s' % name)
    return getattr(attribute_owner, attribute_name)


def decode_base64(sb):
    try:
        if isinstance(sb, str):
            sb_bytes = bytes(sb, 'ascii')
        elif isinstance(sb, bytes):
            sb_bytes = sb
        else:
            return sb
        return base64.b64decode(sb_bytes).decode("utf-8")
    except Exception as ex:
        logging.error("decode_base64 %s: %s" % (sb, ex))
        return sb


def encode_base64(sb):
    try:
        if isinstance(sb, str):
            sb_bytes = bytes(sb, 'ascii')
        elif isinstance(sb, bytes):
            sb_bytes = sb
        return base64.b64encode(sb_bytes).decode("utf-8")
    except Exception as ex:
        logging.error("encode_base64 %s: %s" % (sb, ex))
        return sb


def create_archive_file(work_dir, archive_filename, files):
    if not archive_filename.startswith("/"):
        archive_filename = os.path.join(work_dir, archive_filename)

    with tarfile.open(archive_filename, "w:gz", dereference=True) as tar:
        for local_file in files:
            if os.path.isfile(local_file):
                # base_name = os.path.basename(local_file)
                tar.add(local_file, arcname=os.path.basename(local_file))
            elif os.path.isdir(local_file):
                for root, dirs, fs in os.walk(local_file):
                    for f in fs:
                        file_path = os.path.join(root, f)
                        tar.add(file_path, arcname=os.path.relpath(file_path, local_file))
    return archive_filename


class SecureString(object):
    def __init__(self, value):
        self._value = value

    def __str__(self):
        return '****'


def get_unique_id_for_dict(dict_):
    ret = hashlib.sha1(json.dumps(dict_, sort_keys=True).encode()).hexdigest()
    # logging.debug("get_unique_id_for_dict, type: %s: %s, ret: %s" % (type(dict_), dict_, ret))
    return ret
