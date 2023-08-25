#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2022 - 2023


import argparse
import logging

import os
import re
import json

import configparser


is_unicode_defined = True
try:
    _ = unicode('test')
except NameError:
    is_unicode_defined = False


def is_string(value):
    if is_unicode_defined and type(value) in [str, unicode] or type(value) in [str]:   # noqa F821
        return True
    else:
        return False


def as_parse_env(dct):
    for key in dct:
        value = dct[key]
        if is_string(value) and '$' in value:
            env_matches = re.findall('\$\{*([^\}]+)\}*', value)     # noqa W605
            for env_name in env_matches:
                if env_name not in os.environ:
                    print("WARN: %s is defined in configmap but is not defined in environments" % env_name)
                else:
                    env_name1 = r'${%s}' % env_name
                    env_name2 = r'$%s' % env_name
                    value = value.replace(env_name1, os.environ.get(env_name)).replace(env_name2, os.environ.get(env_name))
        dct[key] = value
    return dct


def convert_section_json(data_conf):
    for section in data_conf:
        for item in data_conf[section]:
            if type(data_conf[section][item]) in [list, tuple, dict]:
                data_conf[section][item] = json.dumps(data_conf[section][item])
    return data_conf


def merge_configs(source_file_path, dest_file_path):
    """
    Merge  configuration file.
    """

    if source_file_path and dest_file_path:
        if os.path.exists(source_file_path) and os.path.exists(dest_file_path):
            with open(source_file_path, 'r') as f:
                data = json.load(f, object_hook=as_parse_env)
                if dest_file_path in data:
                    data_conf = data[dest_file_path]
                    data_conf = convert_section_json(data_conf)
                    parser = configparser.ConfigParser()
                    parser.read(dest_file_path)
                    parser.read_dict(data_conf)
                    with open(dest_file_path, 'w') as dest_file:
                        parser.write(dest_file)


def create_oidc_token():
    if 'PANDA_AUTH_ID_TOKEN' in os.environ:
        config_root = os.environ.get('PANDA_CONFIG_ROOT', '/tmp')
        token_file = os.path.join(config_root, '.token')
        token = {"id_token": os.environ.get('PANDA_AUTH_ID_TOKEN', ""),
                 "token_type": "Bearer"}

        with open(token_file, 'w') as f:
            f.write(json.dumps(token))


logging.getLogger().setLevel(logging.INFO)
parser = argparse.ArgumentParser(description="Merge configuration file from configmap")
parser.add_argument('-s', '--source', default=None, help='Source config file path (in .json format)')
parser.add_argument('-d', '--destination', default=None, help='Destination file path')
parser.add_argument('-c', '--create_oidc_token', default=False, action='store_true', help='Create the oidc token based on environment')
args = parser.parse_args()

merge_configs(args.source, args.destination)
if args.create_oidc_token:
    create_oidc_token()
