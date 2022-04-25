# -*- coding: utf-8 -*-
# Copyright CERN since 2021
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# cloned from the rucio project: https://github.com/rucio/rucio/blob/master/tools/merge_rucio_configs.py

import argparse
import logging

import os
import json
import sys

import configparser


# Multi-word sections used in kubernetes are slightly different from what idds expects.
# Usually, it's just a .replace('-', '_'), but not for hermes2, which doesn't follow any convention.
multi_word_sections = {
    'idds_transformer': 'idds_transformer',
}


def load_flat_config(flat_config):
    """
    takes a dict of the form: {"section_option": "value"}
    and converts to {"section": {"option": "value"}
    """
    config_dict = {}
    for flat_key, config_value in flat_config.items():
        section = option = None
        # Try parsing a multi-word section
        for mw_key in multi_word_sections:
            if flat_key.startswith(mw_key + '_'):
                section = mw_key
                option = flat_key[len(mw_key) + 1:]

        # It didn't match any known multi-word section, assume it's a single word
        if not section:
            section, option = flat_key.split('_', maxsplit=1)

        config_dict.setdefault(section, {})[option] = config_value
    return config_dict


def fix_multi_word_sections(config_dict):
    return {multi_word_sections.get(section, section): config_for_section for section, config_for_section in config_dict.items()}


def config_len(config_dict):
    return sum(len(option) for _, option in config_dict.items())


def merge_configs(source_file_paths, dest_file_path, prefix, use_env=True, replace_whole_file=False, env_string=None, logger=logging.log):
    """
    Merge multiple configuration sources into one destination file.
    On conflicting values, relies on the default python's ConfigParser behavior: the value from last source wins.
    Sources can be either .ini or .json files. Json is supported as a compromise solution for easier integration
    with kubernetes (because both python and helm natively support it).
    If use_env=True, env variables starting with RUCIO_CFG_ are also merged as the last (highest priority) source.
    """

    if replace_whole_file and env_string:
        with open(dest_file_path, 'w') as dest_file:
            dest_file.write(env_string)
    else:
        parser = configparser.ConfigParser()
        for file_path in source_file_paths:
            if not os.path.exists(file_path):
                logger(logging.WARNING, "Skipping config file {}: file doesn't exist".format(file_path))
                continue

            try:
                if file_path.endswith('.json'):
                    with open(file_path, 'r') as f:
                        file_config = fix_multi_word_sections(json.load(f))
                        parser.read_dict(file_config)
                else:
                    local_parser = configparser.ConfigParser()
                    local_parser.read(file_path)
                    file_config = {section: {option: value for option, value in section_proxy.items()} for section, section_proxy in local_parser.items()}

                parser.read_dict(file_config)
                logger(logging.INFO, "Merged {} configuration values from {}".format(config_len(file_config), file_path))
            except Exception as error:
                logger(logging.WARNING, "Skipping config file {} due to error: {}".format(file_path, error))

        if use_env:
            # env variables use the following format: "RUCIO_CFG_{section.substitute('-','_').upper}_{option.substitute('-', '_').upper}"
            env_config = {}
            for env_key, env_value in os.environ.items():
                if not env_key.startswith(prefix):
                    continue
                env_key = env_key[len(prefix):].lower()  # convert "RUCIO_CFG_WHATEVER" to "whatever"
                env_config[env_key] = env_value

            env_config = fix_multi_word_sections(load_flat_config(env_config))
            parser.read_dict(env_config)
            logger(logging.INFO, "Merged {} configuration values from ENV".format(config_len(env_config)))

        if dest_file_path:
            with open(dest_file_path, 'w') as dest_file:
                parser.write(dest_file)
        else:
            parser.write(sys.stdout)


logging.getLogger().setLevel(logging.INFO)
parser = argparse.ArgumentParser(description="Merge multiple configuration sources into one configuration file")
parser.add_argument("--use-env", action="store_true", default=False, help='Also source config from RUCIO_CFG_* env variables')
parser.add_argument('-s', '--source', type=str, nargs='*', help='Source config file paths (in .json or .ini format)')
parser.add_argument('-d', '--destination', default=None, help='Destination file path')
parser.add_argument('-p', '--prefix', default=None, help='Prefix to match the env')
parser.add_argument('-r', "--replace-whole-file", action="store_true", default=False, help='whether to replace the whole file with the env-string')
parser.add_argument('-e', '--env-string', default=None, help='The environment string to replace the whole file')
args = parser.parse_args()

merge_configs(args.source or [], args.destination, use_env=args.use_env, prefix=args.prefix,
              replace_whole_file=args.replace_whole_file, env_string=args.env_string)
