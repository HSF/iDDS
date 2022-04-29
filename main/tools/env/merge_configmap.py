#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2022


import argparse
import logging

import os
import json

import configparser


def merge_configs(source_file_path, dest_file_path):
    """
    Merge  configuration file.
    """

    if source_file_path and dest_file_path:
        if os.path.exists(source_file_path) and os.path.exists(dest_file_path):
            with open(source_file_path, 'r') as f:
                data = json.load(f)
                if dest_file_path in data:
                    data_conf = data[dest_file_path]
                    parser = configparser.ConfigParser()
                    parser.read(dest_file_path)
                    parser.read_dict(data_conf)
                    with open(dest_file_path, 'w') as dest_file:
                        parser.write(dest_file)


logging.getLogger().setLevel(logging.INFO)
parser = argparse.ArgumentParser(description="Merge configuration file from configmap")
parser.add_argument('-s', '--source', default=None, help='Source config file path (in .json format)')
parser.add_argument('-d', '--destination', default=None, help='Destination file path')
args = parser.parse_args()

merge_configs(args.source, args.destination)
