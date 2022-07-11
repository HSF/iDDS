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


def config_api_host(conf_file_template="data/conf.js.template", conf_file='data/conf.js', hostname=None):
    with open(conf_file_template, 'r') as f:
        template = f.read()
    template = template.format(api_host_name=hostname)
    with open(conf_file, 'w') as f:
        f.write(template)


logging.getLogger().setLevel(logging.INFO)
parser = argparse.ArgumentParser(description="config iDDS monitor")
parser.add_argument('-s', '--source', default=None, help='Source config file path')
parser.add_argument('-d', '--destination', default=None, help='Destination file path')
parser.add_argument('--host', default=None, help='idds host name')
args = parser.parse_args()


config_api_host(conf_file_template=args.source, conf_file=args.destination, hostname=args.host)
