#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019 - 2025

# Metadata is in pyproject.toml. This file handles data_files and scripts
# which require glob patterns not supported in pyproject.toml.

import glob
from setuptools import setup

data_files = [
    ('etc/idds/', glob.glob('etc/idds/*.template')),
    ('etc/idds/rest', glob.glob('etc/idds/rest/*')),
    ('etc/idds/auth', glob.glob('etc/idds/auth/*template')),
    ('etc/idds/website', glob.glob('etc/idds/website/*')),
    ('etc/idds/supervisord.d', glob.glob('etc/idds/supervisord.d/*')),
    ('etc/idds/condor/client', glob.glob('etc/idds/condor/client/*')),
    ('etc/idds/condor/server', glob.glob('etc/idds/condor/server/*')),
    ('etc/condor/collector', glob.glob('etc/condor/collector/*')),
    ('etc/condor/submitter', glob.glob('etc/condor/submitter/*')),
    ('etc/panda', glob.glob('etc/panda/*')),
    ('etc/sql', glob.glob('etc/sql/*')),
    ('config_default/', glob.glob('config_default/*')),
    ('tools/env/', glob.glob('tools/env/*')),
]

scripts = glob.glob('bin/*')

setup(
    data_files=data_files,
    scripts=scripts,
)

