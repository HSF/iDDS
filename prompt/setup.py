#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019 - 2025

# Metadata is in pyproject.toml. This file handles data_files and scripts
# which require glob patterns not supported in pyproject.toml.

import glob
from setuptools import setup

data_files = [
    ('tools/prompt/env/', glob.glob('tools/prompt/env/*.yml')),
    ('tools/prompt/make/', glob.glob('tools/prompt/make/*')),
]

scripts = glob.glob('bin/*')

setup(
    data_files=data_files,
    scripts=scripts,
)
