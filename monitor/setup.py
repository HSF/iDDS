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

import os
from setuptools import setup


def get_data_files(dest, src):
    """Walk a source directory and produce data_files entries."""
    data = []
    for root, dirs, files in os.walk(src):
        if root.endswith('/dist') or root.endswith('/build') or 'egg-info' in root:
            continue
        dest_dir = os.path.join(dest, root)
        src_files = [os.path.join(root, f) for f in files]
        if src_files:
            data.append((dest_dir, src_files))
    return data


data_files = []
data_files += get_data_files('monitor/', './data')

setup(
    data_files=data_files,
)
