#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019

import os
import subprocess
import sys


def setup(argv):
    current_dir = os.path.dirname(os.path.realpath(__file__))
    packages = ['common', 'main', 'client', 'atlas', 'workflow', 'doma', 'website', 'monitor']
    for package in packages:
        path = os.path.join(current_dir, '%s/setup.py' % package)

        cmd = 'python ' + path + ' ' + ' clean --all'
        print(cmd)
        subprocess.call(cmd, shell=True)

        cmd = 'python ' + path + ' ' + ' '.join(argv)
        print(cmd)
        subprocess.call(cmd, shell=True)


setup(sys.argv[1:])
