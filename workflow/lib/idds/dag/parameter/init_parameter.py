#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019 - 2020


class InitParameter(object):
    """Init parameter"""

    def __init__(self, name):
        self.type = 'init'
        self.params = {}

    def add(self, name, value):
        self.params[name] = value

    def get(self, name):
        self.params.get(name, None)
