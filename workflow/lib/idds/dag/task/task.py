#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019 - 2020

"""
{‘type’: ‘transformation’,
 ‘sandbox’: <sandbox>,
 ‘executable’: <executable>,
 ‘arguments’: ‘--in=%IN_DATASET --out=%OUT_DATASET --out_json=%OUT_JSON’,
 parameters: {‘IN_DATASET’: <indataset>,
              ‘OUT_DATASET’: <outdataset>,
              ‘OUT_JSON’: <output_j
             }
}
"""

class Task(object):

    instance_num = 0

    def __init__(self, executable, arguments=None, sandbox=None, parameters={}, name=None):
        self.executable = executable
        self.arguments = arguments
        self.sandbox = sandbox
        self.parameters = parameters
        self.instance_num += 1
        if not name:
            self.name = 'task_' + str(self.instance_num) 
        else:
            self.name = name

    def map_parameters(self, name, value);
        if name not in self.parameters:
            raise Exeception("Parameter %s is not available in %s" % (name, self.name))

        
