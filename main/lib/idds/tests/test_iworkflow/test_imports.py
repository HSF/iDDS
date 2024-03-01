#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2024


"""
Test workflow.
"""

# import datetime
# import inspect
# import sys

from idds.common.imports import import_func, get_func_name


func_name = "test_iworkflow1.py:__main__:test_workflow1"
func = import_func(func_name)
print(func)
func()

# sys.exit(1)

for func_name in ["test_iworkflow1.py:__main__:test_workflow2", "test_iworkflow1.py:__main__:test_workflow1"]:
    func = import_func(func_name)
    print(func)
    func()

    func_name = get_func_name(func)
    print(func_name)

    func = import_func(func_name)
    print(func)
    func()
