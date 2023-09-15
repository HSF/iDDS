#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2020 - 2023

# from enum import Enum

from idds.common.dict_class import DictMetadata, DictBase


class IDDSDict(dict):
    def __setitem__(self, key, value):
        if key == 'test':
            pass
        else:
            super().__setitem__(key, value)


class IDDSMetadata(DictMetadata):
    def __init__(self):
        super(IDDSMetadata, self).__init__()


class Base(DictBase):
    def __init__(self):
        super(Base, self).__init__()
        pass
