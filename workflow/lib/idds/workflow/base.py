#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2020

import pickle
import urllib
# from enum import Enum

from idds.common.dict_class import DictClass


class Base(DictClass):
    def __init__(self):
        pass

    def serialize(self):
        return urllib.parse.quote_from_bytes(pickle.dumps(self))

    @staticmethod
    def deserialize(obj):
        # return urllib.parse.unquote_to_bytes(pickle.loads(obj))
        return pickle.loads(urllib.parse.unquote_to_bytes(obj))
