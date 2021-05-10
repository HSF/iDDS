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


class IDDSDict(dict):
    def __setitem__(self, key, value):
        if key == 'test':
            pass
        else:
            super().__setitem__(key, value)


class IDDSMetadata(DictClass):
    def __init__(self):
        pass

    def add_item(self, key, value):
        setattr(self, key, value)

    def get_item(self, key, default):
        return getattr(self, key, default)


class Base(DictClass):
    def __init__(self):
        self.metadata = IDDSMetadata()
        pass

    def add_metadata_item(self, key, value):
        self.metadata.add_item(key, value)

    def get_metadata_item(self, key, default=None):
        return self.metadata.get_item(key, default)

    def load_metadata(self):
        pass

    @property
    def metadata(self):
        return self._metadata

    @metadata.setter
    def metadata(self, value):
        self._metadata = value
        self.load_metadata()

    def IDDSProperty(self, attribute):
        def _get(self, attribute):
            self.get_metadata_item(attribute, None)

        def _set(self, attribute, value):
            self.add_metadata_item(attribute, value)

        attribute = property(_get, _set)
        return attribute

    def serialize(self):
        return urllib.parse.quote_from_bytes(pickle.dumps(self))

    @staticmethod
    def deserialize(obj):
        # return urllib.parse.unquote_to_bytes(pickle.loads(obj))
        return pickle.loads(urllib.parse.unquote_to_bytes(obj))
