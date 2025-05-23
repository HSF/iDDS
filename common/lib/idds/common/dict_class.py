#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2020 - 2025

"""
Dict class.
"""

import base64
import json
import logging
import pickle
import urllib
import zlib
import inspect
from enum import Enum


class DictClass(object):
    def __init__(self):
        self._zip_items = []
        self._not_auto_unzip_items = []

    def zip_data(self, data, name=None):
        try:
            if type(data) in [str] and data.startswith("idds_zip:"):
                # already zipped
                return data

            # Convert to JSON string
            json_str = json.dumps(data)

            # Compress with zlib
            compressed = zlib.compress(json_str.encode())

            # Encode to base64 for safe storage/transfer
            compressed_str = base64.b64encode(compressed).decode()

            return "idds_zip:" + compressed_str
        except Exception as ex:
            print(f"Dict_class failed to zip data: {ex}")
        return data

    def unzip_data(self, data):
        try:
            if type(data) not in [str] or not data.startswith("idds_zip:"):
                # not zipped data
                return data

            # remove the head 'idds_zip:'
            actual_data = data[9:]

            # Decode from base64
            decoded = base64.b64decode(actual_data)

            # Decompress with zlib
            decompressed = zlib.decompress(decoded).decode()

            # Convert back to dictionary
            original_data = json.loads(decompressed)

            return original_data
        except Exception as ex:
            print(f"Dict_class failed to unzip data: {ex}")
        return data

    @property
    def zip_items(self):
        if hasattr(self, '_zip_items'):
            return self._zip_items
        else:
            return []

    @zip_items.setter
    def zip_items(self, value):
        self._zip_items = value

    @property
    def not_auto_unzip_items(self):
        if hasattr(self, '_not_auto_unzip_items'):
            return self._not_auto_unzip_items
        else:
            return []

    @not_auto_unzip_items.setter
    def not_auto_unzip_items(self, value):
        self._not_auto_unzip_items = value

    def should_zip(self, key):
        if key in self.zip_items:
            return True
        return False

    def should_auto_unzip(self, key):
        if key in self.zip_items and key not in self.not_auto_unzip_items:
            return True
        return False

    def should_unzip(self, key):
        if key in self.zip_items:
            return True
        return False

    def to_dict_l(self, d):
        # print('to_dict_l')
        # print(d)
        if not d:
            return d

        if hasattr(d, 'refresh'):
            d.refresh()

        if hasattr(d, 'to_dict'):
            return d.to_dict()
        elif isinstance(d, dict):
            new_d = {}
            for k, v in d.items():
                new_d[k] = self.to_dict_l(v)
            return new_d
        elif isinstance(d, list):
            new_d = []
            for k in d:
                new_d.append(self.to_dict_l(k))
            return new_d
        elif inspect.ismethod(d):
            return {'idds_method': d.__name__, 'idds_method_class_id': d.__self__.get_internal_id()}
        return d

    def to_dict(self):
        # print('to_dict')
        ret = {'class': self.__class__.__name__,
               'module': self.__class__.__module__,
               'attributes': {}}

        if hasattr(self, 'refresh'):
            self.refresh()

        for key, value in self.__dict__.items():
            # print(key)
            # print(value)
            # if not key.startswith('__') and not key.startswith('_'):
            if not key.startswith('__'):
                if key in ['logger']:
                    new_value = None
                elif hasattr(self, 'should_zip') and self.should_zip(key):
                    new_value = self.zip_data(value, name=key)
                else:
                    new_value = self.to_dict_l(value)
                ret['attributes'][key] = new_value
        return ret

    @staticmethod
    def is_class(d):
        if d and isinstance(d, dict) and 'class' in d and 'module' in d and 'attributes' in d:
            return True
        return False

    @staticmethod
    def is_class_method(d):
        if d and isinstance(d, dict) and 'idds_method' in d and 'idds_method_class_id' in d:
            return True
        return False

    @staticmethod
    def is_class_attribute(d):
        if d and isinstance(d, dict) and 'idds_attribute' in d and 'idds_method_class_id' in d:
            return True
        return False

    @staticmethod
    def load_instance(d):
        module = __import__(d['module'], fromlist=[None])
        cls = getattr(module, d['class'])
        if issubclass(cls, Enum):
            impl = cls(d['attributes']['_value_'])
        else:
            impl = cls()
        return impl

    @staticmethod
    def load_instance_method(d):
        # not do anything. Will load the method in Workflow class.
        return d

    @staticmethod
    def load_instance_attribute(d):
        # not do anything. Will load the method in Workflow class.
        return d

    @staticmethod
    def from_dict(d):
        if not d:
            return d

        # print("from_dict: %s" % str(d))
        # print("is_class: %s" % DictClass.is_class(d))
        if isinstance(d, DictBase):
            d.metadata = d.metadata

        if DictClass.is_class(d):
            impl = DictClass.load_instance(d)
            last_items = {}
            for key, value in d['attributes'].items():
                # print(key)
                if key in ['logger']:
                    continue
                elif key == "_metadata":
                    last_items[key] = value
                # elif key == 'output_data':
                #     continue
                else:
                    value = DictClass.from_dict(value)
                setattr(impl, key, value)

            # unzip
            for key, value in impl.__dict__.items():
                if hasattr(impl, 'should_auto_unzip') and impl.should_auto_unzip(key):
                    new_value = impl.unzip_data(value)
                    setattr(impl, key, new_value)

            # print("last_items: %s" % str(last_items))
            for key, value in last_items.items():
                value = DictClass.from_dict(value)
                setattr(impl, key, value)

            return impl
        elif DictClass.is_class_method(d):
            impl = DictClass.load_instance_method(d)
            return impl
        elif DictClass.is_class_attribute(d):
            impl = DictClass.load_instance_attribute(d)
            return impl
        elif isinstance(d, dict):
            for k, v in d.items():
                d[k] = DictClass.from_dict(v)
            return d
        elif isinstance(d, list):
            new_d = []
            for k in d:
                new_d.append(DictClass.from_dict(k))
            return new_d
        else:
            return d

        return d


class DictMetadata(DictClass):
    def __init__(self):
        super(DictMetadata, self).__init__()
        pass

    def add_item(self, key, value):
        setattr(self, key, value)

    def get_item(self, key, default):
        return getattr(self, key, default)


class DictBase(DictClass):
    def __init__(self):
        super(DictBase, self).__init__()
        self.metadata = DictMetadata()
        pass

    def add_metadata_item(self, key, value):
        self.metadata.add_item(key, value)

    def get_metadata_item(self, key, default=None):
        return self.metadata.get_item(key, default)

    def refresh(self):
        pass

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

    def get_class_name(self):
        return self.__class__.__name__

    def setup_logger(self):
        """
        Setup logger
        """
        self.logger = logging.getLogger(self.get_class_name())
