#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2022

import logging
import uuid
import redis

from idds.common.constants import Sections
from idds.common.config import config_has_section, config_list_options
from idds.common.utils import json_dumps, json_loads


class Singleton(object):
    _instance = None

    def __new__(class_, *args, **kwargs):
        if not isinstance(class_._instance, class_):
            class_._instance = object.__new__(class_, *args, **kwargs)
            class_._instance._initialized = False
        return class_._instance


class RedisCache(Singleton):
    """
    Redis cache
    """

    def __init__(self, logger=None):
        if not self._initialized:
            self._initialized = True

            super(RedisCache, self).__init__()
            self._id = str(uuid.uuid4())[:8]
            self.logger = logger
            self.setup_logger(self.logger)
            self.config_section = Sections.Cache
            attrs = self.load_attributes()
            if 'host' in attrs and attrs['host']:
                self.host = attrs['host']
            else:
                self.host = 'localhost'
            if 'port' in attrs and attrs['port']:
                self.port = int(attrs['port'])
            else:
                self.port = 6379
            self.cache = redis.Redis(host=self.host, port=self.port, db=0)

    def setup_logger(self, logger=None):
        """
        Setup logger
        """
        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger(self.get_class_name())

    def get_class_name(self):
        return self.__class__.__name__

    def load_attributes(self):
        self.logger.info("Loading config for section: %s" % self.config_section)
        attrs = {}
        if config_has_section(self.config_section):
            options = config_list_options(self.config_section)
            for option, value in options:
                if isinstance(value, str) and value.lower() == 'true':
                    value = True
                if isinstance(value, str) and value.lower() == 'false':
                    value = False
                attrs[option] = value
        return attrs

    def set(self, key, value, expire_seconds=21600):
        value = json_dumps(value)
        self.cache.set(key, value, ex=expire_seconds)

    def get(self, key, default=None):
        value = self.cache.get(key)
        if value:
            value = json_loads(value)
        if not value:
            return default
        return value

    def hset(self, key, value, expire_seconds=21600):
        value = json_dumps(value)
        self.cache.hset(key, value)
        self.cache.expire(key, expire_seconds)

    def hget(self, key, default=None):
        value = self.cache.hget(key)
        if value:
            value = json_loads(value)
        if not value:
            return default
        return value


def get_redis_cache():
    cache = RedisCache()
    return cache
