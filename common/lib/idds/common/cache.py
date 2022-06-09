#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2022

import json
from dogpile.cache import make_region

from idds.common.config import config_has_section, config_has_option, config_get


def get_cache_url():
    if config_has_section('cache') and config_has_option('cache', 'url'):
        return config_get('cache', 'url')
    return '127.0.0.1:11211'


def make_region_memcached(expiration_time, function_key_generator=None):
    """
    Make and configure a dogpile.cache.memcached region
    """
    if function_key_generator:
        region = make_region(function_key_generator=function_key_generator)
    else:
        region = make_region()

    region.configure(
        'dogpile.cache.memcached',
        expiration_time=expiration_time,
        arguments={
            'url': get_cache_url,
            'distributed_lock': True,
            'memcached_expire_time': expiration_time + 60,  # must be bigger than expiration_time
        }
    )

    return region


REGION = make_region_memcached(expiration_time=3600)


def update_cache(key, data):
    REGION.set(key, json.dumps(data))


def get_cache(key):
    data = REGION.get(key)
    if data:
        return json.loads(data)
    return data


def delete_cache(key):
    REGION.delete(key)
