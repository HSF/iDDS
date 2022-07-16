#!/usr/bin/env python

import json
import re
import os


is_unicode_defined = True
try:
    _ = unicode('test')
except NameError:
    is_unicode_defined = False


def is_string(value):
    if is_unicode_defined and type(value) in [str, unicode] or type(value) in [str]:   # noqa F821
        return True
    else:
        return False


def as_parse_env(dct):
    print(dct)
    for key in dct:
        value = dct[key]
        print(value)
        print(type(value))
        if is_string(value) and value.startswith('$'):
            env_match = re.search('\$\{*([^\}]+)\}*', value)     # noqa W605
            env_name = env_match.group(1)
            if env_name not in os.environ:
                print("Error: %s is defined in configmap but is not defined in environments" % env_name)
            else:
                dct[key] = os.environ[env_name]
    return dct


if __name__ == '__main__':
    json_string = """
{    "/opt/idds/config/panda.cfg":
        {"panda":
            {"panda_url_ssl": "${PANDA_URL_SSL}",
             "panda_url": "${PANDA_URL}",
             "panda_monitor_url": "${PANDA_MONITOR_URL}",
             "# PANDA_AUTH_VO": "panda_dev",
             "panda_auth": "${PANDA_AUTH}",
             "panda_auth_vo": "${PANDA_AUTH_VO}",
             "panda_config_root": "${PANDA_CONFIG_ROOT}",
             "pandacache_url": "${PANDACACHE_URL}",
             "panda_verify_host": "${PANDA_VERIFY_HOST}",
             "test1": {"test2": "${TEST_ENV}"}
            }
        }
}
"""
    print("test1")
    test1 = json.loads(json_string)
    print(test1)
    print("test2")
    test1 = json.loads(json_string, object_hook=as_parse_env)
    print(test1)
