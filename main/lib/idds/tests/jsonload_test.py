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
    # print(dct)
    for key in dct:
        value = dct[key]
        # print(value)
        # print(type(value))
        if is_string(value) and '$' in value:
            env_matchs = re.findall('\$\{*([^\}]+)\}*', value)     # noqa W605
            print("env_matchs")
            print(env_matchs)
            for env_name in env_matchs:
                print(env_name)
                if env_name not in os.environ:
                    print("Error: %s is defined in configmap but is not defined in environments" % env_name)
                else:
                    # dct[key] = os.environ[env_name]
                    print('${%s}' % env_name)
                    print(os.environ.get(env_name))
                    env_name1 = r'${%s}' % env_name
                    env_name2 = r'$%s' % env_name
                    print(env_name1)
                    print(env_name2)
                    print(value.replace(env_name1, os.environ.get(env_name)))
                    value = value.replace(env_name1, os.environ.get(env_name)).replace(env_name2, os.environ.get(env_name))
            dct[key] = value
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
            },
         "database":
            {"default": "postgresql://${IDDS_DATABASE_USER}:${IDDS_DATABASE_PASSWORD}@${IDDS_DATABASE_HOST}/${IDDS_DATABASE_NAME}",
             "schema": "${IDDS_DATABASE_SCHEMA}",
             "pool_size": 20,
             "pool_recycle": 3600,
             "echo": 0,
             "pool_reset_on_return": "rollback"
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
