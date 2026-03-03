#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019 - 2025

# Metadata is in pyproject.toml. This file handles data_files and scripts
# which require glob patterns not supported in pyproject.toml.

import glob
import io
import os
import re
import sys
import sysconfig

from setuptools import setup

data_files = [
    ('etc/idds/', glob.glob('etc/idds/*.template')),
    ('etc/idds/rest', glob.glob('etc/idds/rest/*')),
    ('etc/idds/auth', glob.glob('etc/idds/auth/*template')),
    ('etc/idds/website', glob.glob('etc/idds/website/*')),
    ('etc/idds/supervisord.d', glob.glob('etc/idds/supervisord.d/*')),
    ('etc/idds/condor/client', glob.glob('etc/idds/condor/client/*')),
    ('etc/idds/condor/server', glob.glob('etc/idds/condor/server/*')),
    ('etc/condor/collector', glob.glob('etc/condor/collector/*')),
    ('etc/condor/submitter', glob.glob('etc/condor/submitter/*')),
    ('etc/panda', glob.glob('etc/panda/*')),
    ('etc/sql', glob.glob('etc/sql/*')),
    ('config_default/', glob.glob('config_default/*')),
    ('tools/env/', glob.glob('tools/env/*')),
]

scripts = glob.glob('bin/*')


def get_python_lib():
    return sysconfig.get_paths()["purelib"]


def get_python_bin_path():
    return sysconfig.get_paths()["scripts"]


def get_python_home():
    return sys.exec_prefix


def get_data_path():
    return sysconfig.get_paths()["data"]


def replace_python_path(conf_files, python_lib_path, install_bin_path, install_home_path):
    """Rewrite Apache config templates with resolved Python paths.

    This mirrors the behavior from the legacy setup.py so that the
    httpd-idds-*.conf templates get concrete paths baked in at build time.
    """
    for conf_file in conf_files:
        if not os.path.exists(conf_file):
            continue
        new_file = conf_file.replace('.template', '.install_template')
        with io.open(conf_file, 'r', encoding='utf8') as f:
            template = f.read()
        template = template.format(
            python_site_packages_path=python_lib_path,
            GLOBAL='GLOBAL',
            REQUEST_METHOD='REQUEST_METHOD',
            python_site_home_path=install_home_path,
            python_site_bin_path=install_bin_path,
        )
        with io.open(new_file, 'w', encoding='utf8') as f:
            f.write(template)


def replace_data_path(wsgi_file, install_data_path):
    """Rewrite WSGI template to point at the installed idds.cfg.

    This mirrors the behavior from the legacy setup.py so that
    bin/idds.wsgi is generated from bin/idds.wsgi.template.
    """
    if not os.path.exists(wsgi_file):
        return
    new_file = wsgi_file.replace('.template', '')
    with io.open(wsgi_file, 'r', encoding='utf8') as f:
        template = f.read()
    template = template.format(
        idds_config_path=os.path.join(install_data_path, 'etc/idds/idds.cfg')
    )
    with io.open(new_file, 'w', encoding='utf8') as f:
        f.write(template)


# Perform template rewrites at build/install time, matching legacy behavior
install_lib_path = get_python_lib()
install_bin_path = get_python_bin_path()
install_home_path = get_python_home()
install_data_path = get_data_path()

rest_conf_files = ['etc/idds/rest/httpd-idds-443-py39-cc7.conf.template']
replace_python_path(rest_conf_files, install_lib_path, install_bin_path, install_home_path)
wsgi_file = 'bin/idds.wsgi.template'
replace_data_path(wsgi_file, install_data_path)


setup(
    data_files=data_files,
    scripts=scripts,
)

