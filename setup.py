#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019


import glob
import os
import re
import sys
from distutils.sysconfig import get_python_lib
from setuptools import setup, find_packages

sys.path.insert(0, os.path.abspath('lib/'))

with open('lib/idds/version.py', "rt", encoding="utf8") as f:
    version = re.search(r'release_version = "(.*?)"', f.read()).group(1)


with open('README.md', "rt", encoding="utf8") as f:
    readme = f.read()


def get_reqs_from_file(requirements_file):
    if os.path.exists(requirements_file):
        return open(requirements_file, 'r').read().split('\n')
    return []


def parse_requirements(requirements_files):
    requirements = []
    for requirements_file in requirements_files:
        for line in get_reqs_from_file(requirements_file):
            line = line.split('#')[0]
            line = line.strip()
            if line.startswith('- ') and not line.endswith(':'):
                line = line.replace('- ', '').strip()
                if len(line) and 'python==' not in line:
                    requirements.append(line)
    return requirements


def replace_python_lib_path(conf_files, python_lib_path):
    for conf_file in conf_files:
        new_file = conf_file.replace('.template', '.install_template')
        with open(conf_file, 'r') as f:
            template = f.read()
        template = template.format(python_site_packages_path=python_lib_path, GLOBAL='GLOBAL')
        with open(new_file, 'w') as f:
            f.write(template)


install_lib_path = get_python_lib()
rest_conf_files = ['etc/idds/rest/httpd-idds-443-py36-cc7.conf.template']
replace_python_lib_path(rest_conf_files, install_lib_path)

requirements_files = ['tools/env/environment.yml']
install_requires = parse_requirements(requirements_files=requirements_files)
extras_requires = dict(mysql=['mysqlclient'])
data_files = [
    # config and cron files
    ('etc/idds/', glob.glob('etc/idds/*.template')),
    ('etc/idds/rest', glob.glob('etc/idds/rest/*template')),
]
scripts = glob.glob('bin/*')

setup(
    name="iDDS",
    version=version,
    description='intelligent Data Delivery Service(iDDS) Package',
    long_description=readme,
    license='GPL',
    author='IRIS-HEP Team',
    author_email='atlas-adc-panda@cern.ch',
    python_requires='>=3.6',
    packages=find_packages('lib/'),
    package_dir={'': 'lib'},
    install_requires=install_requires,
    extras_require=extras_requires,
    include_package_data=True,
    data_files=data_files,
    scripts=scripts,
    project_urls={
        'Documentation': 'https://github.com/HSF/iDDS/wiki',
        'Source': 'https://github.com/HSF/iDDS',
    },
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Programming Language :: Python :: 3.6',
    ],
)
