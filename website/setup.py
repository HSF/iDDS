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
import io
import os
import re
import sys
import shutil
from distutils.sysconfig import get_python_lib
from setuptools import setup, Distribution
from setuptools.command.install import install


current_dir = os.getcwd()
working_dir = os.path.dirname(os.path.realpath(__file__))
os.chdir(working_dir)


with io.open('./version.py', "rt", encoding="utf8") as f:
    version = re.search(r'release_version = "(.*?)"', f.read()).group(1)


with io.open('README.md', "rt", encoding="utf8") as f:
    readme = f.read()


class OnlyGetScriptPath(install):
    def run(self):
        self.distribution.install_scripts = self.install_scripts


def get_python_bin_path():
    " Get the directory setuptools installs scripts to for current python "
    dist = Distribution({'cmdclass': {'install': OnlyGetScriptPath}})
    dist.dry_run = True  # not sure if necessary
    dist.parse_config_files()
    command = dist.get_command_obj('install')
    command.ensure_finalized()
    command.run()
    return dist.install_scripts


def get_python_home():
    return sys.exec_prefix


def get_data_path():
    return sys.prefix


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


def get_files(idir):
    files = []
    for f in os.listdir(idir):
        ifile = os.path.join(idir, f)
        if ifile.startswith("./"):
            ifile = ifile[2:]
        if os.path.isfile(ifile):
            files.append(ifile)
    return files


def get_data_files(dest, src):
    data = []
    data.append((dest, get_files(src)))
    for root, dirs, files in os.walk(src):
        if 'dist' in root or 'build' in root or 'egg-info' in root:
            continue
        for idir in dirs:
            if idir == 'dist' or idir == 'build' or idir.endswith('.egg-info'):
                continue
            idir = os.path.join(root, idir)
            if idir.startswith("./"):
                idir = idir[2:]
            dest_dir = os.path.join(dest, idir)
            i_data = (dest_dir, get_files(idir))
            data.append(i_data)
    return data


for build_dir in ['build', 'dist', 'idds_website.egg-info']:
    if os.path.exists(build_dir):
        print("removing %s............................." % build_dir)
        shutil.rmtree(build_dir)

install_lib_path = get_python_lib()
install_bin_path = get_python_bin_path()
install_home_path = get_python_home()
install_data_path = get_data_path()

requirements_files = ['tools/env/environment.yml']
install_requires = parse_requirements(requirements_files=requirements_files)
install_requires = install_requires

data_files = [
    # config and cron files
    ('etc/idds/', glob.glob('etc/idds/*.template')),
    ('etc/idds/rest', glob.glob('etc/idds/rest/*template')),
    ('tools/env/', glob.glob('tools/env/*.yml')),
    # ('website/', glob.glob('*', recursive=True))
    # ('website/', get_all_files('.')),
]
data_files += get_data_files('website/', '.')

scripts = glob.glob('bin/*')

setup(
    name="idds-website",
    version=version,
    description='intelligent Data Delivery Service(iDDS) Package',
    long_description=readme,
    long_description_content_type='text/markdown',
    license='GPL',
    author='IRIS-HEP Team',
    author_email='atlas-adc-panda@cern.ch',
    python_requires='>=3.6',
    # packages=find_packages('lib/'),
    # package_dir={'': 'lib'},
    install_requires=install_requires,
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

os.chdir(current_dir)
