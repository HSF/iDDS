#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019 - 2025


import glob
import logging
import io
import os
import re
import sys
import sysconfig
from setuptools import setup, find_packages
from setuptools.command.install import install
from wheel.bdist_wheel import bdist_wheel


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


EXCLUDED_PACKAGE = ["idds"]


class CustomInstallCommand(install):
    """Custom install command to exclude top-level 'idds' during installation."""
    def run(self):
        # Remove 'idds' from the list of packages before installation
        logger.info("idds-atlas installing")
        logger.info(f"self.distribution.packages: {self.distribution.packages}")
        self.distribution.packages = [
            pkg for pkg in self.distribution.packages if pkg not in EXCLUDED_PACKAGE
        ]
        logger.info(f"self.distribution.packages: {self.distribution.packages}")
        super().run()


class CustomBdistWheel(bdist_wheel):
    """Custom wheel builder to exclude the 'idds' package but keep subpackages."""
    def finalize_options(self):
        # Exclude only the top-level 'idds', not its subpackages
        logger.info("idds-workflow wheel installing")
        logger.info(f"self.distribution.packages: {self.distribution.packages}")
        included_packages = [
            pkg for pkg in find_packages('lib/')
            if pkg not in EXCLUDED_PACKAGE
        ]
        self.distribution.packages = included_packages
        logger.info(f"self.distribution.packages: {self.distribution.packages}")
        super().finalize_options()

    def run(self):
        logger.info("CustomBdistWheel is running!")  # Debug print
        super().run()


current_dir = os.getcwd()
working_dir = os.path.dirname(os.path.realpath(__file__))
os.chdir(working_dir)


with io.open('lib/idds/core/version.py', "rt", encoding="utf8") as f:
    version = re.search(r'release_version = "(.*?)"', f.read()).group(1)


with io.open('README.md', "rt", encoding="utf8") as f:
    readme = f.read()


def get_python_lib():
    return sysconfig.get_paths()["purelib"]


def get_python_bin_path():
    return sysconfig.get_paths()["scripts"]


def get_python_home():
    return sys.exec_prefix


def get_data_path():
    return sysconfig.get_paths()["data"]


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


def replace_python_path(conf_files, python_lib_path, install_bin_path, install_home_path):
    for conf_file in conf_files:
        new_file = conf_file.replace('.template', '.install_template')
        with open(conf_file, 'r') as f:
            template = f.read()
        template = template.format(python_site_packages_path=python_lib_path,
                                   GLOBAL='GLOBAL',
                                   REQUEST_METHOD='REQUEST_METHOD',
                                   python_site_home_path=install_home_path,
                                   python_site_bin_path=install_bin_path)
        with open(new_file, 'w') as f:
            f.write(template)


def replace_data_path(wsgi_file, install_data_path):
    new_file = wsgi_file.replace('.template', '')
    with open(wsgi_file, 'r') as f:
        template = f.read()
    template = template.format(idds_config_path=os.path.join(install_data_path, 'etc/idds/idds.cfg'))
    with open(new_file, 'w') as f:
        f.write(template)


install_lib_path = get_python_lib()
install_bin_path = get_python_bin_path()
install_home_path = get_python_home()
install_data_path = get_data_path()

rest_conf_files = ['etc/idds/rest/httpd-idds-443-py39-cc7.conf.template']
replace_python_path(rest_conf_files, install_lib_path, install_bin_path, install_home_path)
wsgi_file = 'bin/idds.wsgi.template'
replace_data_path(wsgi_file, install_data_path)

requirements_files = ['tools/env/environment.yml']
install_requires = parse_requirements(requirements_files=requirements_files)
install_requires = install_requires

extras_requires = dict(mysql=['mysqlclient'])

data_files = [
    # config and cron files
    ('etc/idds/', glob.glob('etc/idds/*.template')),
    ('etc/idds/rest', glob.glob('etc/idds/rest/*')),
    ('etc/idds/auth', glob.glob('etc/idds/auth/*template')),
    ('etc/idds/website', glob.glob('etc/idds/website/*')),
    ('etc/idds/supervisord.d', glob.glob('etc/idds/supervisord.d/*')),
    ('etc/idds/condor/client', glob.glob('etc/idds/condor/client/*')),
    ('etc/idds/condor/server', glob.glob('etc/idds/condor/server/*')),
    ('etc/condor/client', glob.glob('etc/condor/client/*')),
    ('etc/condor/server', glob.glob('etc/condor/server/*')),
    ('etc/sql', glob.glob('etc/sql/*')),
    ('config_default/', glob.glob('config_default/*')),
    ('tools/env/', glob.glob('tools/env/*')),
]

print(data_files)

scripts = glob.glob('bin/*')

setup(
    name="idds-server",
    version=version,
    description='intelligent Data Delivery Service(iDDS) Package',
    long_description=readme,
    long_description_content_type='text/markdown',
    license='GPL',
    author='IRIS-HEP Team',
    author_email='atlas-adc-panda@cern.ch',
    python_requires='>=3.6',
    packages=find_packages(where='lib'),
    package_dir={'': 'lib'},
    install_requires=install_requires,
    extras_require=extras_requires,
    include_package_data=True,
    data_files=data_files,
    scripts=scripts,
    cmdclass={
        'install': CustomInstallCommand,  # Exclude 'idds' during installation
        'bdist_wheel': CustomBdistWheel,
    },
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
