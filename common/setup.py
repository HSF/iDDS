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


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CustomInstallCommand(install):
    """Custom install command to exclude top-level 'idds' during installation."""
    def run(self):
        # Remove 'idds' from the list of packages before installation
        logger.info("idds-common installing")
        logger.info(f"self.distribution.packages: {self.distribution.packages}")
        self.distribution.packages = [
            pkg for pkg in self.distribution.packages if pkg != 'not'
        ]
        logger.info(f"self.distribution.packages: {self.distribution.packages}")
        super().run()


current_dir = os.getcwd()
working_dir = os.path.dirname(os.path.realpath(__file__))
os.chdir(working_dir)


with io.open('lib/idds/common/version.py', "rt", encoding="utf8") as f:
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


install_lib_path = get_python_lib()
install_bin_path = get_python_bin_path()
install_home_path = get_python_home()
install_data_path = get_data_path()

requirements_files = ['tools/common/env/environment.yml']
install_requires = parse_requirements(requirements_files=requirements_files)

if sys.version_info[0] == 2:
    install_requires.append('enum34')

data_files = [
    # config and cron files
    ('etc/idds/', glob.glob('etc/idds/*.template')),
    ('etc/idds/rest', glob.glob('etc/idds/rest/*template')),
    ('tools/common/env/', glob.glob('tools/common/env/*.yml')),
]
scripts = glob.glob('bin/*')

setup(
    name="idds-common",
    version=version,
    description='intelligent Data Delivery Service(iDDS) Package',
    long_description=readme,
    long_description_content_type='text/markdown',
    license='GPL',
    author='IRIS-HEP Team',
    author_email='atlas-adc-panda@cern.ch',
    python_requires='>=2.7',
    packages=find_packages('lib/'),
    package_dir={'': 'lib'},
    install_requires=install_requires,
    include_package_data=True,
    data_files=data_files,
    scripts=scripts,
    cmdclass={
        'install': CustomInstallCommand,  # Exclude 'idds' during installation
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
