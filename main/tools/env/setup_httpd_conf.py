#!/usr/bin/env python3
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019 - 2025

"""
Post-install helper: generate Apache httpd config from templates.

Run this script AFTER 'pip install idds-server' (or after any pip install that
places the iDDS package under a conda/virtualenv prefix).  It resolves the
correct Python site-packages, home, and bin paths from the *running* interpreter
and writes a ready-to-copy *.install_template file for each Apache conf template.

Usage (run as the install user, with the conda/venv already activated):

    python /opt/idds/tools/env/setup_httpd_conf.py

Or, with an explicit install prefix:

    python /opt/idds/tools/env/setup_httpd_conf.py --prefix /opt/idds

Then copy the generated .install_template to httpd conf.d, e.g.:

    cp /opt/idds/etc/idds/rest/httpd-idds-443-py39-cc7.conf.install_template \\
       /etc/httpd/conf.d/httpd-idds-443-py39-cc7.conf
"""

import argparse
import io
import os
import sys
import sysconfig
import glob


def get_python_lib():
    return sysconfig.get_paths()["purelib"]


def get_python_bin_path():
    return sysconfig.get_paths()["scripts"]


def get_python_home():
    return sys.exec_prefix


def replace_python_path(conf_files, python_lib_path, install_bin_path, install_home_path):
    """Expand {python_site_packages_path} / {python_site_home_path} / {python_site_bin_path}
    placeholders in each template and write a *.install_template beside it."""
    for conf_file in conf_files:
        if not os.path.exists(conf_file):
            print(f"  [skip] {conf_file} not found")
            continue
        new_file = conf_file.replace('.template', '.install_template')
        with io.open(conf_file, 'r', encoding='utf8') as f:
            template = f.read()
        rendered = template.format(
            python_site_packages_path=python_lib_path,
            GLOBAL='GLOBAL',
            REQUEST_METHOD='REQUEST_METHOD',
            python_site_home_path=install_home_path,
            python_site_bin_path=install_bin_path,
        )
        with io.open(new_file, 'w', encoding='utf8') as f:
            f.write(rendered)
        print(f"  [ok]   {conf_file}")
        print(f"         -> {new_file}")


def main():
    parser = argparse.ArgumentParser(
        description="Generate Apache httpd *.install_template files for iDDS."
    )
    parser.add_argument(
        "--prefix",
        default=None,
        help="iDDS install prefix (default: auto-detected from running Python, i.e. sys.prefix)",
    )
    args = parser.parse_args()

    if args.prefix:
        # When a custom prefix is given we still use the *running* interpreter's
        # lib/bin paths, which should already be under that prefix if the correct
        # conda env / venv is activated.
        prefix = args.prefix
    else:
        prefix = sys.prefix

    python_lib_path = get_python_lib()
    install_bin_path = get_python_bin_path()
    install_home_path = get_python_home()

    print(f"Python prefix      : {prefix}")
    print(f"Site-packages path : {python_lib_path}")
    print(f"Bin path           : {install_bin_path}")
    print(f"Home path          : {install_home_path}")
    print()

    # Look for all httpd conf templates shipped under the install prefix
    template_glob = os.path.join(prefix, "etc", "idds", "rest", "*.conf.template")
    conf_files = sorted(glob.glob(template_glob))

    if not conf_files:
        print(f"No *.conf.template files found under {template_glob}")
        print("Make sure the iDDS server package is installed and the prefix is correct.")
        sys.exit(1)

    print(f"Generating .install_template files from {len(conf_files)} template(s):")
    replace_python_path(conf_files, python_lib_path, install_bin_path, install_home_path)

    print()
    print("Done.  Copy the desired .install_template to /etc/httpd/conf.d/ to activate.")


if __name__ == "__main__":
    main()
