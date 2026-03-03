#!/usr/bin/env python3
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019 - 2025

"""
Post-install / startup helper: resolve and rewrite iDDS path templates.

Run this script on the target machine AFTER pip-installing the iDDS server
package (or at container startup via start-daemon.sh).  It uses the *running*
Python interpreter's prefix/site-packages/bin paths so that no build-time
temporary paths are ever embedded.

Usage
-----
    python3 /opt/idds/tools/env/setup_idds_path.py [--prefix /opt/idds]

Two things are done:

1. replace_python_path
   For every ``*.conf.template`` found under ``<prefix>/etc/idds/rest/``,
   expand ``{python_site_packages_path}``, ``{python_site_home_path}``, and
   ``{python_site_bin_path}`` and write a ``*.conf.install_template`` beside it.
   The install_template can then be copied to ``/etc/httpd/conf.d/``.

2. replace_data_path
   Copy ``<prefix>/bin/idds.wsgi.template`` → ``<prefix>/bin/idds.wsgi``.
   The template already computes ``IDDS_CONFIG`` from ``sys.prefix`` at runtime,
   so no paths are hard-coded.
"""

import argparse
import glob
import io
import os
import sys
import sysconfig


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------

def get_python_lib():
    return sysconfig.get_paths()["purelib"]


def get_python_bin_path():
    return sysconfig.get_paths()["scripts"]


def get_python_home():
    return sys.exec_prefix


def get_data_path():
    return sysconfig.get_paths()["data"]


# ---------------------------------------------------------------------------
# Template rewrite functions
# ---------------------------------------------------------------------------

def replace_python_path(conf_files, python_lib_path, install_bin_path, install_home_path):
    """Expand Python path placeholders in Apache conf templates.

    For each ``*.conf.template`` file in *conf_files*, substitute:
      - ``{python_site_packages_path}``  →  *python_lib_path*
      - ``{python_site_home_path}``      →  *install_home_path*
      - ``{python_site_bin_path}``       →  *install_bin_path*

    and write the result as a ``*.conf.install_template`` beside the source.
    """
    for conf_file in conf_files:
        if not os.path.exists(conf_file):
            print(f"  [skip] {conf_file} (not found)")
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


def replace_data_path(wsgi_file, install_data_path=None):
    """Generate the WSGI entry-point from its template.

    Simply copies ``idds.wsgi.template`` → ``idds.wsgi``.  The template itself
    computes ``IDDS_CONFIG`` from ``sys.prefix`` at runtime, so no build-time
    path is baked in.  *install_data_path* is accepted for API compatibility but
    is not used.
    """
    if not os.path.exists(wsgi_file):
        print(f"  [skip] {wsgi_file} (not found)")
        return
    new_file = wsgi_file.replace('.template', '')
    with io.open(wsgi_file, 'r', encoding='utf8') as f:
        template = f.read()
    with io.open(new_file, 'w', encoding='utf8') as f:
        f.write(template)
    print(f"  [ok]   {wsgi_file}")
    print(f"         -> {new_file}")


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Rewrite iDDS Apache conf and WSGI templates with installed Python paths."
    )
    parser.add_argument(
        "--prefix",
        default=None,
        help=(
            "iDDS install prefix (default: sys.prefix of the running interpreter). "
            "Activate the correct conda env / venv before running so that "
            "site-packages and bin paths are resolved correctly."
        ),
    )
    args = parser.parse_args()

    prefix = args.prefix or sys.prefix

    python_lib_path = get_python_lib()
    install_bin_path = get_python_bin_path()
    install_home_path = get_python_home()

    print("=== iDDS path setup ===")
    print(f"Install prefix     : {prefix}")
    print(f"Site-packages      : {python_lib_path}")
    print(f"Bin path           : {install_bin_path}")
    print(f"Python home        : {install_home_path}")
    print()

    # 1. Apache httpd conf templates
    template_glob = os.path.join(prefix, "etc", "idds", "rest", "*.conf.template")
    conf_files = sorted(glob.glob(template_glob))

    if conf_files:
        print(f"Generating Apache conf .install_template files ({len(conf_files)} found):")
        replace_python_path(conf_files, python_lib_path, install_bin_path, install_home_path)
    else:
        print(f"[info] No *.conf.template files found under {template_glob}")

    print()

    # 2. WSGI entry-point
    wsgi_template = os.path.join(install_bin_path, "idds.wsgi.template")
    print("Generating idds.wsgi from template:")
    replace_data_path(wsgi_template)

    print()
    print("Done.")
    if conf_files:
        print("Copy the desired .install_template to /etc/httpd/conf.d/ to activate, e.g.:")
        print(f"  cp {prefix}/etc/idds/rest/httpd-idds-443-py39-cc7.conf.install_template \\")
        print("     /etc/httpd/conf.d/httpd-idds-443-py39-cc7.conf")


if __name__ == "__main__":
    main()
