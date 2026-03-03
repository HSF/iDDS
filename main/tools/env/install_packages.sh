#!/bin/bash
set -m
for package in common workflow client main doma atlas website monitor ;
do
  python3 -m pip install $package/dist/*.whl 2>/dev/null || python3 -m pip install $package/dist/*.tar.gz
done

# Generate Apache httpd *.install_template files with paths resolved from the
# *installed* Python environment (sys.prefix, site-packages, etc.).
# This must run AFTER pip install so the paths are those of the target env,
# not the temporary build environment.
echo "Generating httpd conf .install_template files ..."
python3 "$(python3 -c 'import sys; print(sys.prefix)')/tools/env/setup_httpd_conf.py" || true
