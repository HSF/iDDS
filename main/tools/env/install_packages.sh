#!/bin/bash
set -m
for package in common workflow client main doma atlas website monitor ;
do
  python3 -m pip install $package/dist/*.whl 2>/dev/null || python3 -m pip install $package/dist/*.tar.gz
done
