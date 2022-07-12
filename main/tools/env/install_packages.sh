#!/bin/bash
set -m
for package in common workflow client main doma atlas website monitor ;
do
  python3 -m pip install `ls $package/dist/*.tar.gz`
done
