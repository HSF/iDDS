#!/bin/bash

CurrentDir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
RootDir="$( dirname $( dirname "$CurrentDir" ))"
echo $RootDir

export IDDS_HOME=$RootDir
export IDDS_CONFIG=$RootDir/etc/idds/idds.cfg.sqlite
export PYTHONPATH=$RootDir/lib:$PYTHONPATH

#python tools/orm/create_database.py
#python -m unittest2 discover -v lib/ess/tests/ "test_*.py"
#python tools/orm/destory_database.py

