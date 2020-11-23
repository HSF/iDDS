#!/bin/bash

CurrentDir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
RootDir="$( dirname $( dirname "$CurrentDir" ))"
echo $RootDir

export ESS_HOME=$RootDir
export PYTHONPATH=$RootDir/lib:$PYTHONPATH

python -m unittest2 discover -v main/lib/idds/tests/ "test_*.py"
