#!/bin/bash
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019

# pip install git+https://github.com/wguanicedew/iDDS.git@dev
# git clone -b dev https://github.com/wguanicedew/iDDS.git
# cd iDDS
# bash main/tools/env/install_idds.sh

CurrentDir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
RootDir="$( dirname "$( dirname "$( dirname "$CurrentDir" )" )" )"

python ${RootDir}/build_all.py install


# ruff check .
# ruff check . --fix
# black --check .
# auto-format black

bash ${RootDir}/workflow/tools/workflow/make/make.sh

echo cp ${RootDir}/workflow/bin/run_workflow_wrapper ~/www/wiscgroup/
cp ${RootDir}/workflow/bin/run_workflow_wrapper ~/www/wiscgroup/
echo cp ${RootDir}/workflow/bin/run_workflow_wrapper /eos/user/w/wguan/www/
cp ${RootDir}/workflow/bin/run_workflow_wrapper /eos/user/w/wguan/www/

# echo scp workflow/bin/run_workflow_wrapper root@ai-idds-04:/data/iddssv1/srv/var/trf/user/
# scp workflow/bin/run_workflow_wrapper root@ai-idds-04:/data/iddssv1/srv/var/trf/user/

rm -fr ${RootDir}/workflow/bin/run_workflow_wrapper

# prompt wrapper
bash ${RootDir}/prompt/tools/prompt/make/make.sh

echo cp ${RootDir}/prompt/bin/run_prompt_wrapper ~/www/wiscgroup/
cp ${RootDir}/prompt/bin/run_prompt_wrapper ~/www/wiscgroup/
echo cp ${RootDir}/prompt/bin/run_prompt_wrapper /eos/user/w/wguan/www/
cp ${RootDir}/prompt/bin/run_prompt_wrapper /eos/user/w/wguan/www/

rm -fr ${RootDir}/prompt/bin/run_prompt_wrapper
