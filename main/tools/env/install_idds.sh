#!/bin/bash
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019

python setup.py install --old-and-unmanageable --force

bash workflow/tools/make/make.sh

echo cp workflow/bin/run_workflow_wrapper ~/www/wiscgroup/
cp workflow/bin/run_workflow_wrapper ~/www/wiscgroup/

echo scp workflow/bin/run_workflow_wrapper root@ai-idds-04:/data/iddssv1/srv/var/trf/user/
scp workflow/bin/run_workflow_wrapper root@ai-idds-04:/data/iddssv1/srv/var/trf/user/

rm -fr workflow/bin/run_workflow_wrapper
