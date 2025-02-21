#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2021


import io
import re
import sys

new_version = sys.argv[1]


ver_files = ['main/lib/idds/core/version.py',
             'atlas/lib/idds/atlas/version.py',
             'common/lib/idds/common/version.py',
             'workflow/lib/idds/workflow/version.py',
             'client/lib/idds/client/version.py',
             'doma/lib/idds/doma/version.py',
             'website/lib/idds/website/version.py',
             'monitor/lib/idds/monitor/version.py']


env_files = ['atlas/tools/atlas/env/environment.yml',
             'common/tools/common/env/environment.yml',
             'main/tools/env/environment.yml',
             'client/tools/client/env/environment.yml',
             'doma/tools/doma/env/environment.yml',
             'workflow/tools/workflow/env/environment.yml']


for ver_file in ver_files:
    print(ver_file)
    with io.open(ver_file, "rt", encoding="utf8") as f:
        version = re.search(r'release_version = "(.*?)"', f.read()).group(1)

    with io.open(ver_file, "rt", encoding="utf8") as f:
        data = f.read()

    data = data.replace(version, new_version)
    with io.open(ver_file, "wt", encoding="utf8") as f:
        f.write(data)

for env_file in env_files:
    print(env_file)

    data = None
    with io.open(env_file, "rt", encoding="utf8") as f:
        for line in f:
            stripline = line.strip()
            if stripline.startswith('- idds-'):
                pkg, ver = line.split("==")
                line = pkg + '==' + new_version + "\n"
            if data is None:
                data = line
            else:
                data = data + line
            # print(data)
    if data.endswith('\n'):
        data = data[:-1]

    with io.open(env_file, "wt", encoding="utf8") as f:
        f.write(data)
