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


ver_files = ['main/lib/idds/version.py',
             'atlas/lib/idds/atlas/version.py',
             'common/lib/idds/common/version.py',
             'workflow/lib/idds/workflow/version.py',
             'client/lib/idds/client/version.py',
             'doma/lib/idds/doma/version.py']


for ver_file in ver_files:
    print(ver_file)
    with io.open(ver_file, "rt", encoding="utf8") as f:
        version = re.search(r'release_version = "(.*?)"', f.read()).group(1)

    with io.open(ver_file, "rt", encoding="utf8") as f:
        data = f.read()

    data = data.replace(version, new_version)
    with io.open(ver_file, "wt", encoding="utf8") as f:
        f.write(data)
