#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2021 - 2022

import argparse
import sys
import os.path
base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(base_path)
os.chdir(base_path)

from idds.orm.base.utils import destroy_database, destroy_everything      # noqa E402


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--destroy-everything", action="store_true", default=False, help='Destroy all tables+constraints')
    args = parser.parse_args()
    if args.destroy_everything:
        destroy_everything()
    else:
        destroy_database()
