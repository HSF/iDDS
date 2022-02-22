#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2021


from idds.workflowv2.utils import show_relation_map


relation_map = [
                   {   # noqa E126
                       "work": {
                           "external_id": None,
                           "workload_id": 8492
                       }
                   },
                   {
                       "work": {
                           "external_id": None,
                           "workload_id": 8493
                       }
                   },
                   {
                       "work": {
                           "external_id": None,
                           "workload_id": 8491
                       }
                   },
                   {
                       "work": {
                           "external_id": None,
                           "workload_id": 8496
                       }
                   },
                   {
                       "work": {
                           "external_id": None,
                           "workload_id": 8494
                       }
                   },
                   {
                       "next_works": [
                           {
                               "work": {
                                   "external_id": None,
                                   "workload_id": 8497
                               }
                           }
                       ],
                       "work": {
                           "external_id": None,
                           "workload_id": 8495
                       }
                   }
               ]    # noqa E126


if __name__ == "__main__":
    show_relation_map(relation_map)
