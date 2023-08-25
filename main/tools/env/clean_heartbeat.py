#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2023

import socket
from idds.common.utils import pid_exists
from idds.core import health as core_health


def clean_heartbeat():
    hostname = socket.getfqdn()
    health_items = core_health.retrieve_health_items()
    pids, pid_not_exists = [], []
    for health_item in health_items:
        if health_item['hostname'] == hostname:
            pid = health_item['pid']
            if pid not in pids:
                pids.append(pid)
    for pid in pids:
        if not pid_exists(pid):
            pid_not_exists.append(pid)
    if pid_not_exists:
        core_health.clean_health(hostname=hostname, pids=pid_not_exists, older_than=None)


if __name__ == "__main__":
    clean_heartbeat()
