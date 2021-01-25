#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019 - 2020


from idds.common.constants import (WorkprogressStatus)
from idds.common.utils import is_sub


def get_workprogresses_status(wp_status):
    if not wp_status:
        return False

    if not isinstance(wp_status, (list, tuple)):
        wp_status = [wp_status]
    if len(wp_status) == 1:
        return wp_status[0]
    elif is_sub(wp_status, [WorkprogressStatus.Finished]):
        return WorkprogressStatus.Finished
    elif is_sub(wp_status, [WorkprogressStatus.Finished, WorkprogressStatus.SubFinished]):
        return WorkprogressStatus.SubFinished
    elif is_sub(wp_status, [WorkprogressStatus.Finished, WorkprogressStatus.SubFinished,
                            WorkprogressStatus.Failed]):
        return WorkprogressStatus.Failed
    elif is_sub(wp_status, [WorkprogressStatus.Finished, WorkprogressStatus.SubFinished,
                            WorkprogressStatus.Failed, WorkprogressStatus.Cancelled]):
        return WorkprogressStatus.Cancelled
    elif is_sub(wp_status, [WorkprogressStatus.Finished, WorkprogressStatus.SubFinished,
                            WorkprogressStatus.Failed, WorkprogressStatus.Cancelled,
                            WorkprogressStatus.Transforming]):
        return WorkprogressStatus.Transforming
    elif is_sub(wp_status, [WorkprogressStatus.Finished, WorkprogressStatus.SubFinished,
                            WorkprogressStatus.Failed, WorkprogressStatus.Cancelled,
                            WorkprogressStatus.Transforming, WorkprogressStatus.Cancelling]):
        return WorkprogressStatus.Cancelling
    else:
        return WorkprogressStatus.Transforming
