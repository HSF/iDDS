from idds.common.constants import RequestStatus       # noqa F401
from idds.common.utils import json_loads              # noqa F401

from lsst.ctrl.bps import BPS_DEFAULTS, BPS_SEARCH_ORDER, DEFAULT_MEM_FMT, DEFAULT_MEM_UNIT, BpsConfig    # noqa F401
from lsst.ctrl.bps.panda.utils import (               # noqa F401
    add_final_idds_work,                              # noqa F401
    add_idds_work,                                    # noqa F401
    copy_files_for_distribution,                      # noqa F401
    get_idds_client,                                  # noqa F401
    get_idds_result,                                  # noqa F401
)

default_config = BpsConfig(BPS_DEFAULTS)

idds_client = get_idds_client(default_config)

wms_workflow_id = 4112
# only check the request status
ret = idds_client.get_requests(request_id=wms_workflow_id)
print(ret)
# note: good to check the ret at first to make sure it's successful (see ctrl_bps_panda)
print(ret[1][1][0]['status'])

# to show the status of different tasks
ret = idds_client.get_requests(request_id=wms_workflow_id, with_detail=True)
print(ret)

workloads = []
for workload in ret[1][1]:
    workloads.append(workload['transform_workload_id'])
print(workloads)

# show one workload file information
workload_1 = workloads[0]
ret = idds_client.get_contents_output_ext(request_id=wms_workflow_id, workload_id=workload_1)
print(ret)
