# from idds.common.constants import RequestStatus       # noqa F401
# from idds.common.utils import json_loads              # noqa F401

import idds.common.utils as idds_utils                # noqa F401
import pandaclient.idds_api                           # noqa F401


idds_client = pandaclient.idds_api.get_api(idds_utils.json_dumps, idds_host=None, compress=True, manager=True)

# wms_workflow_id = 4112
wms_workflow_id = 5194
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
workload_0 = workloads[0]
ret = idds_client.get_contents_output_ext(request_id=wms_workflow_id, workload_id=workload_0)
print(ret)

workload_1 = workloads[1]
ret = idds_client.get_contents_output_ext(request_id=wms_workflow_id, workload_id=workload_1)
print(ret)

workload_2 = workloads[2]
ret = idds_client.get_contents_output_ext(request_id=wms_workflow_id, workload_id=workload_2)
print(ret)
