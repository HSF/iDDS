import pandaclient.idds_api as idds_api
import idds.common.utils as idds_utils

# setup connection and get jtids
conn = idds_api.get_api(idds_utils.json_dumps, idds_host=None, compress=True, manager=True)
reqid = 5460
ret = conn.get_requests(request_id=int(reqid), with_detail=True)
print(ret)
jtids = [task["transform_workload_id"] for task in ret[1][1] if task["transform_status"]["attributes"]["_name_"] != "Finished"]
print(jtids)

# this first jtid works fine
jtid = jtids[0]
print(jtid)
ret = conn.get_contents_output_ext(request_id=reqid, workload_id=jtid)
print(ret[1][0])
# print(ret[1][1])

# other jtids do not work
jtid = jtids[1]
print(jtid)
ret = conn.get_contents_output_ext(request_id=reqid, workload_id=jtid)
print(ret[1][0])
print(ret[1][1])
