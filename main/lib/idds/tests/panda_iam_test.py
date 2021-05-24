
import pandatools.idds_api
import idds.common.utils as idds_utils

c = pandatools.idds_api.get_api(idds_utils.json_dumps, idds_host='https://aipanda160.cern.ch:443/idds', compress=True, manager=True)
# c = pandatools.idds_api.get_api(idds_utils.json_dumps, idds_host='https://aipanda015.cern.ch:443/idds', compress=True, manager=True)

ret = c.abort(request_id=38)
print(ret)

"""
ret = c.resume(request_id=27)
print(ret)

ret = c.retry(request_id=27)
print(ret)

ret = c.get_status(request_id=27)
if ret[0] == 0 and ret[1][0] is True:
    print(ret[1][-1])

ret = c.get_status(request_id=27, with_detail=True)
if ret[0] == 0 and ret[1][0] is True:
    print(ret[1][-1])
"""
