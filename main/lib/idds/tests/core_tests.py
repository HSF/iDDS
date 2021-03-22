from idds.common.utils import json_dumps
from idds.core.requests import get_requests
from idds.core.messages import retrieve_messages
from idds.core.transforms import get_transforms
from idds.core.workprogress import get_workprogresses
from idds.core.processings import get_processings


rets = get_requests(request_id=91, with_detail=True)
for req in rets:
    # print(rets)
    print(json_dumps(req, sort_keys=True, indent=4))
    pass

reqs = get_requests()
# print(len(reqs))
for req in reqs:
    if req['request_id'] == 91:
        # print(req)
        # print(req['request_metadata']['workflow'].to_dict())
        # print(json_dumps(req, sort_keys=True, indent=4))
        pass

tfs = get_transforms()
for tf in tfs:
    if tf['request_id'] == 91:
        # print(tf)
        # print(tf['transform_metadata']['work'].to_dict())
        # print(json_dumps(tf, sort_keys=True, indent=4))
        pass

msgs = retrieve_messages()
for msg in msgs:
    # if msg['msg_id'] in [323720]:
    # if True:
    # if msg['request_id'] in [208]:
    if msg['transform_id'] in [1186]:
        print(msg)
        # print(msg['msg_content'])
        pass
    pass

wps = get_workprogresses()
for wp in wps:
    if wp['workprogress_id'] == 79:
        # print(json_dumps(wp, sort_keys=True, indent=4))
        pass

prs = get_processings()
for pr in prs:
    if pr['request_id'] == 91:
        # print(json_dumps(pr, sort_keys=True, indent=4))
        pass
