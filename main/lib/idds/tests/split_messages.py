#!/usr/bin/env python

from idds.common.constants import MessageStatus                        # noqa F401
from idds.core.messages import retrieve_messages, add_messages         # noqa F401


def clean_msg(msg):
    new_msg = {'msg_type': msg['msg_type'],
               'status': MessageStatus.New,
               'source': msg['source'],
               'destination': msg['destination'],
               'request_id': msg['request_id'],
               'workload_id': msg['workload_id'],
               'transform_id': msg['transform_id'],
               'num_contents': msg['num_contents'],
               'msg_content': msg['msg_content']}
    return new_msg


for req_id in range(1073, 1084):
    msgs = retrieve_messages(request_id=req_id)
    number_contents = 0
    for msg in msgs:
        # print(json_dumps(msg['msg_content'], sort_keys=True, indent=4))
        # if msg['num_contents'] > 10000 and msg['status'] == MessageStatus.New:
        if msg['num_contents'] >= 10000:
            print(msg['request_id'], msg['msg_id'], msg['num_contents'])
            msg = clean_msg(msg)
            add_messages([msg], bulk_size=10000)
        # break
