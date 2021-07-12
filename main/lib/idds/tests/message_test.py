
from idds.core.messages import retrieve_messages

"""
msgs = retrieve_messages(bulk=1000000)
to_check_msgs = []
for msg in msgs:
    # print(msg['msg_id'], msg)
    if msg['msg_content']['workload_id'] == 20525133:
        to_check_msgs.append(msg)
for to_check_msg in to_check_msgs:
    print(to_check_msg)
    contents = to_check_msg['msg_content']['files']
    print("number of files: %s" % len(contents))
    for content in contents:
        pass
        # print(content)
"""
msgs = retrieve_messages(bulk_size=1000000)
for msg in msgs:
    if msg['request_id'] == 152:
        print(msg)
