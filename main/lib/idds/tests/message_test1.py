
import json

from idds.core.messages import retrieve_messages

msgs = retrieve_messages()
for msg in msgs:
    if msg['msg_id'] in [323720]:
        # print(msg)
        msg_sent = json.dumps(msg['msg_content'])
        print(msg_sent)

        recover_msg = json.loads(msg_sent)
        if recover_msg['relation_type'] == 'output':
            # only parse messages from output contents
            points = recover_msg['files']
            for point in points:
                hp_loss = point['path']
                hp_loss = json.loads(hp_loss)
                hp, loss = hp_loss
                print(hp)
                print(loss)
                model_id, hp_point = hp
                print(model_id)
                print(hp_point)
