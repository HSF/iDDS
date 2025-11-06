# Prompt Agent 

This guide describes the workflow of the prompt **transceiver** agent.

---

## 1. Message format

All message header should contain 'run_id'. The transformer will use the 'run_id' to make sure that one transformer can only consume messages with a specified 'run_id'.

```
headers={
  'persistent': 'true',
  'ttl': self.timetolive,     # default 12 * 3600 * 1000. milliseconds
  'vo': 'eic',
  'msg_type': <message_type>,
  'run_id': run_id,
}
```

The message format should include 'msg_type', 'run_id', 'created_at' and 'content'. For different types of messages, the content can be different.

```
msg = {
  'msg_type': '<message_type>',
  'run_id': 20250914185722,
  'created_at': datetime.datetime.utcnow(),
  'content': {}
}
```

For result/reponse message, it should inlcude 'requested_at' (copy from the request message's 'created_at') and 'processed_at' in the content. For example, for a 'slice' message, it has a 'created_at'. When a transformer successfully processes it, the result message should copy the slice's 'created_at' to  'requested_at' and add 'processed_at' in the content. In this way, we can evaluate the delay.

```
msg = {
  'msg_type': 'slice_result',
  'run_id': 20250914185722,
  'created_at': datetime.datetime.utcnow(),
  'content': {
    'requested_at': <copied from slice's create_at>,
    'processing_start_at': <utctime>,
    'processed_at': <utctime>
  }
}
```
---

## 2. Input messages from SWF Processing Agent 

Here is just a suggestion. EIC can change the message 'content' format.

**run imminent:**

(https://github.com/BNLNPPS/swf-daqsim-agent)

```
start_msg = {
  'msg_type': 'run_imminent',
  'run_id': 20250914185722,
  'created_at': datetime.datetime.utcnow(),
  'content': {
    'num_workers': 2,
    'num_cores_per_worker": 10,
    'num_ram_per_core': 4000.0,      # MB
    # copied from daqsim
    "msg_type": "start_run", "req_id": 1, "run_id": 20250914185722, "ts": "20250914185722". 
    }
  }
```

**slice message:**

```
slice_msg = {
  'msg_type': 'slice',
  'run_id': 20250914185722,
  'created_at': datetime.datetime.utcnow(),
  'content': {
    "run_id": 20250914185722, "state": "no_beam", "substate": "calib", "filename": "swf.20250914.185724.767135.no_beam.calib.stf", "start": "20250914185722420185", "end": "20250914185724767135", "checksum": "ad:3915264619", "size": 191, "msg_type": "stf_gen", "req_id": 1
    }
}
```

**run end:**

```
stop_msg = {
  'msg_type': 'run_stop',
  'run_id': 20250914185722,
  'created_at': datetime.datetime.utcnow(),
  'content': {
    "req_id": 1, "run_id": 20250914185722, "ts": "20250914185722"
  }
}
```

---

## 3. Worker handler

(Here we keep the original message from the PA(at least keep the timestamp), for debugging and evaluation to check the delays)
When receiving 'run_imminent' message, start workers:

```
# Create iDDS workflow and PanDA tasks (with PanDA jobs)
task_id = create_workflow_task(msg)

# generate worker adjuster message and send the message to Harvester
start_worker_msg = {
  'msg_type': 'adjuster_worker',
  'run_id': 20250914185722,
  'created_at': datetime.datetime.utcnow(),
  'content': {
    'num_workers': start_msg['num_workers'],
    'num_cores_per_worker": start_msg['num_cores_per_worker'],
    'num_ram_per_core': start_msg['num_ram_per_core'],
    'requested_at': start_msg['created_at']
  }
}
```

When receiving 'run_end' message, stop workers:

```
# Close to PanDA task to avoid PanDA retrying jobs
task_id = get_task_id_from_run_id(end_msg['run_id'])
close_panda_task(task_id)

# generate worker adjuster message and send the message to Harvester
stop_worker_msg = {
  'msg_type': 'adjuster_worker',
  'run_id': 20250914185722,
  'created_at': datetime.datetime.utcnow(),
  'content': {
    'num_workers': 0,
    'num_cores_per_worker": 0,
    'num_ram_per_core': 0,
    'requested_at': stop_msg['created_at']
  }
}

# broadcast messages to all transformer workers attached to this task
stop_transformer_msg = {
  'msg_type': 'stop_transformer',
  'run_id': 20250914185722,
  'created_at': datetime.datetime.utcnow(),
  'content': {
    'requested_at': stop_msg['created_at']
  }
}

```

---

## 4. (Todo) Create Workflow task

Create iDDS workflow and PanDA task. iDDS poller will automatically monitor the task to check the number of jobs.

```
# todo
```

If there are not enough panda jobs/workers (because of some failurees), iDDS needs to send messages to PanDA to create new jobs; Currently PanDA supports to generate jobs based on Rucio's 'transfer-done' messages. Here we need to create fake Rucio messages to trigger PanDA to generate new jobs.

```
# to generate fake Rucio 'transfer-done' messages, to trigger PanDA to generate new jobs
transfer_done_msg = {
  'event_type': 'transfer-done',
  'created_at': datetime.datetime.utcnow(),
  'payload': {
    'activity': activity,
    'name': name,
    'scope': scope,
    'dst-rse': rse
  }
}
```

## 5. Slice handler

(Not needed currently)

```
```

## 6. Transformer

The transformer runs in the pilot. It receives the slice_msg (from queue) and stop_transformer_msg (from topic) messages.

```
# slice result
slice_result_msg = {
  'msg_type': 'slice_result',
  'run_id': 20250914185722,
  'created_at': datetime.datetime.utcnow(),
  'content': {
    'requested_at': <copied from slice's created_at>,
    'processing_start_at': <utctime>,
    'processed_at': <utctime>,
    ...
    'result': {'state': , 'attribute': 'value'}
    
  }
}

# transformer_start
transformer_start_msg = {
  'msg_type': 'transformer_start',
  'run_id': slice_msg['run_id'],
  'created_at': datetime.datetime.utcnow(),
  'content': {'hostname': socket.getFQAN(), 'id': <pilot_id>, ...}
}

# transformer_end
transformer_end_msg = {
  'msg_type': 'transformer_end',
  'run_id': slice_msg['run_id'],
  'created_at': datetime.datetime.utcnow(),
  'content': {'hostname': socket.getFQAN(), 'id': <pilot_id>, ...}
}

# transformer_heartbeat
transformer_heartbeat_msg = {
  'msg_type': 'transformer_heartbeat',
  'run_id': slice_msg['run_id'],
  'created_at': datetime.datetime.utcnow(),
  'content': {'hostname': socket.getFQAN(), 'id': <pilot_id>, ...}
}
```