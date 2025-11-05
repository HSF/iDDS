# Prompt Agent 

This guide describes the workflow of the prompt **transceiver** agent.

---

## 1. Input messages from SWF Processing Agent 

**run imminent:**

https://github.com/BNLNPPS/swf-daqsim-agent

```
start_msg = {
  'msg_type': 'run_imminent',
  'run_id': 20250914185722,
  'create_at': datetime.datetime.utcnow(),
  'num_workers': 2,
  'num_cores_per_worker": 10,
  'num_ram_per_core': 4000.0,      # MB
  'content': {
    "msg_type": "start_run", "req_id": 1, "run_id": 20250914185722, "ts": "20250914185722"
    }
  }
````

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
  'create_at': datetime.datetime.utcnow(),
  'content': {
    "msg_type": "end_run", "req_id": 1, "run_id": 20250914185722, "ts": "20250914185722"
    }
  }
````

---

## 2. Worker handler

(Here we keep the original message from the PA(at least keep the timestamp), for debugging and evaluation to check the delays)
When receiving 'run_imminent' message, start workers:

```
# Create iDDS workflow and PanDA tasks (with PanDA jobs)
task_id = create_workflow_task(msg)

# generate worker adjuster message and send the message to Harvester
start_worker_msg = {
  'msg_type': 'adjuster_worker',
  'num_workers': start_msg['num_workers'],
  'num_cores_per_worker": start_msg['num_cores_per_worker'],
  'num_ram_per_core': start_msg['num_ram_per_core'],
  'create_at': datetime.datetime.utcnow(),
  'content': start_msg,
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
  'num_workers': 0,
  'num_cores_per_worker": 0,
  'num_ram_per_core': 0,
  'create_at': datetime.datetime.utcnow(),
  'content': end_msg
  }

# broadcast messages to all transformer workers attached to this task
headers={
  'persistent': 'true',
  'ttl': self.timetolive,
  'vo': 'eic',
  'msg_type': 'end_transformer',
  'task_id': task_id,
})
stop_transformer_msg = {
  'msg_type': 'stop_transformer',
  'create_at': datetime.datetime.utcnow(),
  'content': end_msg
  }

```

---

## 3. (Todo) Create Workflow task

Create iDDS workflow and PanDA task. iDDS poller will automatically monitor the task to check the number of jobs.

```
# todo
```

If there are not enough panda jobs (because of some failurees), iDDS needs to send messages to PanDA to create new jobs;

```
# to generate rucio 'transfer-done' messages
transfer_done_msg = {
  'event_type': 'transfer-done':
  'payload': {
    'activity': activity,
    'name': name,
    'scope': scope,
    'dst-rse': rse
  }
}
```

## 4. Slice handler

When receiving slice messages from the PA, sending message to the transformer (adding task_id)

```
headers={
  'persistent': 'true',
  'ttl': self.timetolive,
  'vo': 'eic',
  'msg_type': 'transform_slice',
  'task_id': task_id,
})

transformer_msg = {
  'msg_type': 'transform_slice',
  'run_id': slice_msg['run_id'],
  'created_at': datetime.datetime.utcnow(),
  'task_id': task_id, 
  'content': slice_msg
}
```

## 5. Transformer

The transformer runs in the pilot. It receives the transformer_msg (from queue) and end_transformer_msg (from topic) messages.

```
# slice result
transformer_result_msg = {
  'msg_type': 'transform_slice_result',
  'run_id': slice_msg['run_id'],
  'created_at': datetime.datetime.utcnow(),
  'task_id': task_id, 
  'content': slice_result
}

# transformer_start
transformer_start_msg = {
  'msg_type': 'transformer_start',
  'run_id': slice_msg['run_id'],
  'created_at': datetime.datetime.utcnow(),
  'task_id': task_id, 
  'content': {'hostname': socket.getFQAN()}
}

# transformer_end
transformer_end_msg = {
  'msg_type': 'transformer_end',
  'run_id': slice_msg['run_id'],
  'created_at': datetime.datetime.utcnow(),
  'task_id': task_id, 
  'content': {'hostname': socket.getFQAN()}
}

# transformer_heartbeat
transformer_heartbeat_msg = {
  'msg_type': 'transformer_heartbeat',
  'run_id': slice_msg['run_id'],
  'created_at': datetime.datetime.utcnow(),
  'task_id': task_id, 
  'content': {'hostname': socket.getFQAN()}
}
```