## Prompt Agent

This document describes the message contract and workflow for the prompt "transceiver" agent used by iDDS.

Goals:
- Define a minimal, consistent message format used by the transceiver, transformer, and worker handlers.
- Provide concrete examples for common message types (run start/stop, slice, and transformer lifecycle events).

---

### 1. Message contract (headers)

All messages MUST include `instance`, `msg_type`, and `run_id` in the headers. Use `instance` to identify the deployment (for example `prod` or `dev_<userid>`), `msg_type` to indicate the purpose of the message, and `run_id` to scope messages to a specific run. Keeping these fields consistent makes it easier to extend message types: processors only need to understand changes to `content` while the outer contract remains stable.

Broker-side filtering (selectors) is used so messages are filtered before delivery. On ActiveMQ, use a `topic` when you want broadcasts (every subscriber receives the message) and a `queue` for point-to-point delivery (one consumer receives each message). In production you typically run multiple agent processes per `instance` behind a queue so work is distributed. To avoid fetching and discarding messages locally, use header selectors (broker-side) — e.g. a STOMP selector like:

```python
headers['selector'] = "instance='prod' AND run_id='12345'"
```

Defining standard headers enables consistent use of selectors across the system.

Example headers (Python dict):

```python
headers = {
    'persistent': 'true',
    'ttl': self.timetolive,     # default: 12 * 3600 * 1000 (milliseconds)
    'vo': 'eic',
    'instance': instance_name,  # e.g. 'prod', 'dev1', 'dev_<userid>'
    'msg_type': message_type,
    'run_id': run_id,
}
```

Message body keys (recommended):
- `instance` (string)
- `msg_type` (string)
- `run_id` (int or string)
- `created_at` (UTC timestamp)
- `content` (dict): message-specific payload

Result/response messages should include timing fields inside `content` to support latency measurements:
- `requested_at` (copied from the request's `created_at`)
- `processing_start_at`
- `processed_at`

Example `slice_result`:

```python
msg = {
    'instance': instance_name,
    'msg_type': 'slice_result',
    'run_id': run_id,
    'created_at': datetime.datetime.utcnow(),
    'content': {
        'requested_at': slice_created_at,
        'processing_start_at': processing_start,
        'processed_at': processing_end,
        'result': { ... },
    }
}
```

---

### 2. Input messages from the SWF Processing Agent

The `content` schema is flexible and can be adapted by EIC. Below are suggested, well-formed examples.

Run imminent (start):

```python
start_msg = {
    'instance': instance_name,
    'msg_type': 'run_imminent',
    'run_id': run_id,
    'created_at': datetime.datetime.utcnow(),
    'content': {
        'num_workers': 2,
        'num_cores_per_worker': 10,
        'num_ram_per_core': 4000.0,  # MB
        # optional: copy of original DAQ/PA fields
        'msg_type': 'start_run',
        'req_id': 1,
        'ts': '20250914185722',
    }
}
```

Slice message example:

```python
slice_msg = {
    'instance': instance_name,
    'msg_type': 'slice',
    'run_id': run_id,
    'created_at': datetime.datetime.utcnow(),
    'content': {
        'run_id': run_id,
        'state': 'no_beam',
        'substate': 'calib',
        'filename': 'swf.20250914.185724.767135.no_beam.calib.stf',
        'start': '20250914185722420185',
        'end': '20250914185724767135',
        'checksum': 'ad:3915264619',
        'size': 191,
        'msg_type': 'stf_gen',
        'req_id': 1,
    }
}
```

Run stop (end):

```python
stop_msg = {
    'instance': instance_name,
    'msg_type': 'run_stop',
    'run_id': run_id,
    'created_at': datetime.datetime.utcnow(),
    'content': {
        'req_id': 1,
        'run_id': run_id,
        'ts': '20250914185722',
    }
}
```

---

### 3. Worker handler behavior

When receiving `run_imminent`, the worker handler should:

- Create an iDDS workflow and PanDA tasks (via `create_workflow_task(msg)`).
- Send an adjuster message to Harvester to start workers.

Example adjuster message to start workers:

```python
start_worker_msg = {
    'instance': instance_name,
    'msg_type': 'adjuster_worker',
    'run_id': run_id,
    'created_at': datetime.datetime.utcnow(),
    'content': {
        'num_workers': start_msg['content']['num_workers'],
        'num_cores_per_worker': start_msg['content']['num_cores_per_worker'],
        'num_ram_per_core': start_msg['content']['num_ram_per_core'],
        'requested_at': start_msg['created_at'],
    }
}
```

When receiving `run_stop`, the handler should close the PanDA task and send stop adjuster messages:

```python
# close PanDA task to avoid retries
task_id = get_task_id_from_run_id(stop_msg['run_id'])
close_panda_task(task_id)

stop_worker_msg = {
    'instance': instance_name,
    'msg_type': 'adjuster_worker',
    'run_id': run_id,
    'created_at': datetime.datetime.utcnow(),
    'content': {
        'num_workers': 0,
        'num_cores_per_worker': 0,
        'num_ram_per_core': 0,
        'requested_at': stop_msg['created_at'],
    }
}

stop_transformer_msg = {
    'instance': instance_name,
    'msg_type': 'stop_transformer',
    'run_id': run_id,
    'created_at': datetime.datetime.utcnow(),
    'content': {
        'requested_at': stop_msg['created_at'],
    }
}
```

---

### 4. Creating workflow tasks (TODO)

Create iDDS workflow and PanDA task. The iDDS poller will monitor the task and the number of jobs. If the PanDA task has fewer jobs than expected (failures or other issues), iDDS can trigger PanDA to create new jobs.

One way to trigger job creation is to emit a synthetic Rucio `transfer-done` event that PanDA recognizes:

```python
transfer_done_msg = {
    'event_type': 'transfer-done',
    'created_at': datetime.datetime.utcnow(),
    'payload': {
        'activity': activity,
        'name': name,
        'scope': scope,
        'dst-rse': rse,
    }
}
```

---

### 5. Slice handler

Currently not required in the baseline; keep this section for future transformer-side forwarding logic and debugging helpers.

---

### 6. Transformer lifecycle messages

The transformer (running inside a pilot) consumes `slice` messages from the queue and may also receive `stop_transformer` via a topic.

Key lifecycle messages:

```python
slice_result_msg = {
    'instance': instance_name,
    'msg_type': 'slice_result',
    'run_id': run_id,
    'created_at': datetime.datetime.utcnow(),
    'content': {
        'requested_at': slice_created_at,
        'processing_start_at': processing_start,
        'processed_at': processing_end,
        'result': { ... },
    }
}

transformer_start_msg = {
    'instance': instance_name,
    'msg_type': 'transformer_start',
    'run_id': run_id,
    'created_at': datetime.datetime.utcnow(),
    'content': {'hostname': hostname, 'id': pilot_id}
}

transformer_end_msg = {
    'instance': instance_name,
    'msg_type': 'transformer_end',
    'run_id': run_id,
    'created_at': datetime.datetime.utcnow(),
    'content': {'hostname': hostname, 'id': pilot_id}
}

transformer_heartbeat_msg = {
    'instance': instance_name,
    'msg_type': 'transformer_heartbeat',
    'run_id': run_id,
    'created_at': datetime.datetime.utcnow(),
    'content': {'hostname': hostname, 'id': pilot_id}
}
```

---

### 7. Transformer subscription example

Use `client-individual` ack and selectors to ensure transformers only receive their intended messages. Example STOMP headers:

```python
ack = 'client-individual'
headers = {
    'vo': 'eic',
    'selector': "instance='{}' AND run_id='{}'".format(instance_name, run_id),
    'activemq.prefetchSize': '1',
}
```

---

Notes:
- Keep message timestamps in UTC.
- Preserve original PA/DAQ timestamps in `content` (useful for latency/evaluation).
- The examples above use Python dict literals but the wire format should be your chosen serialization (JSON, msgpack, etc.).
