import json
import socket
import asyncio
from nats.aio.client import Client as NATS


nats_server = {"nats_url": "nats://idds-dev-rest-0.idds-dev-rest.panda.svc.cluster.local:4222", "nats_token": "my_default_token"}
nats_server = {'nats_url': 'nats://idds-dev-rest-0.idds-dev-rest.panda.svc.cluster.local:4222', 'nats_token': 'my_default_token'}


async def send():
    nats = NATS()
    await nats.connect(servers=[nats_server["nats_url"]], token=nats_server["nats_token"])

    event = {"type": 1, "a": 123}
    js = nats.jetstream()
    await js.publish("event.UpdateProcessing", json.dumps(event).encode("utf-8"), timeout=5)
    await nats.flush()
    print("published")


async def get():
    nats = NATS()
    await nats.connect(servers=[nats_server["nats_url"]], token=nats_server["nats_token"])
    js = nats.jetstream()
    msg = await js.get_msg(stream_name="event_stream", subject="event.UpdateProcessing2", direct=True, seq=1, next=False)
    print(msg)
    print(type(msg))
    data = msg.data.decode()
    print(data)
    print(type(data))
    # msg.ack()
    print(f"stream: {msg.stream}, seq: {msg.seq}, subject: {msg.subject}")
    await js.delete_msg(stream=msg.stream, seq=msg.seq)


async def get_and_ack():
    nc = NATS()
    await nc.connect(servers=[nats_server["nats_url"]], token=nats_server["nats_token"])

    js = nc.jetstream()

    # Attach to a durable consumer (FIFO delivery)
    short_hostname = socket.gethostname().split(".")[0]
    durable = f"event_UpdateProcessing_{short_hostname}"   # must without "."

    # sub = await js.pull_subscribe("event.UpdateProcessing", durable="worker1")
    sub = await js.pull_subscribe("event.UpdateProcessing", durable=durable)

    # Fetch one message
    msgs = await sub.fetch(1)
    for msg in msgs:
        print(type(msg))
        print("Got message:", msg.data.decode())
        print('----------------------')
        print('Subject:', msg.subject)
        print('Reply  :', msg.reply)
        print('Data   :', msg.data)
        print('Headers:', msg.header)
        await msg.ack()  # This works, because msg is a `Msg`, not a RawStreamMsg

    await nc.close()


async def nc_get_and_ack():
    nc = NATS()
    await nc.connect(servers=[nats_server["nats_url"]], token=nats_server["nats_token"])

    sub = await nc.subscribe("event1.UpdateProcessing", max_msgs=1)
    msg = await sub.next_msg(timeout=5)
    if msg:
        print(type(msg))
        print("Got message:", msg.data.decode())
        print('----------------------')
        print('Subject:', msg.subject)
        print('Reply  :', msg.reply)
        print('Data   :', msg.data)
        print('Headers:', msg.header)
        # await msg.ack()  # This works, because msg is a `Msg`, not a RawStreamMsg

    await nc.close()


if __name__ == "__main__":
    # asyncio.run(send())
    # asyncio.run(get())
    asyncio.run(get_and_ack())
    # asyncio.run(nc_get_and_ack())
