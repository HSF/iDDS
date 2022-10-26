import sys

from pandaclient import Client

task_id = sys.argv[1]

status, output = Client.retryTask(task_id)
print(status)
print(output)
