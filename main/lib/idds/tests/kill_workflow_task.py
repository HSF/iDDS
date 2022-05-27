#!/usr/bin/env python

"""
Test client.
"""

import argparse
# import os

# os.environ['PANDA_AUTH'] = 'oidc'
# os.environ['PANDA_URL_SSL'] = 'https://pandaserver-doma.cern.ch:25443/server/panda'
# os.environ['PANDA_URL'] = 'http://pandaserver-doma.cern.ch:25080/server/panda'
# os.environ['PANDA_AUTH_VO'] = 'Rubin'
# os.environ['PANDA_CONFIG_ROOT'] = '~/.panda/'

from idds.common.constants import RequestStatus, ProcessingStatus    # noqa E402
import idds.common.utils as idds_utils                               # noqa E402
import pandaclient.idds_api                                          # noqa E402


parser = argparse.ArgumentParser()
parser.add_argument('--workflow_id', dest='workflow_id', action='store', help='Workflow to kill', required=True)
parser.add_argument('--task_id', dest='task_id', action='store', help='Task to kill', required=False)


def kill_workflow_task(idds_server, request_id, task_id=None):
    c = pandaclient.idds_api.get_api(idds_utils.json_dumps,
                                     idds_host=idds_server, compress=True, manager=True)
    if task_id is None:
        ret = c.abort(request_id=request_id)
    else:
        ret = c.abort_tasks(request_id=request_id, task_id=task_id)

    print("Command is sent to iDDS: ", str(ret))


if __name__ == '__main__':
    host = "https://aipanda015.cern.ch:443/idds"

    args = parser.parse_args()
    kill_workflow_task(host, args.workflow_id, args.task_id)
