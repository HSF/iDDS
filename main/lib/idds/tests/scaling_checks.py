#!/usr/bin/env python

from tabulate import tabulate

from idds.common.utils import json_dumps                 # noqa F401
from idds.common.constants import ContentStatus          # noqa F401
from idds.core.requests import get_requests              # noqa F401
from idds.core.messages import retrieve_messages         # noqa F401
from idds.core.transforms import get_transforms          # noqa F401
from idds.core.workprogress import get_workprogresses    # noqa F401
from idds.core.processings import get_processings        # noqa F401
from idds.core import transforms as core_transforms      # noqa F401


def show_req_transforms(request_id):
    reqs = get_requests(request_id=request_id, with_detail=False)
    trfs = get_transforms(request_id=request_id)
    prs = get_processings(request_id=request_id)
    reqs_times = {}
    trfs_times = {}
    prs_times = {}
    for req in reqs:
        reqs_times[req['request_id']] = {'created_at': req['created_at']}
    for trf in trfs:
        trfs_times[trf['transform_id']] = {'request_id': trf['request_id'], 'created_at': trf['created_at']}
    for pr in prs:
        prs_times[pr['processing_id']] = {'request_id': pr['request_id'], 'transform_id': pr['transform_id'], 'created_at': pr['created_at']}

    table = []
    title = ['request_id', 'req_created_at', 'transform_id', 'tf_created_at', 'processing_id', 'pr_created_at', 'request-processing']
    table.append(title)
    pr_ids = sorted(list(prs_times.keys()))
    for pr_id in pr_ids:
        req_id = prs_times[pr_id]['request_id']
        tf_id = prs_times[pr_id]['transform_id']
        row = [req_id, reqs_times[req_id]['created_at'], tf_id, trfs_times[tf_id]['created_at'], pr_id, prs_times[pr_id]['created_at'], (prs_times[pr_id]['created_at'] - reqs_times[req_id]['created_at']).seconds / 60]
        table.append(row)
    print(tabulate(table))


if __name__ == '__main__':
    req_ids = [i for i in range(100, 110)]
    # req_ids = [99]
    for req_id in req_ids:
        show_req_transforms(req_id)
