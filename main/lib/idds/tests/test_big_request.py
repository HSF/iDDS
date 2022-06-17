import logging
import sys
import datetime


from idds.common.utils import setup_logging, json_dumps                 # noqa F401
setup_logging(__name__, stream=sys.stdout, loglevel=logging.DEBUG)

from idds.common.constants import ContentStatus, ContentType, ContentRelationType, ContentLocking          # noqa F401
from idds.core.requests import get_requests              # noqa F401
from idds.core.messages import retrieve_messages         # noqa F401
from idds.core.transforms import get_transforms          # noqa F401
from idds.core.workprogress import get_workprogresses    # noqa F401
from idds.core.processings import get_processings        # noqa F401
from idds.core import transforms as core_transforms      # noqa F401
from idds.orm.contents import get_input_contents         # noqa F401
from idds.core.transforms import release_inputs_by_collection, release_inputs_by_collection_old     # noqa F401


def show_works(req):
    workflow = req['processing_metadata']['workflow']
    print(workflow.independent_works)
    print(len(workflow.independent_works))
    print(workflow.works_template.keys())
    print(len(workflow.works_template.keys()))
    print(workflow.work_sequence.keys())
    print(len(workflow.work_sequence.keys()))
    print(workflow.works.keys())
    print(len(workflow.works.keys()))

    work_ids = []
    for i_id in workflow.works:
        work = workflow.works[i_id]
        print(i_id)
        print(work.work_name)
        print(work.task_name)
        print(work.work_id)
        work_ids.append(work.work_id)
    print(work_ids)


print("start at: ", datetime.datetime.utcnow())
# 283511, 283517
# reqs = get_requests(request_id=599, with_detail=True, with_metadata=True)
# reqs = get_requests(request_id=283511, with_request=True, with_detail=False, with_metadata=True)
# reqs = get_requests(request_id=298163, with_request=True, with_detail=False, with_metadata=True)
# reqs = get_requests(request_id=298557, with_request=True, with_detail=False, with_metadata=True)
# reqs = get_requests(request_id=299111, with_request=True, with_detail=False, with_metadata=True)
# reqs = get_requests(request_id=299235, with_request=True, with_detail=False, with_metadata=True)
# reqs = get_requests(request_id=965, with_request=True, with_detail=False, with_metadata=True)
reqs = get_requests(request_id=274, with_request=True, with_detail=False, with_metadata=True)
print("got requests at: ", datetime.datetime.utcnow())
for req in reqs:
    # print(req['request_id'])
    # print(rets)
    # print(json_dumps(req, sort_keys=True, indent=4))
    # show_works(req)
    pass
    workflow = req['request_metadata']['workflow']

    print("generating works at: ", datetime.datetime.utcnow())
    works = workflow.get_new_works()
    print("generated works at: ", datetime.datetime.utcnow())

    for work in works:
        new_work = work
        new_work.add_proxy(workflow.get_proxy())
print("end at: ", datetime.datetime.utcnow())
