import sys            # noqa F401
import datetime       # noqa F401

from idds.common.utils import json_dumps, setup_logging                 # noqa F401
from idds.common.constants import ContentStatus, ContentType, ContentRelationType, ContentLocking          # noqa F401
from idds.core.requests import get_requests              # noqa F401
from idds.core.messages import retrieve_messages         # noqa F401
from idds.core.transforms import get_transforms, get_transform          # noqa F401
from idds.core.workprogress import get_workprogresses    # noqa F401
from idds.core.processings import get_processings        # noqa F401
from idds.core import transforms as core_transforms      # noqa F401
from idds.orm.contents import get_contents               # noqa F401
from idds.core.transforms import release_inputs_by_collection, release_inputs_by_collection_old     # noqa F401
from idds.workflowv2.workflow import Workflow            # noqa F401
from idds.workflowv2.work import Work                    # noqa F401


setup_logging(__name__)


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


def print_workflow(workflow, layers=0):
    prefix = " " * layers * 4
    for run in workflow.runs:
        print(prefix + "run: " + str(run) + ", has_loop_condition: " + str(workflow.runs[run].has_loop_condition()))
        # if workflow.runs[run].has_loop_condition():
        #     print(prefix + " Loop condition: %s" % json_dumps(workflow.runs[run].loop_condition, sort_keys=True, indent=4))
        for work_id in workflow.runs[run].works:
            print(prefix + " " + str(work_id) + " " + str(type(workflow.runs[run].works[work_id])))
            if type(workflow.runs[run].works[work_id]) in [Workflow]:
                print(prefix + "   parent_num_run: " + workflow.runs[run].works[work_id].parent_num_run + ", num_run: " + str(workflow.runs[run].works[work_id].num_run))
                print_workflow(workflow.runs[run].works[work_id], layers=layers + 1)
                # print(prefix + "   is_terminated: " + str(workflow.runs[run].works[work_id].is_terminated()))
                # print(prefix + "   is_finished: " + str(workflow.runs[run].works[work_id].is_finished()))
            # elif type(workflow.runs[run].works[work_id]) in [Work]:
            else:
                work = workflow.runs[run].works[work_id]
                tf = get_transform(transform_id=work.get_work_id())
                if tf:
                    transform_work = tf['transform_metadata']['work']
                    # print(json_dumps(transform_work, sort_keys=True, indent=4))
                    work.sync_work_data(status=tf['status'], substatus=tf['substatus'], work=transform_work, workload_id=tf['workload_id'])

                print(prefix + "   or: " + str(work.or_custom_conditions) + " and: " + str(work.and_custom_conditions))
                print(prefix + "   output: " + str(work.output_data))
                print(prefix + "   " + workflow.runs[run].works[work_id].task_name + ", num_run: " + str(workflow.runs[run].works[work_id].num_run))
                print(prefix + "   workload_id: " + str(work.workload_id))
                print(prefix + "   is_terminated: " + str(workflow.runs[run].works[work_id].is_terminated()))
                print(prefix + "   is_finished: " + str(workflow.runs[run].works[work_id].is_finished()))
        if workflow.runs[run].has_loop_condition():
            print(prefix + " Loop condition status: %s" % workflow.runs[run].get_loop_condition_status())
            print(prefix + " Loop condition: %s" % json_dumps(workflow.runs[run].loop_condition, sort_keys=True, indent=4))


def print_workflow_template(workflow, layers=0):
    prefix = " " * layers * 4
    print(prefix + str(workflow.template.internal_id) + ", has_loop_condition: " + str(workflow.template.has_loop_condition()))
    for work_id in workflow.template.works:
        print(prefix + " " + str(work_id) + " " + str(type(workflow.template.works[work_id])))
        if type(workflow.template.works[work_id]) in [Workflow]:
            print(prefix + "   parent_num_run: " + str(workflow.template.works[work_id].parent_num_run) + ", num_run: " + str(workflow.template.works[work_id].num_run))
            print_workflow_template(workflow.template.works[work_id], layers=layers + 1)
        else:
            print(prefix + "   " + workflow.template.works[work_id].task_name + ", num_run: " + str(workflow.template.works[work_id].num_run))


# 283511, 283517
# reqs = get_requests(request_id=599, with_detail=True, with_metadata=True)
# reqs = get_requests(request_id=283511, with_request=True, with_detail=False, with_metadata=True)
# reqs = get_requests(request_id=298163, with_request=True, with_detail=False, with_metadata=True)
# reqs = get_requests(request_id=298557, with_request=True, with_detail=False, with_metadata=True)
# reqs = get_requests(request_id=299111, with_request=True, with_detail=False, with_metadata=True)
# reqs = get_requests(request_id=299235, with_request=True, with_detail=False, with_metadata=True)
# reqs = get_requests(request_id=965, with_request=True, with_detail=False, with_metadata=True)
# reqs = get_requests(request_id=350695, with_request=True, with_detail=False, with_metadata=True)

# reqs = get_requests(request_id=370028, with_request=True, with_detail=False, with_metadata=True)
# reqs = get_requests(request_id=370400, with_request=True, with_detail=False, with_metadata=True)
# reqs = get_requests(request_id=371204, with_request=True, with_detail=False, with_metadata=True)
# reqs = get_requests(request_id=372678, with_request=True, with_detail=False, with_metadata=True)
# reqs = get_requests(request_id=373602, with_request=True, with_detail=False, with_metadata=True)
# reqs = get_requests(request_id=376086, with_request=True, with_detail=False, with_metadata=True)
# reqs = get_requests(request_id=380474, with_request=True, with_detail=False, with_metadata=True)
# reqs = get_requests(request_id=381520, with_request=True, with_detail=False, with_metadata=True)
# reqs = get_requests(request_id=28182323, with_request=True, with_detail=False, with_metadata=True)
# reqs = get_requests(request_id=385554, with_request=True, with_detail=False, with_metadata=True)
reqs = get_requests(request_id=545851, with_request=True, with_detail=False, with_metadata=True)
for req in reqs:
    # print(req['request_id'])
    # print(req)
    # print(rets)
    print(json_dumps(req, sort_keys=True, indent=4))
    # show_works(req)
    pass
    if 'build_workflow' in req['request_metadata']:
        workflow = req['request_metadata']['build_workflow']
        # workflow.get_new_works()
        print(workflow.runs.keys())
        # print(workflow.runs["1"])
        print(json_dumps(workflow.runs["1"], sort_keys=True, indent=4))
    elif 'workflow' in req['request_metadata']:
        workflow = req['request_metadata']['workflow']
        # workflow.get_new_works()
        print(workflow.runs.keys())
        # print(workflow.runs["1"])
        # print(json_dumps(workflow.runs["1"], sort_keys=True, indent=4))

    # print(workflow.runs["1"].works.keys())
    # print(workflow.runs["1"].has_loop_condition())
    # print(workflow.runs["1"].works["7aa1ec08"])
    # print(json_dumps(workflow.runs["1"].works["048a1811"], indent=4))
    # print(workflow.runs["1"].works["7aa1ec08"].runs.keys())
    # print(workflow.runs["1"].works["7aa1ec08"].runs["1"].has_loop_condition())
    # print(workflow.runs["1"].works["7aa1ec08"].runs["1"].works.keys())

    # print(json_dumps(workflow.runs["1"].works["7aa1ec08"].runs["1"], indent=4))
    if hasattr(workflow, 'get_relation_map'):
        # print(json_dumps(workflow.get_relation_map(), sort_keys=True, indent=4))
        pass

    print("workflow")
    print_workflow(workflow)
    new_works = workflow.get_new_works()
    print('new_works:' + str(new_works))
    all_works = workflow.get_all_works()
    print('all_works:' + str(all_works))
    for work in all_works:
        print("work %s signature: %s" % (work.get_work_id(), work.signature))

    # print("workflow template")
    print_workflow_template(workflow)

    # workflow.sync_works()

    print("workflow template")
    print(json_dumps(workflow.template, sort_keys=True, indent=4))
