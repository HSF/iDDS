
from idds.common.utils import json_dumps                 # noqa F401
from idds.common.constants import ContentStatus, ContentRelationType          # noqa F401
from idds.core.requests import get_requests              # noqa F401
from idds.core.messages import retrieve_messages         # noqa F401
from idds.core.transforms import get_transforms, release_inputs          # noqa F401
from idds.core.workprogress import get_workprogresses    # noqa F401
from idds.core.processings import get_processings        # noqa F401
from idds.core import transforms as core_transforms      # noqa F401
from idds.core.catalog import get_contents, update_contents  # noqa F401
from idds.orm.contents import get_input_contents             # noqa F401


request_ids = [368, 369, 370, 371, 372, 373, 374, 375, 376]
request_ids = [2895]
for request_id in request_ids:
    contents = get_contents(request_id=request_id, status=ContentStatus.Available)
    ret_contents = {}
    for content in contents:
        if content['content_relation_type'] == ContentRelationType.Output:   # InputDependency
            if content['coll_id'] not in ret_contents:
                ret_contents[content['coll_id']] = []
            ret_contents[content['coll_id']].append(content)

    for ret_content in ret_contents:
        print("coll_id: %s, num_contents: %s" % (ret_content, len(ret_contents[ret_content])))
        # break

    updated_contents = core_transforms.release_inputs_by_collection(ret_contents)
    for update_content in updated_contents:
        print(update_content)
        # break

    update_contents(updated_contents)
