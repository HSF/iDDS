import time                       # noqa F401
from datetime import datetime     # noqa F401

from idds.common.utils import json_dumps, setup_logging                 # noqa F401
from idds.common.constants import ContentStatus, ContentType, ContentRelationType, ContentLocking          # noqa F401
from idds.core.requests import get_requests              # noqa F401
from idds.core.messages import retrieve_messages         # noqa F401
from idds.core.transforms import get_transforms, get_transform          # noqa F401
from idds.core.workprogress import get_workprogresses    # noqa F401
from idds.core.processings import get_processings        # noqa F401
from idds.core import transforms as core_transforms      # noqa F401
from idds.core.transforms import release_inputs_by_collection, release_inputs_by_collection_old     # noqa F401
from idds.workflowv2.workflow import Workflow            # noqa F401
from idds.workflowv2.work import Work                    # noqa F401

from idds.orm import contents as orm_contents       # noqa F401
from idds.core import catalog as core_catalog       # noqa F401


setup_logging(__name__)

request_id = 3347
transform_id = 26788   # 3028, 3029

# ret = core_catalog.update_contents_to_others_by_dep_id(request_id=request_id, transform_id=transform_id)
# print(ret)

# ret = core_catalog.update_contents_from_others_by_dep_id(request_id=request_id, transform_id=transform_id)
# print(ret)

for req_id in [3350, 3407, 3414, 3420]:
    start = time.time()
    ret = core_catalog.get_updated_transforms_by_content_status(request_id=req_id)
    print('request_id: %s, time: %s' % (req_id, time.time() - start))
    print(ret)
