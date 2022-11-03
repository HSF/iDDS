import sys            # noqa F401
import datetime       # noqa F401

from idds.common.utils import json_dumps, setup_logging                 # noqa F401
from idds.common.constants import ContentStatus, ContentType, ContentRelationType, ContentLocking          # noqa F401
from idds.core.requests import get_requests              # noqa F401
from idds.core.messages import retrieve_messages         # noqa F401
from idds.core.transforms import get_transforms, get_transform, update_transform          # noqa F401
from idds.core.workprogress import get_workprogresses    # noqa F401
from idds.core.processings import get_processings        # noqa F401
from idds.core import transforms as core_transforms      # noqa F401
from idds.orm.contents import get_input_contents         # noqa F401
from idds.core.transforms import release_inputs_by_collection, release_inputs_by_collection_old     # noqa F401
from idds.workflowv2.workflow import Workflow            # noqa F401
from idds.workflowv2.work import Work                    # noqa F401


setup_logging(__name__)

tfs = get_transforms()
# tfs = get_transforms(transform_id=350723)
for tf in tfs:
    print(tf['transform_id'])
    work = tf['transform_metadata']['work']
    name = work.get_work_name()
    update_transform(transform_id=tf['transform_id'], parameters={'name': name})
    # break
