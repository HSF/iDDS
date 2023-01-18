
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


setup_logging(__name__)

output_total = 0
output_processed = 0
reqs = get_requests(with_transform=True)
for req in reqs:
    if "HSC" in req['name'] or "hsc" in req['name']:
        print("name: %s, output_total: %s, output_processed: %s" % (req['name'], req['output_total_files'], req['output_processed_files']))
        if req['output_total_files'] and req['output_processed_files']:
            output_total += req['output_total_files']
            output_processed += req['output_processed_files']

print("Total: %s, processed: %s" % (output_total, output_processed))
