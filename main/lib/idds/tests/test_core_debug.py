import sys    # noqa F401

from idds.common.constants import ContentStatus
from idds.common.utils import json_loads
from idds.orm.contents import update_contents_ext, update_contents    # noqa F401


params = [{'content_id': 188308635, 'status': ContentStatus.Available},
          {'content_id': 188308636, 'status': ContentStatus.Available},
          {'content_id': 188308637, 'status': ContentStatus.Available},
          {'content_id': 188308638, 'status': ContentStatus.Available},
          {'content_id': 188308639, 'status': ContentStatus.Available}]
params = [{'content_id': 188741016, 'status': ContentStatus.Available},
          {'content_id': 188741017, 'status': ContentStatus.Available},
          {'content_id': 188741018, 'status': ContentStatus.Available},
          {'content_id': 188741019, 'status': ContentStatus.Available},
          {'content_id': 188741020, 'status': ContentStatus.Available},
          {'content_id': 188741021, 'status': ContentStatus.Available},
          {'content_id': 188741022, 'status': ContentStatus.Available},
          {'content_id': 188741023, 'status': ContentStatus.Available},
          {'content_id': 188741024, 'status': ContentStatus.Available},
          {'content_id': 188741025, 'status': ContentStatus.Available},
          {'content_id': 188741026, 'status': ContentStatus.Available},
          {'content_id': 188741027, 'status': ContentStatus.Available}]
# update_contents(params)

# sys.exit(0)

file = '/afs/cern.ch/user/w/wguan/workdisk/iDDS/test/test.log'
f = open(file, "r")
c = f.read()
j = json_loads(c)
contents_ext = j['update_contents_ext']

new_list = []

for col in contents_ext:
    new_l = {}
    for k in col:
        if col[k] is not None:
            new_l[k] = col[k]
        # new_l['job_status'] = 10
    new_list.append(new_l)
    # break

print("new_list")
# print(new_list)
print("new_list len: %s" % len(new_list))
update_contents_ext(new_list)
# update_contents_ext(contents_ext)
