from idds.common.constants import ThrottlerStatus
from idds.core import throttlers as core_throttlers

throttler = {'site': 'Default',
             'status': ThrottlerStatus.Active,
             'num_requests': 15,
             'num_transforms': 90,
             'new_contents': 200000,
             'queue_contents': 50000}
core_throttlers.add_throttler(**throttler)
