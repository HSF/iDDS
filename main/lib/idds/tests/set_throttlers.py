from idds.common.constants import ThrottlerStatus
from idds.core import throttlers as core_throttlers

throttler = {'site': 'Default',
             'status': ThrottlerStatus.Active,
             'new_contents': 100000,
             'queue_contents': 50000}
core_throttlers.add_throttler(**throttler)
