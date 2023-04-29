
import os

os.environ['PANDA_AUTH'] = 'oidc'
os.environ['PANDA_URL_SSL'] = 'https://pandaserver-doma.cern.ch:25443/server/panda'
os.environ['PANDA_URL'] = 'http://pandaserver-doma.cern.ch:25080/server/panda'
os.environ['PANDA_AUTH_VO'] = 'Rubin'
os.environ['PANDA_CONFIG_ROOT'] = '~/.panda/'

os.environ['PANDA_URL_SSL'] = 'https://rubin-panda-server-dev.slac.stanford.edu:8443/server/panda'
os.environ['PANDA_URL'] = 'https://rubin-panda-server-dev.slac.stanford.edu:80/server/panda'


from pandaclient import panda_api             # noqa E402
import pandaclient.idds_api                   # noqa E402
import idds.common.utils as idds_utils        # noqa E402

c = panda_api.get_api()
c.hello(verbose=True)

c = pandaclient.idds_api.get_api(idds_utils.json_dumps, idds_host='https://aipanda160.cern.ch:443/idds', compress=True, manager=True)
# c = pandaclient.idds_api.get_api(idds_utils.json_dumps, idds_host='https://aipanda015.cern.ch:443/idds', compress=True, manager=True)

ret = c.abort(request_id=38)
print(ret)

"""
ret = c.resume(request_id=27)
print(ret)

ret = c.retry(request_id=27)
print(ret)

ret = c.get_status(request_id=27)
if ret[0] == 0 and ret[1][0] is True:
    print(ret[1][-1])

ret = c.get_status(request_id=27, with_detail=True)
if ret[0] == 0 and ret[1][0] is True:
    print(ret[1][-1])
"""
