
import os
import sys
import datetime

os.environ['PANDA_URL'] = 'http://ai-idds-01.cern.ch:25080/server/panda'
os.environ['PANDA_URL_SSL'] = 'https://ai-idds-01.cern.ch:25443/server/panda'

from pandatools import Client  # noqa E402


"""
jobids = [1276493]
jobs_list = Client.getJobStatus(jobids, verbose=0)[1]
for job_info in jobs_list:
    # if job_info.Files and len(job_info.Files) > 0:
    print(job_info)
    print(job_info.Files)
    print(job_info.Files[0])
    for f in job_info.Files:
        # print(dir(f))
        print(f._attributes)
        print(f.values())
        print(f.type)
sys.exit(0)

jediTaskID = 998
ret = Client.getPandaIDsWithTaskID(jediTaskID, verbose=False)
# print(ret)
jobids = ret[1]
# print(jobids)

ret = Client.getJobStatus(ids=jobids, verbose=False)
print(ret)

ret = Client.getFullJobStatus(ids=jobids, verbose=False)
# print(ret)

ret = Client.getJediTaskDetails({'jediTaskID': jediTaskID}, True, True, verbose=False)
print(ret)
"""

# task_ids = [1565, 1566, 1567, 1568, 1570, 1572, 1575, 1576, 1579, 1580, 1581, 1582, 1584, 1585, 1586, 1587, 1588, 1589, 1590, 1591, 1592, 1593, 1597, 1598, 1599, 1601, 1602, 1603, 1604, 1607, 1608, 1609, 1610, 1611, 1612, 1613, 1617]
# task_ids = [i for i in range(1091, 1104)] + [i for i in range(1274, 1392)]
# task_ids = [812]
# task_ids += [i for i in range(815, 822)]
# task_ids = [827, 830, 913, 914, 916, 917, 1030, 1031, 1033, 1034, 1036, 1048, 1090, 1392]
# task_ids += [i for i in range(833, 839)]
# task_ids += [i for i in range(1048, 1078)]
# task_ids = [i for i in range(1855, 1856)]
task_ids = [i for i in range(2000, 2014)]
for task_id in task_ids:
    Client.killTask(task_id)

"""
jobids = []
Client.getJobStatus(ids=jobids, verbose=False)

Client.getJediTaskDetails(taskDict,fullFlag,withTaskInfo,verbose=False)

Client.getFullJobStatus(ids, verbose=False)
"""

# getJobIDsJediTasksInTimeRange(timeRange, dn=None, minTaskID=None, verbose=False, task_type='user')
# /DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=atlpilo1/CN=614260/CN=Robot: ATLAS Pilot1

start_time = datetime.datetime.utcnow() - datetime.timedelta(hours=5)
start_time = start_time.strftime('%Y-%m-%d %H:%M:%S')
ret = Client.getJobIDsJediTasksInTimeRange(start_time, verbose=False)
print(ret)

# ret = Client.getJobIDsJediTasksInTimeRange(start_time, task_type='test', verbose=False)
# print(ret)
# ret = Client.getJobIDsJediTasksInTimeRange(start_time, dn='/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=atlpilo1/CN=614260/CN=Robot: ATLAS Pilot1', task_type='test', verbose=False)
# print(ret)
# ret = Client.getJobIDsJediTasksInTimeRange(start_time, dn='atlpilo1', task_type='test', verbose=False)
# print(ret)


taskID = 1595
newOpts = {}
# warning for PQ
# site = newOpts.get('site', None)
# excludedSite = newOpts.get('excludedSite', None)
# for JEDI
"""
status, out = Client.retryTask(taskID, verbose=True, properErrorCode=True, newParams=newOpts)
print(status)
print(out)
"""

sys.exit(0)
