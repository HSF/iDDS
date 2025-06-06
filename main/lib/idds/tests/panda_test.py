
import os   # noqa E402
import sys
import datetime

os.environ['PANDA_URL'] = 'http://pandaserver-doma.cern.ch:25080/server/panda'
# os.environ['PANDA_URL_SSL'] = 'https://pandaserver-doma.cern.ch:25443/server/panda'
os.environ['PANDA_URL_SSL'] = 'https://pandaserver-doma.cern.ch:443/server/panda'

os.environ['PANDA_BEHIND_REAL_LB'] = "1"
os.environ['PANDA_URL'] = 'http://rubin-panda-server-dev.slac.stanford.edu:80/server/panda'
os.environ['PANDA_URL_SSL'] = 'https://rubin-panda-server-dev.slac.stanford.edu:8443/server/panda'

# os.environ['PANDA_URL_SSL'] = 'https://panda-doma-k8s-panda.cern.ch/server/panda'
# os.environ['PANDA_URL'] = 'http://panda-doma-k8s-panda.cern.ch:25080/server/panda'

os.environ['PANDA_URL'] = 'https://usdf-panda-server.slac.stanford.edu:8443/server/panda'
os.environ['PANDA_URL_SSL'] = 'https://usdf-panda-server.slac.stanford.edu:8443/server/panda'

# os.environ['PANDA_URL_SSL'] = 'https://pandaserver01.sdcc.bnl.gov:25443/server/panda'
# os.environ['PANDA_URL'] = 'https://pandaserver01.sdcc.bnl.gov:25443/server/panda'

from pandaclient import Client  # noqa E402


task_ids = [160871, 160873, 160874, 160872, 160875]
task_ids = [41161806]
for task_id in task_ids:
    print("retry %s" % task_id)
    # ret = Client.retryTask(task_id, verbose=True)
    # print(ret)

# sys.exit(0)

task_ids = [i for i in range(157023, 157050)]
task_ids = []
task_ids = [161488, 154806, 154805, 153413]
task_ids = [i for i in range(191, 220)]
task_ids = [162763, 162753]
task_ids = [i for i in range(164147, 164384)]
task_ids += [162282, 162283, 162588]
task_ids = [i for i in range(163930, 164147)]
task_ids = [161142, 160648]
task_ids = [165124, 165130, 165135] + [i for i in range(165143, 165149)]
task_ids = [i for i in range(251, 282)]
task_ids = [282, 322, 323, 324, 325]
task_ids = [i for i in range(165243, 165277)]
task_ids = [165277]
task_ids = [i for i in range(5838, 5912)]
task_ids = [165290, 165295, 165299, 165728]
task_ids = []
task_ids = [i for i in range(166636, 166778)]
task_ids = [166253, 166254]
task_ids = [167759]
task_ids = [i for i in range(167781, 167785)]
task_ids = [i for i in range(166799, 167877)]
task_ids = [i for i in range(167997, 168003)]
task_ids = [688, 8686, 8695, 8696]
task_ids = [i for i in range(8958, 9634)]
task_ids = [i for i in range(8752, 8958)]
task_ids = [168645, 168638]
task_ids = [168747, 168761, 168763]
task_ids = [13413]
task_ids = [168859, 168861, 168862]
task_ids = [i for i in range(9021, 9222)]
task_ids = [i for i in range(169155, 169178)]
task_ids = [169182, 169183, 169184]

task_ids = [5975, 8442, 10741, 10742, 10744, 10745, 10746, 10747]
task_ids = [15507, 15516, 15520, 15526, 15534, 15535, 15539, 15679, 15715]
task_ids = [169181, 169198, 169199, 169201, 169206] + [i for i in range(169210, 169232)]
task_ids = [169236, 169237, 169238, 169239, 169240, 169241]
task_ids = [169272, 169273, 169312, 169313]
task_ids = [169307, 169308, 169309, 169310, 169311, 169312, 169313, 169314]
task_ids = [i for i in range(10147, 10150)]
task_ids = [30, 31, 34, 32, 33, 35]
task_ids = [169786, 169787]
task_ids = [i for i in range(10173, 10204)]
task_ids = [108, 109, 106, 107, 112]
task_ids = [i for i in range(2921, 2927)]
task_ids = [124, 68, 75, 78, 79]
task_ids = [19654]
task_ids = [16700, 16704, 17055, 17646, 17792, 18509, 19754, 21666, 21714, 21739, 16148, 16149, 16150]
task_ids = [473, 472] + [i for i in range(325, 345)]
task_ids = [476, 477, 478]
task_ids = [937, 938, 940, 941]
task_ids = [124, 619]
task_ids = [22707, 22708, 22709, 22710, 23211, 23212, 22155, 22158]
task_ids = [24483, 24484, 25895, 26126, 26450, 26451, 26452, 26454, 26994, 27025, 27029]
task_ids = [161489, 161496, 161502, 161508, 161514, 161520, 161526, 161532, 161538, 161544, 161550, 161556, 161562]
task_ids = [3174, 3198, 3209, 3230, 3252, 3266, 3284, 3292, 3300, 3312, 3350, 3379, 3387]
task_ids = [1548, 1555]
task_ids = [3906, 4266, 4267, 4357, 4358, 4414, 4416, 4417, 4418]
task_ids = [4559, 4558, 4725, 4727, 4732, 4734, 4737, 4741]
task_ids = [i for i in range(7178, 7512)]
task_ids = [i for i in range(7512, 7560)]
task_ids = [i for i in range(7979, 8140)]
task_ids = [i for i in range(8163, 8185)]
task_ids = [i for i in range(8279, 8287)]
task_ids = [39293, 39296, 39315]
for task_id in task_ids:
    print("Killing %s" % task_id)
    ret = Client.killTask(task_id, verbose=True)
    print(ret)

# sys.exit(0)

jediTaskID = 166303
ret = Client.getJediTaskDetails({'jediTaskID': jediTaskID}, True, True, verbose=False)
print(ret)

# sys.exit(0)

# jobids = [52690679]
jobids = [9]
"""
jobs_list_status = Client.getJobStatus(jobids, verbose=1)
print(jobs_list_status)
jobs_list = jobs_list_status[1]
print(jobs_list)
for job_info in jobs_list:
    if job_info is not None:
        # if job_info.Files and len(job_info.Files) > 0:
        print(job_info)
        print(job_info.attemptNr)
        print(job_info.maxAttempt)
        print(job_info.Files)
        print(job_info.Files[0])
        for f in job_info.Files:
            # print(dir(f))
            print(f._attributes)
            print(f.values())
            print(f.type)
"""

jobids = [66573292]
jobids = [67228019]
job_ids = [67228019]
ret = Client.getFullJobStatus(ids=jobids, verbose=False)
print(ret)

jobs_list = ret[1]
# print(jobs_list)
for job_info in jobs_list:
    if job_info:
        print(job_info)
        print(job_info.eventService)
        print(job_info.jobStatus)
        print(job_info.jobSubStatus)
        print(job_info.jobsetID)
        print(job_info.taskID)
        print(job_info.jediTaskID)
        print(job_info.Files)
        for job_file in job_info.Files:
            print(job_file.type)
            print(job_file.lfn)
# sys.exit(0)

jediTaskID = 166303
ret = Client.get_files_in_datasets(jediTaskID, verbose=False)
print(ret)

print("get events")
panda_ids = [{'task_id': 157016, 'panda_id': 53943290}]
panda_ids = [{'task_id': 166303, 'panda_id': 66573292}]
panda_ids = [{'task_id': 166643, 'panda_id': 66988434}]
panda_ids = [{'task_id': 166943, 'panda_id': 67228019}]
panda_ids = [{"task_id": 167852, "panda_id": 67486349}]
panda_ids = [{"task_id": 167852, "panda_id": 67486348}]
ret = Client.get_events_status(panda_ids, verbose=True)
print(ret)

panda_ids = [{'task_id': 166943, 'panda_id': 67228018}]
ret = Client.get_events_status(panda_ids, verbose=True)
print(ret)

newOpts = {}
taskIDs = [5050]
taskIDs = [5007, 5008, 5009, 5011]
for taskID in taskIDs:
    status, out = Client.retryTask(taskID, verbose=True, properErrorCode=True, newParams=newOpts)
    print(status)
    print(out)

sys.exit(0)

"""
jediTaskID = 10517    # 10607
jediTaskID = 146329
ret = Client.getJediTaskDetails({'jediTaskID': jediTaskID}, True, True, verbose=False)
print(ret)
# ret = Client.getTaskStatus(jediTaskID, verbose=False)
# print(ret)

# ret = Client.getTaskStatus(jediTaskID, verbose=False)
# print(ret)

task_info = ret[1]
jobids = task_info['PandaID']
ret = Client.getJobStatus(ids=jobids, verbose=False)
print(ret)

if ret[0] == 0:
    jobs = ret[1]
    left_jobids = []
    ret_jobs = []
    print(len(jobs))
    for jobid, jobinfo in zip(jobids, jobs):
        if jobinfo is None:
            left_jobids.append(jobid)
        else:
            ret_jobs.append(jobinfo)
    if left_jobids:
        print(len(left_jobids))
        ret = Client.getFullJobStatus(ids=left_jobids, verbose=False)
        print(ret)
        print(len(ret[1]))
    ret_jobs = ret_jobs + ret[1]
    print(len(ret_jobs))
"""

# sys.exit(0)


jediTaskID = 152096
# jediTaskID = 154357
print(jediTaskID)

ret = Client.getTaskStatus(jediTaskID)
print(ret)

ret = Client.getPandaIDsWithTaskID(jediTaskID, verbose=False)
print(ret)
jobids = ret[1]
print(jobids)

ret = Client.getJobStatus(ids=jobids, verbose=False)
print(ret)

ret = Client.getFullJobStatus(ids=jobids, verbose=False)
print(ret)

ret = Client.getJediTaskDetails({'jediTaskID': jediTaskID}, True, True, verbose=False)
print(ret)

# sys.exit(0)

task_ids = []
# task_ids = [1565, 1566, 1567, 1568, 1570, 1572, 1575, 1576, 1579, 1580, 1581, 1582, 1584, 1585, 1586, 1587, 1588, 1589, 1590, 1591, 1592, 1593, 1597, 1598, 1599, 1601, 1602, 1603, 1604, 1607, 1608, 1609, 1610, 1611, 1612, 1613, 1617]
# task_ids = [i for i in range(1091, 1104)] + [i for i in range(1274, 1392)]
# task_ids = [812]
# task_ids += [i for i in range(815, 822)]
# task_ids = [827, 830, 913, 914, 916, 917, 1030, 1031, 1033, 1034, 1036, 1048, 1090, 1392]
# task_ids += [i for i in range(833, 839)]
# task_ids += [i for i in range(1048, 1078)]
# task_ids = [i for i in range(1855, 1856)]
# task_ids = [i for i in range(1840, 1850)] + [i for i in range(1990, 2000)]
# task_ids = [2549, 2560]
# task_ids = [i for i in range(3692, 3723)]
# task_ids = [3834, 3835, 3836]
# task_ids = [i for i in range(141294, 142200)] + [i for i in range(141003, 141077)] + [i for i in range(141145, 141255)]
# task_ids = [140954, 140955, 142228]
# task_ids = [i for i in range(142507, 142651)]
# task_ids = [i for i in range(140349, 140954)] + [142268, 142651]
# task_ids = [1851] + [i for i in range(4336, 4374)] + [i for i in range(133965, 136025)]
# task_ids = [832, 2347, 3045, 66860, 67036] + [i for i in range(121273, 140349)]
# task_ids = [i for i in range(144088, 144111)] + [144891, 144892]
# task_ids = [i for i in range(150050, 150065)]
# task_ids = [150607, 150619, 150649, 150637, 150110, 150111]
# task_ids = [150864, 150897, 150910]
# task_ids = [151114, 151115]
# task_ids = [i for i in range(151444, 151453)]
task_ids = [i for i in range(45, 53)]
# task_ids = []
task_ids = [i for i in range(156974, 156981)]
task_ids = [i for i in range(157023, 157050)]
for task_id in task_ids:
    print("Killing %s" % task_id)
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
# ret = Client.getJobIDsJediTasksInTimeRange(start_time, verbose=False)
# print(ret)

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
taskIDs = [5050]
for taskID in taskIDs:
    status, out = Client.retryTask(taskID, verbose=True, properErrorCode=True, newParams=newOpts)
    print(status)
    print(out)

sys.exit(0)
