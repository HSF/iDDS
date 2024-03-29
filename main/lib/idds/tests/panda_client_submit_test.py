import uuid
from pandaclient import Client

inFileList = ['file1']

logDatasetName = 'panda.jeditest.log.{0}'.format(uuid.uuid4())

taskParamMap = {}

taskParamMap['nFilesPerJob'] = 1
taskParamMap['nFiles'] = len(inFileList)
# taskParamMap['nEventsPerInputFile']  = 10000
# taskParamMap['nEventsPerJob'] = 10000
# taskParamMap['nEvents'] = 25000
taskParamMap['noInput'] = True
taskParamMap['pfnList'] = inFileList
# taskParamMap['mergeOutput'] = True
taskParamMap['taskName'] = str(uuid.uuid4())
taskParamMap['userName'] = 'someone'
taskParamMap['vo'] = 'wlcg'
taskParamMap['taskPriority'] = 900
# taskParamMap['reqID'] = reqIdx
taskParamMap['architecture'] = ''
taskParamMap['transUses'] = ''
taskParamMap['transHome'] = None
# taskParamMap['transPath'] = 'https://atlpan.web.cern.ch/atlpan/bash-c'
taskParamMap['transPath'] = 'https://atlpan.web.cern.ch/atlpan/bash-c-enc'
taskParamMap['encJobParams'] = True
taskParamMap['processingType'] = 'step1'
taskParamMap['prodSourceLabel'] = 'test'
taskParamMap['taskType'] = 'test'
taskParamMap['workingGroup'] = 'lsst'
taskParamMap['coreCount'] = 1
# taskParamMap['walltime'] = 1
# taskParamMap['cloud'] = 'LSST'
# taskParamMap['site'] = 'DOMA_LSST_GOOGLE_TEST'
# taskParamMap['site'] = 'DOMA_LSST_SLAC_TEST'
taskParamMap['cloud'] = 'US'
taskParamMap['site'] = 'SLAC_TEST'

taskParamMap['ramCount'] = 1000
# taskParamMap['ramUnit'] = 'MB'
taskParamMap['ramUnit'] = 'MBPerCoreFixed'

taskParamMap['retryRamOffset'] = 2000
taskParamMap['retryRamStep'] = 0.3

"""
taskParamMap['log'] = {'dataset': logDatasetName,
                       'type': 'template',
                       'param_type': 'log',
                       'token': 'local',
                       'destination': 'local',
                       'value': '{0}.${{SN}}.log.tgz'.format(logDatasetName)}
"""

taskParamMap['jobParameters'] = [
    {'type': 'constant',
     'value': 'echo aaa; ls; echo',
     },
    {'type': 'constant',
     'value': '"\'${IN/L}\'"',    # noqa E123
     },
    ]                             # noqa E123

print(Client.insertTaskParams(taskParamMap))
