import sys
import uuid

from userinterface import Client

inFileList = ["file1", "file2", "file3"]

logDatasetName = f"panda.jeditest.log.{uuid.uuid4()}"

taskParamMap = {}

taskParamMap["nFilesPerJob"] = 1
taskParamMap["nFiles"] = len(inFileList)
# taskParamMap['nEventsPerInputFile']  = 10000
# taskParamMap['nEventsPerJob'] = 10000
# taskParamMap['nEvents'] = 25000
taskParamMap["noInput"] = True
taskParamMap["pfnList"] = inFileList
# taskParamMap['mergeOutput'] = True
taskParamMap["taskName"] = str(uuid.uuid4())
taskParamMap["userName"] = "someone"
taskParamMap["vo"] = "wlcg"
taskParamMap["taskPriority"] = 900
# taskParamMap['reqID'] = reqIdx
taskParamMap["architecture"] = ""
taskParamMap["transUses"] = ""
taskParamMap["transHome"] = None
taskParamMap["transPath"] = "https://atlpan.web.cern.ch/atlpan/bash-c"
taskParamMap["processingType"] = "step1"
taskParamMap["prodSourceLabel"] = "test"
taskParamMap["taskType"] = "test"
taskParamMap["workingGroup"] = "lsst"
# taskParamMap['coreCount'] = 1
# taskParamMap['walltime'] = 1
taskParamMap["cloud"] = "US"
taskParamMap["site"] = "BNL_OSG_1"

"""
taskParamMap['log'] = {'dataset': logDatasetName,
                       'type':'template',
                       'param_type':'log',
                       'token':'local',
                       'destination':'local',
                       'value':'{0}.${{SN}}.log.tgz'.format(logDatasetName)}
"""

taskParamMap["jobParameters"] = [
    {
        "type": "constant",
        "value": 'singularity exec docker://busybox echo "hello world with ${IN/L}"',
    },
]

print(Client.insertTaskParams(taskParamMap))
