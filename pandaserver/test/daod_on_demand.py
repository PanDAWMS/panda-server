import re
import sys
import json
import uuid
import math

from pandaserver.taskbuffer.Initializer import initializer
from pandajedi.jedicore import JediTaskBuffer

jediTaskID = sys.argv[1]

initializer.init()
taskBuffer = JediTaskBuffer.JediTaskBuffer(None)
proxy = taskBuffer.proxyPool.getProxy()

s, o = proxy.getClobObj(
    "select JEDI_TASK_PARAMETERS from atlas_deft.t_task where taskid=:task_id",
    {":task_id": jediTaskID},
)

taskParamStr = o[0][0]

t = json.loads(taskParamStr)

s, o = proxy.querySQLS(
    "select cpuTime,cpuTimeUnit,ioIntensity,ioIntensityUnit,ramCount,ramUnit,outDiskCount,outDiskUnit,workDiskCount,workDiskUnit "
    "FROM atlas_panda.jedi_tasks where jeditaskid=:task_id",
    {":task_id": jediTaskID},
)
(
    cpuTime,
    cpuTimeUnit,
    ioIntensity,
    ioIntensityUnit,
    ramCount,
    ramUnit,
    outDiskCount,
    outDiskUnit,
    workDiskCount,
    workDiskUnit,
) = o[0]

tname = "{}.{}".format(str(uuid.uuid4()), jediTaskID)

t.update(
    {
        "taskName": tname,
        "userName": "pandasrv1",
        "prodSourceLabel": "test",
        "taskPriority": 100000,
        "skipScout": True,
        "cpuTime": cpuTime,
        "cpuTimeUnit": cpuTimeUnit,
        "ioIntensity": ioIntensity,
        "ioIntensityUnit": ioIntensityUnit,
        "ramCount": ramCount,
        "ramUnit": ramUnit,
        "outDiskCount": outDiskCount,
        "outDiskUnit": outDiskUnit,
        "workDiskCount": workDiskCount,
        "workDiskUnit": workDiskUnit,
        "t1Weight": -1,
    }
)

i = t["log"]
i["dataset"] = "panda.{}.log".format(tname)
i["value"] = re.sub(r".{}.".format(jediTaskID), r".{}.".format(tname), i["value"])

if "toStaging" in t:
    del t["toStaging"]

if "inputPreStaging" in t:
    del t["inputPreStaging"]

if "nGBPerJob" in t:
    t["nGBPerJob"] = math.ceil(t["nGBPerJob"] / 2)

newJ = []
for i in t["jobParameters"]:
    if "dataset" in i:
        if i["param_type"] == "output":
            i["dataset"] = "panda.{}.{}".format(tname, i["dataset"].split(".")[4])
            i["value"] = re.sub(r".{}.".format(jediTaskID), r".{}.".format(tname), i["value"])
    elif i["type"] == "template":
        i["value"] = re.sub(r" {}.".format(jediTaskID), r" {}.".format(tname), i["value"])

k = list(t.keys())
k.sort()

for kk in k:
    print(kk, t[kk])

jonStr = json.dumps(t)

print(proxy.insertTaskParams_JEDI(t["vo"], t["prodSourceLabel"], t["userName"], t["taskName"], jonStr))
