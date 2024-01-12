import argparse
import datetime
import time

import pandaserver.userinterface.Client as Client

# password
from pandaserver.config import panda_config
from pandaserver.taskbuffer.OraDBProxy import DBProxy

proxyS = DBProxy()
proxyS.connect(panda_config.dbhost, panda_config.dbpasswd, panda_config.dbuser, panda_config.dbname)


optP = argparse.ArgumentParser(conflict_handler="resolve", description="Reassign jobs in a task")
optP.add_argument("taskid", action="store", metavar="TASKID", help="taskID of the task")
optP.add_argument(
    "-m",
    dest="limit",
    type=int,
    action="store",
    default=60,
    metavar="MIMUTES",
    help="time limit in minute",
)
optP.add_argument(
    "-9",
    action="store_const",
    const=True,
    dest="forceKill",
    default=False,
    help="kill jobs before next heartbeat is coming",
)
optP.add_argument(
    "--keepUnmerged",
    action="store_const",
    const=True,
    dest="keepUnmerged",
    default=False,
    help="generate new jobs after kiliing old jobs, to keep unmerged events",
)
options = optP.parse_args()

taskid = options.taskid

print("")
print(f"trying to reassign jobs with modificationTime < CURRENT-{options.limit}min. Change the limit using -m if necessary")

codeV = 51
if options.forceKill:
    codeV = 9

jobs = []
jediJobs = []

timeLimit = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(minutes=options.limit)
varMap = {}
varMap[":modificationTime"] = timeLimit
varMap[":taskID"] = taskid
sql = "SELECT PandaID,lockedby FROM ATLAS_PANDA.jobsDefined4 WHERE taskID=:taskID AND modificationTime<:modificationTime "
status, res = proxyS.querySQLS(sql, varMap)
if res is not None:
    for id, lockedby in res:
        if lockedby == "jedi":
            jediJobs.append(id)
        else:
            jobs.append(id)


varMap = {}
varMap[":js1"] = "activated"
varMap[":js2"] = "starting"
varMap[":modificationTime"] = timeLimit
varMap[":taskID"] = taskid
sql = "SELECT PandaID,lockedby FROM ATLAS_PANDA.jobsActive4 WHERE jobStatus IN (:js1,:js2) AND taskID=:taskID AND modificationTime<:modificationTime "
status, res = proxyS.querySQLS(sql, varMap)
if res is not None:
    for id, lockedby in res:
        if lockedby == "jedi":
            jediJobs.append(id)
        else:
            jobs.append(id)

varMap = {}
varMap[":jobStatus"] = "waiting"
varMap[":modificationTime"] = timeLimit
varMap[":taskID"] = taskid
sql = "SELECT PandaID,lockedby FROM ATLAS_PANDA.jobsWaiting4 WHERE jobStatus=:jobStatus AND taskID=:taskID AND modificationTime<:modificationTime "
status, res = proxyS.querySQLS(sql, varMap)
if res is not None:
    for id, lockedby in res:
        if lockedby == "jedi":
            jediJobs.append(id)
        else:
            jobs.append(id)

# reassign
jobs.sort()
if len(jobs):
    nJob = 100
    iJob = 0
    while iJob < len(jobs):
        print(f"reassign  {str(jobs[iJob:iJob + nJob])}")
        Client.reassignJobs(jobs[iJob : iJob + nJob])
        iJob += nJob
        time.sleep(10)

if len(jediJobs) != 0:
    nJob = 100
    iJob = 0
    while iJob < len(jediJobs):
        print(f"kill JEDI jobs {str(jediJobs[iJob:iJob + nJob])}")
        Client.killJobs(jediJobs[iJob : iJob + nJob], codeV, keepUnmerged=options.keepUnmerged)
        iJob += nJob

print(f"\nreassigned {len(jobs + jediJobs)} jobs")
