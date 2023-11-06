import optparse
import time

import pandaserver.userinterface.Client as Client

# password
from pandaserver.config import panda_config
from pandaserver.taskbuffer.OraDBProxy import DBProxy

aSrvID = None

optP = optparse.OptionParser(conflict_handler="resolve")
optP.add_option(
    "-9",
    action="store_const",
    const=True,
    dest="forceKill",
    default=False,
    help="kill jobs before next heartbeat is coming",
)
options, args = optP.parse_args()

useMailAsIDV = False
if options.killOwnProdJobs:
    useMailAsIDV = True

proxyS = DBProxy()
proxyS.connect(panda_config.dbhost, panda_config.dbpasswd, panda_config.dbuser, panda_config.dbname)

jobs = []

varMap = {}
varMap[":prodSourceLabel"] = "managed"
varMap[":taskID"] = args[0]
varMap[":pandaIDl"] = args[1]
varMap[":pandaIDu"] = args[2]
sql = "SELECT PandaID FROM %s WHERE prodSourceLabel=:prodSourceLabel AND taskID=:taskID AND PandaID BETWEEN :pandaIDl AND :pandaIDu ORDER BY PandaID"
for table in [
    "ATLAS_PANDA.jobsActive4",
    "ATLAS_PANDA.jobsWaiting4",
    "ATLAS_PANDA.jobsDefined4",
]:
    status, res = proxyS.querySQLS(sql % table, varMap)
    if res is not None:
        for (id,) in res:
            if id not in jobs:
                jobs.append(id)

print(f"The number of jobs to be killed : {len(jobs)}")
if len(jobs):
    nJob = 100
    iJob = 0
    while iJob < len(jobs):
        print(f"kill {str(jobs[iJob:iJob + nJob])}")
        if options.forceKill:
            Client.killJobs(jobs[iJob : iJob + nJob], 9, useMailAsID=useMailAsIDV)
        else:
            Client.killJobs(jobs[iJob : iJob + nJob], useMailAsID=useMailAsIDV)
        iJob += nJob
        time.sleep(1)
