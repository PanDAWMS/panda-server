import optparse
import time

import pandaserver.userinterface.Client as Client

# password
from pandaserver.config import panda_config
from pandaserver.taskbuffer.OraDBProxy import DBProxy

aSrvID = None

usageStr = """%prog [options] <priority>

Description: kill jobs with low priorities below a given value"""
optP = optparse.OptionParser(conflict_handler="resolve", usage=usageStr)
optP.add_option(
    "-9",
    action="store_const",
    const=True,
    dest="forceKill",
    default=False,
    help="kill jobs before next heartbeat is coming",
)
optP.add_option(
    "--running",
    action="store_const",
    const=True,
    dest="killRunning",
    default=False,
    help="kill running jobs to free up CPU slots. jobs will be killed regardless of job status if omitted",
)
optP.add_option("--site", action="store", dest="site", default=None, help="computingSite")
optP.add_option("--cloud", action="store", dest="cloud", default=None, help="cloud")
optP.add_option(
    "--maxJobs",
    action="store",
    dest="maxJobs",
    default=None,
    help="max number of jobs to be killed",
)
options, args = optP.parse_args()

if options.cloud is None and options.site is None:
    optP.error("--site=<computingSite> and/or --cloud=<cloud> is required")

proxyS = DBProxy()
proxyS.connect(panda_config.dbhost, panda_config.dbpasswd, panda_config.dbuser, panda_config.dbname)

jobsMap = {}

if len(args) == 0:
    optP.error("priority is required")

varMap = {}
varMap[":prodSourceLabel"] = "managed"
varMap[":currentPriority"] = args[0]
sql = "SELECT PandaID,currentPriority FROM %s WHERE prodSourceLabel=:prodSourceLabel AND currentPriority<:currentPriority "
if options.killRunning:
    sql += "AND jobStatus=:jobStatus "
    varMap[":jobStatus"] = "running"
if options.cloud is not None:
    sql += "AND cloud=:cloud "
    varMap[":cloud"] = options.cloud
if options.site is not None:
    sql += "AND computingSite=:site "
    varMap[":site"] = options.site
for table in [
    "ATLAS_PANDA.jobsActive4",
    "ATLAS_PANDA.jobsWaiting4",
    "ATLAS_PANDA.jobsDefined4",
]:
    status, res = proxyS.querySQLS(sql % table, varMap)
    if res is not None:
        for id, prio in res:
            if prio not in jobsMap:
                jobsMap[prio] = []
            if id not in jobsMap[prio]:
                jobsMap[prio].append(id)

# order by PandaID and currentPriority
jobs = []
prioList = sorted(jobsMap)
for prio in prioList:
    # reverse order by PandaID to kill newer jobs
    ids = sorted(jobsMap[prio])
    ids.reverse()
    jobs += ids

if options.maxJobs is not None:
    jobs = jobs[: int(options.maxJobs)]

print(f"The number of jobs with priorities below {args[0]} : {len(jobs)}")
if len(jobs):
    nJob = 100
    iJob = 0
    while iJob < len(jobs):
        print(f"kill {str(jobs[iJob:iJob + nJob])}")
        if options.forceKill:
            Client.killJobs(jobs[iJob : iJob + nJob], 9)
        else:
            Client.killJobs(jobs[iJob : iJob + nJob])
        iJob += nJob
        time.sleep(1)
