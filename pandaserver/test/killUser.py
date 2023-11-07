import optparse
import sys

import pandaserver.userinterface.Client as Client

# password
from pandaserver.config import panda_config
from pandaserver.taskbuffer.OraDBProxy import DBProxy

optP = optparse.OptionParser(conflict_handler="resolve")
optP.add_option("--user", action="store", dest="user", default=None, help="prodUserName")
optP.add_option("--jobID", action="store", dest="jobID", default=None, help="jobDefinitionID")
optP.add_option(
    "--jobsetID",
    action="store",
    dest="jobsetID",
    default=None,
    help="jobsetID, or 'all' to kill all jobs",
)
optP.add_option(
    "--prodSourceLabel",
    action="store",
    dest="prodSourceLabel",
    default=None,
    help="additional prodSourceLabel",
)


options, args = optP.parse_args()

if options.user is None:
    print("--user=<prodUserName> is required")
    sys.exit(1)
if options.jobID is None and options.jobsetID is None:
    print("--jobID=<jobDefinitionID> or --jobsetID=<jobsetID or 'all'> is required")
    sys.exit(1)


proxyS = DBProxy()
proxyS.connect(panda_config.dbhost, panda_config.dbpasswd, panda_config.dbuser, panda_config.dbname)

prodUserName = sys.argv[1]

varMap = {}
varMap[":src1"] = "user"
varMap[":src2"] = "panda"
varMap[":prodUserName"] = options.user
srcSQL = "(:src1,:src2"
if options.jobID is not None:
    varMap[":jobDefinitionID"] = options.jobID
if options.jobsetID not in (None, "all"):
    varMap[":jobsetID"] = options.jobsetID
if options.prodSourceLabel is not None:
    varMap[":src3"] = options.prodSourceLabel
    srcSQL += ",:src3"
srcSQL += ")"

jobs = []
tables = [
    "ATLAS_PANDA.jobsActive4",
    "ATLAS_PANDA.jobsWaiting4",
    "ATLAS_PANDA.jobsDefined4",
]
for table in tables:
    sql = f"SELECT PandaID FROM {table} WHERE prodUserName=:prodUserName AND prodSourceLabel IN {srcSQL} "
    if options.jobID is not None:
        sql += "AND jobDefinitionID=:jobDefinitionID "
    if options.jobsetID not in (None, "all"):
        sql += "AND jobsetID=:jobsetID "
    sql += "ORDER BY PandaID "
    status, res = proxyS.querySQLS(sql, varMap)
    if res is not None:
        for (id,) in res:
            if id not in jobs:
                jobs.append(id)
if len(jobs):
    iJob = 0
    nJob = 1000
    while iJob < len(jobs):
        subJobs = jobs[iJob : iJob + nJob]
        print(f"kill {str(subJobs)} {iJob}/{len(jobs)}")
        Client.killJobs(subJobs, code=9)
        iJob += nJob
else:
    print("no job was killed")
