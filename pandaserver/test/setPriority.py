import optparse
import sys

# password
from pandaserver.config import panda_config
from pandaserver.taskbuffer.OraDBProxy import DBProxy

usage = """%prog <taskID> <priority>

  Set a priority to jobs in a task"""

optP = optparse.OptionParser(usage=usage, conflict_handler="resolve")
options, args = optP.parse_args()


proxyS = DBProxy()
proxyS.connect(panda_config.dbhost, panda_config.dbpasswd, panda_config.dbuser, panda_config.dbname)

varMap = {}
varMap[":prodSourceLabel"] = "managed"
varMap[":taskID"] = sys.argv[1]
varMap[":prio"] = sys.argv[2]
sql = "UPDATE %s SET currentPriority=:prio WHERE prodSourceLabel=:prodSourceLabel AND taskID=:taskID"
for table in [
    "ATLAS_PANDA.jobsActive4",
    "ATLAS_PANDA.jobsWaiting4",
    "ATLAS_PANDA.jobsDefined4",
]:
    status, res = proxyS.querySQLS(sql % table, varMap)
